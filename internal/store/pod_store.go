package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// PodRecord is the monitoring-system coordinate for one Spark pod (driver or
// executor). It outlives the pod itself: after deletion the Kubernetes API no
// longer returns the pod, but name/node and the container time window are
// still needed to look up historical CPU/memory/disk metrics.
type PodRecord struct {
	UID                 string            `json:"uid"`
	ApplicationID       string            `json:"applicationId"`
	Namespace           string            `json:"namespace"`
	Name                string            `json:"name"`
	Node                string            `json:"node"`
	Role                string            `json:"role"` // driver, executor
	ExecutorID          string            `json:"executorId,omitempty"`
	Labels              map[string]string `json:"labels,omitempty"`
	Phase               string            `json:"phase"`
	Status              string            `json:"status"` // pending, running, succeeded, failed, deleted
	CreatedAt           time.Time         `json:"createdAt"`
	ContainerStartedAt  *time.Time        `json:"containerStartedAt,omitempty"`
	ContainerFinishedAt *time.Time        `json:"containerFinishedAt,omitempty"`
	DeletionTimestamp   *time.Time        `json:"deletionTimestamp,omitempty"`
	FirstSeenAt         time.Time         `json:"firstSeenAt"`
	LastUpdatedAt       time.Time         `json:"lastUpdatedAt"`
	DeletedAt           *time.Time        `json:"deletedAt,omitempty"`
}

// PodRecordFilter narrows a pod record query. A zero Limit returns all matches.
type PodRecordFilter struct {
	ApplicationID string
	Role          string
	Node          string
	Status        string
	Start         *time.Time
	End           *time.Time
	Limit         int
}

// PodStore keeps driver/executor pod coordinates in memory and mirrors them
// to a local JSON file. When the number of records exceeds the limit, the
// oldest deleted records are evicted; live pods are never evicted.
type PodStore struct {
	mu         sync.RWMutex
	records    map[string]*PodRecord
	filePath   string
	maxRecords int
	dirty      bool
	stopCh     chan struct{}
}

// NewPodStore loads any persisted records from filePath (if set) and starts
// a background flush loop. maxRecords <= 0 uses the default.
func NewPodStore(filePath string, maxRecords int) *PodStore {
	if maxRecords <= 0 {
		maxRecords = 10000
	}
	s := &PodStore{
		records:    make(map[string]*PodRecord),
		filePath:   filePath,
		maxRecords: maxRecords,
		stopCh:     make(chan struct{}),
	}
	s.load()
	go s.persistLoop()
	return s
}

// Stop terminates the flush loop and performs a final save.
func (s *PodStore) Stop() {
	close(s.stopCh)
	s.save()
}

// Upsert creates or updates the record for a Spark pod. On deletion it marks
// the record deleted and timestamps it.
func (s *PodStore) Upsert(pod *corev1.Pod, deleted bool) {
	appID := AppIDFromPod(pod)
	role := SparkRole(pod)
	if appID == "" || role == "" {
		return
	}

	key := podKey(pod)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.records[key]
	if !exists {
		created := pod.CreationTimestamp.Time
		if created.IsZero() {
			created = now
		}
		rec = &PodRecord{
			UID:           key,
			ApplicationID: appID,
			Namespace:     pod.Namespace,
			Name:          pod.Name,
			FirstSeenAt:   now,
			CreatedAt:     created,
		}
		s.records[key] = rec
	}

	if created := pod.CreationTimestamp.Time; !created.IsZero() && created.Before(rec.CreatedAt) {
		rec.CreatedAt = created
	}
	rec.ApplicationID = appID
	rec.Namespace = pod.Namespace
	rec.Name = pod.Name
	rec.Node = pod.Spec.NodeName
	rec.Role = role
	rec.ExecutorID = pod.Labels["spark-exec-id"]
	if pod.Labels != nil {
		labels := make(map[string]string, len(pod.Labels))
		for key, value := range pod.Labels {
			labels[key] = value
		}
		rec.Labels = labels
	} else {
		rec.Labels = nil
	}
	rec.Phase = string(pod.Status.Phase)
	rec.Status = podStatus(pod, deleted)
	if started, finished := containerTimes(pod); started != nil {
		rec.ContainerStartedAt = started
		if finished != nil {
			rec.ContainerFinishedAt = finished
		}
	}
	if pod.DeletionTimestamp != nil {
		deletionTimestamp := pod.DeletionTimestamp.Time
		rec.DeletionTimestamp = &deletionTimestamp
	}
	rec.LastUpdatedAt = now
	if deleted {
		deletedAt := now
		rec.DeletedAt = &deletedAt
	}

	s.evictLocked()
	s.dirty = true
}

// podKey identifies a pod by UID, falling back to namespace/name for objects
// without a UID.
func podKey(pod *corev1.Pod) string {
	if pod.UID != "" {
		return string(pod.UID)
	}
	return pod.Namespace + "/" + pod.Name
}

// containerTimes resolves the main (non-init) container start/finish times,
// falling back to the pod start time when container status is unavailable.
func containerTimes(pod *corev1.Pod) (started, finished *time.Time) {
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Terminated != nil {
			term := cs.State.Terminated
			if !term.StartedAt.IsZero() {
				t := term.StartedAt.Time
				started = &t
			}
			if !term.FinishedAt.IsZero() {
				t := term.FinishedAt.Time
				finished = &t
			}
			return started, finished
		}
		if cs.State.Running != nil {
			t := cs.State.Running.StartedAt.Time
			started = &t
			return started, nil
		}
	}
	if pod.Status.StartTime != nil {
		t := pod.Status.StartTime.Time
		started = &t
	}
	return started, nil
}

// evictLocked drops the oldest deleted records until the size is within the
// limit. Live pods are never evicted.
func (s *PodStore) evictLocked() {
	for len(s.records) > s.maxRecords {
		var oldestKey string
		var oldestTime time.Time
		for key, rec := range s.records {
			if rec.DeletedAt == nil {
				continue
			}
			ts := *rec.DeletedAt
			if oldestKey == "" || ts.Before(oldestTime) {
				oldestKey = key
				oldestTime = ts
			}
		}
		if oldestKey == "" {
			return
		}
		delete(s.records, oldestKey)
	}
}

// GetRecords returns pod records matching filter, ordered by lastUpdatedAt
// descending and optionally truncated to filter.Limit.
func (s *PodStore) GetRecords(filter PodRecordFilter) []PodRecord {
	s.mu.RLock()
	result := make([]PodRecord, 0, len(s.records))
	for _, rec := range s.records {
		if filter.ApplicationID != "" && rec.ApplicationID != filter.ApplicationID {
			continue
		}
		if filter.Role != "" && rec.Role != filter.Role {
			continue
		}
		if filter.Node != "" && rec.Node != filter.Node {
			continue
		}
		if filter.Status != "" && rec.Status != filter.Status {
			continue
		}
		if filter.Start != nil && rec.LastUpdatedAt.Before(*filter.Start) {
			continue
		}
		if filter.End != nil && rec.LastUpdatedAt.After(*filter.End) {
			continue
		}
		result = append(result, *rec)
	}
	s.mu.RUnlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].LastUpdatedAt.After(result[j].LastUpdatedAt)
	})
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}
	return result
}

func (s *PodStore) persistLoop() {
	if s.filePath == "" {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.save()
		}
	}
}

func (s *PodStore) snapshot() []PodRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PodRecord, 0, len(s.records))
	for _, rec := range s.records {
		out = append(out, *rec)
	}
	return out
}

func (s *PodStore) load() {
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return
	}
	var records []PodRecord
	if err := json.Unmarshal(data, &records); err != nil {
		klog.Warningf("Failed to parse pod records file %s: %v", s.filePath, err)
		return
	}
	s.mu.Lock()
	for i := range records {
		rec := records[i]
		uid := rec.UID
		if uid == "" {
			uid = rec.Namespace + "/" + rec.Name
		}
		s.records[uid] = &rec
	}
	s.mu.Unlock()
	klog.Infof("Loaded %d pod records from %s", len(records), s.filePath)
}

func (s *PodStore) save() {
	if s.filePath == "" {
		return
	}

	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}
	s.dirty = false
	s.mu.Unlock()

	records := s.snapshot()

	if dir := filepath.Dir(s.filePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			klog.Warningf("Failed to create pod records dir %s: %v", dir, err)
			return
		}
	}

	data, err := json.Marshal(records)
	if err != nil {
		return
	}

	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		klog.Warningf("Failed to write pod records file: %v", err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		klog.Warningf("Failed to finalize pod records file: %v", err)
	}
}
