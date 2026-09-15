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

// ApplicationRecord is the log-analysis credential for one Spark
// application. Downstream uses applicationId to fetch the event log from the
// Spark history server / S3. The application lifecycle is driven by its
// single driver pod; executor pods only refresh identity and timestamps.
type ApplicationRecord struct {
	ApplicationID    string     `json:"applicationId"`
	SparkAppName     string     `json:"sparkAppName,omitempty"`
	SparkAppSelector string     `json:"sparkAppSelector,omitempty"`
	Namespace        string     `json:"namespace"`
	DriverPodName    string     `json:"driverPodName,omitempty"`
	Status           string     `json:"status"` // unknown, pending, running, succeeded, failed, deleted
	ChangedAt        time.Time  `json:"changedAt"`
	CreatedAt        time.Time  `json:"createdAt"`
	FirstSeenAt      time.Time  `json:"firstSeenAt"`
	LastUpdatedAt    time.Time  `json:"lastUpdatedAt"`
	FinishedAt       *time.Time `json:"finishedAt,omitempty"`
	DeletedAt        *time.Time `json:"deletedAt,omitempty"`
}

// AppStore keeps application credentials in memory and mirrors them to a
// local JSON file so history survives restarts. When the number of records
// exceeds the limit, the oldest deleted records are evicted; live
// applications are never evicted.
type AppStore struct {
	mu          sync.RWMutex
	records     map[string]*ApplicationRecord
	lastChanged time.Time
	filePath    string
	maxRecords  int
	dirty       bool
	stopCh      chan struct{}
}

// NewAppStore loads any persisted records from filePath (if set) and starts
// a background flush loop. maxRecords <= 0 uses the default.
func NewAppStore(filePath string, maxRecords int) *AppStore {
	if maxRecords <= 0 {
		maxRecords = 1000
	}
	s := &AppStore{
		records:    make(map[string]*ApplicationRecord),
		filePath:   filePath,
		maxRecords: maxRecords,
		stopCh:     make(chan struct{}),
	}
	s.load()
	go s.persistLoop()
	return s
}

// Stop terminates the flush loop and performs a final save.
func (s *AppStore) Stop() {
	close(s.stopCh)
	s.save()
}

// Upsert creates or updates the record for the application owning pod. Only
// driver pods drive status/finishedAt/deletedAt and changedAt; executor churn
// merely refreshes lastUpdatedAt and never reaches the incremental feed.
func (s *AppStore) Upsert(pod *corev1.Pod, deleted bool) {
	appID := AppIDFromPod(pod)
	if appID == "" {
		return
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.records[appID]
	lifecycleChanged := false
	if !exists {
		created := pod.CreationTimestamp.Time
		if created.IsZero() {
			created = now
		}
		rec = &ApplicationRecord{
			ApplicationID: appID,
			Namespace:     pod.Namespace,
			Status:        "unknown",
			FirstSeenAt:   now,
			CreatedAt:     created,
		}
		s.records[appID] = rec
		lifecycleChanged = true
	}

	if created := pod.CreationTimestamp.Time; !created.IsZero() && created.Before(rec.CreatedAt) {
		rec.CreatedAt = created
	}
	if rec.Namespace == "" {
		rec.Namespace = pod.Namespace
	}
	if name := pod.Labels["spark-app-name"]; name != "" {
		rec.SparkAppName = name
	}
	if selector := pod.Labels["spark-app-selector"]; selector != "" {
		rec.SparkAppSelector = selector
	}

	if isDriverPod(pod) {
		rec.DriverPodName = pod.Name
		if nextStatus := podStatus(pod, deleted); rec.Status != nextStatus {
			rec.Status = nextStatus
			lifecycleChanged = true
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			if rec.FinishedAt == nil {
				finished := now
				if _, containerFinished := containerTimes(pod); containerFinished != nil {
					finished = *containerFinished
				}
				rec.FinishedAt = &finished
				lifecycleChanged = true
			}
		}
		if deleted && rec.DeletedAt == nil {
			deletedAt := now
			rec.DeletedAt = &deletedAt
			lifecycleChanged = true
		}
	}

	if lifecycleChanged {
		// Stamp a strictly increasing change time, bumped by at least 1ms so
		// that multiple changes inside the same millisecond never collide.
		next := now
		if !s.lastChanged.IsZero() {
			if floor := s.lastChanged.Add(time.Millisecond); next.Before(floor) {
				next = floor
			}
		}
		s.lastChanged = next
		rec.ChangedAt = next
		s.dirty = true
	}

	rec.LastUpdatedAt = now
	if s.evictLocked() {
		s.dirty = true
	}
}

// evictLocked drops the oldest deleted records until the size is within the
// limit. Live applications are never evicted.
func (s *AppStore) evictLocked() bool {
	evicted := false
	for len(s.records) > s.maxRecords {
		var oldestID string
		var oldestTime time.Time
		for id, rec := range s.records {
			if rec.DeletedAt == nil {
				continue
			}
			ts := *rec.DeletedAt
			if oldestID == "" || ts.Before(oldestTime) {
				oldestID = id
				oldestTime = ts
			}
		}
		if len(oldestID) == 0 {
			return evicted
		}
		delete(s.records, oldestID)
		evicted = true
	}
	return evicted
}

// GetRecord returns one application record by id.
func (s *AppStore) GetRecord(appID string) (ApplicationRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[appID]
	if !ok {
		return ApplicationRecord{}, false
	}
	return *rec, true
}

// GetChanges returns records whose changedAt is strictly after since,
// optionally filtered by exact applicationId and a set of statuses, ordered
// by changedAt ascending. A positive limit pages the result (limit+1 probe):
// at most limit records are returned and hasMore reports whether another page
// exists. Evicted deleted records can never be returned, so a since older than
// the retained window may miss terminal states.
func (s *AppStore) GetChanges(since time.Time, limit int, appID string, statuses []string) (records []ApplicationRecord, hasMore bool) {
	statusSet := make(map[string]struct{}, len(statuses))
	for _, st := range statuses {
		if st != "" {
			statusSet[st] = struct{}{}
		}
	}
	s.mu.RLock()
	matched := make([]ApplicationRecord, 0, len(s.records))
	for _, rec := range s.records {
		if !rec.ChangedAt.After(since) {
			continue
		}
		if appID != "" && rec.ApplicationID != appID {
			continue
		}
		if len(statusSet) > 0 {
			if _, ok := statusSet[rec.Status]; !ok {
				continue
			}
		}
		matched = append(matched, *rec)
	}
	s.mu.RUnlock()

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].ChangedAt.Before(matched[j].ChangedAt)
	})

	if limit > 0 && len(matched) > limit {
		matched = matched[:limit]
		return matched, true
	}
	return matched, false
}

func (s *AppStore) persistLoop() {
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

func (s *AppStore) load() {
	if s.filePath == "" {
		return
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return
	}
	records := make([]ApplicationRecord, 0)
	if err := json.Unmarshal(data, &records); err != nil {
		// Transitional layout wrapped the array with a monotonic counter.
		var wrapped struct {
			Revision int64               `json:"revision"`
			Records  []ApplicationRecord `json:"records"`
		}
		if wrappedErr := json.Unmarshal(data, &wrapped); wrappedErr != nil {
			klog.Warningf("Failed to parse applications file %s: %v", s.filePath, err)
			return
		}
		records = wrapped.Records
	}

	s.mu.Lock()
	s.ingestLocked(records)
	s.mu.Unlock()
	klog.Infof("Loaded %d application records from %s", len(records), s.filePath)
}

// ingestLocked indexes loaded records and rebuilds the change-time clock.
// Records without a changedAt (older file layouts) are backfilled in
// lastUpdatedAt order, stepping by 1ms from the earliest timestamp.
func (s *AppStore) ingestLocked(records []ApplicationRecord) {
	var missing []*ApplicationRecord
	var latest time.Time
	for i := range records {
		rec := records[i]
		s.records[rec.ApplicationID] = &rec
		if rec.ChangedAt.After(latest) {
			latest = rec.ChangedAt
		}
		if rec.ChangedAt.IsZero() {
			missing = append(missing, s.records[rec.ApplicationID])
		}
	}
	if len(missing) == 0 {
		s.lastChanged = latest
		return
	}

	sort.Slice(missing, func(i, j int) bool {
		return missing[i].LastUpdatedAt.Before(missing[j].LastUpdatedAt)
	})
	clock := missing[0].LastUpdatedAt.Add(-time.Millisecond)
	if clock.Before(latest) {
		clock = latest
	}
	for _, rec := range missing {
		clock = clock.Add(time.Millisecond)
		rec.ChangedAt = clock
	}
	s.lastChanged = clock
}

// snapshotLocked returns the records as a bare JSON array; caller must hold mu.
func (s *AppStore) snapshotLocked() []ApplicationRecord {
	out := make([]ApplicationRecord, 0, len(s.records))
	for _, rec := range s.records {
		out = append(out, *rec)
	}
	return out
}

func (s *AppStore) save() {
	if s.filePath == "" {
		return
	}

	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}
	s.dirty = false
	records := s.snapshotLocked()
	s.mu.Unlock()

	if dir := filepath.Dir(s.filePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			klog.Warningf("Failed to create applications dir %s: %v", dir, err)
			return
		}
	}

	data, err := json.Marshal(records)
	if err != nil {
		return
	}

	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		klog.Warningf("Failed to write applications file: %v", err)
		return
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		klog.Warningf("Failed to finalize applications file: %v", err)
	}
}

// SparkRole returns the spark-role label (driver/executor) when the pod
// belongs to a Spark application with a resolvable applicationId.
func SparkRole(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	role := pod.Labels["spark-role"]
	if role != "driver" && role != "executor" {
		return ""
	}
	if AppIDFromPod(pod) == "" {
		return ""
	}
	return role
}

// isDriverPod reports whether the pod is a Spark driver.
func isDriverPod(pod *corev1.Pod) bool {
	return pod.Labels != nil && pod.Labels["spark-role"] == "driver"
}

// AppIDFromPod resolves the Spark application id, preferring the internal
// appSparkID label, then the applicationId label, and finally
// spark-app-selector for stock Spark deployments.
func AppIDFromPod(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	if appID := pod.Labels["appSparkID"]; appID != "" {
		return appID
	}
	if appID := pod.Labels["applicationId"]; appID != "" {
		return appID
	}
	return pod.Labels["spark-app-selector"]
}

// podStatus derives a human-readable status from the pod phase and deleted flag.
func podStatus(pod *corev1.Pod, deleted bool) string {
	if deleted {
		return "deleted"
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return "succeeded"
	case corev1.PodFailed:
		return "failed"
	case corev1.PodRunning:
		return "running"
	case corev1.PodPending:
		return "pending"
	default:
		return string(pod.Status.Phase)
	}
}
