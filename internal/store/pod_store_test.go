package store

import (
	"path/filepath"
	"testing"
	"time"

	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodStoreOneApplicationManyPods(t *testing.T) {
	s := NewPodStore("", 0)
	driver := testutil.WithUID(testutil.DriverPod("app-n-driver", "app-n", corev1.PodRunning, time.Now()), "uid-driver")
	exec1 := testutil.WithUID(testutil.ExecutorPod("app-n-exec-1", "app-n"), "uid-exec-1")
	exec2 := testutil.WithUID(testutil.ExecutorPod("app-n-exec-2", "app-n"), "uid-exec-2")

	s.Upsert(driver, false)
	s.Upsert(exec1, false)
	s.Upsert(exec2, false)

	all := s.GetRecords(PodRecordFilter{})
	if len(all) != 3 {
		t.Fatalf("expected 3 pod records, got %+v", all)
	}
	drivers := s.GetRecords(PodRecordFilter{Role: "driver"})
	if len(drivers) != 1 || drivers[0].Node != "node-1" || drivers[0].ExecutorID != "" {
		t.Fatalf("unexpected driver record: %+v", drivers)
	}
	execs := s.GetRecords(PodRecordFilter{ApplicationID: "app-n", Role: "executor"})
	if len(execs) != 2 {
		t.Fatalf("expected 2 executors, got %+v", execs)
	}
	for _, rec := range execs {
		if rec.ExecutorID == "" || rec.Node != "node-2" {
			t.Fatalf("executor identity fields not captured: %+v", rec)
		}
	}

	s.Upsert(testutil.WithUID(testutil.ExecutorPod("app-n-exec-1", "app-n"), "uid-exec-1"), true)
	deleted := s.GetRecords(PodRecordFilter{ApplicationID: "app-n", Status: "deleted"})
	if len(deleted) != 1 || deleted[0].Name != "app-n-exec-1" || deleted[0].DeletedAt == nil {
		t.Fatalf("expected 1 deleted executor, got %+v", deleted)
	}
}

func TestPodStoreContainerTimesAndDeletionTimestamp(t *testing.T) {
	s := NewPodStore("", 0)
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	finish := time.Now().Add(-time.Minute).Truncate(time.Second)
	pod := testutil.WithUID(testutil.DriverPod("app-t-driver", "app-t", corev1.PodSucceeded, start), "uid-t")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "spark-kubernetes-driver",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					StartedAt:  metav1.NewTime(start),
					FinishedAt: metav1.NewTime(finish),
				},
			},
		},
	}
	pod.ObjectMeta.DeletionTimestamp = &metav1.Time{Time: finish.Add(10 * time.Second)}

	s.Upsert(pod, false)
	recs := s.GetRecords(PodRecordFilter{ApplicationID: "app-t"})
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %+v", recs)
	}
	rec := recs[0]
	if rec.ContainerStartedAt == nil || !rec.ContainerStartedAt.Equal(start) {
		t.Fatalf("expected containerStartedAt %v, got %v", start, rec.ContainerStartedAt)
	}
	if rec.ContainerFinishedAt == nil || !rec.ContainerFinishedAt.Equal(finish) {
		t.Fatalf("expected containerFinishedAt %v, got %v", finish, rec.ContainerFinishedAt)
	}
	if rec.DeletionTimestamp == nil || !rec.DeletionTimestamp.Equal(finish.Add(10*time.Second)) {
		t.Fatalf("expected deletionTimestamp, got %v", rec.DeletionTimestamp)
	}
}

func TestPodStoreRunningContainerStartTime(t *testing.T) {
	s := NewPodStore("", 0)
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	pod := testutil.WithUID(testutil.DriverPod("app-r-driver", "app-r", corev1.PodRunning, time.Now()), "uid-r")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "spark-kubernetes-driver",
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(start)},
			},
		},
	}

	s.Upsert(pod, false)
	recs := s.GetRecords(PodRecordFilter{ApplicationID: "app-r"})
	if len(recs) != 1 || recs[0].ContainerStartedAt == nil || !recs[0].ContainerStartedAt.Equal(start) {
		t.Fatalf("expected running container startedAt, got %+v", recs)
	}
	if recs[0].ContainerFinishedAt != nil {
		t.Fatalf("running container must not have finishedAt, got %v", recs[0].ContainerFinishedAt)
	}
}

func TestPodStorePersistenceRoundTrip(t *testing.T) {
	file := filepath.Join(t.TempDir(), "pod-records.json")

	s1 := NewPodStore(file, 10)
	s1.Upsert(testutil.WithUID(testutil.DriverPod("app-d-driver", "app-d", corev1.PodSucceeded, time.Now()), "uid-d"), true)
	s1.Upsert(testutil.WithUID(testutil.ExecutorPod("app-d-exec-1", "app-d"), "uid-e"), true)
	s1.Stop()

	s2 := NewPodStore(file, 10)
	defer s2.Stop()
	recs := s2.GetRecords(PodRecordFilter{})
	if len(recs) != 2 {
		t.Fatalf("expected 2 loaded records, got %+v", recs)
	}
}

func TestPodStoreEvictionKeepsLive(t *testing.T) {
	s := NewPodStore("", 100)
	base := time.Now().Truncate(time.Second)

	s.Upsert(testutil.WithUID(testutil.DriverPod("old1-driver", "old1", corev1.PodSucceeded, base.Add(-3*time.Hour)), "old1"), true)
	s.Upsert(testutil.WithUID(testutil.DriverPod("old2-driver", "old2", corev1.PodSucceeded, base.Add(-2*time.Hour)), "old2"), true)
	s.Upsert(testutil.WithUID(testutil.DriverPod("live-driver", "live", corev1.PodRunning, base), "live"), false)

	s.mu.Lock()
	for uid, at := range map[string]time.Time{
		"old1": base.Add(-3 * time.Hour),
		"old2": base.Add(-2 * time.Hour),
	} {
		ts := at
		s.records[uid].DeletedAt = &ts
	}
	s.maxRecords = 2
	s.evictLocked()
	s.mu.Unlock()

	recs := s.GetRecords(PodRecordFilter{})
	if len(recs) != 2 {
		t.Fatalf("expected 2 records after eviction (limit 2), got %d", len(recs))
	}
	if _, ok := s.records["old1"]; ok {
		t.Fatal("oldest deleted record should have been evicted")
	}
	if _, ok := s.records["live"]; !ok {
		t.Fatal("live pod must never be evicted")
	}
}

func TestPodStoreLimitAndOrder(t *testing.T) {
	s := NewPodStore("", 0)
	s.Upsert(testutil.WithUID(testutil.ExecutorPod("app-l-exec-1", "app-l"), "e1"), false)
	time.Sleep(2 * time.Millisecond)
	s.Upsert(testutil.WithUID(testutil.ExecutorPod("app-l-exec-2", "app-l"), "e2"), false)

	recs := s.GetRecords(PodRecordFilter{Limit: 1})
	if len(recs) != 1 || recs[0].Name != "app-l-exec-2" {
		t.Fatalf("limit should return only the newest record, got %+v", recs)
	}
}

func TestPodStoreCapturesLabels(t *testing.T) {
	s := NewPodStore("", 0)
	labels := map[string]string{
		"spark-role":         "executor",
		"appSparkID":         "app-l",
		"applicationId":      "app-l",
		"spark-app-selector": "app-l",
		"spark-exec-id":      "1",
		"custom-label":       "v",
	}
	pod := testutil.WithUID(testutil.PlainPod("app-l-exec-1", labels, corev1.PodRunning), "uid-labels")
	s.Upsert(pod, false)

	recs := s.GetRecords(PodRecordFilter{ApplicationID: "app-l"})
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %+v", recs)
	}
	got := recs[0].Labels
	if len(got) != len(labels) {
		t.Fatalf("expected %d labels, got %d: %+v", len(labels), len(got), got)
	}
	for key, want := range labels {
		if got[key] != want {
			t.Fatalf("label %s = %q, want %q", key, got[key], want)
		}
	}

	// The stored labels must be a defensive copy, not the informer's map.
	labels["custom-label"] = "mutated"
	if got["custom-label"] != "v" {
		t.Fatalf("record labels must be a defensive copy, got %q", got["custom-label"])
	}

	// Labels must survive deletion (the tombstone still carries labels).
	s.Upsert(testutil.WithUID(testutil.PlainPod("app-l-exec-1", labels, corev1.PodRunning), "uid-labels"), true)
	deleted := s.GetRecords(PodRecordFilter{ApplicationID: "app-l", Status: "deleted"})
	if len(deleted) != 1 || len(deleted[0].Labels) == 0 {
		t.Fatalf("labels must survive deletion, got %+v", deleted)
	}
}
