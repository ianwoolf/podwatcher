package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
)

func TestAppStoreDriverLifecycle(t *testing.T) {
	s := NewAppStore("", 0)
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	pod := testutil.DriverPod("app-a-driver", "app-a", corev1.PodRunning, created)

	s.Upsert(pod, false)
	rec, ok := s.GetRecord("app-a")
	if !ok {
		t.Fatal("expected application record")
	}
	if rec.DriverPodName != "app-a-driver" || rec.Status != "running" || rec.DeletedAt != nil {
		t.Fatalf("unexpected running record: %+v", rec)
	}
	if !rec.CreatedAt.Equal(created) {
		t.Fatalf("createdAt should come from pod creationTimestamp, got %v want %v", rec.CreatedAt, created)
	}
	if rec.SparkAppName != "spark-pi-yunikorn" {
		t.Fatalf("expected spark app name preserved, got %q", rec.SparkAppName)
	}
	if rec.ChangedAt.IsZero() {
		t.Fatal("lifecycle creation must stamp changedAt")
	}

	s.Upsert(testutil.DriverPod("app-a-driver", "app-a", corev1.PodSucceeded, created), false)
	rec, _ = s.GetRecord("app-a")
	if rec.Status != "succeeded" || rec.FinishedAt == nil {
		t.Fatalf("expected succeeded with finishedAt: %+v", rec)
	}

	s.Upsert(testutil.DriverPod("app-a-driver", "app-a", corev1.PodSucceeded, created), true)
	rec, _ = s.GetRecord("app-a")
	if rec.Status != "deleted" || rec.DeletedAt == nil {
		t.Fatalf("expected deleted with deletedAt: %+v", rec)
	}
}

func TestAppStoreExecutorDoesNotDriveStatus(t *testing.T) {
	s := NewAppStore("", 0)

	s.Upsert(testutil.ExecutorPod("app-e-exec-1", "app-e"), false)
	rec, ok := s.GetRecord("app-e")
	if !ok {
		t.Fatal("executor discovery should create an application record")
	}
	if rec.Status != "unknown" || rec.DriverPodName != "" {
		t.Fatalf("executor-only record must stay unknown without driver, got %+v", rec)
	}

	s.Upsert(testutil.DriverPod("app-e-driver", "app-e", corev1.PodRunning, time.Now()), false)
	rec, _ = s.GetRecord("app-e")
	if rec.Status != "running" || rec.DriverPodName != "app-e-driver" {
		t.Fatalf("driver should drive the application status, got %+v", rec)
	}

	s.Upsert(testutil.ExecutorPod("app-e-exec-1", "app-e"), true)
	rec, _ = s.GetRecord("app-e")
	if rec.Status != "running" || rec.DeletedAt != nil {
		t.Fatalf("executor deletion must not affect application status, got %+v", rec)
	}
}

func TestAppStoreAppIDFallback(t *testing.T) {
	s := NewAppStore("", 0)
	pod := testutil.PlainPod("sel-driver", map[string]string{"spark-role": "driver", "spark-app-selector": "spark-selector-xyz"}, corev1.PodRunning)
	s.Upsert(pod, false)
	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 1 || recs[0].ApplicationID != "spark-selector-xyz" {
		t.Fatalf("expected selector fallback, got %+v", recs)
	}
}

func TestAppIDFromPod(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{name: "appSparkID takes precedence", labels: map[string]string{"appSparkID": "internal-1", "applicationId": "legacy-1", "spark-app-selector": "selector-1"}, want: "internal-1"},
		{name: "appSparkID only", labels: map[string]string{"appSparkID": "internal-2"}, want: "internal-2"},
		{name: "applicationId fallback", labels: map[string]string{"applicationId": "legacy-3", "spark-app-selector": "selector-3"}, want: "legacy-3"},
		{name: "selector fallback", labels: map[string]string{"spark-app-selector": "selector-4"}, want: "selector-4"},
		{name: "empty values fall through", labels: map[string]string{"appSparkID": "", "applicationId": "", "spark-app-selector": ""}, want: ""},
		{name: "nil labels", labels: nil, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := testutil.PlainPod("probe-pod", tt.labels, corev1.PodRunning)
			if got := AppIDFromPod(pod); got != tt.want {
				t.Fatalf("AppIDFromPod() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAppStorePersistenceRoundTrip(t *testing.T) {
	file := filepath.Join(t.TempDir(), "applications.json")

	s1 := NewAppStore(file, 10)
	s1.Upsert(testutil.DriverPod("app-d-driver", "app-d", corev1.PodSucceeded, time.Now().Add(-30*time.Minute)), true)
	s1.Stop()

	s2 := NewAppStore(file, 10)
	defer s2.Stop()
	recs, _ := s2.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 1 {
		t.Fatalf("expected 1 loaded record, got %d", len(recs))
	}
	if recs[0].ApplicationID != "app-d" || recs[0].Status != "deleted" || recs[0].DeletedAt == nil {
		t.Fatalf("persisted deleted record mismatch: %+v", recs[0])
	}
}

func TestAppStoreEvictionKeepsLive(t *testing.T) {
	s := NewAppStore("", 100)
	base := time.Now().Truncate(time.Second)

	s.Upsert(testutil.DriverPod("old1-driver", "old1", corev1.PodSucceeded, base.Add(-3*time.Hour)), true)
	s.Upsert(testutil.DriverPod("old2-driver", "old2", corev1.PodSucceeded, base.Add(-2*time.Hour)), true)
	s.Upsert(testutil.DriverPod("old3-driver", "old3", corev1.PodSucceeded, base.Add(-time.Hour)), true)
	s.Upsert(testutil.DriverPod("live-driver", "live", corev1.PodRunning, base), false)

	s.mu.Lock()
	for id, at := range map[string]time.Time{
		"old1": base.Add(-3 * time.Hour),
		"old2": base.Add(-2 * time.Hour),
		"old3": base.Add(-time.Hour),
	} {
		ts := at
		s.records[id].DeletedAt = &ts
	}
	s.maxRecords = 3
	s.evictLocked()
	s.mu.Unlock()

	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 3 {
		t.Fatalf("expected 3 records after eviction (limit 3), got %d", len(recs))
	}
	if _, ok := s.GetRecord("old1"); ok {
		t.Fatal("oldest deleted record should have been evicted")
	}
	if _, ok := s.GetRecord("live"); !ok {
		t.Fatal("live application must never be evicted")
	}
}

func TestAppStoreGetChangesByTimestamp(t *testing.T) {
	s := NewAppStore("", 0)
	s.Upsert(testutil.DriverPod("a-driver", "app-a", corev1.PodRunning, time.Now()), false)
	s.Upsert(testutil.DriverPod("b-driver", "app-b", corev1.PodSucceeded, time.Now()), true)

	all, hasMore := s.GetChanges(time.Time{}, 0, "", nil)
	if len(all) != 2 || hasMore {
		t.Fatalf("expected 2 changes without paging, got %d hasMore=%v", len(all), hasMore)
	}
	if all[0].ApplicationID != "app-a" || all[1].ApplicationID != "app-b" {
		t.Fatalf("expected changedAt ascending order, got %+v", all)
	}

	caughtUp, _ := s.GetChanges(all[len(all)-1].ChangedAt, 0, "", nil)
	if len(caughtUp) != 0 {
		t.Fatalf("expected empty page at high water, got %+v", caughtUp)
	}

	page, pageMore := s.GetChanges(time.Time{}, 1, "", nil)
	if len(page) != 1 || !pageMore || page[0].ApplicationID != "app-a" {
		t.Fatalf("expected first page with app-a and hasMore, got %+v more=%v", page, pageMore)
	}
	page2, page2More := s.GetChanges(page[0].ChangedAt, 1, "", nil)
	if len(page2) != 1 || page2More || page2[0].ApplicationID != "app-b" {
		t.Fatalf("expected second page with app-b, got %+v more=%v", page2, page2More)
	}

	if running, _ := s.GetChanges(time.Time{}, 0, "", []string{"running"}); len(running) != 1 || running[0].ApplicationID != "app-a" {
		t.Fatalf("status filter failed: %+v", running)
	}
	if both, _ := s.GetChanges(time.Time{}, 0, "", []string{"running", "deleted"}); len(both) != 2 {
		t.Fatalf("multi-status filter should return both records, got %+v", both)
	}
	if one, _ := s.GetChanges(time.Time{}, 0, "app-a", nil); len(one) != 1 || one[0].ApplicationID != "app-a" {
		t.Fatalf("applicationId filter should return only app-a, got %+v", one)
	}
	if none, _ := s.GetChanges(time.Time{}, 0, "app-missing", nil); len(none) != 0 {
		t.Fatalf("unknown applicationId should match nothing, got %+v", none)
	}
}

func TestAppStoreChangedAtLifecycleOnly(t *testing.T) {
	s := NewAppStore("", 0)

	s.Upsert(testutil.ExecutorPod("app-r-exec-1", "app-r"), false)
	rec, ok := s.GetRecord("app-r")
	if !ok || rec.Status != "unknown" || rec.ChangedAt.IsZero() {
		t.Fatalf("new executor record should be unknown with changedAt, got %+v ok=%v", rec, ok)
	}
	first := rec.ChangedAt
	s.Upsert(testutil.ExecutorPod("app-r-exec-2", "app-r"), false)
	s.Upsert(testutil.ExecutorPod("app-r-exec-1", "app-r"), true)
	rec, _ = s.GetRecord("app-r")
	if !rec.ChangedAt.Equal(first) {
		t.Fatalf("executor churn must not advance changedAt, got %v want %v", rec.ChangedAt, first)
	}

	created := time.Now().Add(-time.Minute).Truncate(time.Second)
	cases := []struct {
		name    string
		pod     *corev1.Pod
		deleted bool
		status  string
	}{
		{"running driver", testutil.DriverPod("app-r-driver", "app-r", corev1.PodRunning, created), false, "running"},
		{"succeeded driver", testutil.DriverPod("app-r-driver", "app-r", corev1.PodSucceeded, created), false, "succeeded"},
		{"deleted driver", testutil.DriverPod("app-r-driver", "app-r", corev1.PodSucceeded, created), true, "deleted"},
	}
	prev := first
	for _, tc := range cases {
		s.Upsert(tc.pod, tc.deleted)
		rec, _ = s.GetRecord("app-r")
		if !rec.ChangedAt.After(prev) || rec.Status != tc.status {
			t.Fatalf("%s: expected changedAt after %v and status %s, got %+v", tc.name, prev, tc.status, rec)
		}
		prev = rec.ChangedAt
	}
	if rec.FinishedAt == nil || rec.DeletedAt == nil {
		t.Fatalf("expected finishedAt and deletedAt set, got %+v", rec)
	}

	s.Upsert(testutil.DriverPod("app-r-driver", "app-r", corev1.PodSucceeded, created), true)
	rec, _ = s.GetRecord("app-r")
	if !rec.ChangedAt.Equal(prev) {
		t.Fatalf("duplicate terminal upsert must not advance changedAt, got %v want %v", rec.ChangedAt, prev)
	}
}

func TestAppStoreChangedAtStrictlyIncreasing(t *testing.T) {
	s := NewAppStore("", 0)

	const n = 50
	for i := 0; i < n; i++ {
		appID := fmt.Sprintf("app-bulk-%d", i)
		s.Upsert(testutil.ExecutorPod(appID+"-exec", appID), false)
	}

	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != n {
		t.Fatalf("expected %d records, got %d", n, len(recs))
	}
	for i := 1; i < len(recs); i++ {
		gap := recs[i].ChangedAt.Sub(recs[i-1].ChangedAt)
		if gap < time.Millisecond {
			t.Fatalf("changedAt gap below 1ms at %d: %v (%v -> %v)", i, gap, recs[i-1].ChangedAt, recs[i].ChangedAt)
		}
	}
}

func TestAppStorePersistenceContinuesChangedAt(t *testing.T) {
	file := filepath.Join(t.TempDir(), "applications.json")

	s1 := NewAppStore(file, 10)
	created := time.Now().Add(-time.Minute).Truncate(time.Second)
	s1.Upsert(testutil.DriverPod("app-c-driver", "app-c", corev1.PodRunning, created), false)
	s1.Upsert(testutil.DriverPod("app-c-driver", "app-c", corev1.PodSucceeded, created), false)
	s1.Stop()

	s2 := NewAppStore(file, 10)
	defer s2.Stop()
	rec, ok := s2.GetRecord("app-c")
	if !ok || rec.ChangedAt.IsZero() {
		t.Fatalf("expected loaded record with changedAt, got %+v ok=%v", rec, ok)
	}
	loaded := rec.ChangedAt

	s2.Upsert(testutil.DriverPod("app-c-driver", "app-c", corev1.PodSucceeded, created), true)
	rec, _ = s2.GetRecord("app-c")
	if !rec.ChangedAt.After(loaded) {
		t.Fatalf("changedAt must continue after restored value, got %v want > %v", rec.ChangedAt, loaded)
	}
}

func TestAppStoreLoadsLegacyBareArray(t *testing.T) {
	file := filepath.Join(t.TempDir(), "applications.json")
	older := ApplicationRecord{ApplicationID: "old-a", Status: "succeeded", LastUpdatedAt: time.Now().Add(-time.Hour)}
	newer := ApplicationRecord{ApplicationID: "old-b", Status: "deleted", LastUpdatedAt: time.Now()}
	data, err := json.Marshal([]ApplicationRecord{older, newer})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s := NewAppStore(file, 10)
	defer s.Stop()

	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 2 {
		t.Fatalf("expected 2 legacy records, got %+v", recs)
	}
	if recs[0].ApplicationID != "old-a" || recs[1].ApplicationID != "old-b" {
		t.Fatalf("legacy records should be ordered by lastUpdatedAt, got %+v", recs)
	}
	if recs[0].ChangedAt.IsZero() || !recs[1].ChangedAt.After(recs[0].ChangedAt) {
		t.Fatalf("legacy changedAt should be backfilled ascending, got %v %v", recs[0].ChangedAt, recs[1].ChangedAt)
	}
}

func TestAppStoreLoadsTransitionalWrappedLayout(t *testing.T) {
	file := filepath.Join(t.TempDir(), "applications.json")
	records := []ApplicationRecord{
		{ApplicationID: "wrap-a", Status: "running", LastUpdatedAt: time.Now().Add(-time.Hour)},
		{ApplicationID: "wrap-b", Status: "deleted", LastUpdatedAt: time.Now()},
	}
	wrapped := struct {
		Revision int64               `json:"revision"`
		Records  []ApplicationRecord `json:"records"`
	}{Revision: 2, Records: records}
	data, err := json.Marshal(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s := NewAppStore(file, 10)
	defer s.Stop()

	recs, _ := s.GetChanges(time.Time{}, 0, "", nil)
	if len(recs) != 2 || recs[0].ApplicationID != "wrap-a" || recs[1].ApplicationID != "wrap-b" {
		t.Fatalf("expected wrapped legacy records ordered, got %+v", recs)
	}
	if recs[0].ChangedAt.IsZero() || !recs[1].ChangedAt.After(recs[0].ChangedAt) {
		t.Fatalf("wrapped legacy changedAt should be backfilled, got %v %v", recs[0].ChangedAt, recs[1].ChangedAt)
	}
}
