package handler

import (
	"testing"
	"time"

	"podwatcher/internal/store"
	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestHandler() *PodEventHandler {
	return NewPodEventHandler(store.NewAppStore("", 0), store.NewPodStore("", 0))
}

func TestHandlerTracksDriverAndExecutor(t *testing.T) {
	h := newTestHandler()

	h.OnAdd(testutil.ExecutorPod("app-exec-1", "app-x"), false)
	h.OnAdd(testutil.PlainPod("web-pod", map[string]string{"app": "web"}, corev1.PodRunning), false)

	if evs := h.GetEvents(0); len(evs) != 0 {
		t.Fatalf("executor and unrelated pods must not produce driver events, got %+v", evs)
	}
	pods := h.GetPodRecords(store.PodRecordFilter{})
	if len(pods) != 1 || pods[0].Role != "executor" || pods[0].ApplicationID != "app-x" {
		t.Fatalf("expected 1 executor pod record, got %+v", pods)
	}
	apps, _ := h.GetApplications(time.Time{}, 0, "", nil)
	if len(apps) != 1 || apps[0].ApplicationID != "app-x" || apps[0].Status != "unknown" {
		t.Fatalf("executor-only discovery should yield an unknown application, got %+v", apps)
	}
}

func TestHandlerDriverAddDelete(t *testing.T) {
	h := newTestHandler()
	driver := testutil.DriverPod("app-a-driver", "app-a", corev1.PodRunning, metav1.Now().Time)

	h.OnAdd(driver, false)
	evs := h.GetEvents(0)
	if len(evs) != 1 || evs[0].Type != "ADDED" {
		t.Fatalf("expected 1 ADDED event, got %+v", evs)
	}
	apps, _ := h.GetApplications(time.Time{}, 0, "", nil)
	if len(apps) != 1 || apps[0].ApplicationID != "app-a" || apps[0].Status != "running" {
		t.Fatalf("expected running application, got %+v", apps)
	}

	h.OnDelete(driver)
	evs = h.GetEvents(0)
	if len(evs) != 2 || evs[0].Type != "DELETED" {
		t.Fatalf("expected newest DELETED event, got %+v", evs)
	}
	apps, _ = h.GetApplications(time.Time{}, 0, "", nil)
	if len(apps) != 1 || apps[0].Status != "deleted" || apps[0].DeletedAt == nil {
		t.Fatalf("expected deleted application with timestamp, got %+v", apps)
	}
	pods := h.GetPodRecords(store.PodRecordFilter{})
	if len(pods) != 1 || pods[0].Status != "deleted" || pods[0].DeletedAt == nil {
		t.Fatalf("expected deleted pod record, got %+v", pods)
	}
}

func TestHandlerInitialSyncMarkedInitial(t *testing.T) {
	h := newTestHandler()
	h.OnAdd(testutil.DriverPod("app-i-driver", "app-i", corev1.PodPending, metav1.Now().Time), true)
	evs := h.GetEvents(0)
	if len(evs) != 1 || evs[0].Type != "INITIAL" {
		t.Fatalf("expected INITIAL event from initial list, got %+v", evs)
	}
}

func TestHandlerInitialSyncBootstrapsStores(t *testing.T) {
	h := newTestHandler()
	created := time.Now().Add(-time.Hour).Truncate(time.Second)

	h.OnAdd(testutil.DriverPod("app-b-driver", "app-b", corev1.PodPending, created), true)

	// Anchored in the past, the snapshot record is invisible to a caught-up
	// incremental cursor and not stamped with the restart time.
	if recent, _ := h.GetApplications(time.Now(), 0, "", nil); len(recent) != 0 {
		t.Fatalf("initial-sync bootstrap must not surface in recent changes, got %+v", recent)
	}
	all, _ := h.GetApplications(time.Time{}, 0, "", nil)
	if len(all) != 1 || !all[0].ChangedAt.Equal(created) {
		t.Fatalf("bootstrap application should be anchored to creationTimestamp, got %+v", all)
	}
	pods := h.GetPodRecords(store.PodRecordFilter{ApplicationID: "app-b"})
	if len(pods) != 1 || !pods[0].LastUpdatedAt.Equal(created) {
		t.Fatalf("bootstrap pod record should be anchored to creationTimestamp, got %+v", pods)
	}
}

func TestHandlerTerminalTransitions(t *testing.T) {
	h := newTestHandler()
	running := testutil.DriverPod("app-f-driver", "app-f", corev1.PodRunning, metav1.Now().Time)
	failed := testutil.DriverPod("app-f-driver", "app-f", corev1.PodFailed, metav1.Now().Time)

	h.OnAdd(running, false)
	h.OnUpdate(running, failed)

	evs := h.GetEvents(0)
	if len(evs) != 2 || evs[0].Type != "MODIFIED" || evs[0].Phase != "Failed" {
		t.Fatalf("expected newest MODIFIED Failed event, got %+v", evs)
	}
	apps, _ := h.GetApplications(time.Time{}, 0, "", nil)
	if apps[0].Status != "failed" || apps[0].FinishedAt == nil {
		t.Fatalf("expected failed application with finishedAt, got %+v", apps[0])
	}

	h2 := newTestHandler()
	old := testutil.DriverPod("app-s-driver", "app-s", corev1.PodRunning, metav1.Now().Time)
	succ := testutil.DriverPod("app-s-driver", "app-s", corev1.PodSucceeded, metav1.Now().Time)
	h2.OnAdd(old, false)
	h2.OnUpdate(old, succ)
	evs = h2.GetEvents(0)
	if len(evs) != 2 || evs[0].Type != "MODIFIED" || evs[0].Phase != "Succeeded" {
		t.Fatalf("expected MODIFIED Succeeded event, got %+v", evs)
	}
	if recs, _ := h2.GetApplications(time.Time{}, 0, "", nil); recs[0].Status != "succeeded" {
		t.Fatalf("expected succeeded status, got %q", recs[0].Status)
	}

	// A non-terminal transition (e.g. Pending->Running) must not log an event
	// but still keeps the stores current.
	h3 := newTestHandler()
	pending := testutil.DriverPod("app-r-driver", "app-r", corev1.PodPending, metav1.Now().Time)
	running2 := testutil.DriverPod("app-r-driver", "app-r", corev1.PodRunning, metav1.Now().Time)
	h3.OnAdd(pending, false)
	h3.OnUpdate(pending, running2)
	if evs := h3.GetEvents(0); len(evs) != 1 {
		t.Fatalf("expected only the ADDED event for non-terminal transition, got %d", len(evs))
	}
	if recs, _ := h3.GetApplications(time.Time{}, 0, "", nil); len(recs) != 1 || recs[0].Status != "running" {
		t.Fatalf("store should reflect running, got %+v", recs)
	}
}

func TestHandlerOneApplicationManyPods(t *testing.T) {
	h := newTestHandler()
	driver := testutil.DriverPod("app-n-driver", "app-n", corev1.PodRunning, time.Now())

	h.OnAdd(driver, false)
	drv, _ := h.GetApplications(time.Time{}, 0, "app-n", nil)
	driverChanged := drv[0].ChangedAt
	h.OnAdd(testutil.ExecutorPod("app-n-exec-1", "app-n"), false)
	h.OnAdd(testutil.ExecutorPod("app-n-exec-2", "app-n"), false)

	if apps, _ := h.GetApplications(time.Time{}, 0, "", nil); len(apps) != 1 {
		t.Fatalf("expected 1 application for driver+executors, got %+v", apps)
	}
	pods := h.GetPodRecords(store.PodRecordFilter{ApplicationID: "app-n"})
	if len(pods) != 3 {
		t.Fatalf("expected 3 pod records for the application, got %+v", pods)
	}
	if evs := h.GetEvents(0); len(evs) != 1 {
		t.Fatalf("diagnostic stream must stay driver-only, got %+v", evs)
	}

	// Deleting an executor must not change the application lifecycle.
	h.OnDelete(testutil.ExecutorPod("app-n-exec-1", "app-n"))
	apps, _ := h.GetApplications(time.Time{}, 0, "", nil)
	if apps[0].Status != "running" || apps[0].DeletedAt != nil {
		t.Fatalf("executor deletion must not affect application status, got %+v", apps[0])
	}
	if !apps[0].ChangedAt.Equal(driverChanged) {
		t.Fatalf("executor churn must not advance changedAt, got %v want %v", apps[0].ChangedAt, driverChanged)
	}
	execs := h.GetPodRecords(store.PodRecordFilter{ApplicationID: "app-n", Role: "executor", Status: "deleted"})
	if len(execs) != 1 || execs[0].Name != "app-n-exec-1" {
		t.Fatalf("expected 1 deleted executor record, got %+v", execs)
	}

	if filtered, _ := h.GetApplications(time.Time{}, 0, "app-n", nil); len(filtered) != 1 || filtered[0].ApplicationID != "app-n" {
		t.Fatalf("applicationId filter should return only app-n, got %+v", filtered)
	}
	if filtered, _ := h.GetApplications(time.Time{}, 0, "app-other", nil); len(filtered) != 0 {
		t.Fatalf("unknown applicationId should match nothing, got %+v", filtered)
	}
}
