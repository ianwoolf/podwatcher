package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"podwatcher/internal/config"
	"podwatcher/internal/store"
	"podwatcher/internal/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTopLevelQueryFields(t *testing.T) {
	for _, queue := range []string{"root.default", ""} {
		t.Run("queue="+queue, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var doc map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
					t.Error(err)
					return
				}
				for key, want := range map[string]string{"applicationId": "app-a", "podName": "driver", "queue": queue} {
					var got string
					if err := json.Unmarshal(doc[key], &got); err != nil || got != want {
						t.Errorf("%s: got %q, want %q (err=%v)", key, got, want, err)
					}
				}
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()
			c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
			if err != nil {
				t.Fatal(err)
			}
			pod := testutil.DriverPod("driver", "app-a", corev1.PodSucceeded, time.Now())
			// Use the same canonical app id as the store even if another label differs.
			pod.Labels["applicationId"] = "other-id"
			if queue != "" {
				pod.Labels["queue"] = queue
			}
			for _, kind := range []string{"ADDED", "DELETED"} {
				if err := c.Publish(context.Background(), kind, pod, false); err != nil {
					t.Fatal(err)
				}
			}
			finished := time.Now()
			app := store.ApplicationView{ApplicationRecord: store.ApplicationRecord{ApplicationID: "app-a", DriverPodName: "driver", FinishedAt: &finished}, DriverLabels: pod.Labels}
			if err := c.PublishApplication(context.Background(), app); err != nil {
				t.Fatal(err)
			}
			if calls != 3 {
				t.Fatalf("got %d documents", calls)
			}
		})
	}
}

func TestPodEventLifecycleTimes(t *testing.T) {
	created := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	deletionRequested := created.Add(time.Minute)
	var events []Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if _, exists := body["initial"]; exists {
			t.Error("ambiguous initial field must not be exported")
		}
		if _, exists := body["isInitialSnapshot"]; !exists {
			t.Error("snapshot marker missing")
		}
		data, _ := json.Marshal(body)
		var event Event
		if err := json.Unmarshal(data, &event); err != nil {
			t.Error(err)
		}
		events = append(events, event)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	pod := testutil.DriverPod("driver", "app", corev1.PodRunning, created)
	pod.DeletionTimestamp = &metav1.Time{Time: deletionRequested}
	before := time.Now().UTC()
	if err := c.Publish(context.Background(), "ADDED", pod, true); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(context.Background(), "DELETED", pod, false); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	for _, event := range events {
		if event.AddedAt == nil || !event.AddedAt.Equal(created) {
			t.Errorf("creation time lost: %+v", event)
		}
	}
	if !events[0].Initial || events[0].DeletedAt != nil {
		t.Error("snapshot add must not carry deletion time")
	}
	deleted := events[1].DeletedAt
	if deleted == nil || deleted.Before(before) || deleted.After(after) || deleted.Equal(deletionRequested) {
		t.Errorf("deletion must use observation time: %v", deleted)
	}
	pod.CreationTimestamp = metav1.Time{}
	if err := c.Publish(context.Background(), "ADDED", pod, false); err != nil {
		t.Fatal(err)
	}
	if events[2].AddedAt != nil {
		t.Error("unknown creation time must be omitted")
	}
}

func TestPublishCompletedApplication(t *testing.T) {
	finished := time.Now().Add(-time.Hour).UTC()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var doc Completion
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			t.Error(err)
		}
		if doc.Type != "APPLICATION_COMPLETED" || doc.Cluster != "cluster" || !doc.FinishedAt.Equal(finished) || doc.IndexedAt.Before(finished) || doc.Application.Status != "succeeded" || doc.Application.ApplicationID != "app" {
			t.Errorf("incorrect completion: %+v", doc)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	app := store.ApplicationView{ApplicationRecord: store.ApplicationRecord{ApplicationID: "app", Namespace: "default", Status: "succeeded", FinishedAt: &finished}, DriverPhase: "Succeeded"}
	if err := c.PublishApplication(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	app.Status = "deleted"
	if err := c.PublishApplication(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != paths[1] {
		t.Fatalf("one summary per application: %v", paths)
	}
	app.FinishedAt = nil
	if err := c.PublishApplication(context.Background(), app); err == nil {
		t.Fatal("missing completion time must fail")
	}
}

func TestPublishAuthenticationPayloadAndRetry(t *testing.T) {
	t.Setenv("TEST_ES_PASSWORD", "test-secret")
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "elastic" || pass != "test-secret" {
			t.Error("incorrect auth")
		}
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/events/_doc/") {
			t.Errorf("incorrect request %s %s", r.Method, r.URL.Path)
		}
		paths = append(paths, r.URL.Path)
		var event Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		if event.Cluster != "cluster-a" || event.Type != "ADDED" || event.Initial || event.Node != "node-1" || event.Status.Phase != corev1.PodRunning || event.Timestamp.IsZero() {
			t.Errorf("incorrect event: %+v", event)
		}
		if len(paths) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Username: "elastic", PasswordEnv: "TEST_ES_PASSWORD", Index: "events"}, "cluster-a")
	if err != nil {
		t.Fatal(err)
	}
	pod := testutil.WithUID(testutil.DriverPod("driver", "app-a", corev1.PodRunning, time.Now()), "uid-1")
	pod.ResourceVersion = "123"
	if err := c.Publish(context.Background(), "ADDED", pod, false); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != paths[1] {
		t.Fatalf("retry must keep document id: %v", paths)
	}
}

func TestPublishRejectsPermanentFailureWithoutRetry(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("sensitive response"))
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	err = c.Publish(context.Background(), "DELETED", testutil.ExecutorPod("executor", "app"), false)
	if err == nil || calls != 1 || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("unexpected error/calls: %v / %d", err, calls)
	}
}

func TestClientValidation(t *testing.T) {
	t.Setenv("TEST_ES_MISSING", "")
	for _, cfg := range []config.ElasticsearchConfig{
		{Address: "invalid", Index: "events"},
		{Address: "http://user:secret@localhost:9200", Index: "events"},
		{Address: "http://localhost:9200", Index: ""},
		{Address: "http://localhost:9200", Index: "INVALID"},
		{Address: "http://localhost:9200", Index: "events", Username: "elastic", PasswordEnv: "TEST_ES_MISSING"},
	} {
		if _, err := NewClient(cfg, "cluster"); err == nil {
			t.Error("expected invalid configuration to fail")
		}
	}
}

func TestLifecycleDocumentIDsAndNoPodSpec(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if _, exists := body["spec"]; exists {
			t.Error("pod spec must not be exported")
		}
		encoded, _ := json.Marshal(body)
		if strings.Contains(string(encoded), "private-env-value") {
			t.Error("environment leaked into event")
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	c, err := NewClient(config.ElasticsearchConfig{Address: srv.URL, Index: "events"}, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	pod := testutil.WithUID(testutil.ExecutorPod("executor", "app"), "uid")
	pod.ResourceVersion = "123"
	pod.Spec.Containers = []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: "PASSWORD", Value: "private-env-value"}}}}
	for _, event := range []struct {
		kind    string
		initial bool
	}{{"ADDED", false}, {"ADDED", false}, {"ADDED", true}, {"DELETED", false}} {
		if err := c.Publish(context.Background(), event.kind, pod, event.initial); err != nil {
			t.Fatal(err)
		}
	}
	if paths[0] != paths[1] || paths[0] == paths[2] || paths[0] == paths[3] || paths[2] == paths[3] {
		t.Fatalf("incorrect document IDs: %v", paths)
	}
}
