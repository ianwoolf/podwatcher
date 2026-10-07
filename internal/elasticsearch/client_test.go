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
)

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
