package elasticsearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"podwatcher/internal/config"
	"podwatcher/internal/store"

	es "github.com/elastic/go-elasticsearch/v8"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Event preserves pod metadata and status without exporting the pod spec,
// which can contain environment credentials.
type Event struct {
	ApplicationID string            `json:"applicationId"`
	PodName       string            `json:"podName"`
	Queue         string            `json:"queue"`
	Type          string            `json:"type"`
	Cluster       string            `json:"cluster"`
	Timestamp     time.Time         `json:"@timestamp"`
	Initial       bool              `json:"isInitialSnapshot"`
	AddedAt       *time.Time        `json:"addedAt,omitempty"`
	DeletedAt     *time.Time        `json:"deletedAt,omitempty"`
	Metadata      metav1.ObjectMeta `json:"metadata"`
	Status        corev1.PodStatus  `json:"status"`
	Node          string            `json:"node"`
}

type Client struct {
	timeout time.Duration
	sdk     *es.Client
	index   string
	cluster string
}

func NewClient(cfg config.ElasticsearchConfig, cluster string) (*Client, error) {
	u, err := url.Parse(cfg.Address)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("elasticsearch.address must be an HTTP(S) URL without credentials, query or fragment")
	}
	if cfg.Index == "" || strings.ContainsAny(cfg.Index, "/\\ *?\"<>|,#:") || strings.ToLower(cfg.Index) != cfg.Index || strings.HasPrefix(cfg.Index, "_") || strings.HasPrefix(cfg.Index, "-") || strings.HasPrefix(cfg.Index, "+") || cfg.Index == "." || cfg.Index == ".." {
		return nil, fmt.Errorf("invalid elasticsearch.index")
	}
	password := os.Getenv(cfg.PasswordEnv)
	if cfg.Username != "" && (cfg.PasswordEnv == "" || password == "") {
		return nil, fmt.Errorf("elasticsearch password environment variable %q is empty", cfg.PasswordEnv)
	}
	if cfg.TimeoutSeconds < 0 || int64(cfg.TimeoutSeconds) > int64((1<<63-1)/time.Second) {
		return nil, fmt.Errorf("invalid elasticsearch.timeoutSeconds: use positive seconds")
	}
	if cfg.TimeoutSeconds == 0 {
		cfg.TimeoutSeconds = 20
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify} // Explicit configuration for self-signed ES endpoints.
	sdk, err := es.NewClient(es.Config{
		Transport:               transport,
		Addresses:               []string{cfg.Address},
		Username:                cfg.Username,
		Password:                password,
		EnableCompatibilityMode: true,
		// The existing write loop owns retry policy and per-attempt timeouts.
		DisableRetry: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Elasticsearch SDK")
	}
	return &Client{sdk: sdk, index: cfg.Index, cluster: cluster, timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}, nil
}

// Publish uses a stable id so retries and watch replays overwrite the same event.
// INITIAL adds have their own id and cannot replace a real ADDED event.
func (c *Client) Publish(ctx context.Context, eventType string, pod *corev1.Pod, initial bool) error {
	identity := string(pod.UID)
	if identity == "" {
		identity = pod.Namespace + "/" + pod.Name
	}
	idBytes, _ := json.Marshal([]interface{}{c.cluster, identity, pod.ResourceVersion, eventType, initial})
	hash := sha256.Sum256(idBytes)
	id := hex.EncodeToString(hash[:])
	now := time.Now().UTC()
	event := Event{ApplicationID: store.AppIDFromPod(pod), PodName: pod.Name, Queue: pod.Labels["queue"], Type: eventType, Cluster: c.cluster, Timestamp: now, Initial: initial, Metadata: pod.ObjectMeta, Status: pod.Status, Node: pod.Spec.NodeName}
	if !pod.CreationTimestamp.IsZero() {
		addedAt := pod.CreationTimestamp.Time.UTC()
		event.AddedAt = &addedAt
	}
	if eventType == "DELETED" {
		event.DeletedAt = &now
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return c.write(ctx, id, body)
}

// Completion is distinct from Pod events and keeps status under application
// to avoid conflicting with the existing object-valued status ES field.
type Completion struct {
	ApplicationID string                `json:"applicationId"`
	PodName       string                `json:"podName"`
	Queue         string                `json:"queue"`
	Type          string                `json:"type"`
	Cluster       string                `json:"cluster"`
	Timestamp     time.Time             `json:"@timestamp"`
	IndexedAt     time.Time             `json:"indexedAt"`
	FinishedAt    time.Time             `json:"finishedAt"`
	Application   store.ApplicationView `json:"application"`
}

func (c *Client) PublishApplication(ctx context.Context, app store.ApplicationView) error {
	if app.FinishedAt == nil {
		return fmt.Errorf("application has no finishedAt")
	}
	identity, _ := json.Marshal([]string{c.cluster, app.Namespace, app.ApplicationID, "APPLICATION_COMPLETED"})
	hash := sha256.Sum256(identity)
	now := time.Now().UTC()
	app.Cluster = c.cluster
	// Deletion after success/failure must retain the actual completion outcome.
	if app.DriverPhase == string(corev1.PodSucceeded) {
		app.Status = "succeeded"
	}
	if app.DriverPhase == string(corev1.PodFailed) {
		app.Status = "failed"
	}
	body, err := json.Marshal(Completion{ApplicationID: app.ApplicationID, PodName: app.DriverPodName, Queue: app.DriverLabels["queue"], Type: "APPLICATION_COMPLETED", Cluster: c.cluster, Timestamp: now, IndexedAt: now, FinishedAt: *app.FinishedAt, Application: app})
	if err != nil {
		return err
	}
	return c.write(ctx, hex.EncodeToString(hash[:]), body)
}

func (c *Client) write(ctx context.Context, id string, body []byte) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
		resp, requestErr := c.sdk.Index(c.index, bytes.NewReader(body), c.sdk.Index.WithDocumentID(id), c.sdk.Index.WithContext(attemptCtx))
		if requestErr != nil {
			cancel()
			// Do not log transport errors: they may contain connection credentials.
			err = fmt.Errorf("Elasticsearch request failed")
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		cancel()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		err = fmt.Errorf("Elasticsearch returned HTTP %d", resp.StatusCode)
		if resp.StatusCode != 429 && resp.StatusCode < 500 {
			return err
		}
	}
	return err
}
