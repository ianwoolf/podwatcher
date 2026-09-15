package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"podwatcher/internal/handler"
	"podwatcher/internal/store"
	"podwatcher/pkg/k8s"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PodService struct {
	k8sClient    *k8s.Client
	eventHandler *handler.PodEventHandler
}

func NewPodService(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler) *PodService {
	return &PodService{
		k8sClient:    k8sClient,
		eventHandler: eventHandler,
	}
}

func (s *PodService) ListPods(ctx context.Context, namespace string, labelSelector string) ([]corev1.Pod, error) {
	opts := metav1.ListOptions{}

	if labelSelector != "" {
		opts.LabelSelector = labelSelector
	}

	pods, err := s.k8sClient.Clientset.CoreV1().Pods(namespace).List(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	return pods.Items, nil
}

func (s *PodService) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	pod, err := s.k8sClient.Clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod: %w", err)
	}

	return pod, nil
}

func (s *PodService) GetEvents(limit int) []handler.PodEvent {
	return s.eventHandler.GetEvents(limit)
}

func (s *PodService) GetStats() map[string]int {
	return s.eventHandler.GetStats()
}

func (s *PodService) GetPodCountByPhase(ctx context.Context, namespace string) (map[string]int, error) {
	pods, err := s.ListPods(ctx, namespace, "")
	if err != nil {
		return nil, err
	}

	counts := make(map[string]int)
	for _, pod := range pods {
		counts[string(pod.Status.Phase)]++
	}

	return counts, nil
}

// PodRecordsQuery narrows a historical pod coordinate query. A zero Limit
// returns all matches.
type PodRecordsQuery struct {
	ApplicationID string
	Role          string
	Node          string
	Status        string
	StartTime     string
	EndTime       string
	Limit         int
}

// GetApplications returns one page of the incremental application feed.
// sinceParam/limitParam are the raw query values.
func (s *PodService) GetApplications(sinceParam, limitParam, appID, statusParam string) ([]store.ApplicationRecord, bool, error) {
	since := time.Time{}
	if sinceParam != "" {
		parsed, err := time.Parse(time.RFC3339Nano, sinceParam)
		if err != nil {
			return nil, false, fmt.Errorf("invalid since %q: use an RFC3339 timestamp", sinceParam)
		}
		since = parsed
	}

	limit := 100
	if limitParam != "" {
		parsed, err := strconv.Atoi(limitParam)
		if err != nil {
			return nil, false, fmt.Errorf("invalid limit %q: use an integer", limitParam)
		}
		limit = parsed
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 200 {
		limit = 200
	}

	records, hasMore := s.eventHandler.GetApplications(since, limit, appID, parseStatuses(statusParam))
	return records, hasMore, nil
}

// parseStatuses splits the comma-separated status query value into trimmed,
// non-empty statuses. An empty parameter yields nil, which disables filtering.
func parseStatuses(statusParam string) []string {
	if statusParam == "" {
		return nil
	}
	parts := strings.Split(statusParam, ",")
	statuses := make([]string, 0, len(parts))
	for _, part := range parts {
		if st := strings.TrimSpace(part); st != "" {
			statuses = append(statuses, st)
		}
	}
	return statuses
}

func (s *PodService) GetApplication(appID string) (store.ApplicationRecord, bool) {
	return s.eventHandler.GetApplication(appID)
}

func (s *PodService) GetPodRecords(query PodRecordsQuery) ([]store.PodRecord, error) {
	start, end, err := parseTimeWindow(query.StartTime, query.EndTime)
	if err != nil {
		return nil, err
	}
	return s.eventHandler.GetPodRecords(store.PodRecordFilter{
		ApplicationID: query.ApplicationID,
		Role:          query.Role,
		Node:          query.Node,
		Status:        query.Status,
		Start:         start,
		End:           end,
		Limit:         query.Limit,
	}), nil
}

func parseTimeWindow(startTime, endTime string) (start, end *time.Time, err error) {
	if startTime != "" {
		parsed, parseErr := time.Parse(time.RFC3339, startTime)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("invalid startTime format, use RFC3339: %w", parseErr)
		}
		start = &parsed
	}
	if endTime != "" {
		parsed, parseErr := time.Parse(time.RFC3339, endTime)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("invalid endTime format, use RFC3339: %w", parseErr)
		}
		end = &parsed
	}
	return start, end, nil
}
