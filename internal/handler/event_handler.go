package handler

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type PodEvent struct {
	Type      string            `json:"type"` // ADDED, MODIFIED, DELETED
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Phase     string            `json:"phase"`
	Node      string            `json:"node"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// SparkPodInfo 记录单个被删除的 Spark Pod 信息
type SparkPodInfo struct {
	Name              string    `json:"name"`
	Namespace         string    `json:"namespace"`
	ApplicationID     string    `json:"applicationId"`
	SparkAppSelector  string    `json:"sparkAppSelector"`
	Phase             string    `json:"phase"`
	Node              string    `json:"node"`
	DeletedAt         time.Time `json:"deletedAt"`
}

// SparkApplication 记录一个 Spark Application 的所有被删除 Pod
type SparkApplication struct {
	ApplicationID    string         `json:"applicationId"`
	SparkAppSelector string         `json:"sparkAppSelector"`
	Pods             []SparkPodInfo `json:"pods"`
	FirstDeletedAt   time.Time      `json:"firstDeletedAt"`
	LastDeletedAt    time.Time      `json:"lastDeletedAt"`
	PodCount         int            `json:"podCount"`
}

// SparkAppCollector 收集 DELETED 事件中的 Spark Pod，按 application 分组
type SparkAppCollector struct {
	applications  map[string]*SparkApplication
	lock          sync.RWMutex
	maxApps       int
	maxPodsPerApp int
}

func NewSparkAppCollector(maxApps, maxPodsPerApp int) *SparkAppCollector {
	return &SparkAppCollector{
		applications:  make(map[string]*SparkApplication),
		maxApps:       maxApps,
		maxPodsPerApp: maxPodsPerApp,
	}
}

func (c *SparkAppCollector) isSparkPod(pod *corev1.Pod) bool {
	if pod.Labels == nil {
		return false
	}
	_, hasAppID := pod.Labels["applicationId"]
	_, hasSelector := pod.Labels["spark-app-selector"]
	return hasAppID || hasSelector
}

func (c *SparkAppCollector) getApplicationID(pod *corev1.Pod) string {
	if appID, ok := pod.Labels["applicationId"]; ok && appID != "" {
		return appID
	}
	if selector, ok := pod.Labels["spark-app-selector"]; ok && selector != "" {
		return selector
	}
	return ""
}

func (c *SparkAppCollector) AddPod(pod *corev1.Pod) {
	appID := c.getApplicationID(pod)
	if appID == "" {
		return
	}

	now := time.Now()
	sparkPod := SparkPodInfo{
		Name:             pod.Name,
		Namespace:        pod.Namespace,
		ApplicationID:    pod.Labels["applicationId"],
		SparkAppSelector: pod.Labels["spark-app-selector"],
		Phase:            string(pod.Status.Phase),
		Node:             pod.Spec.NodeName,
		DeletedAt:        now,
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	app, exists := c.applications[appID]
	if !exists {
		app = &SparkApplication{
			ApplicationID:    appID,
			SparkAppSelector: sparkPod.SparkAppSelector,
			Pods:             make([]SparkPodInfo, 0),
			FirstDeletedAt:   now,
			LastDeletedAt:    now,
		}
		c.applications[appID] = app
	}

	app.Pods = append(app.Pods, sparkPod)
	app.PodCount = len(app.Pods)
	if now.Before(app.FirstDeletedAt) {
		app.FirstDeletedAt = now
	}
	if now.After(app.LastDeletedAt) {
		app.LastDeletedAt = now
	}

	// 每个 application 限制 pod 数量
	if len(app.Pods) > c.maxPodsPerApp {
		app.Pods = app.Pods[len(app.Pods)-c.maxPodsPerApp:]
		app.PodCount = len(app.Pods)
	}

	// 超过上限淘汰最早被删除的
	if len(c.applications) > c.maxApps {
		var oldestID string
		var oldestTime time.Time
		for id, a := range c.applications {
			if oldestID == "" || a.FirstDeletedAt.Before(oldestTime) {
				oldestID = id
				oldestTime = a.FirstDeletedAt
			}
		}
		delete(c.applications, oldestID)
	}

	klog.Infof("[SPARK APP] Collected deleted pod %s/%s for application %s", pod.Namespace, pod.Name, appID)
}

func (c *SparkAppCollector) GetByTimeRange(startTime, endTime time.Time) []SparkApplication {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 淘汰两周前的数据
	cutoff := time.Now().Add(-14 * 24 * time.Hour)
	for id, app := range c.applications {
		if app.LastDeletedAt.Before(cutoff) {
			delete(c.applications, id)
		}
	}

	var result []SparkApplication
	for _, app := range c.applications {
		// application 中任何一个 pod 的 DeletedAt 在时间范围内即匹配
		matched := false
		for _, pod := range app.Pods {
			if (pod.DeletedAt.Equal(startTime) || pod.DeletedAt.After(startTime)) &&
				(pod.DeletedAt.Equal(endTime) || pod.DeletedAt.Before(endTime)) {
				matched = true
				break
			}
		}
		if matched {
			result = append(result, *app)
		}
	}
	return result
}

type PodEventHandler struct {
	events         []PodEvent
	eventsLock     sync.RWMutex
	maxEvents      int
	sparkCollector *SparkAppCollector
}

func NewPodEventHandler() *PodEventHandler {
	return &PodEventHandler{
		events:         make([]PodEvent, 0),
		maxEvents:      1000,
		sparkCollector: NewSparkAppCollector(1000, 100),
	}
}

func (h *PodEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		klog.Error("Failed to convert object to Pod")
		return
	}

	eventType := "ADDED"
	if isInInitialList {
		eventType = "INITIAL"
	}

	// 只保留 Succeeded
	if pod.Status.Phase != corev1.PodSucceeded {
		return
	}

	event := PodEvent{
		Type:      eventType,
		Namespace: pod.Namespace,
		Name:      pod.Name,
		Phase:     string(pod.Status.Phase),
		Node:      pod.Spec.NodeName,
		Labels:    pod.Labels,
		Timestamp: time.Now(),
	}

	h.addEvent(event)
	klog.Infof("[POD %s] %s/%s - Phase: %s - Node: %s - Labels: %v - Annotations: %v",
		eventType, pod.Namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName, pod.Labels, pod.Annotations)
}

func (h *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldPod, ok1 := oldObj.(*corev1.Pod)
	newPod, ok2 := newObj.(*corev1.Pod)

	if !ok1 || !ok2 {
		klog.Error("Failed to convert object to Pod")
		return
	}

	if oldPod.Status.Phase != newPod.Status.Phase {
		// 只保留变到 Succeeded
		if newPod.Status.Phase != corev1.PodSucceeded {
			return
		}

		event := PodEvent{
			Type:      "MODIFIED",
			Namespace: newPod.Namespace,
			Name:      newPod.Name,
			Phase:     string(newPod.Status.Phase),
			Node:      newPod.Spec.NodeName,
			Labels:    newPod.Labels,
			Timestamp: time.Now(),
		}
		h.addEvent(event)
		klog.Infof("[POD MODIFIED] %s/%s: %s -> %s - Labels: %v - Annotations: %v",
			newPod.Namespace, newPod.Name, oldPod.Status.Phase, newPod.Status.Phase, newPod.Labels, newPod.Annotations)
	}
}

func (h *PodEventHandler) OnDelete(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Error("Failed to convert object to Pod or tombstone")
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			klog.Error("Failed to convert tombstone object to Pod")
			return
		}
	}

	event := PodEvent{
		Type:      "DELETED",
		Namespace: pod.Namespace,
		Name:      pod.Name,
		Phase:     string(pod.Status.Phase),
		Node:      pod.Spec.NodeName,
		Labels:    pod.Labels,
		Timestamp: time.Now(),
	}

	h.addEvent(event)
	klog.Infof("[POD DELETED] %s/%s - Labels: %v - Annotations: %v", pod.Namespace, pod.Name, pod.Labels, pod.Annotations)

	if h.sparkCollector.isSparkPod(pod) {
		h.sparkCollector.AddPod(pod)
	}
}

func (h *PodEventHandler) addEvent(event PodEvent) {
	h.eventsLock.Lock()
	defer h.eventsLock.Unlock()

	h.events = append([]PodEvent{event}, h.events...)

	if len(h.events) > h.maxEvents {
		h.events = h.events[:h.maxEvents]
	}
}

func (h *PodEventHandler) GetEvents(limit int) []PodEvent {
	h.eventsLock.RLock()
	defer h.eventsLock.RUnlock()

	if limit > 0 && limit < len(h.events) {
		return h.events[:limit]
	}
	return h.events
}

func (h *PodEventHandler) GetStats() map[string]int {
	h.eventsLock.RLock()
	defer h.eventsLock.RUnlock()

	stats := make(map[string]int)
	for _, event := range h.events {
		stats[event.Type]++
	}
	return stats
}

func (h *PodEventHandler) GetSparkApps(startTime, endTime time.Time) []SparkApplication {
	return h.sparkCollector.GetByTimeRange(startTime, endTime)
}
