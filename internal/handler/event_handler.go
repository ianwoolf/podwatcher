package handler

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type PodEvent struct {
	Type      string    `json:"type"` // ADDED, MODIFIED, DELETED
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Phase     string    `json:"phase"`
	Node      string    `json:"node"`
	Timestamp time.Time `json:"timestamp"`
}

type PodEventHandler struct {
	events     []PodEvent
	eventsLock sync.RWMutex
	maxEvents  int
}

func NewPodEventHandler() *PodEventHandler {
	return &PodEventHandler{
		events:    make([]PodEvent, 0),
		maxEvents: 1000,
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

	event := PodEvent{
		Type:      eventType,
		Namespace: pod.Namespace,
		Name:      pod.Name,
		Phase:     string(pod.Status.Phase),
		Node:      pod.Spec.NodeName,
		Timestamp: time.Now(),
	}

	h.addEvent(event)
	klog.Infof("[POD %s] %s/%s - Phase: %s - Node: %s",
		eventType, pod.Namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName)
}

func (h *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	oldPod, ok1 := oldObj.(*corev1.Pod)
	newPod, ok2 := newObj.(*corev1.Pod)

	if !ok1 || !ok2 {
		klog.Error("Failed to convert object to Pod")
		return
	}

	if oldPod.Status.Phase != newPod.Status.Phase {
		event := PodEvent{
			Type:      "MODIFIED",
			Namespace: newPod.Namespace,
			Name:      newPod.Name,
			Phase:     string(newPod.Status.Phase),
			Node:      newPod.Spec.NodeName,
			Timestamp: time.Now(),
		}
		h.addEvent(event)
		klog.Infof("[POD MODIFIED] %s/%s: %s -> %s",
			newPod.Namespace, newPod.Name, oldPod.Status.Phase, newPod.Status.Phase)
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
		Timestamp: time.Now(),
	}

	h.addEvent(event)
	klog.Infof("[POD DELETED] %s/%s", pod.Namespace, pod.Name)
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
