package informer

import (
	"fmt"
	"time"

	"podwatcher/internal/handler"
	"podwatcher/pkg/k8s"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type PodInformerManager struct {
	factory      informers.SharedInformerFactory
	informer     cache.SharedIndexInformer
	stopCh       chan struct{}
	eventHandler *handler.PodEventHandler
}

func NewPodInformerManager(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler) *PodInformerManager {
	factory := informers.NewSharedInformerFactory(k8sClient.Clientset, 60*time.Second)

	informer := factory.Core().V1().Pods().Informer()

	return &PodInformerManager{
		factory:      factory,
		informer:     informer,
		stopCh:       make(chan struct{}),
		eventHandler: eventHandler,
	}
}

func NewPodInformerManagerWithFilter(k8sClient *k8s.Client, eventHandler *handler.PodEventHandler, namespace string) *PodInformerManager {
	factory := informers.NewSharedInformerFactoryWithOptions(
		k8sClient.Clientset,
		60*time.Second,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.namespace", namespace).String()
		}),
	)

	informer := factory.Core().V1().Pods().Informer()

	return &PodInformerManager{
		factory:      factory,
		informer:     informer,
		stopCh:       make(chan struct{}),
		eventHandler: eventHandler,
	}
}

func (m *PodInformerManager) Start() error {
	m.informer.AddEventHandler(m.eventHandler)

	m.factory.Start(m.stopCh)

	klog.Info("Waiting for informer cache to sync...")

	if !cache.WaitForCacheSync(m.stopCh, m.informer.HasSynced) {
		return fmt.Errorf("failed to sync informer cache")
	}

	klog.Info("Informer cache sync completed")
	return nil
}

func (m *PodInformerManager) Stop() {
	close(m.stopCh)
	klog.Info("Informer stopped")
}

func (m *PodInformerManager) GetStore() cache.Store {
	return m.informer.GetStore()
}

func (m *PodInformerManager) HasSynced() bool {
	return m.informer.HasSynced()
}
