package externallabels

import (
	"context"
	"fmt"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// ExternalLabelsLabelKey is the label key used to identify ConfigMaps that
	// contain external labels. A ConfigMap must have this label set to "true"
	// to be discovered by the ConfigMapProvider.
	ExternalLabelsLabelKey   = "ibm.kubecost.com/external-labels"
	ExternalLabelsLabelValue = "true"
	ExternalLabelsSelector   = ExternalLabelsLabelKey + "=" + ExternalLabelsLabelValue
)

// ConfigMapProvider watches ConfigMaps in a given namespace that carry the
// label ibm.kubecost.com/external-labels=true and exposes their data as
// a merged key/value map via Labels.
type ConfigMapProvider struct {
	client    kubernetes.Interface
	namespace string

	mu     sync.RWMutex
	labels map[string]string
}

// NewConfigMapProvider creates a ConfigMapProvider that watches ConfigMaps in
// the given namespace.
func NewConfigMapProvider(client kubernetes.Interface, namespace string) (*ConfigMapProvider, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client must not be nil")
	}
	return &ConfigMapProvider{
		client:    client,
		namespace: namespace,
		labels:    make(map[string]string),
	}, nil
}

// Start launches the informer that keeps the cached labels up to date.
// It blocks until ctx is cancelled.
func (cmp *ConfigMapProvider) Start(ctx context.Context) error {
	lw := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = ExternalLabelsSelector
			return cmp.client.CoreV1().ConfigMaps(cmp.namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.LabelSelector = ExternalLabelsSelector
			return cmp.client.CoreV1().ConfigMaps(cmp.namespace).Watch(ctx, opts)
		},
	}

	informer := cache.NewSharedInformer(lw, &v1.ConfigMap{}, 0)

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			cmp.onConfigMapUpdate(obj)
		},
		UpdateFunc: func(_, newObj any) {
			cmp.onConfigMapUpdate(newObj)
		},
		DeleteFunc: func(obj any) {
			cmp.rebuildLabels(informer)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start configMap external labels provider: %w", err)
	}

	log.Infof("ExternalLabels: ConfigMapProvider: started in namespace %q with selector %q", cmp.namespace, ExternalLabelsSelector)
	informer.Run(ctx.Done())
	return nil
}

// Labels returns a copy of the currently cached external labels.
func (cmp *ConfigMapProvider) Labels(_ context.Context) (map[string]string, error) {
	cmp.mu.RLock()
	defer cmp.mu.RUnlock()
	out := make(map[string]string, len(cmp.labels))
	for k, v := range cmp.labels {
		out[k] = v
	}
	return out, nil
}

func (cmp *ConfigMapProvider) onConfigMapUpdate(obj any) {
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		return
	}
	cmp.mu.Lock()
	defer cmp.mu.Unlock()
	for k, v := range cm.Data {
		cmp.labels[k] = v
	}
	log.Debugf("ExternalLabels: ConfigMapProvider: merged %d label(s) from ConfigMap %s/%s", len(cm.Data), cm.Namespace, cm.Name)
}

// rebuildLabels re-reads all ConfigMaps currently in the informer's store and
// rebuilds the label cache. Called on delete to remove keys from deleted CMs.
func (p *ConfigMapProvider) rebuildLabels(informer cache.SharedInformer) {
	merged := make(map[string]string)
	for _, obj := range informer.GetStore().List() {
		cm, ok := obj.(*v1.ConfigMap)
		if !ok {
			continue
		}
		for k, v := range cm.Data {
			merged[k] = v
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.labels = merged
	log.Debugf("ExternalLabels: ConfigMapProvider: rebuilt label cache from %d ConfigMap(s)", len(informer.GetStore().List()))
}
