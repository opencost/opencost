package externallabels

import (
	"context"
	"maps"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
)

// ConfigMapProvider maintains a key/value map of external labels sourced from a
// ConfigMap. It is intended to be wired up as a ConfigMapWatcher WatchFunc —
// the caller is responsible for registering it against the appropriate ConfigMap
// name via ConfigMapWatchers.AddWatcher.
type ConfigMapProvider struct {
	mu     sync.RWMutex
	cfg    Config
	labels map[string]string
}

// NewConfigMapProvider creates a ConfigMapProvider with an empty label cache.
func NewConfigMapProvider() *ConfigMapProvider {
	cfProvider := &ConfigMapProvider{
		labels: make(map[string]string),
	}

	return cfProvider
}

// Update replaces the cached labels with the full contents of the ConfigMap.
// Its signature matches watcher.ConfigMapWatcher.WatchFunc so it can be
// registered directly:
//
//	configWatchers.Add("my-external-labels", provider.Update)
func (cmp *ConfigMapProvider) Update(name string, data map[string]string) error {
	cmp.mu.Lock()
	defer cmp.mu.Unlock()
	cmp.labels = data
	log.Debugf("ExternalLabels: ConfigMapProvider: updated %d label(s) from ConfigMap %s", len(data), name)
	return nil
}

// Labels returns a copy of the currently cached external labels.
func (cmp *ConfigMapProvider) Labels(_ context.Context) (map[string]string, error) {
	cmp.mu.RLock()
	defer cmp.mu.RUnlock()
	return maps.Clone(cmp.labels), nil
}
