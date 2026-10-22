package external

import (
	"maps"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
)

// NodeLabelProvider maintains a key/value map of external labels sourced from any
// watcher function. It is intended to be wired up to a WatchFunc such as ConfigMapWatcher
// the caller is responsible for registering it.
type NodeLabelProvider struct {
	mu     sync.RWMutex
	labels map[string]string
}

// NewNodeLabelProvider creates a NodeLabelProvider with an empty label cache.
func NewNodeLabelProvider() *NodeLabelProvider {
	return &NodeLabelProvider{
		labels: make(map[string]string),
	}
}

// Update replaces the cached labels with the full contents of any source of data.
func (nlp *NodeLabelProvider) Update(name string, data map[string]string) error {
	nlp.mu.Lock()
	defer nlp.mu.Unlock()
	// Clone to avoid retaining a reference to a map that may be mutated by the caller.
	nlp.labels = maps.Clone(data)
	log.Debugf("External: NodeLabelProvider: updated %d label(s) %s", len(data), name)
	return nil
}

// Labels returns a copy of the currently cached external labels.
func (nlp *NodeLabelProvider) Labels() (map[string]string, error) {
	nlp.mu.RLock()
	defer nlp.mu.RUnlock()
	return maps.Clone(nlp.labels), nil
}
