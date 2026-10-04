package scrape

import (
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"k8s.io/apimachinery/pkg/types"
)

// persistedIndexTTL is how long an index entry is retained after it was last seen in the cluster cache. It needs to
// outlast the window where dependent objects (e.g. pods on a deleted node) remain in the cache after their referent
// has been removed.
const persistedIndexTTL = time.Hour

type persistedIndexEntry struct {
	uid      types.UID
	lastSeen time.Time
}

// persistedIndex retains key to UID mappings across scrapes. Objects are not always removed from the cluster cache in
// dependency order, so an object can outlive the object it references by name (e.g. a pod whose node has already been
// deleted). Retaining recently seen entries prevents these lookups from resolving to an empty UID, which would
// overwrite the previously scraped value. A nil *persistedIndex performs no retention.
type persistedIndex[K comparable] struct {
	name    string
	lock    sync.Mutex
	entries map[K]persistedIndexEntry
}

func newPersistedIndex[K comparable](name string) *persistedIndex[K] {
	return &persistedIndex[K]{
		name:    name,
		entries: make(map[K]persistedIndexEntry),
	}
}

// update records the entries of current, which always take precedence over retained entries, evicts entries which
// have not been seen within the ttl, and returns a new index containing current along with any retained entries.
func (pi *persistedIndex[K]) update(current map[K]types.UID) map[K]types.UID {
	if pi == nil {
		return current
	}

	pi.lock.Lock()
	defer pi.lock.Unlock()

	now := time.Now()
	for key, uid := range current {
		pi.entries[key] = persistedIndexEntry{uid: uid, lastSeen: now}
	}

	result := make(map[K]types.UID, len(pi.entries))
	for key, entry := range pi.entries {
		if now.Sub(entry.lastSeen) > persistedIndexTTL {
			delete(pi.entries, key)
			continue
		}
		if _, ok := current[key]; !ok {
			log.Debugf("%s index: retaining UID '%s' for '%v' which is no longer in the cluster cache", pi.name, entry.uid, key)
		}
		result[key] = entry.uid
	}
	return result
}

// pvcKey is a composite key for a PersistentVolumeClaim (name + namespace).
type pvcKey struct {
	name      string
	namespace string
}

// buildNodeIndex returns a map from node name to UID.
func buildNodeIndex(nodes []*clustercache.Node) map[string]types.UID {
	m := make(map[string]types.UID, len(nodes))
	for _, node := range nodes {
		m[node.Name] = node.UID
	}
	return m
}

// buildNamespaceIndex returns a map from namespace name to UID.
func buildNamespaceIndex(namespaces []*clustercache.Namespace) map[string]types.UID {
	m := make(map[string]types.UID, len(namespaces))
	for _, ns := range namespaces {
		m[ns.Name] = ns.UID
	}
	return m
}

// buildPVCIndex returns a map from (name, namespace) to PVC UID.
func buildPVCIndex(pvcs []*clustercache.PersistentVolumeClaim) map[pvcKey]types.UID {
	m := make(map[pvcKey]types.UID, len(pvcs))
	for _, pvc := range pvcs {
		m[pvcKey{name: pvc.Name, namespace: pvc.Namespace}] = pvc.UID
	}
	return m
}

// buildPVIndex returns a map from PV name to UID.
func buildPVIndex(pvs []*clustercache.PersistentVolume) map[string]types.UID {
	m := make(map[string]types.UID, len(pvs))
	for _, pv := range pvs {
		m[pv.Name] = pv.UID
	}
	return m
}

// podKey is a composite key for a Pod (namespace + name).
type podKey struct {
	namespace string
	name      string
}

// buildPodIndex returns a map from (namespace, name) to Pod UID.
func buildPodIndex(pods []*clustercache.Pod) map[podKey]types.UID {
	m := make(map[podKey]types.UID, len(pods))
	for _, pod := range pods {
		m[podKey{namespace: pod.Namespace, name: pod.Name}] = pod.UID
	}
	return m
}
