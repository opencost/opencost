package scrape

import (
	"github.com/opencost/opencost/core/pkg/clustercache"
	"k8s.io/apimachinery/pkg/types"
)

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
