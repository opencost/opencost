package scrape

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuildNodeIndex_Empty(t *testing.T) {
	m := buildNodeIndex(nil)
	require.Empty(t, m)
}

func TestBuildNodeIndex(t *testing.T) {
	nodes := []*clustercache.Node{
		{Name: "node-a", UID: "uid-a"},
		{Name: "node-b", UID: "uid-b"},
	}
	m := buildNodeIndex(nodes)
	require.Equal(t, types.UID("uid-a"), m["node-a"])
	require.Equal(t, types.UID("uid-b"), m["node-b"])
	require.Len(t, m, 2)
}

func TestBuildNamespaceIndex_Empty(t *testing.T) {
	m := buildNamespaceIndex(nil)
	require.Empty(t, m)
}

func TestBuildNamespaceIndex(t *testing.T) {
	namespaces := []*clustercache.Namespace{
		{Name: "default", UID: "uid-default"},
		{Name: "kube-system", UID: "uid-kube-system"},
	}
	m := buildNamespaceIndex(namespaces)
	require.Equal(t, types.UID("uid-default"), m["default"])
	require.Equal(t, types.UID("uid-kube-system"), m["kube-system"])
	require.Len(t, m, 2)
}

func TestBuildPVCIndex_Empty(t *testing.T) {
	m := buildPVCIndex(nil)
	require.Empty(t, m)
}

func TestBuildPVCIndex(t *testing.T) {
	pvcs := []*clustercache.PersistentVolumeClaim{
		{Name: "pvc-a", Namespace: "ns-1", UID: "uid-pvc-a"},
		{Name: "pvc-a", Namespace: "ns-2", UID: "uid-pvc-a-ns2"}, // same name, different namespace
		{Name: "pvc-b", Namespace: "ns-1", UID: "uid-pvc-b"},
	}
	m := buildPVCIndex(pvcs)
	require.Equal(t, types.UID("uid-pvc-a"), m[pvcKey{name: "pvc-a", namespace: "ns-1"}])
	require.Equal(t, types.UID("uid-pvc-a-ns2"), m[pvcKey{name: "pvc-a", namespace: "ns-2"}])
	require.Equal(t, types.UID("uid-pvc-b"), m[pvcKey{name: "pvc-b", namespace: "ns-1"}])
	require.Len(t, m, 3)
}

func TestBuildPVIndex_Empty(t *testing.T) {
	m := buildPVIndex(nil)
	require.Empty(t, m)
}

func TestBuildPVIndex(t *testing.T) {
	pvs := []*clustercache.PersistentVolume{
		{Name: "pv-a", UID: "uid-pv-a"},
		{Name: "pv-b", UID: "uid-pv-b"},
	}
	m := buildPVIndex(pvs)
	require.Equal(t, types.UID("uid-pv-a"), m["pv-a"])
	require.Equal(t, types.UID("uid-pv-b"), m["pv-b"])
	require.Len(t, m, 2)
}

// backdate moves the last seen time of an entry into the past.
func backdate[K comparable](pi *persistedIndex[K], key K, d time.Duration) {
	entry := pi.entries[key]
	entry.lastSeen = entry.lastSeen.Add(-d)
	pi.entries[key] = entry
}

func TestPersistedIndex_Nil(t *testing.T) {
	var pi *persistedIndex[string]
	current := map[string]types.UID{"node-a": "uid-a"}
	require.Equal(t, current, pi.update(current))
}

func TestPersistedIndex_RetainsMissingEntries(t *testing.T) {
	pi := newPersistedIndex[string]("test")

	m := pi.update(map[string]types.UID{"node-a": "uid-a", "node-b": "uid-b"})
	require.Equal(t, map[string]types.UID{"node-a": "uid-a", "node-b": "uid-b"}, m)

	backdate(pi, "node-b", persistedIndexTTL/2)
	m = pi.update(map[string]types.UID{"node-a": "uid-a"})
	require.Equal(t, map[string]types.UID{"node-a": "uid-a", "node-b": "uid-b"}, m)
}

func TestPersistedIndex_CurrentOverwritesRetained(t *testing.T) {
	pi := newPersistedIndex[string]("test")

	pi.update(map[string]types.UID{"node-a": "uid-a-old"})
	m := pi.update(map[string]types.UID{"node-a": "uid-a-new"})
	require.Equal(t, map[string]types.UID{"node-a": "uid-a-new"}, m)
}

func TestPersistedIndex_EvictsExpiredEntries(t *testing.T) {
	pi := newPersistedIndex[string]("test")

	pi.update(map[string]types.UID{"node-a": "uid-a", "node-b": "uid-b"})

	backdate(pi, "node-b", 2*persistedIndexTTL)
	m := pi.update(map[string]types.UID{"node-a": "uid-a"})
	require.Equal(t, map[string]types.UID{"node-a": "uid-a"}, m)
	require.Len(t, pi.entries, 1)
}

func TestPersistedIndex_ResultIsIndependent(t *testing.T) {
	pi := newPersistedIndex[pvcKey]("test")

	key := pvcKey{name: "pvc-a", namespace: "ns-1"}
	m := pi.update(map[pvcKey]types.UID{key: "uid-pvc-a"})
	m[key] = "modified"

	m = pi.update(nil)
	require.Equal(t, types.UID("uid-pvc-a"), m[key])
}
