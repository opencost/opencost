package scrape

import (
	"testing"

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