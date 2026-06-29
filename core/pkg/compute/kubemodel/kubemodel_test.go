package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

const testClusterUID = "cluster-uid-1"

func newTestWindow() (time.Time, time.Time) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	return start, start.Add(time.Hour)
}

func newTestKubeModel(t *testing.T) (*KubeModel, *source.MockOpenCostDataSource) {
	t.Helper()
	ds := source.NewMockOpenCostDataSource()
	ds.ResolutionValue = 5 * time.Minute
	km, err := NewKubeModel(testClusterUID, ds)
	require.NoError(t, err)
	return km, ds
}

// seedCluster sets the minimum overrides needed for computeCluster to succeed.
func seedCluster(ds *source.MockOpenCostDataSource, start, end time.Time) {
	ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
		{UID: testClusterUID, Cluster: "my-cluster", Provider: "aws"},
	})
	ds.Querier.SetOverride(source.QueryClusterUptime, []*source.UptimeResult{
		{UID: testClusterUID, First: start, Last: end},
	})
}

// ---- NewKubeModel ----

func TestNewKubeModel_NilDataSource(t *testing.T) {
	_, err := NewKubeModel(testClusterUID, nil)
	require.Error(t, err)
}

// ---- computeCluster ----

func TestComputeCluster_HappyPath(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)
	require.NotNil(t, kms.Cluster)
	assert.Equal(t, testClusterUID, kms.Cluster.UID)
	assert.Equal(t, "my-cluster", kms.Cluster.Name)
}

func TestComputeCluster_UIDNotFound(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)

	ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
		{UID: "other-uid", Cluster: "other-cluster"},
	})

	_, err := km.ComputeKubeModelSet(start, end)
	require.Error(t, err)
	assert.Contains(t, err.Error(), testClusterUID)
}

func TestComputeCluster_NoUptime(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)

	ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
		{UID: testClusterUID, Cluster: "my-cluster"},
	})
	// QueryClusterUptime left at NoOp — cluster Start/End stay zero, fail window
	// validation inside RegisterCluster, so kms.Cluster is not set.

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)
	assert.Nil(t, kms.Cluster)
}

// ---- computeNodes ----

func TestComputeNodes_HappyPath(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryNodeInfo, []*source.NodeInfoResult{
		{UID: "node-1", Node: "node-a", ProviderID: "aws:///us-east-1a/i-abc"},
		{UID: "node-2", Node: "node-b", ProviderID: "aws:///us-east-1b/i-def"},
	})
	ds.Querier.SetOverride(source.QueryNodeUptime, []*source.UptimeResult{
		{UID: "node-1", First: start, Last: end},
		{UID: "node-2", First: start, Last: end},
	})
	ds.Querier.SetOverride(source.QueryNodeLabels, []*source.NodeLabelsResult{
		{UID: "node-1", Labels: map[string]string{"zone": "us-east-1a"}},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	assert.Len(t, kms.Nodes, 2)
	n1 := kms.Nodes["node-1"]
	require.NotNil(t, n1)
	assert.Equal(t, "node-a", n1.Name)
	assert.Equal(t, "aws:///us-east-1a/i-abc", n1.ProviderID)
	assert.Equal(t, map[string]string{"zone": "us-east-1a"}, n1.Labels)
}

func TestComputeNodes_ResourceCapacities(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryNodeInfo, []*source.NodeInfoResult{
		{UID: "node-1", Node: "node-a"},
	})
	ds.Querier.SetOverride(source.QueryNodeUptime, []*source.UptimeResult{
		{UID: "node-1", First: start, Last: end},
	})
	ds.Querier.SetOverride(source.QueryNodeResourceCapacities, []*source.ResourceResult{
		{UID: "node-1", Resource: "cpu", Unit: "cores", Value: 4.0},
		{UID: "node-1", Resource: "memory", Unit: "bytes", Value: 16 * 1024 * 1024 * 1024},
	})
	ds.Querier.SetOverride(source.QueryNodeResourcesAllocatable, []*source.ResourceResult{
		{UID: "node-1", Resource: "cpu", Unit: "cores", Value: 3.9},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	n := kms.Nodes["node-1"]
	require.NotNil(t, n)
	assert.NotEmpty(t, n.ResourceCapacities)
	assert.NotEmpty(t, n.ResourcesAllocatable)
}

func TestComputeNodes_LocalStorage(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryNodeInfo, []*source.NodeInfoResult{
		{UID: "node-1", Node: "node-a"},
	})
	ds.Querier.SetOverride(source.QueryNodeUptime, []*source.UptimeResult{
		{UID: "node-1", First: start, Last: end},
	})
	ds.Querier.SetOverride(source.QueryKMLocalStorageBytes, []*source.UIDValueResult{
		{UID: "node-1", Value: 500 * 1024 * 1024 * 1024},
	})
	ds.Querier.SetOverride(source.QueryKMLocalStorageUsedAvg, []*source.NodeUIDValueResult{
		{UID: "node-1", Value: 100 * 1024 * 1024 * 1024},
	})
	ds.Querier.SetOverride(source.QueryKMLocalStorageUsedMax, []*source.NodeUIDValueResult{
		{UID: "node-1", Value: 200 * 1024 * 1024 * 1024},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	n := kms.Nodes["node-1"]
	require.NotNil(t, n)
	assert.Equal(t, float64(500*1024*1024*1024), n.FileSystem.CapacityBytes)
	assert.Equal(t, float64(100*1024*1024*1024), n.FileSystem.UsageByteAvg)
	assert.Equal(t, float64(200*1024*1024*1024), n.FileSystem.UsageByteMax)
}

func TestComputeNodes_EmptyResults(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)
	assert.Empty(t, kms.Nodes)
}

// ---- computePods ----

func TestComputePods_HappyPath(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryPodInfo, []*source.PodInfoResult{
		{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1", NodeUID: "node-1"},
	})
	ds.Querier.SetOverride(source.QueryPodUptime, []*source.UptimeResult{
		{UID: "pod-1", First: start, Last: end},
	})
	ds.Querier.SetOverride(source.QueryPodLabels, []*source.PodLabelsResult{
		{UID: "pod-1", Labels: map[string]string{"app": "my-app"}},
	})
	ds.Querier.SetOverride(source.QueryPodAnnotations, []*source.PodAnnotationsResult{
		{UID: "pod-1", Annotations: map[string]string{"note": "val"}},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	pod := kms.Pods["pod-1"]
	require.NotNil(t, pod)
	assert.Equal(t, "my-pod", pod.Name)
	assert.Equal(t, "ns-1", pod.NamespaceUID)
	assert.Equal(t, "node-1", pod.NodeUID)
	assert.Equal(t, map[string]string{"app": "my-app"}, pod.Labels)
	assert.Equal(t, map[string]string{"note": "val"}, pod.Annotations)
}

func TestComputePods_Owners(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryPodInfo, []*source.PodInfoResult{
		{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
	})
	ds.Querier.SetOverride(source.QueryPodUptime, []*source.UptimeResult{
		{UID: "pod-1", First: start, Last: end},
	})
	ds.Querier.SetOverride(source.QueryPodOwners, []*source.OwnerResult{
		{UID: "pod-1", OwnerUID: "rs-1", OwnerKind: "ReplicaSet", Controller: true},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	pod := kms.Pods["pod-1"]
	require.NotNil(t, pod)
	require.Len(t, pod.Owners, 1)
	assert.Equal(t, "rs-1", pod.Owners[0].UID)
	assert.True(t, pod.Owners[0].Controller)
}

func TestComputePods_PVCVolumes(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryPodInfo, []*source.PodInfoResult{
		{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
	})
	ds.Querier.SetOverride(source.QueryPodUptime, []*source.UptimeResult{
		{UID: "pod-1", First: start, Last: end},
	})
	ds.Querier.SetOverride(source.QueryPodPVCVolumes, []*source.PodPVCVolumeResult{
		{UID: "pod-1", PVCUID: "pvc-1", PodVolumeName: "data"},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	pod := kms.Pods["pod-1"]
	require.NotNil(t, pod)
	require.Len(t, pod.PVCVolumes, 1)
	assert.Equal(t, "pvc-1", pod.PVCVolumes[0].PersistentVolumeClaimUID)
	assert.Equal(t, "data", pod.PVCVolumes[0].Name)
}

// ---- computeContainers ----

func TestComputeContainers_HappyPath(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryContainerUptime, []*source.ContainerUptimeResult{
		{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "app"},
		{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "sidecar"},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)
	assert.Len(t, kms.Containers, 2)
}

func TestComputeContainers_ResourceRequestsAndLimits(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ds.Querier.SetOverride(source.QueryContainerUptime, []*source.ContainerUptimeResult{
		{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "app"},
	})
	ds.Querier.SetOverride(source.QueryContainerResourceRequests, []*source.ContainerResourceResult{
		{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "cpu", Unit: "cores", Value: 0.5}, Container: "app"},
		{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "memory", Unit: "bytes", Value: 128 * 1024 * 1024}, Container: "app"},
	})
	ds.Querier.SetOverride(source.QueryContainerResourceLimits, []*source.ContainerResourceResult{
		{ResourceResult: source.ResourceResult{UID: "pod-1", Resource: "cpu", Unit: "cores", Value: 1.0}, Container: "app"},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	c := findContainer(kms, "pod-1", "app")
	require.NotNil(t, c)
	assert.NotEmpty(t, c.ResourceRequests)
	assert.NotEmpty(t, c.ResourceLimits)
}

func TestComputeContainers_CPUAndRAMUsage(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	ts := float64(start.Unix())
	ds.Querier.SetOverride(source.QueryContainerUptime, []*source.ContainerUptimeResult{
		{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "app"},
	})
	ds.Querier.SetOverride(source.QueryCPUCoresAllocated, []*source.CPUCoresAllocatedResult{
		{UID: "pod-1", Container: "app", Data: []*util.Vector{{Timestamp: ts, Value: 0.25}}},
	})
	ds.Querier.SetOverride(source.QueryCPUUsageAvg, []*source.CPUUsageAvgResult{
		{UID: "pod-1", Container: "app", Data: []*util.Vector{{Timestamp: ts, Value: 0.1}}},
	})
	ds.Querier.SetOverride(source.QueryCPUUsageMax, []*source.CPUUsageMaxResult{
		{UID: "pod-1", Container: "app", Data: []*util.Vector{{Timestamp: ts, Value: 0.2}}},
	})
	ds.Querier.SetOverride(source.QueryRAMBytesAllocated, []*source.RAMBytesAllocatedResult{
		{UID: "pod-1", Container: "app", Data: []*util.Vector{{Timestamp: ts, Value: 256 * 1024 * 1024}}},
	})
	ds.Querier.SetOverride(source.QueryRAMUsageAvg, []*source.RAMUsageAvgResult{
		{UID: "pod-1", Container: "app", Data: []*util.Vector{{Timestamp: ts, Value: 100 * 1024 * 1024}}},
	})
	ds.Querier.SetOverride(source.QueryRAMUsageMax, []*source.RAMUsageMaxResult{
		{UID: "pod-1", Container: "app", Data: []*util.Vector{{Timestamp: ts, Value: 200 * 1024 * 1024}}},
	})

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)

	c := findContainer(kms, "pod-1", "app")
	require.NotNil(t, c)
	assert.Equal(t, 0.25, c.CPUCoresAllocated)
	assert.Equal(t, 0.1, c.CPUCoreUsageAvg)
	assert.Equal(t, 0.2, c.CPUCoreUsageMax)
	assert.Equal(t, float64(256*1024*1024), c.RAMBytesAllocated)
	assert.Equal(t, float64(100*1024*1024), c.RAMBytesUsageAvg)
	assert.Equal(t, float64(200*1024*1024), c.RAMBytesUsageMax)
}

func TestComputeContainers_EmptyResults(t *testing.T) {
	start, end := newTestWindow()
	km, ds := newTestKubeModel(t)
	seedCluster(ds, start, end)

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)
	assert.Empty(t, kms.Containers)
}

func findContainer(kms *kubemodel.KubeModelSet, podUID, name string) *kubemodel.Container {
	for _, c := range kms.Containers {
		if c.PodUID == podUID && c.Name == name {
			return c
		}
	}
	return nil
}
