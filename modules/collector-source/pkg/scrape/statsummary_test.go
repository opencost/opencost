package scrape

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// mockClusterCache implements clustercache.ClusterCache for testing.
// Only GetAllNodes and GetAllPersistentVolumeClaims return meaningful data.
type mockClusterCache struct {
	nodes []*clustercache.Node
	pvcs  []*clustercache.PersistentVolumeClaim
}

func (m *mockClusterCache) Run()                                                    {}
func (m *mockClusterCache) Stop()                                                   {}
func (m *mockClusterCache) GetAllNamespaces() []*clustercache.Namespace             { return nil }
func (m *mockClusterCache) GetAllNodes() []*clustercache.Node                       { return m.nodes }
func (m *mockClusterCache) GetAllPods() []*clustercache.Pod                         { return nil }
func (m *mockClusterCache) GetAllServices() []*clustercache.Service                 { return nil }
func (m *mockClusterCache) GetAllDaemonSets() []*clustercache.DaemonSet             { return nil }
func (m *mockClusterCache) GetAllDeployments() []*clustercache.Deployment           { return nil }
func (m *mockClusterCache) GetAllStatefulSets() []*clustercache.StatefulSet         { return nil }
func (m *mockClusterCache) GetAllReplicaSets() []*clustercache.ReplicaSet           { return nil }
func (m *mockClusterCache) GetAllPersistentVolumes() []*clustercache.PersistentVolume {
	return nil
}
func (m *mockClusterCache) GetAllPersistentVolumeClaims() []*clustercache.PersistentVolumeClaim {
	return m.pvcs
}
func (m *mockClusterCache) GetAllStorageClasses() []*clustercache.StorageClass           { return nil }
func (m *mockClusterCache) GetAllJobs() []*clustercache.Job                              { return nil }
func (m *mockClusterCache) GetAllCronJobs() []*clustercache.CronJob                     { return nil }
func (m *mockClusterCache) GetAllPodDisruptionBudgets() []*clustercache.PodDisruptionBudget {
	return nil
}
func (m *mockClusterCache) GetAllReplicationControllers() []*clustercache.ReplicationController {
	return nil
}
func (m *mockClusterCache) GetAllResourceQuotas() []*clustercache.ResourceQuota { return nil }

type mockStatSummaryClient struct {
	results []*stats.Summary
	err     error
}

func (m *mockStatSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	return m.results, m.err
}

func TestStatScraper_Scrape(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)

	testCache := &mockClusterCache{
		nodes: []*clustercache.Node{
			{Name: "node1", UID: types.UID("node-uid-1")},
		},
		pvcs: []*clustercache.PersistentVolumeClaim{
			{Name: "pvc1", Namespace: "namespace1", UID: types.UID("pvc-uid-1")},
		},
	}

	tests := map[string]struct {
		summaries    []*stats.Summary
		err          error
		clusterCache clustercache.ClusterCache
		expected     []metric.Update
	}{
		"nil values": {
			summaries: []*stats.Summary{
				{
					Node: stats.NodeStats{
						NodeName: "node1",
						CPU: &stats.CPUStats{
							Time:                 metav1.Time{Time: start1},
							UsageCoreNanoSeconds: nil,
						},
						Fs: &stats.FsStats{
							Time:          metav1.Time{Time: start1},
							CapacityBytes: nil,
						},
					},
					Pods: []stats.PodStats{
						{
							PodRef: stats.PodReference{
								Name:      "pod1",
								Namespace: "namespace1",
								UID:       "uid1",
							},
							Network: &stats.NetworkStats{
								Time: metav1.Time{Time: start1},
								InterfaceStats: stats.InterfaceStats{
									RxBytes: nil,
									TxBytes: nil,
								},
							},
							VolumeStats: []stats.VolumeStats{
								{
									Name: "vol1",
									PVCRef: &stats.PVCReference{
										Namespace: "namespace1",
										Name:      "pvc1",
									},
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: nil,
									},
								},
							},
							Containers: []stats.ContainerStats{
								{
									Name: "container1",
									CPU: &stats.CPUStats{
										Time:                 metav1.Time{Time: start1},
										UsageCoreNanoSeconds: nil,
									},
									Memory: &stats.MemoryStats{
										Time:            metav1.Time{Time: start1},
										WorkingSetBytes: nil,
									},
									Rootfs: &stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: nil,
									},
								},
							},
						},
					},
				},
			},
			expected: []metric.Update{},
		},
		"nil structs": {
			summaries: []*stats.Summary{
				{
					Node: stats.NodeStats{
						NodeName: "node1",
						CPU:      nil,
						Fs:       nil,
					},
					Pods: []stats.PodStats{
						{
							PodRef: stats.PodReference{
								Name:      "pod1",
								Namespace: "namespace1",
								UID:       "uid1",
							},
							Network:     nil,
							VolumeStats: nil,
							Containers: []stats.ContainerStats{
								{
									Name:   "container1",
									CPU:    nil,
									Memory: nil,
									Rootfs: nil,
								},
							},
						},
					},
				},
			},
			expected: []metric.Update{},
		},
		"single node": {
			clusterCache: testCache,
			summaries: []*stats.Summary{
				{
					Node: stats.NodeStats{
						NodeName: "node1",
						CPU: &stats.CPUStats{
							Time:                 metav1.Time{Time: start1},
							UsageCoreNanoSeconds: util.Ptr(uint64(2000000000)),
						},
						Fs: &stats.FsStats{
							Time:          metav1.Time{Time: start1},
							CapacityBytes: util.Ptr(uint64(2 * util.GB)),
						},
					},
					Pods: []stats.PodStats{
						{
							PodRef: stats.PodReference{
								Name:      "pod1",
								Namespace: "namespace1",
								UID:       "uid1",
							},
							Network: &stats.NetworkStats{
								Time: metav1.Time{Time: start1},
								InterfaceStats: stats.InterfaceStats{
									RxBytes: util.Ptr(uint64(1 * util.MB)),
									TxBytes: util.Ptr(uint64(2 * util.MB)),
								},
							},
							VolumeStats: []stats.VolumeStats{
								{
									Name: "ignoreVol1",
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
								{
									Name: "vol1",
									PVCRef: &stats.PVCReference{
										Namespace: "namespace1",
										Name:      "pvc1",
									},
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
							},
							Containers: []stats.ContainerStats{
								{
									Name: "container1",
									CPU: &stats.CPUStats{
										Time:                 metav1.Time{Time: start1},
										UsageCoreNanoSeconds: util.Ptr(uint64(1000000000)),
									},
									Memory: &stats.MemoryStats{
										Time:            metav1.Time{Time: start1},
										WorkingSetBytes: util.Ptr(uint64(5 * util.MB)),
									},
									Rootfs: &stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
							},
						},
					},
				},
			},
			expected: []metric.Update{
				{
					Name: metric.NodeCPUSecondsTotal,
					Labels: map[string]string{
						source.KubernetesNodeLabel: "node1",
						source.UIDLabel:            "node-uid-1",
						source.ModeLabel:           "",
					},
					Value: 2,
				},
				{
					Name: metric.NodeFSCapacityBytes,
					Labels: map[string]string{
						source.InstanceLabel: "node1",
						source.UIDLabel:      "node-uid-1",
						source.DeviceLabel:   "local",
					},
					Value: float64(2 * util.GB),
				},
				{
					Name: metric.ContainerNetworkReceiveBytesTotal,
					Labels: map[string]string{
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
					},
					Value: float64(1 * util.MB),
				},
				{
					Name: metric.ContainerNetworkTransmitBytesTotal,
					Labels: map[string]string{
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
					},
					Value: float64(2 * util.MB),
				},
				{
					Name: metric.KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						source.PVCLabel:       "pvc1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PVCUIDLabel:    "pvc-uid-1",
					},
					Value: float64(1 * util.GB),
				},
				{
					Name: metric.ContainerCPUUsageSecondsTotal,
					Labels: map[string]string{
						source.ContainerLabel: "container1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
					},
					Value: 1,
				},
				{
					Name: metric.ContainerMemoryWorkingSetBytes,
					Labels: map[string]string{
						source.ContainerLabel: "container1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
					},
					Value: float64(5 * util.MB),
				},
				{
					Name: metric.ContainerFSUsageBytes,
					Labels: map[string]string{
						source.InstanceLabel:  "node1",
						source.DeviceLabel:    "local",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.ContainerLabel: "container1",
					},
					Value: float64(1 * util.GB),
				},
			},
		},
		"single node with error": {
			clusterCache: testCache,
			summaries: []*stats.Summary{
				{
					Node: stats.NodeStats{
						NodeName: "node1",
						CPU: &stats.CPUStats{
							Time:                 metav1.Time{Time: start1},
							UsageCoreNanoSeconds: util.Ptr(uint64(2000000000)),
						},
						Fs: &stats.FsStats{
							Time:          metav1.Time{Time: start1},
							CapacityBytes: util.Ptr(uint64(2 * util.GB)),
						},
					},
					Pods: []stats.PodStats{
						{
							PodRef: stats.PodReference{
								Name:      "pod1",
								Namespace: "namespace1",
								UID:       "uid1",
							},
							Network: &stats.NetworkStats{
								Time: metav1.Time{Time: start1},
								InterfaceStats: stats.InterfaceStats{
									RxBytes: util.Ptr(uint64(1 * util.MB)),
									TxBytes: util.Ptr(uint64(2 * util.MB)),
								},
							},
							VolumeStats: []stats.VolumeStats{
								{
									Name: "ignoreVol1",
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
								{
									Name: "vol1",
									PVCRef: &stats.PVCReference{
										Namespace: "namespace1",
										Name:      "pvc1",
									},
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
							},
							Containers: []stats.ContainerStats{
								{
									Name: "container1",
									CPU: &stats.CPUStats{
										Time:                 metav1.Time{Time: start1},
										UsageCoreNanoSeconds: util.Ptr(uint64(1000000000)),
									},
									Memory: &stats.MemoryStats{
										Time:            metav1.Time{Time: start1},
										WorkingSetBytes: util.Ptr(uint64(5 * util.MB)),
									},
									Rootfs: &stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
							},
						},
					},
				},
			},
			err: fmt.Errorf("failed to retrieve node-XYZ"),
			expected: []metric.Update{
				{
					Name: metric.NodeCPUSecondsTotal,
					Labels: map[string]string{
						source.KubernetesNodeLabel: "node1",
						source.UIDLabel:            "node-uid-1",
						source.ModeLabel:           "",
					},
					Value: 2,
				},
				{
					Name: metric.NodeFSCapacityBytes,
					Labels: map[string]string{
						source.InstanceLabel: "node1",
						source.UIDLabel:      "node-uid-1",
						source.DeviceLabel:   "local",
					},
					Value: float64(2 * util.GB),
				},
				{
					Name: metric.ContainerNetworkReceiveBytesTotal,
					Labels: map[string]string{
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
					},
					Value: float64(1 * util.MB),
				},
				{
					Name: metric.ContainerNetworkTransmitBytesTotal,
					Labels: map[string]string{
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
					},
					Value: float64(2 * util.MB),
				},
				{
					Name: metric.KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						source.PVCLabel:       "pvc1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PVCUIDLabel:    "pvc-uid-1",
					},
					Value: float64(1 * util.GB),
				},
				{
					Name: metric.ContainerCPUUsageSecondsTotal,
					Labels: map[string]string{
						source.ContainerLabel: "container1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
					},
					Value: 1,
				},
				{
					Name: metric.ContainerMemoryWorkingSetBytes,
					Labels: map[string]string{
						source.ContainerLabel: "container1",
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
					},
					Value: float64(5 * util.MB),
				},
				{
					Name: metric.ContainerFSUsageBytes,
					Labels: map[string]string{
						source.InstanceLabel:  "node1",
						source.DeviceLabel:    "local",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.ContainerLabel: "container1",
					},
					Value: float64(1 * util.GB),
				},
			},
		},
		"repeat pvc": {
			clusterCache: testCache,
			summaries: []*stats.Summary{
				{
					Node: stats.NodeStats{
						NodeName: "node1",
					},
					Pods: []stats.PodStats{
						{
							PodRef: stats.PodReference{
								Name:      "pod1",
								Namespace: "namespace1",
								UID:       "uid1",
							},
							VolumeStats: []stats.VolumeStats{
								{
									Name: "vol1",
									PVCRef: &stats.PVCReference{
										Namespace: "namespace1",
										Name:      "pvc1",
									},
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
							},
						},
						{
							PodRef: stats.PodReference{
								Name:      "pod2",
								Namespace: "namespace1",
								UID:       "uid1",
							},
							VolumeStats: []stats.VolumeStats{
								{
									Name: "vol1",
									PVCRef: &stats.PVCReference{
										Namespace: "namespace1",
										Name:      "pvc1",
									},
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: util.Ptr(uint64(1 * util.GB)),
									},
								},
							},
						},
					},
				},
			},
			expected: []metric.Update{
				{
					Name: metric.KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						source.PVCLabel:       "pvc1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uid1",
						source.NodeUIDLabel:   "node-uid-1",
						source.PVCUIDLabel:    "pvc-uid-1",
					},
					Value: float64(1 * util.GB),
				},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := &StatSummaryScraper{
				client:       &mockStatSummaryClient{results: tt.summaries, err: tt.err},
				clusterCache: tt.clusterCache,
			}
			scrapeResults := s.Scrape()

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
				}
			}
		})
	}
}
