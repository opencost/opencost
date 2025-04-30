package scrape

import (
	"testing"
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type mockStatSummaryClient struct {
	results []*stats.Summary
}

func (m *mockStatSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	return m.results, nil
}

func TestStatScraper_Scrape(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	tests := map[string]struct {
		summaries []*stats.Summary
		expected  []metric.UpdateArgs
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
			expected: []metric.UpdateArgs{},
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
			expected: []metric.UpdateArgs{},
		},
		"single node": {
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
			expected: []metric.UpdateArgs{
				{
					MetricName: NodeCPUSecondsTotal,
					Labels: map[string]string{
						"kubernetes_node": "node1",
						"mode":            "",
					},
					Value:     2,
					Timestamp: &start1,
				},
				{
					MetricName: NodeFSCapacityBytes,
					Labels: map[string]string{
						"instance": "node1",
						"device":   "local",
					},
					Value:     float64(2 * util.GB),
					Timestamp: &start1,
				},
				{
					MetricName: ContainerNetworkReceiveBytesTotal,
					Labels: map[string]string{
						"pod":       "uid1",
						"pod_name":  "pod1",
						"namespace": "namespace1",
					},
					Value:     float64(1 * util.MB),
					Timestamp: &start1,
				},
				{
					MetricName: ContainerNetworkTransmitBytesTotal,
					Labels: map[string]string{
						"pod":       "uid1",
						"pod_name":  "pod1",
						"namespace": "namespace1",
					},
					Value:     float64(2 * util.MB),
					Timestamp: &start1,
				},
				{
					MetricName: KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						"persistentvolumeclaim": "pvc1",
						"namespace":             "namespace1",
					},
					Value:     float64(1 * util.GB),
					Timestamp: &start1,
				},
				{
					MetricName: ContainerCPUUsageSecondsTotal,
					Labels: map[string]string{
						"container": "container1",
						"uid":       "uid1",
						"pod":       "pod1",
						"namespace": "namespace1",
						"node":      "node1",
						"instance":  "node1",
					},
					Value:     1,
					Timestamp: &start1,
				},
				{
					MetricName: ContainerMemoryWorkingSetBytes,
					Labels: map[string]string{
						"container": "container1",
						"uid":       "uid1",
						"pod":       "pod1",
						"namespace": "namespace1",
						"node":      "node1",
						"instance":  "node1",
					},
					Value:     float64(5 * util.MB),
					Timestamp: &start1,
				},
				{
					MetricName: ContainerFSUsageBytes,
					Labels: map[string]string{
						"instance": "node1",
						"device":   "local",
					},
					Value:     float64(1 * util.GB),
					Timestamp: &start1,
				},
			},
		},
		"repeat pvc": {
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
			expected: []metric.UpdateArgs{
				{
					MetricName: KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						"persistentvolumeclaim": "pvc1",
						"namespace":             "namespace1",
					},
					Value:     float64(1 * util.GB),
					Timestamp: &start1,
				},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			s := &StatSummaryScraper{
				client:  &mockStatSummaryClient{results: tt.summaries},
				updater: &updateRecorder,
			}
			s.Scrape()

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}
