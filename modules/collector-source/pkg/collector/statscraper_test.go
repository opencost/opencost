package collector

import (
	"testing"
	"time"

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
	start1, _ := time.Parse(time.RFC3339, start1Str)
	tests := map[string]struct {
		summaries []*stats.Summary
		expected  []UpdateArgs
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
			expected: []UpdateArgs{},
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
			expected: []UpdateArgs{},
		},
		"single node": {
			summaries: []*stats.Summary{
				{
					Node: stats.NodeStats{
						NodeName: "node1",
						CPU: &stats.CPUStats{
							Time:                 metav1.Time{Time: start1},
							UsageCoreNanoSeconds: ptr(uint64(2000000000)),
						},
						Fs: &stats.FsStats{
							Time:          metav1.Time{Time: start1},
							CapacityBytes: ptr(uint64(2 * GB)),
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
									RxBytes: ptr(uint64(1 * MB)),
									TxBytes: ptr(uint64(2 * MB)),
								},
							},
							VolumeStats: []stats.VolumeStats{
								{
									Name: "ignoreVol1",
									FsStats: stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: ptr(uint64(1 * GB)),
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
										UsedBytes: ptr(uint64(1 * GB)),
									},
								},
							},
							Containers: []stats.ContainerStats{
								{
									Name: "container1",
									CPU: &stats.CPUStats{
										Time:                 metav1.Time{Time: start1},
										UsageCoreNanoSeconds: ptr(uint64(1000000000)),
									},
									Memory: &stats.MemoryStats{
										Time:            metav1.Time{Time: start1},
										WorkingSetBytes: ptr(uint64(5 * MB)),
									},
									Rootfs: &stats.FsStats{
										Time:      metav1.Time{Time: start1},
										UsedBytes: ptr(uint64(1 * GB)),
									},
								},
							},
						},
					},
				},
			},
			expected: []UpdateArgs{
				{
					metricName: NodeCPUSecondsTotal,
					labels: map[string]string{
						"kubernetes_node": "node1",
						"mode":            "",
					},
					value:     2,
					timestamp: &start1,
				},
				{
					metricName: NodeFSCapacityBytes,
					labels: map[string]string{
						"instance": "node1",
						"device":   "local",
					},
					value:     float64(2 * GB),
					timestamp: &start1,
				},
				{
					metricName: ContainerNetworkReceiveBytesTotal,
					labels: map[string]string{
						"pod":       "uid1",
						"pod_name":  "pod1",
						"namespace": "namespace1",
					},
					value:     float64(1 * MB),
					timestamp: &start1,
				},
				{
					metricName: ContainerNetworkTransmitBytesTotal,
					labels: map[string]string{
						"pod":       "uid1",
						"pod_name":  "pod1",
						"namespace": "namespace1",
					},
					value:     float64(2 * MB),
					timestamp: &start1,
				},
				{
					metricName: KubeletVolumeStatsUsedBytes,
					labels: map[string]string{
						"persistentvolumeclaim": "pvc1",
						"namespace":             "namespace1",
					},
					value:     float64(1 * GB),
					timestamp: &start1,
				},
				{
					metricName: ContainerCPUUsageSecondsTotal,
					labels: map[string]string{
						"container": "container1",
						"uid":       "uid1",
						"pod":       "pod1",
						"namespace": "namespace1",
						"node":      "node1",
						"instance":  "node1",
					},
					value:     1,
					timestamp: &start1,
				},
				{
					metricName: ContainerMemoryWorkingSetBytes,
					labels: map[string]string{
						"container": "container1",
						"uid":       "uid1",
						"pod":       "pod1",
						"namespace": "namespace1",
						"node":      "node1",
						"instance":  "node1",
					},
					value:     float64(5 * MB),
					timestamp: &start1,
				},
				{
					metricName: ContainerFSUsageBytes,
					labels: map[string]string{
						"instance": "node1",
						"device":   "local",
					},
					value:     float64(1 * GB),
					timestamp: &start1,
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
										UsedBytes: ptr(uint64(1 * GB)),
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
										UsedBytes: ptr(uint64(1 * GB)),
									},
								},
							},
						},
					},
				},
			},
			expected: []UpdateArgs{
				{
					metricName: KubeletVolumeStatsUsedBytes,
					labels: map[string]string{
						"persistentvolumeclaim": "pvc1",
						"namespace":             "namespace1",
					},
					value:     float64(1 * GB),
					timestamp: &start1,
				},
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			s := &StatScraper{
				client:    &mockStatSummaryClient{results: tt.summaries},
				collector: &updateRecorder,
			}
			s.Scrape()

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}
