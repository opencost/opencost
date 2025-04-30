package scrape

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// Stat Summary Metrics
const (
	NodeCPUSecondsTotal                = "node_cpu_seconds_total"
	NodeFSCapacityBytes                = "node_fs_capacity_bytes" // replaces container_fs_limit_bytes
	ContainerNetworkReceiveBytesTotal  = "container_network_receive_bytes_total"
	ContainerNetworkTransmitBytesTotal = "container_network_transmit_bytes_total"
	ContainerCPUUsageSecondsTotal      = "container_cpu_usage_seconds_total"
	ContainerMemoryWorkingSetBytes     = "container_memory_working_set_bytes"
	ContainerFSUsageBytes              = "container_fs_usage_bytes"
	KubeletVolumeStatsUsedBytes        = "kubelet_volume_stats_used_bytes"
)

type StatSummaryClient interface {
	GetNodeData() ([]*stats.Summary, error)
}

type StatSummaryScraper struct {
	client  StatSummaryClient
	updater metric.MetricUpdater
}

func NewStatSummaryScraper(client StatSummaryClient) *StatSummaryScraper {
	return &StatSummaryScraper{
		client: client,
	}
}

func (s *StatSummaryScraper) Scrape() {
	nodeStats, err := s.client.GetNodeData()
	if err != nil {
		log.Errorf("error retrieving node stat data: %s", err.Error())
		return
	}

	// track if a pvc has already been seen when updating KubeletVolumeStatsUsedBytes
	seenPVC := map[stats.PVCReference]struct{}{}

	for _, stat := range nodeStats {
		nodeName := stat.Node.NodeName
		if stat.Node.CPU != nil && stat.Node.CPU.UsageCoreNanoSeconds != nil {
			s.updater.Update(
				NodeCPUSecondsTotal,
				map[string]string{
					"kubernetes_node": nodeName,
					"mode":            "", //TODO
				},
				float64(*stat.Node.CPU.UsageCoreNanoSeconds)*1e-9,
				&stat.Node.CPU.Time.Time,
				nil,
			)
		}

		if stat.Node.Fs != nil && stat.Node.Fs.CapacityBytes != nil {
			s.updater.Update(
				NodeFSCapacityBytes,
				map[string]string{
					"instance": nodeName,
					"device":   "local", // This value has to be populated but isn't important here
				},
				float64(*stat.Node.Fs.CapacityBytes),
				&stat.Node.Fs.Time.Time,
				nil,
			)
		}

		for _, pod := range stat.Pods {
			podName := pod.PodRef.Name
			namespace := pod.PodRef.Namespace
			podUID := pod.PodRef.UID

			if pod.Network != nil {
				if pod.Network.RxBytes != nil {
					s.updater.Update(
						ContainerNetworkReceiveBytesTotal,
						map[string]string{
							"pod":       podUID,
							"pod_name":  podName,
							"namespace": namespace,
						},
						float64(*pod.Network.RxBytes),
						&pod.Network.Time.Time,
						nil,
					)
				}

				if pod.Network.TxBytes != nil {
					s.updater.Update(
						ContainerNetworkTransmitBytesTotal,
						map[string]string{
							"pod":       podUID,
							"pod_name":  podName,
							"namespace": namespace,
						},
						float64(*pod.Network.TxBytes),
						&pod.Network.Time.Time,
						nil,
					)
				}
			}

			for _, volumeStats := range pod.VolumeStats {
				if volumeStats.PVCRef == nil || volumeStats.UsedBytes == nil {
					continue
				}
				if _, ok := seenPVC[*volumeStats.PVCRef]; ok {
					continue
				}
				s.updater.Update(
					KubeletVolumeStatsUsedBytes,
					map[string]string{
						"persistentvolumeclaim": volumeStats.PVCRef.Name,
						"namespace":             volumeStats.PVCRef.Namespace,
					},
					float64(*volumeStats.UsedBytes),
					&volumeStats.Time.Time,
					nil,
				)
				seenPVC[*volumeStats.PVCRef] = struct{}{}
			}

			for _, container := range pod.Containers {
				if container.CPU != nil && container.CPU.UsageCoreNanoSeconds != nil {
					s.updater.Update(
						ContainerCPUUsageSecondsTotal,
						map[string]string{
							"container": container.Name,
							"uid":       podUID,
							"pod":       podName,
							"namespace": namespace,
							"node":      nodeName,
							"instance":  nodeName,
						},
						float64(*container.CPU.UsageCoreNanoSeconds)*1e-9,
						&container.CPU.Time.Time,
						nil,
					)
				}
				if container.Memory != nil && container.Memory.WorkingSetBytes != nil {
					s.updater.Update(
						ContainerMemoryWorkingSetBytes,
						map[string]string{
							"container": container.Name,
							"uid":       podUID,
							"pod":       podName,
							"namespace": namespace,
							"node":      nodeName,
							"instance":  nodeName,
						},
						float64(*container.Memory.WorkingSetBytes),
						&container.Memory.Time.Time,
						nil,
					)
				}

				if container.Rootfs != nil && container.Rootfs.UsedBytes != nil {
					s.updater.Update(
						ContainerFSUsageBytes,
						map[string]string{
							"instance": nodeName,
							"device":   "local",
						},
						float64(*container.Rootfs.UsedBytes),
						&container.Rootfs.Time.Time,
						nil,
					)
				}
			}
		}
	}

}
