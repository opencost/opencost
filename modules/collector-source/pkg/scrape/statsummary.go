package scrape

import (
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
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

type StatSummaryScraper struct {
	client util.StatSummaryClient
}

func newStatSummaryScraper(client util.StatSummaryClient) Scraper {
	return &StatSummaryScraper{
		client: client,
	}
}

func (s *StatSummaryScraper) Scrape() []metric.Update {
	var scrapeResults []metric.Update
	nodeStats, err := s.client.GetNodeData()
	if err != nil {
		log.Errorf("error retrieving node stat data: %s", err.Error())
		return scrapeResults
	}

	// track if a pvc has already been seen when updating KubeletVolumeStatsUsedBytes
	seenPVC := map[stats.PVCReference]struct{}{}

	for _, stat := range nodeStats {
		nodeName := stat.Node.NodeName
		if stat.Node.CPU != nil && stat.Node.CPU.UsageCoreNanoSeconds != nil {
			scrapeResults = append(scrapeResults, metric.Update{
				Name: NodeCPUSecondsTotal,
				Labels: map[string]string{
					source.KubernetesNodeLabel: nodeName,
					source.ModeLabel:           "", // TODO
				},
				Value: float64(*stat.Node.CPU.UsageCoreNanoSeconds) * 1e-9,
			})
		}

		if stat.Node.Fs != nil && stat.Node.Fs.CapacityBytes != nil {
			scrapeResults = append(scrapeResults, metric.Update{
				Name: NodeFSCapacityBytes,
				Labels: map[string]string{
					source.InstanceLabel: nodeName,
					source.DeviceLabel:   "local", // This value has to be populated but isn't important here
				},
				Value: float64(*stat.Node.Fs.CapacityBytes),
			})
		}

		for _, pod := range stat.Pods {
			podName := pod.PodRef.Name
			namespace := pod.PodRef.Namespace
			podUID := pod.PodRef.UID

			if pod.Network != nil {
				if pod.Network.RxBytes != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: ContainerNetworkReceiveBytesTotal,
						Labels: map[string]string{
							source.UIDLabel:       podUID,
							source.PodLabel:       podName,
							source.NamespaceLabel: namespace,
						},
						Value: float64(*pod.Network.RxBytes),
					})
				}

				if pod.Network.TxBytes != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: ContainerNetworkTransmitBytesTotal,
						Labels: map[string]string{
							source.UIDLabel:       podUID,
							source.PodLabel:       podName,
							source.NamespaceLabel: namespace,
						},
						Value: float64(*pod.Network.TxBytes),
					})
				}
			}

			for _, volumeStats := range pod.VolumeStats {
				if volumeStats.PVCRef == nil || volumeStats.UsedBytes == nil {
					continue
				}
				if _, ok := seenPVC[*volumeStats.PVCRef]; ok {
					continue
				}
				scrapeResults = append(scrapeResults, metric.Update{
					Name: KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						source.PVCLabel:       volumeStats.PVCRef.Name,
						source.NamespaceLabel: volumeStats.PVCRef.Namespace,
					},
					Value: float64(*volumeStats.UsedBytes),
				})
				seenPVC[*volumeStats.PVCRef] = struct{}{}
			}

			for _, container := range pod.Containers {
				if container.CPU != nil && container.CPU.UsageCoreNanoSeconds != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: ContainerCPUUsageSecondsTotal,
						Labels: map[string]string{
							source.ContainerLabel: container.Name,
							source.PodLabel:       podName,
							source.NamespaceLabel: namespace,
							source.NodeLabel:      nodeName,
							source.InstanceLabel:  nodeName,
						},
						Value: float64(*container.CPU.UsageCoreNanoSeconds) * 1e-9,
					})
				}
				if container.Memory != nil && container.Memory.WorkingSetBytes != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: ContainerMemoryWorkingSetBytes,
						Labels: map[string]string{
							source.ContainerLabel: container.Name,
							source.PodLabel:       podName,
							source.NamespaceLabel: namespace,
							source.NodeLabel:      nodeName,
							source.InstanceLabel:  nodeName,
						},
						Value: float64(*container.Memory.WorkingSetBytes),
					})
				}

				if container.Rootfs != nil && container.Rootfs.UsedBytes != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: ContainerFSUsageBytes,
						Labels: map[string]string{
							source.InstanceLabel: nodeName,
							source.DeviceLabel:   "local",
						},
						Value: float64(*container.Rootfs.UsedBytes),
					})
				}
			}
		}
	}
	return scrapeResults
}
