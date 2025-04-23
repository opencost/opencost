package collector

import (
	"github.com/opencost/opencost/core/pkg/log"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type StatSummaryClient interface {
	GetNodeData() ([]*stats.Summary, error)
}

type StatScraper struct {
	client    StatSummaryClient
	collector MetricsCollector
}

func NewStatScraper(client StatSummaryClient) *StatScraper {
	return &StatScraper{
		client: client,
	}
}

func (s *StatScraper) Scrape() {
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
			s.collector.Update(
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
			s.collector.Update(
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
					s.collector.Update(
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
					s.collector.Update(
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
				s.collector.Update(
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
					s.collector.Update(
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
					s.collector.Update(
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
					s.collector.Update(
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
