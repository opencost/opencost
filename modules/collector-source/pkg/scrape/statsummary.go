package scrape

import (
	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/nodestats"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type StatSummaryScraper struct {
	client nodestats.StatSummaryClient
}

func newStatSummaryScraper(client nodestats.StatSummaryClient) Scraper {
	return &StatSummaryScraper{
		client: client,
	}
}

func (s *StatSummaryScraper) Scrape() []metric.Update {
	var scrapeResults []metric.Update
	nodeStats, err := s.client.GetNodeData()

	// record errors but process successfully retrieved nodes
	errs := make([]error, 0)
	if err != nil {
		if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
			errs = multiErr.Unwrap()
		} else {
			errs = []error{err}
		}

		log.Errorf("error retrieving node stat data: %s", err.Error())
	}

	// track if a pvc has already been seen when updating KubeletVolumeStatsUsedBytes
	seenPVC := map[stats.PVCReference]struct{}{}

	for _, stat := range nodeStats {
		nodeName := stat.Node.NodeName
		if stat.Node.CPU != nil && stat.Node.CPU.UsageCoreNanoSeconds != nil {
			scrapeResults = append(scrapeResults, metric.Update{
				Name: metric.NodeCPUSecondsTotal,
				Labels: map[string]string{
					source.KubernetesNodeLabel: nodeName,
					source.ModeLabel:           "", // TODO
				},
				Value: float64(*stat.Node.CPU.UsageCoreNanoSeconds) * 1e-9,
			})
		}

		if stat.Node.Fs != nil && stat.Node.Fs.CapacityBytes != nil {
			scrapeResults = append(scrapeResults, metric.Update{
				Name: metric.NodeFSCapacityBytes,
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
				networkLabels := map[string]string{
					source.UIDLabel:       podUID,
					source.PodLabel:       podName,
					source.NamespaceLabel: namespace,
				}
				// The network may contain a list of stats or itself be a single stat, if the list is not present
				// scrape the object itself
				if pod.Network.Interfaces != nil {
					for _, networkStat := range pod.Network.Interfaces {
						scrapeNetworkStats(&scrapeResults, networkLabels, networkStat)
					}
				} else {
					scrapeNetworkStats(&scrapeResults, networkLabels, pod.Network.InterfaceStats)
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
					Name: metric.KubeletVolumeStatsUsedBytes,
					Labels: map[string]string{
						source.PVCLabel:       volumeStats.PVCRef.Name,
						source.NamespaceLabel: volumeStats.PVCRef.Namespace,
						source.UIDLabel:       podUID,
					},
					Value: float64(*volumeStats.UsedBytes),
				})
				seenPVC[*volumeStats.PVCRef] = struct{}{}
			}

			for _, container := range pod.Containers {
				if container.CPU != nil && container.CPU.UsageCoreNanoSeconds != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: metric.ContainerCPUUsageSecondsTotal,
						Labels: map[string]string{
							source.ContainerLabel: container.Name,
							source.PodLabel:       podName,
							source.NamespaceLabel: namespace,
							source.NodeLabel:      nodeName,
							source.InstanceLabel:  nodeName,
							source.UIDLabel:       podUID,
						},
						Value: float64(*container.CPU.UsageCoreNanoSeconds) * 1e-9,
					})
				}
				if container.Memory != nil && container.Memory.WorkingSetBytes != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: metric.ContainerMemoryWorkingSetBytes,
						Labels: map[string]string{
							source.ContainerLabel: container.Name,
							source.PodLabel:       podName,
							source.NamespaceLabel: namespace,
							source.NodeLabel:      nodeName,
							source.InstanceLabel:  nodeName,
							source.UIDLabel:       podUID,
						},
						Value: float64(*container.Memory.WorkingSetBytes),
					})
				}

				if container.Rootfs != nil && container.Rootfs.UsedBytes != nil {
					scrapeResults = append(scrapeResults, metric.Update{
						Name: metric.ContainerFSUsageBytes,
						Labels: map[string]string{
							source.InstanceLabel: nodeName,
							source.DeviceLabel:   "local",
							source.UIDLabel:      podUID,
						},
						Value: float64(*container.Rootfs.UsedBytes),
					})
				}
			}
		}
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.NodeStatsScraperName,
		Targets:     len(nodeStats) + len(errs),
		Errors:      errs,
	})

	return scrapeResults
}

func scrapeNetworkStats(scrapeResults *[]metric.Update, labels map[string]string, networkStats stats.InterfaceStats) {
	// Skip stats for cni0 which tracks internal cluster traffic
	if networkStats.Name == "cni0" {
		return
	}
	if networkStats.RxBytes != nil {
		*scrapeResults = append(*scrapeResults, metric.Update{
			Name:   metric.ContainerNetworkReceiveBytesTotal,
			Labels: labels,
			Value:  float64(*networkStats.RxBytes),
		})
	}

	if networkStats.TxBytes != nil {
		*scrapeResults = append(*scrapeResults, metric.Update{
			Name:   metric.ContainerNetworkTransmitBytesTotal,
			Labels: labels,
			Value:  float64(*networkStats.TxBytes),
		})
	}
}
