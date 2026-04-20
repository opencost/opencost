package scrape

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

const unmountedPVsContainer = "unmounted-pvs"

type ClusterCacheScraper struct {
	clusterCache clustercache.ClusterCache
}

func newClusterCacheScraper(clusterCache clustercache.ClusterCache) Scraper {
	return &ClusterCacheScraper{
		clusterCache: clusterCache,
	}
}

func (ccs *ClusterCacheScraper) Scrape() []metric.Update {
	scrapeFuncs := []ScrapeFunc{
		ccs.ScrapeNodes,
		ccs.ScrapeDeployments,
		ccs.ScrapeNamespaces,
		ccs.ScrapePods,
		ccs.ScrapePVCs,
		ccs.ScrapePVs,
		ccs.ScrapeServices,
		ccs.ScrapeStatefulSets,
		ccs.ScrapeReplicaSets,
		ccs.ScrapeResourceQuotas,
	}
	return concurrentScrape(scrapeFuncs...)
}

func (ccs *ClusterCacheScraper) ScrapeNodes() []metric.Update {
	nodes := ccs.clusterCache.GetAllNodes()
	return ccs.scrapeNodes(nodes)
}

func (ccs *ClusterCacheScraper) scrapeNodes(nodes []*clustercache.Node) []metric.Update {
	var scrapeResults []metric.Update

	for _, node := range nodes {
		nodeInfo := map[string]string{
			source.NodeLabel:       node.Name,
			source.ProviderIDLabel: node.SpecProviderID,
			source.UIDLabel:        string(node.UID),
		}

		// Node Capacity
		if node.Status.Capacity != nil {
			if quantity, ok := node.Status.Capacity[v1.ResourceCPU]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceCPU, quantity)
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.KubeNodeStatusCapacityCPUCores,
					Labels: nodeInfo,
					Value:  value,
				})
			}

			if quantity, ok := node.Status.Capacity[v1.ResourceMemory]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceMemory, quantity)
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.KubeNodeStatusCapacityMemoryBytes,
					Labels: nodeInfo,
					Value:  value,
				})
			}
		}

		// Node Allocatable Resources
		if node.Status.Allocatable != nil {
			if quantity, ok := node.Status.Allocatable[v1.ResourceCPU]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceCPU, quantity)
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.KubeNodeStatusAllocatableCPUCores,
					Labels: nodeInfo,
					Value:  value,
				})
			}

			if quantity, ok := node.Status.Allocatable[v1.ResourceMemory]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceMemory, quantity)
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.KubeNodeStatusAllocatableMemoryBytes,
					Labels: nodeInfo,
					Value:  value,
				})
			}
		}

		// node labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(node.Labels)
		nodeLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubeNodeLabels,
			Labels:         nodeInfo,
			Value:          0,
			AdditionalInfo: nodeLabels,
		})

	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.NodeScraperType,
		Targets:     len(nodes),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapeDeployments() []metric.Update {
	deployments := ccs.clusterCache.GetAllDeployments()
	return ccs.scrapeDeployments(deployments)
}

func (ccs *ClusterCacheScraper) scrapeDeployments(deployments []*clustercache.Deployment) []metric.Update {
	var scrapeResults []metric.Update
	for _, deployment := range deployments {
		deploymentInfo := map[string]string{
			source.DeploymentLabel: deployment.Name,
			source.NamespaceLabel:  deployment.Namespace,
			source.UIDLabel:        string(deployment.UID),
		}

		// deployment labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(deployment.MatchLabels)
		deploymentLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DeploymentMatchLabels,
			Labels:         deploymentInfo,
			Value:          0,
			AdditionalInfo: deploymentLabels,
		})
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.DeploymentScraperType,
		Targets:     len(deployments),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapeNamespaces() []metric.Update {
	namespaces := ccs.clusterCache.GetAllNamespaces()
	return ccs.scrapeNamespaces(namespaces)
}

func (ccs *ClusterCacheScraper) scrapeNamespaces(namespaces []*clustercache.Namespace) []metric.Update {
	var scrapeResults []metric.Update
	for _, namespace := range namespaces {
		namespaceInfo := map[string]string{
			source.NamespaceLabel: namespace.Name,
			source.UIDLabel:       string(namespace.UID),
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.NamespaceInfo,
			Labels:         namespaceInfo,
			AdditionalInfo: namespaceInfo,
			Value:          0,
		})

		// namespace labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(namespace.Labels)
		namespaceLabels := util.ToMap(labelNames, labelValues)
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubeNamespaceLabels,
			Labels:         namespaceInfo,
			Value:          0,
			AdditionalInfo: namespaceLabels,
		})

		// namespace annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(namespace.Annotations)
		namespaceAnnotations := util.ToMap(annotationNames, annotationValues)
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubeNamespaceAnnotations,
			Labels:         namespaceInfo,
			Value:          0,
			AdditionalInfo: namespaceAnnotations,
		})
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.NamespaceScraperType,
		Targets:     len(namespaces),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapePods() []metric.Update {
	pods := ccs.clusterCache.GetAllPods()
	pvcs := ccs.clusterCache.GetAllPersistentVolumeClaims()

	return ccs.scrapePods(pods, pvcs)
}

func (ccs *ClusterCacheScraper) scrapePods(pods []*clustercache.Pod, pvcs []*clustercache.PersistentVolumeClaim) []metric.Update {
	// this is only populated if we find gpu resources being requested
	var nodesGpuInfo map[string]*NodeGpuInfo

	// pv allocation and unmounted pvs
	pvcInfo := getPvcsInfo(pvcs)

	// pod info by uid
	podInfoByUid := make(map[string]map[string]string)

	var scrapeResults []metric.Update
	for _, pod := range pods {
		podInfo := map[string]string{
			source.PodLabel:       pod.Name,
			source.NamespaceLabel: pod.Namespace,
			source.UIDLabel:       string(pod.UID),
			source.NodeLabel:      pod.Spec.NodeName,
			source.InstanceLabel:  pod.Spec.NodeName,
		}

		podInfoByUid[string(pod.UID)] = podInfo

		// pod labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(pod.Labels)
		podLabels := util.ToMap(labelNames, labelValues)
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubePodLabels,
			Labels:         podInfo,
			Value:          0,
			AdditionalInfo: podLabels,
		})

		// pod annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(pod.Annotations)
		podAnnotations := util.ToMap(annotationNames, annotationValues)
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubePodAnnotations,
			Labels:         podInfo,
			Value:          0,
			AdditionalInfo: podAnnotations,
		})

		// Determine PVC use data for Pod
		claimed := make(map[string]struct{})
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				name := volume.PersistentVolumeClaim.ClaimName
				key := pod.Namespace + "," + name
				if _, seen := claimed[key]; seen {
					continue
				}

				if pvc, ok := pvcInfo[key]; ok {
					pvc.PodsClaimed = append(pvc.PodsClaimed, string(pod.UID))
					claimed[key] = struct{}{}
				}
			}
		}

		// Pod owner metric
		for _, owner := range pod.OwnerReferences {
			ownerInfo := maps.Clone(podInfo)
			ownerInfo[source.OwnerKindLabel] = owner.Kind
			ownerInfo[source.OwnerNameLabel] = owner.Name
			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.KubePodOwner,
				Labels: ownerInfo,
				Value:  0,
			})
		}

		// Container Status
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Running != nil {
				containerInfo := maps.Clone(podInfo)
				containerInfo[source.ContainerLabel] = status.Name
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.KubePodContainerStatusRunning,
					Labels: containerInfo,
					Value:  0,
				})
			}
		}

		for _, container := range pod.Spec.Containers {
			// gpu "requests" is either the request or limit if it exists
			var gpuRequest *float64

			containerInfo := maps.Clone(podInfo)
			containerInfo[source.ContainerLabel] = container.Name
			// Requests
			if container.Resources.Requests != nil {
				// sorting keys here for testing purposes
				keys := maps.Keys(container.Resources.Requests)
				slices.Sort(keys)
				for _, resourceName := range keys {
					quantity := container.Resources.Requests[resourceName]
					resource, unit, value := toResourceUnitValue(resourceName, quantity)

					// failed to parse the resource type
					if resource == "" {
						log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
						continue
					}

					resourceRequestInfo := maps.Clone(containerInfo)
					resourceRequestInfo[source.ResourceLabel] = resource
					resourceRequestInfo[source.UnitLabel] = unit
					scrapeResults = append(scrapeResults, metric.Update{
						Name:   metric.KubePodContainerResourceRequests,
						Labels: resourceRequestInfo,
						Value:  value,
					})

					// set gpu request if it exists
					if isGpuResourceName(resourceName) {
						gpuRequestValue := value
						gpuRequest = &gpuRequestValue
					}
				}
			}

			// Limits
			if container.Resources.Limits != nil {
				// sorting keys here for testing purposes
				keys := maps.Keys(container.Resources.Limits)
				slices.Sort(keys)
				for _, resourceName := range keys {
					quantity := container.Resources.Limits[resourceName]
					resource, unit, value := toResourceUnitValue(resourceName, quantity)

					// failed to parse the resource type
					if resource == "" {
						log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
						continue
					}

					resourceLimitInfo := maps.Clone(containerInfo)
					resourceLimitInfo[source.ResourceLabel] = resource
					resourceLimitInfo[source.UnitLabel] = unit
					scrapeResults = append(scrapeResults, metric.Update{
						Name:   metric.KubePodContainerResourceLimits,
						Labels: resourceLimitInfo,
						Value:  value,
					})

					// if we didn't set a gpuRequest previously and the limit is a gpu resource,
					// set it to the limit
					if gpuRequest == nil && isGpuResourceName(resourceName) {
						gpuRequestValue := value
						gpuRequest = &gpuRequestValue
					}
				}
			}

			// handle the GPU allocation metric here IFF there exists a request/limit for GPUs
			// we only load the node gpu data map if we run into a container with gpu requests/limits
			if gpuRequest != nil {
				if nodesGpuInfo == nil {
					nodesGpuInfo = ccs.getNodesGpuInfo()
				}

				gpuAlloc := *gpuRequest
				if nodeGpuInfo, ok := nodesGpuInfo[pod.Spec.NodeName]; ok {
					if nodeGpuInfo != nil && nodeGpuInfo.VGPU != 0 {
						gpuAlloc = gpuAlloc * (nodeGpuInfo.GPU / nodeGpuInfo.VGPU)
					}
				}

				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.ContainerGPUAllocation,
					Labels: maps.Clone(containerInfo),
					Value:  gpuAlloc,
				})
			}
		}
	}

	// Iterate through PVC Info after the pods have been tallied and export
	// allocation metrics based on the number of other pods claiming the volume
	for _, pvc := range pvcInfo {
		// unmounted pvs get full allocation
		if len(pvc.PodsClaimed) == 0 {
			labels := map[string]string{
				source.PodLabel:       unmountedPVsContainer,
				source.NamespaceLabel: pvc.Namespace,
				source.UIDLabel:       "",
				source.NodeLabel:      "",
				source.InstanceLabel:  "",
				source.PVCLabel:       pvc.Claim,
				source.PVLabel:        pvc.VolumeName,
			}

			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.PodPVCAllocation,
				Labels: labels,
				Value:  pvc.Requests,
			})

			continue
		}

		// pods get a proportion of pv allocation
		value := pvc.Requests / float64(len(pvc.PodsClaimed))

		for _, podUid := range pvc.PodsClaimed {
			podInfo, ok := podInfoByUid[podUid]
			if !ok {
				continue
			}

			pvcLabels := maps.Clone(podInfo)
			pvcLabels[source.PVCLabel] = pvc.Claim
			pvcLabels[source.PVLabel] = pvc.VolumeName

			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.PodPVCAllocation,
				Labels: pvcLabels,
				Value:  value,
			})
		}
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.PodScraperType,
		Targets:     len(pods),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapePVCs() []metric.Update {
	pvcs := ccs.clusterCache.GetAllPersistentVolumeClaims()
	return ccs.scrapePVCs(pvcs)
}

func (ccs *ClusterCacheScraper) scrapePVCs(pvcs []*clustercache.PersistentVolumeClaim) []metric.Update {
	var scrapeResults []metric.Update
	for _, pvc := range pvcs {
		pvcInfo := map[string]string{
			source.PVCLabel:          pvc.Name,
			source.NamespaceLabel:    pvc.Namespace,
			source.UIDLabel:          string(pvc.UID),
			source.VolumeNameLabel:   pvc.Spec.VolumeName,
			source.StorageClassLabel: getPersistentVolumeClaimClass(pvc),
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:   metric.KubePersistentVolumeClaimInfo,
			Labels: pvcInfo,
			Value:  0,
		})

		if storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.KubePersistentVolumeClaimResourceRequestsStorageBytes,
				Labels: pvcInfo,
				Value:  float64(storage.Value()),
			})
		}
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.PvcScraperType,
		Targets:     len(pvcs),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapePVs() []metric.Update {
	pvs := ccs.clusterCache.GetAllPersistentVolumes()
	return ccs.scrapePVs(pvs)
}

func (ccs *ClusterCacheScraper) scrapePVs(pvs []*clustercache.PersistentVolume) []metric.Update {
	var scrapeResults []metric.Update
	for _, pv := range pvs {
		providerID := pv.Name
		// if a more accurate provider ID is available, use that
		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
			providerID = pv.Spec.CSI.VolumeHandle
		}
		pvInfo := map[string]string{
			source.PVLabel:           pv.Name,
			source.UIDLabel:          string(pv.UID),
			source.StorageClassLabel: pv.Spec.StorageClassName,
			source.ProviderIDLabel:   providerID,
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:   metric.KubecostPVInfo,
			Labels: pvInfo,
			Value:  0,
		})

		if storage, ok := pv.Spec.Capacity[v1.ResourceStorage]; ok {
			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.KubePersistentVolumeCapacityBytes,
				Labels: pvInfo,
				Value:  float64(storage.Value()),
			})
		}
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.PvScraperType,
		Targets:     len(pvs),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapeServices() []metric.Update {
	services := ccs.clusterCache.GetAllServices()
	return ccs.scrapeServices(services)
}

func (ccs *ClusterCacheScraper) scrapeServices(services []*clustercache.Service) []metric.Update {
	var scrapeResults []metric.Update
	for _, service := range services {
		serviceInfo := map[string]string{
			source.ServiceLabel:   service.Name,
			source.NamespaceLabel: service.Namespace,
			source.UIDLabel:       string(service.UID),
		}

		// service labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(service.SpecSelector)
		serviceLabels := util.ToMap(labelNames, labelValues)
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.ServiceSelectorLabels,
			Labels:         serviceInfo,
			Value:          0,
			AdditionalInfo: serviceLabels,
		})

	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.ServiceScraperType,
		Targets:     len(services),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapeStatefulSets() []metric.Update {
	statefulSets := ccs.clusterCache.GetAllStatefulSets()
	return ccs.scrapeStatefulSets(statefulSets)
}

func (ccs *ClusterCacheScraper) scrapeStatefulSets(statefulSets []*clustercache.StatefulSet) []metric.Update {
	var scrapeResults []metric.Update
	for _, statefulSet := range statefulSets {
		statefulSetInfo := map[string]string{
			source.StatefulSetLabel: statefulSet.Name,
			source.NamespaceLabel:   statefulSet.Namespace,
			source.UIDLabel:         string(statefulSet.UID),
		}

		// statefulSet labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(statefulSet.SpecSelector.MatchLabels)
		statefulSetLabels := util.ToMap(labelNames, labelValues)
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.StatefulSetMatchLabels,
			Labels:         statefulSetInfo,
			Value:          0,
			AdditionalInfo: statefulSetLabels,
		})
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.StatefulSetScraperType,
		Targets:     len(statefulSets),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapeReplicaSets() []metric.Update {
	replicaSets := ccs.clusterCache.GetAllReplicaSets()
	return ccs.scrapeReplicaSets(replicaSets)
}

func (ccs *ClusterCacheScraper) scrapeReplicaSets(replicaSets []*clustercache.ReplicaSet) []metric.Update {
	var scrapeResults []metric.Update
	for _, replicaSet := range replicaSets {
		replicaSetInfo := map[string]string{
			source.ReplicaSetLabel: replicaSet.Name,
			source.NamespaceLabel:  replicaSet.Namespace,
			source.UIDLabel:        string(replicaSet.UID),
		}

		// this specific metric exports a special <none> value for name and kind
		// if there are no owners
		if len(replicaSet.OwnerReferences) == 0 {
			ownerInfo := maps.Clone(replicaSetInfo)
			ownerInfo[source.OwnerKindLabel] = source.NoneLabelValue
			ownerInfo[source.OwnerNameLabel] = source.NoneLabelValue
			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.KubeReplicasetOwner,
				Labels: ownerInfo,
				Value:  0,
			})
		} else {
			for _, owner := range replicaSet.OwnerReferences {
				ownerInfo := maps.Clone(replicaSetInfo)
				ownerInfo[source.OwnerKindLabel] = owner.Kind
				ownerInfo[source.OwnerNameLabel] = owner.Name
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.KubeReplicasetOwner,
					Labels: ownerInfo,
					Value:  0,
				})
			}
		}
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.ReplicaSetScraperType,
		Targets:     len(replicaSets),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) ScrapeResourceQuotas() []metric.Update {
	resourceQuotas := ccs.clusterCache.GetAllResourceQuotas()
	return ccs.scrapeResourceQuotas(resourceQuotas)
}

func (ccs *ClusterCacheScraper) scrapeResourceQuotas(resourceQuotas []*clustercache.ResourceQuota) []metric.Update {
	var scrapeResults []metric.Update

	processResource := func(baseLabels map[string]string, name v1.ResourceName, quantity resource.Quantity, metricName string) metric.Update {
		resource, unit, value := toResourceUnitValue(name, quantity)

		labels := maps.Clone(baseLabels)
		labels[source.ResourceLabel] = resource
		labels[source.UnitLabel] = unit

		return metric.Update{
			Name:   metricName,
			Labels: labels,
			Value:  value,
		}
	}

	for _, resourceQuota := range resourceQuotas {
		resourceQuotaInfo := map[string]string{
			source.ResourceQuotaLabel: resourceQuota.Name,
			source.NamespaceLabel:     resourceQuota.Namespace,
			source.UIDLabel:           string(resourceQuota.UID),
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.ResourceQuotaInfo,
			Labels:         resourceQuotaInfo,
			AdditionalInfo: resourceQuotaInfo,
			Value:          0,
		})

		if resourceQuota.Spec.Hard != nil {
			// CPU/memory requests can also be aliased as "cpu" and "memory". For now, however, only scrape the complete names
			// https://kubernetes.io/docs/concepts/policy/resource-quotas/#compute-resource-quota

			if quantity, ok := resourceQuota.Spec.Hard[v1.ResourceRequestsCPU]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceCPU, quantity, metric.KubeResourceQuotaSpecResourceRequests))
			}

			if quantity, ok := resourceQuota.Spec.Hard[v1.ResourceRequestsMemory]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceMemory, quantity, metric.KubeResourceQuotaSpecResourceRequests))
			}

			if quantity, ok := resourceQuota.Spec.Hard[v1.ResourceLimitsCPU]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceCPU, quantity, metric.KubeResourceQuotaSpecResourceLimits))
			}

			if quantity, ok := resourceQuota.Spec.Hard[v1.ResourceLimitsMemory]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceMemory, quantity, metric.KubeResourceQuotaSpecResourceLimits))
			}
		}

		if resourceQuota.Status.Used != nil {
			if quantity, ok := resourceQuota.Status.Used[v1.ResourceRequestsCPU]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceCPU, quantity, metric.KubeResourceQuotaStatusUsedResourceRequests))
			}

			if quantity, ok := resourceQuota.Status.Used[v1.ResourceRequestsMemory]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceMemory, quantity, metric.KubeResourceQuotaStatusUsedResourceRequests))
			}

			if quantity, ok := resourceQuota.Status.Used[v1.ResourceLimitsCPU]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceCPU, quantity, metric.KubeResourceQuotaStatusUsedResourceLimits))
			}

			if quantity, ok := resourceQuota.Status.Used[v1.ResourceLimitsMemory]; ok {
				scrapeResults = append(scrapeResults, processResource(resourceQuotaInfo, v1.ResourceMemory, quantity, metric.KubeResourceQuotaStatusUsedResourceLimits))
			}
		}
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.ResourceQuotaScraperType,
		Targets:     len(resourceQuotas),
		Errors:      nil,
	})

	return scrapeResults
}

// PvcInfo is used to store information about a pvc for tracking volume usage.
type PvcInfo struct {
	Class       string
	Claim       string
	Namespace   string
	VolumeName  string
	Requests    float64
	PodsClaimed []string
}

func getPvcsInfo(pvcs []*clustercache.PersistentVolumeClaim) map[string]*PvcInfo {
	toReturn := make(map[string]*PvcInfo)

	for _, pvc := range pvcs {
		ns := pvc.Namespace
		pvcName := pvc.Name
		volumeName := pvc.Spec.VolumeName
		pvClass := getPersistentVolumeClaimClass(pvc)
		requests := float64(pvc.Spec.Resources.Requests.Storage().Value())

		key := ns + "," + pvcName
		toReturn[key] = &PvcInfo{
			Class:      pvClass,
			Claim:      pvcName,
			Namespace:  ns,
			VolumeName: volumeName,
			Requests:   requests,
		}
	}

	return toReturn
}

// NodeGpuInfo contains the gpu count and vgpu counts for nodes
type NodeGpuInfo struct {
	GPU  float64
	VGPU float64
}

func (ccs *ClusterCacheScraper) getNodesGpuInfo() map[string]*NodeGpuInfo {
	// use a closure to cache allocatableVGPU result instead of calculating
	// it every time we need it
	var allocatableVGPUs *float64
	allocVGPUs := func() (float64, error) {
		if allocatableVGPUs != nil {
			return *allocatableVGPUs, nil
		}

		vgpu, err := getAllocatableVGPUs(ccs.clusterCache.GetAllDaemonSets())
		if err != nil {
			return vgpu, err
		}
		allocatableVGPUs = &vgpu
		return *allocatableVGPUs, nil
	}

	var nodeGpuMap map[string]*NodeGpuInfo = make(map[string]*NodeGpuInfo)
	for _, node := range ccs.clusterCache.GetAllNodes() {
		info, err := gpuInfoFor(node, allocVGPUs)
		if err != nil {
			log.Warnf("Failed to retrieve GPU Info for Node: %s - %s", node.Name, err)
			continue
		}
		nodeGpuMap[node.Name] = info
	}

	return nodeGpuMap
}

// getPersistentVolumeClaimClass returns StorageClassName. If no storage class was
// requested, it returns "".
func getPersistentVolumeClaimClass(claim *clustercache.PersistentVolumeClaim) string {
	// Use beta annotation first
	if class, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	// Special non-empty string to indicate absence of storage class.
	return ""
}

// toResourceUnitValue accepts a resource name and quantity and returns the sanitized resource, the unit, and the value in the units.
// Returns an empty string for resource and unit if there was a failure.
func toResourceUnitValue(resourceName v1.ResourceName, quantity resource.Quantity) (resource string, unit string, value float64) {
	resource = promutil.SanitizeLabelName(string(resourceName))

	switch resourceName {
	case v1.ResourceCPU:
		unit = "core"
		value = float64(quantity.MilliValue()) / 1000
		return

	case v1.ResourceStorage:
		fallthrough
	case v1.ResourceEphemeralStorage:
		fallthrough
	case v1.ResourceMemory:
		unit = "byte"
		value = float64(quantity.Value())
		return
	case v1.ResourcePods:
		unit = "integer"
		value = float64(quantity.Value())
		return
	default:
		if isHugePageResourceName(resourceName) || isAttachableVolumeResourceName(resourceName) {
			unit = "byte"
			value = float64(quantity.Value())
			return
		}

		if isExtendedResourceName(resourceName) {
			unit = "integer"
			value = float64(quantity.Value())
			return
		}
	}

	resource = ""
	unit = ""
	value = 0.0
	return
}

func isGpuResourceName(name v1.ResourceName) bool {
	return name == "nvidia.com/gpu" || name == "k8s.amazonaws.com/vgpu"
}

// isHugePageResourceName checks for a huge page container resource name
func isHugePageResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceHugePagesPrefix)
}

// isAttachableVolumeResourceName checks for attached volume container resource name
func isAttachableVolumeResourceName(name v1.ResourceName) bool {
	return strings.HasPrefix(string(name), v1.ResourceAttachableVolumesPrefix)
}

// isExtendedResourceName checks for extended container resource name
func isExtendedResourceName(name v1.ResourceName) bool {
	if isNativeResource(name) || strings.HasPrefix(string(name), v1.DefaultResourceRequestsPrefix) {
		return false
	}
	// Ensure it satisfies the rules in IsQualifiedName() after converted into quota resource name
	nameForQuota := fmt.Sprintf("%s%s", v1.DefaultResourceRequestsPrefix, string(name))
	if errs := validation.IsQualifiedName(nameForQuota); len(errs) != 0 {
		return false
	}
	return true
}

// isNativeResource checks for a kubernetes.io/ prefixed resource name
func isNativeResource(name v1.ResourceName) bool {
	return !strings.Contains(string(name), "/") || isPrefixedNativeResource(name)
}

func isPrefixedNativeResource(name v1.ResourceName) bool {
	return strings.Contains(string(name), v1.ResourceDefaultNamespacePrefix)
}

// gets the Node GPUs and VGPUs using the node data from k8s. Returns nil if GPUs could not be located for the node.
func gpuInfoFor(
	n *clustercache.Node,
	allocatedVGPUs func() (float64, error),
) (*NodeGpuInfo, error) {
	g, hasGpu := n.Status.Capacity["nvidia.com/gpu"]
	_, hasReplicas := n.Labels["nvidia.com/gpu.replicas"]

	// Case 1: Standard NVIDIA GPU
	if hasGpu && g.Value() != 0 && !hasReplicas {
		return &NodeGpuInfo{
			GPU:  float64(g.Value()),
			VGPU: float64(g.Value()),
		}, nil
	}

	// Case 2: NVIDIA GPU with GPU Feature Discovery (GFD) Pod enabled.
	// Ref: https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-sharing.html#verifying-the-gpu-time-slicing-configuration
	// Ref: https://github.com/NVIDIA/k8s-device-plugin/blob/d899752a424818428f744a946d32b132ea2c0cf1/internal/lm/resource_test.go#L44-L45
	// Ref: https://github.com/NVIDIA/k8s-device-plugin/blob/d899752a424818428f744a946d32b132ea2c0cf1/internal/lm/resource_test.go#L103-L118
	if hasReplicas {
		resultGPU := 0.0
		resultVGPU := 0.0

		if c, ok := n.Labels["nvidia.com/gpu.count"]; ok {
			var err error
			resultGPU, err = strconv.ParseFloat(c, 64)
			if err != nil {
				return nil, fmt.Errorf("could not parse label \"nvidia.com/gpu.count\": %v", err)
			}
		}

		if s, ok := n.Status.Capacity["nvidia.com/gpu.shared"]; ok { // GFD configured `renameByDefault=true`
			resultVGPU = float64(s.Value())
		} else if g, ok := n.Status.Capacity["nvidia.com/gpu"]; ok { // GFD configured `renameByDefault=false`
			resultVGPU = float64(g.Value())
		} else {
			resultVGPU = resultGPU
		}

		return &NodeGpuInfo{
			GPU:  resultGPU,
			VGPU: resultVGPU,
		}, nil
	}

	// Case 3: AWS vGPU
	if vgpu, ok := n.Status.Capacity["k8s.amazonaws.com/vgpu"]; ok {
		vgpuCount, err := allocatedVGPUs()
		if err != nil {
			return nil, err
		}

		vgpuCoeff := 10.0
		if vgpuCount > 0.0 {
			vgpuCoeff = vgpuCount
		}

		if vgpu.Value() != 0 {
			resultGPU := float64(vgpu.Value()) / vgpuCoeff
			resultVGPU := float64(vgpu.Value())
			return &NodeGpuInfo{
				GPU:  resultGPU,
				VGPU: resultVGPU,
			}, nil
		}
	}

	// No GPU found
	return nil, nil
}

func getAllocatableVGPUs(daemonsets []*clustercache.DaemonSet) (float64, error) {
	vgpuCount := 0.0

	for _, ds := range daemonsets {
		dsContainerList := &ds.SpecContainers
		for _, ctnr := range *dsContainerList {
			if ctnr.Args != nil {
				for _, arg := range ctnr.Args {
					if strings.Contains(arg, "--vgpu=") {
						vgpus, err := strconv.ParseFloat(arg[strings.IndexByte(arg, '=')+1:], 64)
						if err != nil {
							log.Errorf("failed to parse vgpu allocation string %s: %v", arg, err)
							continue
						}
						vgpuCount = vgpus
						return vgpuCount, nil
					}

				}
			}
		}
	}
	return vgpuCount, nil
}
