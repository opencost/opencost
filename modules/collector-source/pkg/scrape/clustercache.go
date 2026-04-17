package scrape

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	coreutil "github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// SyncMap provides thread-safe concurrent access to a generic map
type SyncMap[U comparable, T any] struct {
	mu   sync.RWMutex
	data map[U]T
}

// newSyncMap creates a new thread-safe map with the specified initial capacity
func newSyncMap[U comparable, T any](size int) *SyncMap[U, T] {
	return &SyncMap[U, T]{
		data: make(map[U]T, size),
	}
}

// Set adds or updates a key-value mapping
func (sm *SyncMap[U, T]) Set(key U, value T) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.data[key] = value
}

// Get retrieves a value by key. Returns the value and a boolean indicating if it was found.
func (sm *SyncMap[U, T]) Get(key U) (T, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	value, ok := sm.data[key]
	return value, ok
}

type ClusterCacheScraper struct {
	clusterCache clustercache.ClusterCache
}

func newClusterCacheScraper(clusterCache clustercache.ClusterCache) Scraper {
	return &ClusterCacheScraper{
		clusterCache: clusterCache,
	}
}

type pvcKey struct {
	name      string
	namespace string
}

func (ccs *ClusterCacheScraper) Scrape() []metric.Update {
	// retrieve objects for scrape
	nodes := ccs.clusterCache.GetAllNodes()
	deployments := ccs.clusterCache.GetAllDeployments()
	namespaces := ccs.clusterCache.GetAllNamespaces()
	pods := ccs.clusterCache.GetAllPods()
	pvcs := ccs.clusterCache.GetAllPersistentVolumeClaims()
	pvs := ccs.clusterCache.GetAllPersistentVolumes()
	services := ccs.clusterCache.GetAllServices()
	statefulSets := ccs.clusterCache.GetAllStatefulSets()
	daemonSets := ccs.clusterCache.GetAllDaemonSets()
	jobs := ccs.clusterCache.GetAllJobs()
	cronJobs := ccs.clusterCache.GetAllCronJobs()
	replicaSets := ccs.clusterCache.GetAllReplicaSets()
	resourceQuotas := ccs.clusterCache.GetAllResourceQuotas()

	// create scrape indexes. While the pairs being mapped here don't have a 1 to 1 relationship in the general case,
	// we are assuming that in the context of a single snapshot of the cluster they are 1 to 1.
	nodeNameToUID := newSyncMap[string, types.UID](len(nodes))
	for _, node := range nodes {
		nodeNameToUID.Set(node.Name, node.UID)
	}
	namespaceNameToUID := newSyncMap[string, types.UID](len(namespaces))
	for _, ns := range namespaces {
		namespaceNameToUID.Set(ns.Name, ns.UID)
	}
	pvcNameToUID := newSyncMap[pvcKey, types.UID](len(pvcs))
	for _, pvc := range pvcs {
		pvcNameToUID.Set(pvcKey{
			name:      pvc.Name,
			namespace: pvc.Namespace,
		}, pvc.UID)
	}
	pvNameToUID := newSyncMap[string, types.UID](len(pvs))
	for _, pv := range pvs {
		pvNameToUID.Set(pv.Name, pv.UID)
	}

	scrapeFuncs := []ScrapeFunc{
		ccs.GetScrapeNodes(nodes),
		ccs.GetScrapeDeployments(deployments, namespaceNameToUID),
		ccs.GetScrapeNamespaces(namespaces),
		ccs.GetScrapePods(pods, nodeNameToUID, namespaceNameToUID, pvcNameToUID),
		ccs.GetScrapePVCs(pvcs, namespaceNameToUID, pvNameToUID),
		ccs.GetScrapePVs(pvs),
		ccs.GetScrapeServices(services),
		ccs.GetScrapeStatefulSets(statefulSets, namespaceNameToUID),
		ccs.GetScrapeDaemonSets(daemonSets, namespaceNameToUID),
		ccs.GetScrapeJobs(jobs, namespaceNameToUID),
		ccs.GetScrapeCronJobs(cronJobs, namespaceNameToUID),
		ccs.GetScrapeReplicaSets(replicaSets, namespaceNameToUID),
		ccs.GetScrapeResourceQuotas(resourceQuotas, namespaceNameToUID),
	}
	return concurrentScrape(scrapeFuncs...)
}

func (ccs *ClusterCacheScraper) GetScrapeNodes(nodes []*clustercache.Node) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeNodes(nodes)
	}
}

func (ccs *ClusterCacheScraper) scrapeNodes(nodes []*clustercache.Node) []metric.Update {
	var scrapeResults []metric.Update

	for _, node := range nodes {
		nodeInfo := map[string]string{
			source.NodeLabel:       node.Name,
			source.ProviderIDLabel: node.SpecProviderID,
			source.UIDLabel:        string(node.UID),
		}

		if instanceType, ok := coreutil.GetInstanceType(node.Labels); ok {
			nodeInfo[source.InstanceTypeLabel] = instanceType
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.NodeInfo,
			Labels:         nodeInfo,
			AdditionalInfo: nodeInfo,
		})

		// Node Capacity
		scrapeResults = scrapeResourceList(
			metric.NodeResourceCapacities,
			node.Status.Capacity,
			nodeInfo,
			scrapeResults)

		// This block and metric can be removed, when we stop exporting assets and allocations
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
		scrapeResults = scrapeResourceList(
			metric.NodeResourcesAllocatable,
			node.Status.Allocatable,
			nodeInfo,
			scrapeResults)

		// This block and metric can be removed, when we stop exporting assets and allocations
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

func (ccs *ClusterCacheScraper) GetScrapeDeployments(deployments []*clustercache.Deployment, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeDeployments(deployments, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeDeployments(deployments []*clustercache.Deployment, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
	var scrapeResults []metric.Update
	for _, deployment := range deployments {
		nsUID, ok := namespaceIndex.Get(deployment.Namespace)
		if !ok {
			log.Debugf("deployment namespaceUID missing from index for namespace name '%s'", deployment.Namespace)
		}
		deploymentInfo := map[string]string{
			source.UIDLabel:          string(deployment.UID),
			source.NamespaceUIDLabel: string(nsUID),
			source.DeploymentLabel:   deployment.Name,
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DeploymentLabels,
			Labels:         deploymentInfo,
			Value:          0,
			AdditionalInfo: deploymentInfo,
		})

		// deployment labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(deployment.Labels)
		deploymentLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DeploymentLabels,
			Labels:         deploymentInfo,
			Value:          0,
			AdditionalInfo: deploymentLabels,
		})

		// deployment annotations
		annoationNames, annotationValues := promutil.KubeAnnotationsToLabels(deployment.Annotations)
		deploymentAnnotations := util.ToMap(annoationNames, annotationValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DeploymentAnnotations,
			Labels:         deploymentInfo,
			Value:          0,
			AdditionalInfo: deploymentAnnotations,
		})

		// deployment match labels
		deploymentInfo[source.NamespaceLabel] = deployment.Namespace
		matchLabelNames, matchLabelValues := promutil.KubeLabelsToLabels(deployment.MatchLabels)
		deploymentMatchLabels := util.ToMap(matchLabelNames, matchLabelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DeploymentMatchLabels,
			Labels:         deploymentInfo,
			Value:          0,
			AdditionalInfo: deploymentMatchLabels,
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

func (ccs *ClusterCacheScraper) GetScrapeNamespaces(namespaces []*clustercache.Namespace) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeNamespaces(namespaces)
	}
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

func (ccs *ClusterCacheScraper) GetScrapePods(
	pods []*clustercache.Pod,
	nodeIndex,
	namespaceIndex *SyncMap[string, types.UID],
	pvcIndex *SyncMap[pvcKey, types.UID],
) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapePods(pods, nodeIndex, namespaceIndex, pvcIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapePods(
	pods []*clustercache.Pod,
	nodeIndex,
	namespaceIndex *SyncMap[string, types.UID],
	pvcIndex *SyncMap[pvcKey, types.UID],
) []metric.Update {
	var scrapeResults []metric.Update
	for _, pod := range pods {
		nodeUID, ok := nodeIndex.Get(pod.Spec.NodeName)
		if !ok {
			log.Debugf("pod nodeUID missing from index for node name '%s'", pod.Spec.NodeName)
		}
		nsUID, ok := namespaceIndex.Get(pod.Namespace)
		if !ok {
			log.Debugf("pod namespaceUID missing from index for namespace name '%s'", pod.Namespace)
		}
		podInfo := map[string]string{
			source.UIDLabel:          string(pod.UID),
			source.PodLabel:          pod.Name,
			source.NamespaceUIDLabel: string(nsUID),
			source.NodeUIDLabel:      string(nodeUID),
			"host_network":           strconv.FormatBool(pod.Spec.HostNetwork),
		}
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.PodInfo,
			Labels:         podInfo,
			Value:          0,
			AdditionalInfo: podInfo,
		})

		podInfo[source.NamespaceLabel] = pod.Namespace
		podInfo[source.NodeLabel] = pod.Spec.NodeName
		podInfo[source.InstanceLabel] = pod.Spec.NodeName

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

		// Pod owner metric
		for _, owner := range pod.OwnerReferences {
			controller := "false"
			if owner.Controller != nil && *owner.Controller {
				controller = "true"
			}
			ownerInfo := maps.Clone(podInfo)
			ownerInfo[source.OwnerKindLabel] = owner.Kind
			ownerInfo[source.OwnerNameLabel] = owner.Name
			ownerInfo[source.OwnerUIDLabel] = string(owner.UID)
			ownerInfo[source.ContainerLabel] = controller
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
					Name:           metric.KubePodContainerStatusRunning,
					Labels:         containerInfo,
					AdditionalInfo: containerInfo,
					Value:          0,
				})
			}
		}

		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				pvcUID, _ := pvcIndex.Get(pvcKey{
					name:      volume.PersistentVolumeClaim.ClaimName,
					namespace: pod.Namespace,
				})
				podPVCVolumeInfo := map[string]string{
					source.UIDLabel:           string(pod.UID),
					source.PVCUIDLabel:        string(pvcUID),
					source.PodVolumeNameLabel: volume.Name,
				}
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   metric.PodPVCVolume,
					Labels: podPVCVolumeInfo,
					Value:  0,
				})
			}
		}

		for _, container := range pod.Spec.Containers {
			containerInfo := maps.Clone(podInfo)
			containerInfo[source.ContainerLabel] = container.Name
			// Requests
			scrapeResults = scrapeResourceList(
				metric.KubePodContainerResourceRequests,
				container.Resources.Requests,
				containerInfo,
				scrapeResults)

			// Limits
			scrapeResults = scrapeResourceList(
				metric.KubePodContainerResourceLimits,
				container.Resources.Limits,
				containerInfo,
				scrapeResults)
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

func scrapeResourceList(metricName string, resourceList v1.ResourceList, baseLabels map[string]string, scrapeResults []metric.Update) []metric.Update {
	if resourceList != nil {
		// sorting keys here for testing purposes
		keys := maps.Keys(resourceList)
		slices.Sort(keys)
		for _, resourceName := range keys {
			quantity := resourceList[resourceName]
			resource, unit, value := toResourceUnitValue(resourceName, quantity)

			// failed to parse the resource type
			if resource == "" {
				log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
				continue
			}

			resourceRequestInfo := maps.Clone(baseLabels)
			resourceRequestInfo[source.ResourceLabel] = resource
			resourceRequestInfo[source.UnitLabel] = unit
			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metricName,
				Labels: resourceRequestInfo,
				Value:  value,
			})
		}
	}
	return scrapeResults
}

func (ccs *ClusterCacheScraper) GetScrapePVCs(
	pvcs []*clustercache.PersistentVolumeClaim,
	namespaceIndex *SyncMap[string, types.UID],
	pvIndex *SyncMap[string, types.UID],
) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapePVCs(pvcs, namespaceIndex, pvIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapePVCs(
	pvcs []*clustercache.PersistentVolumeClaim,
	namespaceIndex *SyncMap[string, types.UID],
	pvIndex *SyncMap[string, types.UID],
) []metric.Update {
	var scrapeResults []metric.Update
	for _, pvc := range pvcs {
		nsUID, ok := namespaceIndex.Get(pvc.Namespace)
		if !ok {
			log.Debugf("pvc namespaceUID missing from index for namespace name '%s'", pvc.Namespace)
		}
		pvUID, ok := pvIndex.Get(pvc.Spec.VolumeName)
		if !ok && pvc.Spec.VolumeName != "" {
			log.Debugf("pvc volume name missing from index for pv name '%s'", pvc.Spec.VolumeName)
		}
		pvcInfo := map[string]string{
			source.UIDLabel:          string(pvc.UID),
			source.PVCLabel:          pvc.Name,
			source.NamespaceUIDLabel: string(nsUID),
			source.NamespaceLabel:    pvc.Namespace,
			source.VolumeNameLabel:   pvc.Spec.VolumeName,
			source.PVUIDLabel:        string(pvUID),
			source.StorageClassLabel: getPersistentVolumeClaimClass(pvc),
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubePersistentVolumeClaimInfo,
			Labels:         pvcInfo,
			AdditionalInfo: pvcInfo,
			Value:          0,
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

func (ccs *ClusterCacheScraper) GetScrapePVs(pvs []*clustercache.PersistentVolume) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapePVs(pvs)
	}
}

func (ccs *ClusterCacheScraper) scrapePVs(pvs []*clustercache.PersistentVolume) []metric.Update {
	var scrapeResults []metric.Update
	for _, pv := range pvs {
		providerID := pv.Name
		var csiVolumeHandle string
		// if a more accurate provider ID is available, use that
		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
			providerID = pv.Spec.CSI.VolumeHandle
			csiVolumeHandle = pv.Spec.CSI.VolumeHandle
		}
		pvInfo := map[string]string{
			source.UIDLabel:             string(pv.UID),
			source.PVLabel:              pv.Name,
			source.StorageClassLabel:    pv.Spec.StorageClassName,
			source.ProviderIDLabel:      providerID,
			source.CSIVolumeHandleLabel: csiVolumeHandle,
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.KubecostPVInfo,
			Labels:         pvInfo,
			AdditionalInfo: pvInfo,
			Value:          0,
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

func (ccs *ClusterCacheScraper) GetScrapeServices(services []*clustercache.Service) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeServices(services)
	}
}

func (ccs *ClusterCacheScraper) scrapeServices(services []*clustercache.Service) []metric.Update {
	var scrapeResults []metric.Update
	for _, service := range services {
		serviceInfo := map[string]string{
			source.UIDLabel:         string(service.UID),
			source.ServiceLabel:     service.Name,
			source.NamespaceLabel:   service.Namespace,
			source.ServiceTypeLabel: string(service.Type),
		}

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.ServiceInfo,
			Labels:         serviceInfo,
			Value:          0,
			AdditionalInfo: serviceInfo,
		})

		// service selector labels
		selectorNames, selectorValues := promutil.KubeLabelsToLabels(service.SpecSelector)
		serviceLabels := util.ToMap(selectorNames, selectorValues)
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

func (ccs *ClusterCacheScraper) GetScrapeStatefulSets(statefulSets []*clustercache.StatefulSet, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeStatefulSets(statefulSets, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeStatefulSets(statefulSets []*clustercache.StatefulSet, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
	var scrapeResults []metric.Update
	for _, statefulSet := range statefulSets {
		nsUID, ok := namespaceIndex.Get(statefulSet.Namespace)
		if !ok {
			log.Debugf("statefulSet namespaceUID missing from index for namespace name '%s'", statefulSet.Namespace)
		}
		statefulSetInfo := map[string]string{
			source.UIDLabel:          string(statefulSet.UID),
			source.NamespaceUIDLabel: string(nsUID),
			source.StatefulSetLabel:  statefulSet.Name,
		}

		// statefulSet info
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.StatefulSetInfo,
			Labels:         statefulSetInfo,
			Value:          0,
			AdditionalInfo: statefulSetInfo,
		})

		// statefulSet labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(statefulSet.Labels)
		statefulSetLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.StatefulSetLabels,
			Labels:         statefulSetInfo,
			Value:          0,
			AdditionalInfo: statefulSetLabels,
		})

		// statefulSet annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(statefulSet.Annotations)
		statefulSetAnnotations := util.ToMap(annotationNames, annotationValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.StatefulSetAnnotations,
			Labels:         statefulSetInfo,
			Value:          0,
			AdditionalInfo: statefulSetAnnotations,
		})

		// statefulSet match labels
		statefulSetInfo[source.NamespaceLabel] = statefulSet.Namespace
		matchLabelNames, matchLabelValues := promutil.KubeLabelsToLabels(statefulSet.SpecSelector.MatchLabels)
		statefulSetMatchLabels := util.ToMap(matchLabelNames, matchLabelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.StatefulSetMatchLabels,
			Labels:         statefulSetInfo,
			Value:          0,
			AdditionalInfo: statefulSetMatchLabels,
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

func (ccs *ClusterCacheScraper) GetScrapeDaemonSets(daemonSets []*clustercache.DaemonSet, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeDaemonSets(daemonSets, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeDaemonSets(daemonSets []*clustercache.DaemonSet, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
	var scrapeResults []metric.Update
	for _, daemonSet := range daemonSets {
		nsUID, ok := namespaceIndex.Get(daemonSet.Namespace)
		if !ok {
			log.Debugf("daemonSet namespaceUID missing from index for namespace name '%s'", daemonSet.Namespace)
		}
		daemonSetInfo := map[string]string{
			source.UIDLabel:          string(daemonSet.UID),
			source.NamespaceUIDLabel: string(nsUID),
			source.DaemonSetLabel:    daemonSet.Name,
		}

		// daemonSet info
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DaemonSetInfo,
			Labels:         daemonSetInfo,
			Value:          0,
			AdditionalInfo: daemonSetInfo,
		})

		// daemonSet labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(daemonSet.Labels)
		daemonSetLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DaemonSetLabels,
			Labels:         daemonSetInfo,
			Value:          0,
			AdditionalInfo: daemonSetLabels,
		})

		// daemonSet annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(daemonSet.Annotations)
		daemonSetAnnotations := util.ToMap(annotationNames, annotationValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.DaemonSetAnnotations,
			Labels:         daemonSetInfo,
			Value:          0,
			AdditionalInfo: daemonSetAnnotations,
		})
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.DaemonSetScraperType,
		Targets:     len(daemonSets),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) GetScrapeJobs(jobs []*clustercache.Job, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeJobs(jobs, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeJobs(jobs []*clustercache.Job, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
	var scrapeResults []metric.Update
	for _, job := range jobs {
		nsUID, ok := namespaceIndex.Get(job.Namespace)
		if !ok {
			log.Debugf("job namespaceUID missing from index for namespace name '%s'", job.Namespace)
		}
		jobInfo := map[string]string{
			source.UIDLabel:          string(job.UID),
			source.NamespaceUIDLabel: string(nsUID),
			source.JobLabel:          job.Name,
		}

		// job info
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.JobInfo,
			Labels:         jobInfo,
			Value:          0,
			AdditionalInfo: jobInfo,
		})

		// job labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(job.Labels)
		jobLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.JobLabels,
			Labels:         jobInfo,
			Value:          0,
			AdditionalInfo: jobLabels,
		})

		// job annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(job.Annotations)
		jobAnnotations := util.ToMap(annotationNames, annotationValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.JobAnnotations,
			Labels:         jobInfo,
			Value:          0,
			AdditionalInfo: jobAnnotations,
		})
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.JobScraperType,
		Targets:     len(jobs),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) GetScrapeCronJobs(cronJobs []*clustercache.CronJob, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeCronJobs(cronJobs, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeCronJobs(cronJobs []*clustercache.CronJob, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
	var scrapeResults []metric.Update
	for _, cronJob := range cronJobs {
		nsUID, ok := namespaceIndex.Get(cronJob.Namespace)
		if !ok {
			log.Debugf("cronjob namespaceUID missing from index for namespace name '%s'", cronJob.Namespace)
		}
		cronJobInfo := map[string]string{
			source.UIDLabel:          string(cronJob.UID),
			source.NamespaceUIDLabel: string(nsUID),
			source.CronJobLabel:      cronJob.Name,
		}

		// cronjob info
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.CronJobInfo,
			Labels:         cronJobInfo,
			Value:          0,
			AdditionalInfo: cronJobInfo,
		})

		// cronjob labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(cronJob.Labels)
		cronJobLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.CronJobLabels,
			Labels:         cronJobInfo,
			Value:          0,
			AdditionalInfo: cronJobLabels,
		})

		// cronjob annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(cronJob.Annotations)
		cronJobAnnotations := util.ToMap(annotationNames, annotationValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.CronJobAnnotations,
			Labels:         cronJobInfo,
			Value:          0,
			AdditionalInfo: cronJobAnnotations,
		})
	}

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.CronJobScraperType,
		Targets:     len(cronJobs),
		Errors:      nil,
	})

	return scrapeResults
}

func (ccs *ClusterCacheScraper) GetScrapeReplicaSets(replicaSets []*clustercache.ReplicaSet, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeReplicaSets(replicaSets, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeReplicaSets(replicaSets []*clustercache.ReplicaSet, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
	var scrapeResults []metric.Update
	for _, replicaSet := range replicaSets {
		nsUID, ok := namespaceIndex.Get(replicaSet.Namespace)
		if !ok {
			log.Debugf("replicaset namespaceUID missing from index for namespace name '%s'", replicaSet.Namespace)
		}
		replicaSetInfo := map[string]string{
			source.UIDLabel:          string(replicaSet.UID),
			source.NamespaceUIDLabel: string(nsUID),
			source.ReplicaSetLabel:   replicaSet.Name,
		}

		// replicaset info
		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.ReplicaSetInfo,
			Labels:         replicaSetInfo,
			Value:          0,
			AdditionalInfo: replicaSetInfo,
		})

		// replicaset labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(replicaSet.Labels)
		replicaSetLabels := util.ToMap(labelNames, labelValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.ReplicaSetLabels,
			Labels:         replicaSetInfo,
			Value:          0,
			AdditionalInfo: replicaSetLabels,
		})

		// replicaset annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(replicaSet.Annotations)
		replicaSetAnnotations := util.ToMap(annotationNames, annotationValues)

		scrapeResults = append(scrapeResults, metric.Update{
			Name:           metric.ReplicaSetAnnotations,
			Labels:         replicaSetInfo,
			Value:          0,
			AdditionalInfo: replicaSetAnnotations,
		})

		// owner references for backward compatibility
		replicaSetOwnerInfo := map[string]string{
			source.ReplicaSetLabel: replicaSet.Name,
			source.NamespaceLabel:  replicaSet.Namespace,
			source.UIDLabel:        string(replicaSet.UID),
		}

		// this specific metric exports a special <none> value for name and kind
		// if there are no owners
		if len(replicaSet.OwnerReferences) == 0 {
			ownerInfo := maps.Clone(replicaSetOwnerInfo)
			ownerInfo[source.OwnerKindLabel] = source.NoneLabelValue
			ownerInfo[source.OwnerNameLabel] = source.NoneLabelValue
			scrapeResults = append(scrapeResults, metric.Update{
				Name:   metric.KubeReplicasetOwner,
				Labels: ownerInfo,
				Value:  0,
			})
		} else {
			for _, owner := range replicaSet.OwnerReferences {
				controller := "false"
				if owner.Controller != nil && *owner.Controller {
					controller = "true"
				}
				ownerInfo := maps.Clone(replicaSetOwnerInfo)
				ownerInfo[source.OwnerKindLabel] = owner.Kind
				ownerInfo[source.OwnerNameLabel] = owner.Name
				ownerInfo[source.OwnerUIDLabel] = string(owner.UID)
				ownerInfo[source.ControllerLabel] = controller
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

func (ccs *ClusterCacheScraper) GetScrapeResourceQuotas(resourceQuotas []*clustercache.ResourceQuota, namespaceIndex *SyncMap[string, types.UID]) ScrapeFunc {
	return func() []metric.Update {
		return ccs.scrapeResourceQuotas(resourceQuotas, namespaceIndex)
	}
}

func (ccs *ClusterCacheScraper) scrapeResourceQuotas(resourceQuotas []*clustercache.ResourceQuota, namespaceIndex *SyncMap[string, types.UID]) []metric.Update {
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
		nsUID, _ := namespaceIndex.Get(resourceQuota.Namespace)
		resourceQuotaInfo := map[string]string{
			source.UIDLabel:           string(resourceQuota.UID),
			source.NamespaceUIDLabel:  string(nsUID),
			source.ResourceQuotaLabel: resourceQuota.Name,
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
