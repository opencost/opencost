package scrape

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Cluster Cache Metrics
const (
	KubeNodeStatusCapacityCPUCores                        = "kube_node_status_capacity_cpu_cores"
	KubeNodeStatusCapacityMemoryBytes                     = "kube_node_status_capacity_memory_bytes"
	KubeNodeStatusAllocatableCPUCores                     = "kube_node_status_allocatable_cpu_cores"
	KubeNodeStatusAllocatableMemoryBytes                  = "kube_node_status_allocatable_memory_bytes"
	KubeNodeLabels                                        = "kube_node_labels"
	KubePodLabels                                         = "kube_pod_labels"
	KubePodAnnotations                                    = "kube_pod_annotations"
	KubePodOwner                                          = "kube_pod_owner"
	KubePodContainerStatusRunning                         = "kube_pod_container_status_running"
	KubePodContainerResourceRequests                      = "kube_pod_container_resource_requests"
	KubePersistentVolumeClaimInfo                         = "kube_persistentvolumeclaim_info"
	KubePersistentVolumeClaimResourceRequestsStorageBytes = "kube_persistentvolumeclaim_resource_requests_storage_bytes"
	KubecostPVInfo                                        = "kubecost_pv_info"
	KubePersistentVolumeCapacityBytes                     = "kube_persistentvolume_capacity_bytes"
	DeploymentMatchLabels                                 = "deployment_match_labels"
	KubeNamespaceLabels                                   = "kube_namespace_labels"
	KubeNamespaceAnnotations                              = "kube_namespace_annotations"
	ServiceSelectorLabels                                 = "service_selector_labels"
	StatefulSetMatchLabels                                = "statefulSet_match_labels"
	KubeReplicasetOwner                                   = "kube_replicaset_owner"
)

type ClusterCacheScraper struct {
	clusterCache clustercache.ClusterCache
	updater      metric.MetricUpdater
}

func newClusterCacheScraper(clusterCache clustercache.ClusterCache, updater metric.MetricUpdater) Scraper {
	return &ClusterCacheScraper{
		clusterCache: clusterCache,
		updater:      updater,
	}
}

func (ccs *ClusterCacheScraper) Scrape() {
	timestamp := time.Now().UTC()
	nodes := ccs.clusterCache.GetAllNodes()
	deployments := ccs.clusterCache.GetAllDeployments()
	namespaces := ccs.clusterCache.GetAllNamespaces()
	pods := ccs.clusterCache.GetAllPods()
	pvcs := ccs.clusterCache.GetAllPersistentVolumeClaims()
	pvs := ccs.clusterCache.GetAllPersistentVolumes()
	services := ccs.clusterCache.GetAllServices()
	statefulSets := ccs.clusterCache.GetAllStatefulSets()
	replicaSets := ccs.clusterCache.GetAllReplicaSets()

	ccs.scrapeNodes(nodes, timestamp)
	ccs.scrapeDeployments(deployments, timestamp)
	ccs.scrapeNamespaces(namespaces, timestamp)
	ccs.scrapePods(pods, timestamp)
	ccs.scrapePVCs(pvcs, timestamp)
	ccs.scrapePVs(pvs, timestamp)
	ccs.scrapeServices(services, timestamp)
	ccs.scrapeStatefulSets(statefulSets, timestamp)
	ccs.scrapeReplicaSets(replicaSets, timestamp)
}

func (ccs *ClusterCacheScraper) scrapeNodes(nodes []*clustercache.Node, timestamp time.Time) {
	for _, node := range nodes {
		nodeInfo := map[string]string{
			source.NodeLabel:       node.Name,
			source.ProviderIDLabel: node.SpecProviderID,
		}

		// Node Capacity
		if node.Status.Capacity != nil {
			if quantity, ok := node.Status.Capacity[v1.ResourceCPU]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceCPU, quantity)
				ccs.updater.Update(KubeNodeStatusCapacityCPUCores, nodeInfo, value, &timestamp, nil)
			}

			if quantity, ok := node.Status.Capacity[v1.ResourceMemory]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceMemory, quantity)
				ccs.updater.Update(KubeNodeStatusCapacityMemoryBytes, nodeInfo, value, &timestamp, nil)
			}
		}

		// Node Allocatable Resources
		if node.Status.Allocatable != nil {
			if quantity, ok := node.Status.Allocatable[v1.ResourceCPU]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceCPU, quantity)
				ccs.updater.Update(KubeNodeStatusAllocatableCPUCores, nodeInfo, value, &timestamp, nil)
			}

			if quantity, ok := node.Status.Allocatable[v1.ResourceMemory]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceMemory, quantity)
				ccs.updater.Update(KubeNodeStatusAllocatableMemoryBytes, nodeInfo, value, &timestamp, nil)
			}
		}

		// node labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(node.Labels)
		nodeLabels := util.ToMap(labelNames, labelValues)

		ccs.updater.Update(KubeNodeLabels, nodeInfo, 0, &timestamp, nodeLabels)

	}
}

func (ccs *ClusterCacheScraper) scrapeDeployments(deployments []*clustercache.Deployment, timestamp time.Time) {
	for _, deployment := range deployments {
		deploymentInfo := map[string]string{
			source.DeploymentLabel: deployment.Name,
			source.NamespaceLabel:  deployment.Namespace,
		}

		// deployment labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(deployment.MatchLabels)
		deploymentLabels := util.ToMap(labelNames, labelValues)

		ccs.updater.Update(DeploymentMatchLabels, deploymentInfo, 0, &timestamp, deploymentLabels)

	}
}

func (ccs *ClusterCacheScraper) scrapeNamespaces(namespaces []*clustercache.Namespace, timestamp time.Time) {
	for _, namespace := range namespaces {
		namespaceInfo := map[string]string{
			source.NamespaceLabel: namespace.Name,
		}

		// namespace labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(namespace.Labels)
		namespaceLabels := util.ToMap(labelNames, labelValues)
		ccs.updater.Update(KubeNamespaceLabels, namespaceInfo, 0, &timestamp, namespaceLabels)

		// namespace annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(namespace.Annotations)
		namespaceAnnotations := util.ToMap(annotationNames, annotationValues)
		ccs.updater.Update(KubeNamespaceAnnotations, namespaceInfo, 0, &timestamp, namespaceAnnotations)
	}
}

func (ccs *ClusterCacheScraper) scrapePods(pods []*clustercache.Pod, timestamp time.Time) {
	for _, pod := range pods {
		podInfo := map[string]string{
			source.PodLabel:       pod.Name,
			source.NamespaceLabel: pod.Namespace,
			source.UIDLabel:       string(pod.UID),
			source.NodeLabel:      pod.Spec.NodeName,
			source.InstanceLabel:  pod.Spec.NodeName,
		}

		// pod labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(pod.Labels)
		podLabels := util.ToMap(labelNames, labelValues)
		ccs.updater.Update(KubePodLabels, podInfo, 0, &timestamp, podLabels)

		// pod annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(pod.Annotations)
		podAnnotations := util.ToMap(annotationNames, annotationValues)
		ccs.updater.Update(KubePodAnnotations, podInfo, 0, &timestamp, podAnnotations)

		// Pod owner metric
		for _, owner := range pod.OwnerReferences {
			ownerInfo := maps.Clone(podInfo)
			ownerInfo[source.OwnerKindLabel] = owner.Kind
			ownerInfo[source.OwnerNameLabel] = owner.Name
			ccs.updater.Update(KubePodOwner, ownerInfo, 0, &timestamp, nil)
		}

		// Container Status
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Running != nil {
				containerInfo := maps.Clone(podInfo)
				containerInfo[source.ContainerLabel] = status.Name
				ccs.updater.Update(KubePodContainerStatusRunning, containerInfo, 0, &timestamp, nil)
			}
		}

		for _, container := range pod.Spec.Containers {
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
					ccs.updater.Update(KubePodContainerResourceRequests, resourceRequestInfo, value, &timestamp, nil)
				}
			}
		}
	}
}

func (ccs *ClusterCacheScraper) scrapePVCs(pvcs []*clustercache.PersistentVolumeClaim, timestamp time.Time) {
	for _, pvc := range pvcs {
		pvcInfo := map[string]string{
			source.PVCLabel:          pvc.Name,
			source.NamespaceLabel:    pvc.Namespace,
			source.VolumeNameLabel:   pvc.Spec.VolumeName,
			source.StorageClassLabel: getPersistentVolumeClaimClass(pvc),
		}

		ccs.updater.Update(KubePersistentVolumeClaimInfo, pvcInfo, 0, &timestamp, nil)

		if storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			ccs.updater.Update(KubePersistentVolumeClaimResourceRequestsStorageBytes, pvcInfo, float64(storage.Value()), &timestamp, nil)
		}
	}
}

func (ccs *ClusterCacheScraper) scrapePVs(pvs []*clustercache.PersistentVolume, timestamp time.Time) {
	for _, pv := range pvs {
		providerID := pv.Name
		// if a more accurate provider ID is available, use that
		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
			providerID = pv.Spec.CSI.VolumeHandle
		}
		pvInfo := map[string]string{
			source.PVLabel:           pv.Name,
			source.StorageClassLabel: pv.Spec.StorageClassName,
			source.ProviderIDLabel:   providerID,
		}

		ccs.updater.Update(KubecostPVInfo, pvInfo, 0, &timestamp, nil)

		if storage, ok := pv.Spec.Capacity[v1.ResourceStorage]; ok {
			ccs.updater.Update(KubePersistentVolumeCapacityBytes, pvInfo, float64(storage.Value()), &timestamp, nil)
		}
	}
}

func (ccs *ClusterCacheScraper) scrapeServices(services []*clustercache.Service, timestamp time.Time) {
	for _, service := range services {
		serviceInfo := map[string]string{
			source.ServiceLabel:   service.Name,
			source.NamespaceLabel: service.Namespace,
		}

		// service labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(service.SpecSelector)
		serviceLabels := util.ToMap(labelNames, labelValues)
		ccs.updater.Update(ServiceSelectorLabels, serviceInfo, 0, &timestamp, serviceLabels)

	}
}

func (ccs *ClusterCacheScraper) scrapeStatefulSets(statefulSets []*clustercache.StatefulSet, timestamp time.Time) {
	for _, statefulSet := range statefulSets {
		statefulSetInfo := map[string]string{
			source.StatefulSetLabel: statefulSet.Name,
			source.NamespaceLabel:   statefulSet.Namespace,
		}

		// statefulSet labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(statefulSet.SpecSelector.MatchLabels)
		statefulSetLabels := util.ToMap(labelNames, labelValues)
		ccs.updater.Update(StatefulSetMatchLabels, statefulSetInfo, 0, &timestamp, statefulSetLabels)

	}
}

func (ccs *ClusterCacheScraper) scrapeReplicaSets(replicaSets []*clustercache.ReplicaSet, timestamp time.Time) {
	for _, replicaSet := range replicaSets {
		replicaSetInfo := map[string]string{
			source.ReplicaSetLabel: replicaSet.Name,
			source.NamespaceLabel:  replicaSet.Namespace,
		}

		for _, owner := range replicaSet.OwnerReferences {
			ownerInfo := maps.Clone(replicaSetInfo)
			ownerInfo[source.OwnerKindLabel] = owner.Kind
			ownerInfo[source.OwnerNameLabel] = owner.Name
			ccs.updater.Update(KubeReplicasetOwner, ownerInfo, 0, &timestamp, nil)
		}
	}
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
