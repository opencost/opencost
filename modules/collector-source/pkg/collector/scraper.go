package collector

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

type kubernetesScraper struct {
	clusterCache clustercache.ClusterCache
	collector    MetricsCollector
}

func (ks *kubernetesScraper) Scrape() {
	timestamp := time.Now().UTC()
	nodes := ks.clusterCache.GetAllNodes()
	deployments := ks.clusterCache.GetAllDeployments()
	namespaces := ks.clusterCache.GetAllNamespaces()
	pods := ks.clusterCache.GetAllPods()
	pvcs := ks.clusterCache.GetAllPersistentVolumeClaims()
	pvs := ks.clusterCache.GetAllPersistentVolumes()
	services := ks.clusterCache.GetAllServices()
	statefulSets := ks.clusterCache.GetAllStatefulSets()

	ks.scrapeNodes(nodes, timestamp)
	ks.scrapeDeployments(deployments, timestamp)
	ks.scrapeNamespaces(namespaces, timestamp)
	ks.scrapePods(pods, timestamp)
	ks.scrapePVCs(pvcs, timestamp)
	ks.scrapePVs(pvs, timestamp)
	ks.scrapeServices(services, timestamp)
	ks.scrapeStatefulSets(statefulSets, timestamp)
}

func (ks *kubernetesScraper) scrapeNodes(nodes []*clustercache.Node, timestamp time.Time) {
	for _, node := range nodes {
		nodeInfo := map[string]string{
			"node":        node.Name,
			"provider_id": node.SpecProviderID,
		}

		// Node Capacity
		if node.Status.Capacity != nil {
			if quantity, ok := node.Status.Capacity[v1.ResourceCPU]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceCPU, quantity)
				ks.collector.Update(KubeNodeStatusCapacityCPUCores, nodeInfo, value, &timestamp, nil)
			}

			if quantity, ok := node.Status.Capacity[v1.ResourceMemory]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceMemory, quantity)
				ks.collector.Update(KubeNodeStatusCapacityMemoryBytes, nodeInfo, value, &timestamp, nil)
			}
		}

		// Node Allocatable Resources
		if node.Status.Allocatable != nil {
			if quantity, ok := node.Status.Allocatable[v1.ResourceCPU]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceCPU, quantity)
				ks.collector.Update(KubeNodeStatusAllocatableCPUCores, nodeInfo, value, &timestamp, nil)
			}

			if quantity, ok := node.Status.Allocatable[v1.ResourceMemory]; ok {
				_, _, value := toResourceUnitValue(v1.ResourceMemory, quantity)
				ks.collector.Update(KubeNodeStatusAllocatableMemoryBytes, nodeInfo, value, &timestamp, nil)
			}
		}

		// node labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(node.Labels)
		nodeLabels := toMap(labelNames, labelValues)

		ks.collector.Update(KubeNodeLabels, nodeInfo, 0, &timestamp, nodeLabels)

	}
}

func (ks *kubernetesScraper) scrapeDeployments(deployments []*clustercache.Deployment, timestamp time.Time) {
	for _, deployment := range deployments {
		deploymentInfo := map[string]string{
			"deployment": deployment.Name,
			"namespace":  deployment.Namespace,
		}

		// deployment labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(deployment.MatchLabels)
		deploymentLabels := toMap(labelNames, labelValues)

		ks.collector.Update(DeploymentMatchLabels, deploymentInfo, 0, &timestamp, deploymentLabels)

	}
}

func (ks *kubernetesScraper) scrapeNamespaces(namespaces []*clustercache.Namespace, timestamp time.Time) {
	for _, namespace := range namespaces {
		namespaceInfo := map[string]string{
			"namespace": namespace.Name,
		}

		// namespace labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(namespace.Labels)
		namespaceLabels := toMap(labelNames, labelValues)
		ks.collector.Update(KubeNamespaceLabels, namespaceInfo, 0, &timestamp, namespaceLabels)

		// namespace annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(namespace.Annotations)
		namespaceAnnotations := toMap(annotationNames, annotationValues)
		ks.collector.Update(KubeNamespaceAnnotations, namespaceInfo, 0, &timestamp, namespaceAnnotations)
	}
}

func (ks *kubernetesScraper) scrapePods(pods []*clustercache.Pod, timestamp time.Time) {
	for _, pod := range pods {
		podInfo := map[string]string{
			"name":      pod.Name,
			"namespace": pod.Namespace,
			"uid":       string(pod.UID),
			"node":      pod.Spec.NodeName,
		}

		// pod labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(pod.Labels)
		podLabels := toMap(labelNames, labelValues)
		ks.collector.Update(KubePodLabels, podInfo, 0, &timestamp, podLabels)

		// pod annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(pod.Annotations)
		podAnnotations := toMap(annotationNames, annotationValues)
		ks.collector.Update(KubePodAnnotations, podInfo, 0, &timestamp, podAnnotations)

		// Pod owner metric
		for _, owner := range pod.OwnerReferences {
			ownerInfo := maps.Clone(podInfo)
			ownerInfo["owner_kind"] = owner.Kind
			ownerInfo["owner_name"] = owner.Name
			ownerInfo["owner_is_controller"] = fmt.Sprintf("%t", owner.Controller != nil)
			ks.collector.Update(KubePodOwner, ownerInfo, 0, &timestamp, nil)
		}

		// Container Status
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Running != nil {
				containerInfo := maps.Clone(podInfo)
				containerInfo["container"] = status.Name
				ks.collector.Update(KubePodContainerStatusRunning, containerInfo, 0, &timestamp, nil)
			}
		}

		for _, container := range pod.Spec.Containers {
			containerInfo := maps.Clone(podInfo)
			containerInfo["container"] = container.Name
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
					resourceRequestInfo["resource"] = resource
					resourceRequestInfo["unit"] = unit
					ks.collector.Update(KubePodContainerResourceRequests, resourceRequestInfo, value, &timestamp, nil)
				}
			}
		}
	}
}

func (ks *kubernetesScraper) scrapePVCs(pvcs []*clustercache.PersistentVolumeClaim, timestamp time.Time) {
	for _, pvc := range pvcs {
		pvcInfo := map[string]string{
			"name":         pvc.Name,
			"namespace":    pvc.Namespace,
			"volumename":   pvc.Spec.VolumeName,
			"storageclass": getPersistentVolumeClaimClass(pvc),
		}

		ks.collector.Update(KubePersistenVolumeClaimInfo, pvcInfo, 0, &timestamp, nil)

		if storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			ks.collector.Update(KubePersistentVolumeClaimResourceRequestsStorageBytes, pvcInfo, float64(storage.Value()), &timestamp, nil)
		}
	}
}

func (ks *kubernetesScraper) scrapePVs(pvs []*clustercache.PersistentVolume, timestamp time.Time) {
	for _, pv := range pvs {
		providerID := pv.Name
		// if a more accurate provider ID is available, use that
		if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
			providerID = pv.Spec.CSI.VolumeHandle
		}
		pvInfo := map[string]string{
			"name":         pv.Name,
			"storageClass": pv.Spec.StorageClassName,
			"providerID":   providerID,
		}

		ks.collector.Update(KubecostPVInfo, pvInfo, 0, &timestamp, nil)

		if storage, ok := pv.Spec.Capacity[v1.ResourceStorage]; ok {
			ks.collector.Update(KubePersistentVolumeCapacityBytes, pvInfo, float64(storage.Value()), &timestamp, nil)
		}
	}
}

func (ks *kubernetesScraper) scrapeServices(services []*clustercache.Service, timestamp time.Time) {
	for _, service := range services {
		serviceInfo := map[string]string{
			"service":   service.Name,
			"namespace": service.Namespace,
		}

		// service labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(service.SpecSelector)
		serviceLabels := toMap(labelNames, labelValues)
		ks.collector.Update(ServiceSelectorLabels, serviceInfo, 0, &timestamp, serviceLabels)

	}
}

func (ks *kubernetesScraper) scrapeStatefulSets(statefulSets []*clustercache.StatefulSet, timestamp time.Time) {
	for _, statefulSet := range statefulSets {
		statefulSetInfo := map[string]string{
			"name":      statefulSet.Name,
			"namespace": statefulSet.Namespace,
		}

		// statefulSet labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(statefulSet.SpecSelector.MatchLabels)
		statefulSetLabels := toMap(labelNames, labelValues)
		ks.collector.Update(StatefulSetMatchLabels, statefulSetInfo, 0, &timestamp, statefulSetLabels)

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
