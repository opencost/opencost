package collector

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"
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

	ks.scrapeNodes()
}

func (ks *kubernetesScraper) scrapeNodes() {
	timeStamp := time.Now().UTC()
	nodes := ks.clusterCache.GetAllNodes()
	for _, node := range nodes {
		nodeInfo := map[string]string{
			"node":        node.Name,
			"provider_id": node.SpecProviderID,
		}

		// Node Capacity
		for resourceName, quantity := range node.Status.Capacity {
			resource, _, value := toResourceUnitValue(resourceName, quantity)

			// failed to parse the resource type
			if resource == "" {
				log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
				continue
			}

			// KSM v1 Emission
			if resource == "cpu" {
				ks.collector.Update(KubeNodeStatusCapacityCPUCores, nodeInfo, value, &timeStamp)

			}

			if resource == "memory" {
				ks.collector.Update(KubeNodeStatusCapacityMemoryBytes, nodeInfo, value, &timeStamp)
			}
		}

		// Node Allocatable Resources
		for resourceName, quantity := range node.Status.Allocatable {
			resource, _, value := toResourceUnitValue(resourceName, quantity)

			// failed to parse the resource type
			if resource == "" {
				log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
				continue
			}

			// KSM v1 Emission
			if resource == "cpu" {
				ks.collector.Update(KubeNodeStatusAllocatableCPUCores, nodeInfo, value, &timeStamp)
			}
			if resource == "memory" {
				ks.collector.Update(KubeNodeStatusAllocatableMemoryBytes, nodeInfo, value, &timeStamp)
			}

		}

		// node labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(node.Labels)
		nodeLabels := maps.Clone(nodeInfo)
		for i, labelName := range labelNames {
			nodeLabels[labelName] = labelValues[i]
		}
		ks.collector.Update(KubeNodeLabels, nodeLabels, 0, &timeStamp)

	}
}

func (ks *kubernetesScraper) scrapeDeployments() {
	timeStamp := time.Now().UTC()
	deployments := ks.clusterCache.GetAllDeployments()
	for _, deployment := range deployments {
		deploymentInfo := map[string]string{
			"deployment": deployment.Name,
			"namespace":  deployment.Namespace,
		}

		// deployment labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(deployment.MatchLabels)
		deploymentLabels := maps.Clone(deploymentInfo)
		for i, labelName := range labelNames {
			deploymentLabels[labelName] = labelValues[i]
		}
		ks.collector.Update(DeploymentMatchLabels, deploymentLabels, 0, &timeStamp)

	}
}

func (ks *kubernetesScraper) scrapeNamespaces() {
	timeStamp := time.Now().UTC()
	namespaces := ks.clusterCache.GetAllNamespaces()
	for _, namespace := range namespaces {
		namespaceInfo := map[string]string{
			"namespace": namespace.Name,
		}

		// namespace labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(namespace.Labels)
		namespaceLabels := maps.Clone(namespaceInfo)
		for i, labelName := range labelNames {
			namespaceLabels[labelName] = labelValues[i]
		}
		ks.collector.Update(KubeNamespaceLabels, namespaceLabels, 0, &timeStamp)

		// namespace annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(namespace.Labels)
		namespaceAnnotations := maps.Clone(namespaceInfo)
		for i, annotationName := range annotationNames {
			namespaceAnnotations[annotationName] = annotationValues[i]
		}
		ks.collector.Update(KubeNamespaceAnnotations, namespaceAnnotations, 0, &timeStamp)
	}
}

func (ks *kubernetesScraper) scrapePods() {
	timeStamp := time.Now().UTC()
	pods := ks.clusterCache.GetAllPods()
	for _, pod := range pods {
		podInfo := map[string]string{
			"name":      pod.Name,
			"namespace": pod.Namespace,
			"uid":       string(pod.UID),
			"node":      pod.Spec.NodeName,
		}

		// pod labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(pod.Labels)
		podLabels := maps.Clone(podInfo)
		for i, labelName := range labelNames {
			podLabels[labelName] = labelValues[i]
		}
		ks.collector.Update(KubePodLabels, podLabels, 0, &timeStamp)

		// pod annotations
		annotationNames, annotationValues := promutil.KubeAnnotationsToLabels(pod.Labels)
		podAnnotations := maps.Clone(podInfo)
		for i, annotationName := range annotationNames {
			podAnnotations[annotationName] = annotationValues[i]
		}
		ks.collector.Update(KubePodAnnotations, podAnnotations, 0, &timeStamp)

		// Pod owner metric
		for _, owner := range pod.OwnerReferences {
			ownerInfo := maps.Clone(podInfo)
			ownerInfo["owner_kind"] = owner.Kind
			ownerInfo["owner_name"] = owner.Name
			ownerInfo["owner_is_controller"] = fmt.Sprintf("%t", owner.Controller != nil)
			ks.collector.Update(KubePodOwner, ownerInfo, 0, &timeStamp)
		}

		// Container Status
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Running != nil {
				containerInfo := maps.Clone(podInfo)
				containerInfo["container"] = status.Name
				ks.collector.Update(KubePodContainerStatusRunning, containerInfo, 0, &timeStamp)
			}
		}

		for _, container := range pod.Spec.Containers {
			containerInfo := maps.Clone(podInfo)
			containerInfo["container"] = container.Name
			// Requests
			for resourceName, quantity := range container.Resources.Requests {
				resource, unit, value := toResourceUnitValue(resourceName, quantity)

				// failed to parse the resource type
				if resource == "" {
					log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
					continue
				}

				resourceRequestInfo := maps.Clone(containerInfo)
				resourceRequestInfo["resource"] = resource
				resourceRequestInfo["unit"] = unit
				ks.collector.Update(KubePodContainerResourceRequests, resourceRequestInfo, value, &timeStamp)
			}
		}
	}
}

func (ks *kubernetesScraper) scrapePVCs() {
	timeStamp := time.Now().UTC()
	pvcs := ks.clusterCache.GetAllPersistentVolumeClaims()
	for _, pvc := range pvcs {
		pvcInfo := map[string]string{
			"name":         pvc.Name,
			"namespace":    pvc.Namespace,
			"volumename":   pvc.Spec.VolumeName,
			"storageclass": getPersistentVolumeClaimClass(pvc),
		}

		ks.collector.Update(KubePersistenVolumeClaimInfo, pvcInfo, 0, &timeStamp)

		if storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			ks.collector.Update(KubePersistentVolumeClaimResourceRequestsStorageBytes, pvcInfo, float64(storage.Value()), &timeStamp)
		}

	}
}

func (ks *kubernetesScraper) scrapePVs() {
	timeStamp := time.Now().UTC()
	pvs := ks.clusterCache.GetAllPersistentVolumes()
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

		ks.collector.Update(KubecostPVInfo, pvInfo, 0, &timeStamp)

		if storage, ok := pv.Spec.Capacity[v1.ResourceStorage]; ok {
			ks.collector.Update(KubePersistentVolumeCapacityBytes, pvInfo, float64(storage.Value()), &timeStamp)
		}
	}
}

func (ks *kubernetesScraper) scrapeServices() {
	timeStamp := time.Now().UTC()
	services := ks.clusterCache.GetAllServices()
	for _, service := range services {
		serviceInfo := map[string]string{
			"service":   service.Name,
			"namespace": service.Namespace,
		}

		// service labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(service.SpecSelector)
		serviceLabels := maps.Clone(serviceInfo)
		for i, labelName := range labelNames {
			serviceLabels[labelName] = labelValues[i]
		}
		ks.collector.Update(ServiceSelectorLabels, serviceLabels, 0, &timeStamp)

	}
}

func (ks *kubernetesScraper) scrapeStatefulSets() {
	timeStamp := time.Now().UTC()
	statefulSets := ks.clusterCache.GetAllStatefulSets()
	for _, statefulSet := range statefulSets {
		statefulSetInfo := map[string]string{
			"name":      statefulSet.Name,
			"namespace": statefulSet.Namespace,
		}

		// statefulSet labels
		labelNames, labelValues := promutil.KubeLabelsToLabels(statefulSet.SpecSelector.MatchLabels)
		statefulSetLabels := maps.Clone(statefulSetInfo)
		for i, labelName := range labelNames {
			statefulSetLabels[labelName] = labelValues[i]
		}
		ks.collector.Update(StatefulSetMatchLabels, statefulSetLabels, 0, &timeStamp)

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
