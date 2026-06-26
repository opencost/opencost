package metrics

import (
	"sync"
)

//--------------------------------------------------------------------------
//  Configurable Metric Labels (OTel compatibility)
//--------------------------------------------------------------------------

// Default label names (classic Prometheus/Kubernetes style)
var (
	metricsLabelNode                  = "node"
	metricsLabelNamespace             = "namespace"
	metricsLabelPod                   = "pod"
	metricsLabelContainer             = "container"
	metricsLabelPersistentVolume      = "persistentvolume"
	metricsLabelPersistentVolumeClaim = "persistentvolumeclaim"
)

// otelLabelsInitOnce ensures SetOTelMetricLabels is only called once
var otelLabelsInitOnce sync.Once

// SetOTelMetricLabels configures kube-state-metrics-style metrics to use OTel-style label names.
// This should be called during initialization before metrics are registered.
// This function is safe to call multiple times; it only takes effect on the first call.
func SetOTelMetricLabels() {
	otelLabelsInitOnce.Do(func() {
		metricsLabelNode = "k8s_node_name"
		metricsLabelNamespace = "k8s_namespace_name"
		metricsLabelPod = "k8s_pod_name"
		metricsLabelContainer = "k8s_container_name"
		metricsLabelPersistentVolume = "k8s_persistentvolume_name"
		metricsLabelPersistentVolumeClaim = "k8s_persistentvolumeclaim_name"
	})
}

// GetNodeLabel returns the current label name for node (either "node" or "k8s_node_name")
func GetNodeLabel() string {
	return metricsLabelNode
}

// GetNamespaceLabel returns the current label name for namespace
func GetNamespaceLabel() string {
	return metricsLabelNamespace
}

// GetPodLabel returns the current label name for pod
func GetPodLabel() string {
	return metricsLabelPod
}

// GetContainerLabel returns the current label name for container
func GetContainerLabel() string {
	return metricsLabelContainer
}

// GetPersistentVolumeLabel returns the current label name for persistent volume
func GetPersistentVolumeLabel() string {
	return metricsLabelPersistentVolume
}

// GetPersistentVolumeClaimLabel returns the current label name for persistent volume claim
func GetPersistentVolumeClaimLabel() string {
	return metricsLabelPersistentVolumeClaim
}
