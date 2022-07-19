package metrics

import (
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/prometheus/client_golang/prometheus"
)

//--------------------------------------------------------------------------
//  KubecostPodCollector
//--------------------------------------------------------------------------

// KubecostPodCollector is a prometheus collector that emits pod metrics
type KubecostPodLabelsCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpmc KubecostPodLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_pod_annotations", "All annotations for each pod prefix with annotation_", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpmc KubecostPodLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	pods := kpmc.KubeClusterCache.GetAllPods()
	for _, pod := range pods {
		podName := pod.GetName()
		podNS := pod.GetNamespace()

		// Pod Annotations
		labels, values := prom.KubeAnnotationsToLabels(pod.Annotations)
		if len(labels) > 0 {
			ch <- newPodAnnotationMetric("kube_pod_annotations", podNS, podName, labels, values)
		}
	}
}

//--------------------------------------------------------------------------
//  KubePodLabelsCollector
//--------------------------------------------------------------------------

// KubePodLabelsCollector is a prometheus collector that emits pod labels only
type KubePodLabelsCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of pod labels only
// collected by this Collector.
func (kpmc KubePodLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_pod_labels"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_labels", "All labels for each pod prefixed with label_", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_pod_owner"]; !disabled {
		ch <- prometheus.NewDesc("kube_pod_owner", "Information about the Pod's owner", []string{}, nil)
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpmc KubePodLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	pods := kpmc.KubeClusterCache.GetAllPods()
	disabledMetrics := kpmc.metricsConfig.GetDisabledMetricsMap()

	for _, pod := range pods {
		podName := pod.GetName()
		podNS := pod.GetNamespace()
		podUID := string(pod.GetUID())

		// Pod Labels
		if _, disabled := disabledMetrics["kube_pod_labels"]; !disabled {
			labelNames, labelValues := prom.KubePrependQualifierToLabels(pod.GetLabels(), "label_")
			ch <- newKubePodLabelsMetric("kube_pod_labels", podNS, podName, podUID, labelNames, labelValues)
		}

		// Owner References
		if _, disabled := disabledMetrics["kube_pod_owner"]; !disabled {
			for _, owner := range pod.OwnerReferences {
				ch <- newKubePodOwnerMetric("kube_pod_owner", podNS, podName, owner.Name, owner.Kind, owner.Controller != nil)
			}
		}
	}
}
