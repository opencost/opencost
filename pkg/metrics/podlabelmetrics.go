package metrics

import (
	"github.com/opencost/opencost/core/pkg/util/promutil"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
)

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
			labelNames, labelValues := promutil.KubePrependQualifierToLabels(promutil.SanitizeLabels(pod.GetLabels()), "label_")
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
