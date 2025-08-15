package metrics

import (
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubeReplicationControllerCollector
//--------------------------------------------------------------------------

// KubeReplicationControllerCollector is a prometheus collector that generates replication controller metrics
type KubeReplicationControllerCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (krc KubeReplicationControllerCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := krc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_replicationcontroller_info"]; !disabled {
		ch <- prometheus.NewDesc("kube_replicationcontroller_info", "Information about replication controller including UID", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_replicationcontroller_spec_replicas"]; !disabled {
		ch <- prometheus.NewDesc("kube_replicationcontroller_spec_replicas", "Number of desired pods for a replication controller", []string{}, nil)
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (krc KubeReplicationControllerCollector) Collect(ch chan<- prometheus.Metric) {
	replicationControllers := krc.KubeClusterCache.GetAllReplicationControllers()
	disabledMetrics := krc.metricsConfig.GetDisabledMetricsMap()

	for _, rc := range replicationControllers {
		rcName := rc.Name
		rcNS := rc.Namespace
		rcUID := string(rc.UID)

		// Replication controller info metric with UID
		if _, disabled := disabledMetrics["kube_replicationcontroller_info"]; !disabled {
			ch <- newKubeReplicationControllerInfoMetric("kube_replicationcontroller_info", rcName, rcNS, rcUID)
		}

		// Replicas defined
		if _, disabled := disabledMetrics["kube_replicationcontroller_spec_replicas"]; !disabled {
			var replicas int32
			if rc.Spec.Replicas == nil {
				replicas = 1 // defaults to 1
			} else {
				replicas = *rc.Spec.Replicas
			}
			ch <- newKubeReplicationControllerReplicasMetric("kube_replicationcontroller_spec_replicas", rcName, rcNS, rcUID, replicas)
		}
	}
}

//--------------------------------------------------------------------------
//  KubeReplicationControllerInfoMetric
//--------------------------------------------------------------------------

// KubeReplicationControllerInfoMetric is a prometheus.Metric used to encode replication controller info with UID
type KubeReplicationControllerInfoMetric struct {
	fqName                    string
	help                      string
	replicationControllerName string
	namespace                 string
	uid                       string
}

// Creates a new KubeReplicationControllerInfoMetric, implementation of prometheus.Metric
func newKubeReplicationControllerInfoMetric(fqname, replicationControllerName, namespace, uid string) KubeReplicationControllerInfoMetric {
	return KubeReplicationControllerInfoMetric{
		fqName:                    fqname,
		help:                      "Information about replication controller including UID",
		replicationControllerName: replicationControllerName,
		namespace:                 namespace,
		uid:                       uid,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (krcim KubeReplicationControllerInfoMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"replicationcontroller": krcim.replicationControllerName,
		"namespace":             krcim.namespace,
		"uid":                   krcim.uid,
	}
	return prometheus.NewDesc(krcim.fqName, krcim.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (krcim KubeReplicationControllerInfoMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &krcim.namespace,
		},
		{
			Name:  toStringPtr("replicationcontroller"),
			Value: &krcim.replicationControllerName,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &krcim.uid,
		},
	}

	return nil
}

//--------------------------------------------------------------------------
//  KubeReplicationControllerReplicasMetric
//--------------------------------------------------------------------------

// KubeReplicationControllerReplicasMetric is a prometheus.Metric used to encode replication controller replicas
type KubeReplicationControllerReplicasMetric struct {
	fqName                    string
	help                      string
	replicationControllerName string
	namespace                 string
	uid                       string
	replicas                  float64
}

// Creates a new KubeReplicationControllerReplicasMetric, implementation of prometheus.Metric
func newKubeReplicationControllerReplicasMetric(fqname, replicationControllerName, namespace, uid string, replicas int32) KubeReplicationControllerReplicasMetric {
	return KubeReplicationControllerReplicasMetric{
		fqName:                    fqname,
		help:                      "kube_replicationcontroller_spec_replicas Number of desired pods for a replication controller",
		replicationControllerName: replicationControllerName,
		namespace:                 namespace,
		uid:                       uid,
		replicas:                  float64(replicas),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (krcrm KubeReplicationControllerReplicasMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"replicationcontroller": krcrm.replicationControllerName,
		"namespace":             krcrm.namespace,
		"uid":                   krcrm.uid,
	}
	return prometheus.NewDesc(krcrm.fqName, krcrm.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (krcrm KubeReplicationControllerReplicasMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &krcrm.replicas,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &krcrm.namespace,
		},
		{
			Name:  toStringPtr("replicationcontroller"),
			Value: &krcrm.replicationControllerName,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &krcrm.uid,
		},
	}

	return nil
}
