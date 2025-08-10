package metrics

import (
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/util/promutil"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubecostDeploymentCollector
//--------------------------------------------------------------------------

// KubecostDeploymentCollector is a prometheus collector that generates kubecost
// specific deployment metrics.
type KubecostDeploymentCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kdc KubecostDeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["deployment_match_labels"]; disabled {
		return
	}

	ch <- prometheus.NewDesc("deployment_match_labels", "deployment match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kdc KubecostDeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()
	if _, disabled := disabledMetrics["deployment_match_labels"]; disabled {
		return
	}

	ds := kdc.KubeClusterCache.GetAllDeployments()
	for _, deployment := range ds {
		deploymentName := deployment.Name
		deploymentNS := deployment.Namespace
		deploymentUID := string(deployment.UID)

		labels, values := promutil.KubeLabelsToLabels(promutil.SanitizeLabels(deployment.MatchLabels))
		if len(labels) > 0 {
			m := newDeploymentMatchLabelsMetric(deploymentName, deploymentNS, deploymentUID, "deployment_match_labels", labels, values)
			ch <- m
		}
	}

}

//--------------------------------------------------------------------------
//  DeploymentMatchLabelsMetric
//--------------------------------------------------------------------------

// DeploymentMatchLabelsMetric is a prometheus.Metric used to encode deployment match labels
type DeploymentMatchLabelsMetric struct {
	fqName         string
	help           string
	labelNames     []string
	labelValues    []string
	deploymentName string
	namespace      string
	uid            string
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newDeploymentMatchLabelsMetric(name, namespace, uid, fqname string, labelNames, labelvalues []string) DeploymentMatchLabelsMetric {
	return DeploymentMatchLabelsMetric{
		fqName:         fqname,
		labelNames:     labelNames,
		labelValues:    labelvalues,
		help:           "deployment_match_labels Deployment Match Labels",
		deploymentName: name,
		namespace:      namespace,
		uid:            uid,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (dmlm DeploymentMatchLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": dmlm.deploymentName,
		"namespace":  dmlm.namespace,
		"uid":        dmlm.uid,
	}
	return prometheus.NewDesc(dmlm.fqName, dmlm.help, dmlm.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (dmlm DeploymentMatchLabelsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range dmlm.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &dmlm.labelNames[i],
			Value: &dmlm.labelValues[i],
		})
	}
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("namespace"),
		Value: &dmlm.namespace,
	})
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("deployment"),
		Value: &dmlm.deploymentName,
	})
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("uid"),
		Value: &dmlm.uid,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeDeploymentCollector
//--------------------------------------------------------------------------

// KubeDeploymentCollector is a prometheus collector that generates
type KubeDeploymentCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kdc KubeDeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_deployment_spec_replicas"]; !disabled {
		ch <- prometheus.NewDesc("kube_deployment_spec_replicas", "Number of desired pods for a deployment.", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_deployment_status_replicas_available"]; !disabled {
		ch <- prometheus.NewDesc("kube_deployment_status_replicas_available", "The number of available replicas per deployment.", []string{}, nil)
	}

}

// Collect is called by the Prometheus registry when collecting metrics.
func (kdc KubeDeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	deployments := kdc.KubeClusterCache.GetAllDeployments()
	disabledMetrics := kdc.metricsConfig.GetDisabledMetricsMap()

	for _, deployment := range deployments {
		deploymentName := deployment.Name
		deploymentNS := deployment.Namespace
		deploymentUID := string(deployment.UID)

		var replicas int32
		if deployment.SpecReplicas == nil {
			replicas = 1
		} else {
			replicas = *deployment.SpecReplicas
		}

		if _, disabled := disabledMetrics["kube_deployment_spec_replicas"]; !disabled {
			ch <- newKubeDeploymentReplicasMetric("kube_deployment_spec_replicas", deploymentName, deploymentNS, deploymentUID, replicas)
		}
		if _, disabled := disabledMetrics["kube_deployment_status_replicas_available"]; !disabled {
			ch <- newKubeDeploymentStatusAvailableReplicasMetric(
				"kube_deployment_status_replicas_available",
				deploymentName,
				deploymentNS,
				deploymentUID,
				deployment.StatusAvailableReplicas)
		}
	}
}

//--------------------------------------------------------------------------
//  KubeDeploymentReplicasMetric
//--------------------------------------------------------------------------

// KubeDeploymentReplicasMetric is a prometheus.Metric used to encode deployment match labels
type KubeDeploymentReplicasMetric struct {
	fqName     string
	help       string
	deployment string
	namespace  string
	uid        string
	replicas   float64
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newKubeDeploymentReplicasMetric(fqname, deployment, namespace, uid string, replicas int32) KubeDeploymentReplicasMetric {
	return KubeDeploymentReplicasMetric{
		fqName:     fqname,
		help:       "kube_deployment_spec_replicas Number of desired pods for a deployment.",
		deployment: deployment,
		namespace:  namespace,
		uid:        uid,
		replicas:   float64(replicas),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kdr KubeDeploymentReplicasMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": kdr.deployment,
		"namespace":  kdr.namespace,
		"uid":        kdr.uid,
	}
	return prometheus.NewDesc(kdr.fqName, kdr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kdr KubeDeploymentReplicasMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kdr.replicas,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kdr.namespace,
		},
		{
			Name:  toStringPtr("deployment"),
			Value: &kdr.deployment,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kdr.uid,
		},
	}

	return nil
}

//--------------------------------------------------------------------------
//  KubeDeploymentStatusAvailableReplicasMetric
//--------------------------------------------------------------------------

// KubeDeploymentStatusAvailableReplicasMetric is a prometheus.Metric used to encode deployment match labels
type KubeDeploymentStatusAvailableReplicasMetric struct {
	fqName            string
	help              string
	deployment        string
	namespace         string
	uid               string
	replicasAvailable float64
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newKubeDeploymentStatusAvailableReplicasMetric(fqname, deployment, namespace, uid string, replicasAvailable int32) KubeDeploymentStatusAvailableReplicasMetric {
	return KubeDeploymentStatusAvailableReplicasMetric{
		fqName:            fqname,
		help:              "kube_deployment_status_replicas_available The number of available replicas per deployment.",
		deployment:        deployment,
		namespace:         namespace,
		uid:               uid,
		replicasAvailable: float64(replicasAvailable),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kdr KubeDeploymentStatusAvailableReplicasMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": kdr.deployment,
		"namespace":  kdr.namespace,
		"uid":        kdr.uid,
	}
	return prometheus.NewDesc(kdr.fqName, kdr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kdr KubeDeploymentStatusAvailableReplicasMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kdr.replicasAvailable,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kdr.namespace,
		},
		{
			Name:  toStringPtr("deployment"),
			Value: &kdr.deployment,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kdr.uid,
		},
	}

	return nil
}
