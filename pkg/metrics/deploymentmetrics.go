package metrics

import (
	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/prom"

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
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kdc KubecostDeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("deployment_match_labels", "deployment match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kdc KubecostDeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	ds := kdc.KubeClusterCache.GetAllDeployments()

	for _, deployment := range ds {
		deploymentName := deployment.GetName()
		deploymentNS := deployment.GetNamespace()

		labels, values := prom.KubeLabelsToLabels(deployment.Spec.Selector.MatchLabels)
		if len(labels) > 0 {
			m := newDeploymentMatchLabelsMetric(deploymentName, deploymentNS, "deployment_match_labels", labels, values)
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
}

// Creates a new DeploymentMatchLabelsMetric, implementation of prometheus.Metric
func newDeploymentMatchLabelsMetric(name, namespace, fqname string, labelNames, labelvalues []string) DeploymentMatchLabelsMetric {
	return DeploymentMatchLabelsMetric{
		fqName:         fqname,
		labelNames:     labelNames,
		labelValues:    labelvalues,
		help:           "deployment_match_labels Deployment Match Labels",
		deploymentName: name,
		namespace:      namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (dmlm DeploymentMatchLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"deployment": dmlm.deploymentName,
		"namespace":  dmlm.namespace,
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
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeDeploymentCollector
//--------------------------------------------------------------------------

// KubeDeploymentCollector is a prometheus collector that generates
type KubeDeploymentCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kdc KubeDeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	// kube_deployment_status_replicas_available
	// kube_deployment_spec_replicas
	// kube_deployment_status_replicas_available
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kdc KubeDeploymentCollector) Collect(ch chan<- prometheus.Metric) {

}
