package metrics

import (
	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/prom"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubeNamespaceCollector
//--------------------------------------------------------------------------

// KubeNamespaceCollector is a prometheus collector that generates namespace sourced metrics
type KubeNamespaceCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNamespaceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_namespace_annotations", "namespace annotations", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNamespaceCollector) Collect(ch chan<- prometheus.Metric) {
	namespaces := nsac.KubeClusterCache.GetAllNamespaces()
	for _, namespace := range namespaces {
		nsName := namespace.GetName()

		labels, values := prom.KubeAnnotationsToLabels(namespace.Annotations)
		if len(labels) > 0 {
			m := newNamespaceAnnotationsMetric(nsName, "kube_namespace_annotations", labels, values)
			ch <- m
		}
	}
}

//--------------------------------------------------------------------------
//  NamespaceAnnotationsMetric
//--------------------------------------------------------------------------

// NamespaceAnnotationsMetric is a prometheus.Metric used to encode namespace annotations
type NamespaceAnnotationsMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	namespace   string
}

// Creates a new NamespaceAnnotationsMetric, implementation of prometheus.Metric
func newNamespaceAnnotationsMetric(namespace, fqname string, labelNames []string, labelValues []string) NamespaceAnnotationsMetric {
	return NamespaceAnnotationsMetric{
		namespace:   namespace,
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_namespace_annotations Namespace Annotations",
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam NamespaceAnnotationsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"namespace": nam.namespace}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam NamespaceAnnotationsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	for i := range nam.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &nam.labelNames[i],
			Value: &nam.labelValues[i],
		})
	}
	labels = append(labels, &dto.LabelPair{
		Name:  toStringPtr("namespace"),
		Value: &nam.namespace,
	})
	m.Label = labels
	return nil
}
