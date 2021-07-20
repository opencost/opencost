package metrics

import (
	"strings"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
)

var (
	conditionStatuses = []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionFalse, v1.ConditionUnknown}
)

//--------------------------------------------------------------------------
//  KubeNodeCollector
//--------------------------------------------------------------------------

// KubeNodeCollector is a prometheus collector that generates node sourced metrics.
type KubeNodeCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_node_status_capacity_memory_bytes", "node capacity memory bytes", []string{}, nil)
	ch <- prometheus.NewDesc("kube_node_status_capacity_cpu_cores", "node capacity cpu cores", []string{}, nil)
	ch <- prometheus.NewDesc("kube_node_labels", "all labels for each node prefixed with label_", []string{}, nil)
	ch <- prometheus.NewDesc("kube_node_status_condition", "The condition of a cluster node.", []string{}, nil)
	ch <- prometheus.NewDesc("kube_node_status_allocatable", "The allocatable for different resources of a node that are available for scheduling.", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNodeCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := nsac.KubeClusterCache.GetAllNodes()
	for _, node := range nodes {
		nodeName := node.GetName()

		// k8s.io/apimachinery/pkg/api/resource/amount.go and
		// k8s.io/apimachinery/pkg/api/resource/quantity.go for
		// details on the "amount" API. See
		// https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-types
		// for the units of memory and CPU.
		memoryBytes := node.Status.Capacity.Memory().Value()
		ch <- newKubeNodeStatusCapacityMemoryBytesMetric(nodeName, memoryBytes, "kube_node_status_capacity_memory_bytes", nil, nil)

		cpuCores := float64(node.Status.Capacity.Cpu().MilliValue()) / 1000
		ch <- newKubeNodeStatusCapacityCPUCoresMetric(nodeName, cpuCores, "kube_node_status_capacity_cpu_cores", nil, nil)

		// allocatable resources
		for resourceName, quantity := range node.Status.Allocatable {
			resource, unit, value := toResourceUnitValue(resourceName, quantity)

			// failed to parse the resource type
			if resource == "" {
				log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
				continue
			}

			ch <- newKubeNodeStatusAllocatableMetric("kube_node_status_allocatable", nodeName, resource, unit, value)
		}

		// node labels
		labelNames, labelValues := prom.KubePrependQualifierToLabels(node.GetLabels(), "label_")
		ch <- newKubeNodeLabelsMetric(nodeName, "kube_node_labels", labelNames, labelValues)

		// kube_node_status_condition
		// Collect node conditions and while default to false.
		for _, c := range node.Status.Conditions {
			conditions := getConditions(c.Status)

			for _, cond := range conditions {
				ch <- newKubeNodeStatusConditionMetric(nodeName, "kube_node_status_condition", string(c.Type), cond.status, cond.value)
			}
		}

	}
}

//--------------------------------------------------------------------------
//  KubeNodeStatusCapacityMemoryBytesMetric
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityMemoryBytesMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_node_status_capacity_memory_bytes
type KubeNodeStatusCapacityMemoryBytesMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	bytes       int64
	node        string
}

// Creates a new KubeNodeStatusCapacityMemoryBytesMetric, implementation of prometheus.Metric
func newKubeNodeStatusCapacityMemoryBytesMetric(node string, bytes int64, fqname string, labelNames []string, labelValues []string) KubeNodeStatusCapacityMemoryBytesMetric {
	return KubeNodeStatusCapacityMemoryBytesMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_node_status_capacity_memory_bytes Node Capacity Memory Bytes",
		bytes:       bytes,
		node:        node,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeStatusCapacityMemoryBytesMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"node": nam.node}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeStatusCapacityMemoryBytesMetric) Write(m *dto.Metric) error {
	h := float64(nam.bytes)
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
	n := "node"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &nam.node,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeStatusCapacityCPUCoresMetric
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityCPUCoresMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_node_status_capacity_memory_bytes
type KubeNodeStatusCapacityCPUCoresMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	cores       float64
	node        string
}

// Creates a new KubeNodeStatusCapacityCPUCoresMetric, implementation of prometheus.Metric
func newKubeNodeStatusCapacityCPUCoresMetric(node string, cores float64, fqname string, labelNames []string, labelValues []string) KubeNodeStatusCapacityCPUCoresMetric {
	return KubeNodeStatusCapacityCPUCoresMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_node_status_capacity_cpu_cores Node Capacity CPU Cores",
		cores:       cores,
		node:        node,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeStatusCapacityCPUCoresMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"node": nam.node}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeStatusCapacityCPUCoresMetric) Write(m *dto.Metric) error {
	h := nam.cores
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
	n := "node"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &nam.node,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeLabelsCollector
//--------------------------------------------------------------------------
//
// We use this to emit kube_node_labels with all of a node's labels, regardless
// of the whitelist setting introduced in KSM v2. See
// https://github.com/kubernetes/kube-state-metrics/issues/1270#issuecomment-712986441

//--------------------------------------------------------------------------
//  KubeNodeLabelsMetric
//--------------------------------------------------------------------------

// KubeNodeLabelsMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_node_labels
type KubeNodeLabelsMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	node        string
}

// Creates a new KubeNodeLabelsMetric, implementation of prometheus.Metric
func newKubeNodeLabelsMetric(node string, fqname string, labelNames []string, labelValues []string) KubeNodeLabelsMetric {
	return KubeNodeLabelsMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_node_labels all labels for each node prefixed with label_",
		node:        node,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node": nam.node,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeLabelsMetric) Write(m *dto.Metric) error {
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

	nodeString := "node"
	labels = append(labels, &dto.LabelPair{Name: &nodeString, Value: &nam.node})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeStatusConditionMetric
//--------------------------------------------------------------------------

// KubeNodeStatusConditionMetric
type KubeNodeStatusConditionMetric struct {
	fqName    string
	help      string
	node      string
	condition string
	status    string
	value     float64
}

// Creates a new KubeNodeStatusConditionMetric, implementation of prometheus.Metric
func newKubeNodeStatusConditionMetric(node, fqname, condition, status string, value float64) KubeNodeStatusConditionMetric {
	return KubeNodeStatusConditionMetric{
		fqName:    fqname,
		help:      "kube_node_status_condition condition status for nodes",
		node:      node,
		condition: condition,
		status:    status,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubeNodeStatusConditionMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node":      nam.node,
		"condition": nam.condition,
		"status":    nam.status,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubeNodeStatusConditionMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &nam.value,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &nam.node,
		},
		{
			Name:  toStringPtr("condition"),
			Value: &nam.condition,
		},
		{
			Name:  toStringPtr("status"),
			Value: &nam.status,
		},
	}
	return nil
}

// helper type for status condition reporting and metric rollup
type statusCondition struct {
	status string
	value  float64
}

// retrieves the total status conditions and the comparison to the provided condition
func getConditions(cs v1.ConditionStatus) []*statusCondition {
	ms := make([]*statusCondition, len(conditionStatuses))

	for i, status := range conditionStatuses {
		ms[i] = &statusCondition{
			status: strings.ToLower(string(status)),
			value:  boolFloat64(cs == status),
		}
	}

	return ms
}

//--------------------------------------------------------------------------
//  KubeNodeStatusAllocatableMetric
//--------------------------------------------------------------------------

// KubeNodeStatusAllocatableMetric is a prometheus.Metric
type KubeNodeStatusAllocatableMetric struct {
	fqName   string
	help     string
	resource string
	unit     string
	node     string
	value    float64
}

// Creates a new KubeNodeStatusAllocatableMetric, implementation of prometheus.Metric
func newKubeNodeStatusAllocatableMetric(fqname, node, resource, unit string, value float64) KubeNodeStatusAllocatableMetric {
	return KubeNodeStatusAllocatableMetric{
		fqName:   fqname,
		help:     "kube_node_status_allocatable node allocatable",
		node:     node,
		resource: resource,
		unit:     unit,
		value:    value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubeNodeStatusAllocatableMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"node":     kpcrr.node,
		"resource": kpcrr.resource,
		"unit":     kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcrr KubeNodeStatusAllocatableMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("node"),
			Value: &kpcrr.node,
		},
		{
			Name:  toStringPtr("resource"),
			Value: &kpcrr.resource,
		},
		{
			Name:  toStringPtr("unit"),
			Value: &kpcrr.unit,
		},
	}
	return nil
}
