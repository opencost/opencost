package metrics

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

//--------------------------------------------------------------------------
//  StatefulsetCollector
//--------------------------------------------------------------------------

// StatefulsetCollector is a prometheus collector that generates StatefulsetMetrics
type StatefulsetCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc StatefulsetCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("statefulSet_match_labels", "statfulSet match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc StatefulsetCollector) Collect(ch chan<- prometheus.Metric) {
	ds := sc.KubeClusterCache.GetAllStatefulSets()
	for _, statefulset := range ds {
		labels, values := prom.KubeLabelsToLabels(statefulset.Spec.Selector.MatchLabels)
		if len(labels) > 0 {
			m := newStatefulsetMetric(statefulset.GetName(), statefulset.GetNamespace(), "statefulSet_match_labels", labels, values)
			ch <- m
		}
	}
}

//--------------------------------------------------------------------------
//  StatefulsetMetric
//--------------------------------------------------------------------------

// StatefulsetMetric is a prometheus.Metric used to encode statefulset match labels
type StatefulsetMetric struct {
	fqName          string
	help            string
	labelNames      []string
	labelValues     []string
	statefulsetName string
	namespace       string
}

// Creates a new StatefulsetMetric, implementation of prometheus.Metric
func newStatefulsetMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) StatefulsetMetric {
	return StatefulsetMetric{
		fqName:          fqname,
		labelNames:      labelNames,
		labelValues:     labelvalues,
		help:            "statefulSet_match_labels StatefulSet Match Labels",
		statefulsetName: name,
		namespace:       namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s StatefulsetMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"statefulSet": s.statefulsetName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s StatefulsetMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "statefulSet"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.statefulsetName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  DeploymentCollector
//--------------------------------------------------------------------------

// DeploymentCollector is a prometheus collector that generates DeploymentMetrics
type DeploymentCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc DeploymentCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("deployment_match_labels", "deployment match labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc DeploymentCollector) Collect(ch chan<- prometheus.Metric) {
	ds := sc.KubeClusterCache.GetAllDeployments()
	for _, deployment := range ds {
		labels, values := prom.KubeLabelsToLabels(deployment.Spec.Selector.MatchLabels)
		if len(labels) > 0 {
			m := newDeploymentMetric(deployment.GetName(), deployment.GetNamespace(), "deployment_match_labels", labels, values)
			ch <- m
		}
	}
}

//--------------------------------------------------------------------------
//  DeploymentMetric
//--------------------------------------------------------------------------

// DeploymentMetric is a prometheus.Metric used to encode deployment match labels
type DeploymentMetric struct {
	fqName         string
	help           string
	labelNames     []string
	labelValues    []string
	deploymentName string
	namespace      string
}

// Creates a new DeploymentMetric, implementation of prometheus.Metric
func newDeploymentMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) DeploymentMetric {
	return DeploymentMetric{
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
func (s DeploymentMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"deployment": s.deploymentName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s DeploymentMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "deployment"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.deploymentName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  ServiceCollector
//--------------------------------------------------------------------------

// ServiceCollector is a prometheus collector that generates ServiceMetrics
type ServiceCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (sc ServiceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("service_selector_labels", "service selector labels", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (sc ServiceCollector) Collect(ch chan<- prometheus.Metric) {
	svcs := sc.KubeClusterCache.GetAllServices()
	for _, svc := range svcs {
		labels, values := prom.KubeLabelsToLabels(svc.Spec.Selector)
		if len(labels) > 0 {
			m := newServiceMetric(svc.GetName(), svc.GetNamespace(), "service_selector_labels", labels, values)
			ch <- m
		}
	}
}

//--------------------------------------------------------------------------
//  ServiceMetric
//--------------------------------------------------------------------------

// ServiceMetric is a prometheus.Metric used to encode service selector labels
type ServiceMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	serviceName string
	namespace   string
}

// Creates a new ServiceMetric, implementation of prometheus.Metric
func newServiceMetric(name, namespace, fqname string, labelNames []string, labelvalues []string) ServiceMetric {
	return ServiceMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelvalues,
		help:        "service_selector_labels Service Selector Labels",
		serviceName: name,
		namespace:   namespace,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (s ServiceMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"service": s.serviceName, "namespace": s.namespace}
	return prometheus.NewDesc(s.fqName, s.help, s.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (s ServiceMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	var labels []*dto.LabelPair
	for i := range s.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &s.labelNames[i],
			Value: &s.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &s.namespace,
	})
	r := "service"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &s.serviceName,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodMetricCollector
//--------------------------------------------------------------------------

// KubePodMetricCollector is a prometheus collector that emits pod metrics
type KubePodMetricCollector struct {
	KubeClusterCache   clustercache.ClusterCache
	emitPodAnnotations bool
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpmc KubePodMetricCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_pod_labels", "All labels for each pod prefixed with label_", []string{}, nil)
	if kpmc.emitPodAnnotations {
		ch <- prometheus.NewDesc("kube_pod_annotations", "All annotations for each pod prefix with annotation_", []string{}, nil)
	}
	ch <- prometheus.NewDesc("kube_pod_owner", "Information about the Pod's owner", []string{}, nil)
	ch <- prometheus.NewDesc("kube_pod_container_status_running", "Describes whether the container is currently in running state", []string{}, nil)
	ch <- prometheus.NewDesc("kube_pod_container_status_terminated_reason", "Describes the reason the container is currently in terminated state.", []string{}, nil)
	ch <- prometheus.NewDesc("kube_pod_container_status_restarts_total", "The number of container restarts per container.", []string{}, nil)
	ch <- prometheus.NewDesc("kube_pod_container_resource_requests", "The number of requested resource by a container", []string{}, nil)
	ch <- prometheus.NewDesc("kube_pod_container_resource_limits", "The number of requested limit resource by a container.", []string{}, nil)
	ch <- prometheus.NewDesc("kube_pod_status_phase", "The pods current phase.", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpmc KubePodMetricCollector) Collect(ch chan<- prometheus.Metric) {
	pods := kpmc.KubeClusterCache.GetAllPods()
	for _, pod := range pods {
		podName := pod.GetName()
		podNS := pod.GetNamespace()
		podUID := string(pod.GetUID())
		node := pod.Spec.NodeName
		phase := pod.Status.Phase

		// Pod Status Phase
		if phase != "" {
			phases := []struct {
				v bool
				n string
			}{
				{phase == v1.PodPending, string(v1.PodPending)},
				{phase == v1.PodSucceeded, string(v1.PodSucceeded)},
				{phase == v1.PodFailed, string(v1.PodFailed)},
				{phase == v1.PodUnknown, string(v1.PodUnknown)},
				{phase == v1.PodRunning, string(v1.PodRunning)},
			}

			for _, p := range phases {
				ch <- newKubePodStatusPhaseMetric("kube_pod_status_phase", podName, podNS, podUID, p.n, boolFloat64(p.v))
			}
		}

		// Pod Labels
		labelNames, labelValues := prom.KubePrependQualifierToLabels(pod.GetLabels(), "label_")
		ch <- newKubePodLabelsMetric(podName, podNS, podUID, "kube_pod_labels", labelNames, labelValues)

		// Pod Annotations
		if kpmc.emitPodAnnotations {
			labels, values := prom.KubeAnnotationsToLabels(pod.Annotations)

			if len(labels) > 0 {
				ch <- newPodAnnotationMetric(podNS, podName, "kube_pod_annotations", labels, values)
			}
		}

		// Owner References
		for _, owner := range pod.OwnerReferences {
			ch <- newKubePodOwnerMetric("kube_pod_owner", podNS, podName, owner.Name, owner.Kind, owner.Controller != nil)
		}

		// Container Status
		for _, status := range pod.Status.ContainerStatuses {
			ch <- newKubePodContainerStatusRestartsTotalMetric("kube_pod_container_status_restarts_total", podName, podNS, podUID, status.Name, float64(status.RestartCount))
			if status.State.Running != nil {
				ch <- newKubePodContainerStatusRunningMetric("kube_pod_container_status_running", podName, podNS, podUID, status.Name)
			}

			if status.State.Terminated != nil {
				ch <- newKubePodContainerStatusTerminatedReasonMetric(
					"kube_pod_container_status_terminated_reason",
					podName,
					podNS,
					podUID,
					status.Name,
					status.State.Terminated.Reason)
			}
		}

		for _, container := range pod.Spec.Containers {
			// Requests
			for resourceName, quantity := range container.Resources.Requests {
				resource, unit, value := toResourceUnitValue(resourceName, quantity)

				// failed to parse the resource type
				if resource == "" {
					log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
					continue
				}

				ch <- newKubePodContainerResourceRequestsMetric(
					"kube_pod_container_resource_requests",
					podName,
					podNS,
					podUID,
					container.Name,
					node,
					resource,
					unit,
					value)
			}

			// Limits
			for resourceName, quantity := range container.Resources.Limits {
				resource, unit, value := toResourceUnitValue(resourceName, quantity)

				// failed to parse the resource type
				if resource == "" {
					log.DedupedWarningf(5, "Failed to parse resource units and quantity for resource: %s", resourceName)
					continue
				}

				ch <- newKubePodContainerResourceLimitsMetric(
					"kube_pod_container_resource_limits",
					podName,
					podNS,
					podUID,
					container.Name,
					node,
					resource,
					unit,
					value)
			}
		}
	}
}

//--------------------------------------------------------------------------
//  NamespaceAnnotationCollector
//--------------------------------------------------------------------------

// NamespaceAnnotationCollector is a prometheus collector that generates NamespaceAnnotationMetrics
type NamespaceAnnotationCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac NamespaceAnnotationCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_namespace_annotations", "namespace annotations", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac NamespaceAnnotationCollector) Collect(ch chan<- prometheus.Metric) {
	namespaces := nsac.KubeClusterCache.GetAllNamespaces()
	for _, namespace := range namespaces {
		labels, values := prom.KubeAnnotationsToLabels(namespace.Annotations)
		if len(labels) > 0 {
			m := newNamespaceAnnotationsMetric(namespace.GetName(), "kube_namespace_annotations", labels, values)
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
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &nam.namespace,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  PodAnnotationsMetric
//--------------------------------------------------------------------------

// PodAnnotationsMetric is a prometheus.Metric used to encode namespace annotations
type PodAnnotationsMetric struct {
	name        string
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	namespace   string
}

// Creates a new PodAnnotationsMetric, implementation of prometheus.Metric
func newPodAnnotationMetric(namespace, name, fqname string, labelNames []string, labelValues []string) PodAnnotationsMetric {
	return PodAnnotationsMetric{
		namespace:   namespace,
		name:        name,
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_pod_annotations Pod Annotations",
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (pam PodAnnotationsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{"namespace": pam.namespace, "pod": pam.name}
	return prometheus.NewDesc(pam.fqName, pam.help, pam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (pam PodAnnotationsMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	for i := range pam.labelNames {
		labels = append(labels, &dto.LabelPair{
			Name:  &pam.labelNames[i],
			Value: &pam.labelValues[i],
		})
	}
	n := "namespace"
	labels = append(labels, &dto.LabelPair{
		Name:  &n,
		Value: &pam.namespace,
	})
	r := "pod"
	labels = append(labels, &dto.LabelPair{
		Name:  &r,
		Value: &pam.name,
	})
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubeNodeStatusCapacityMemoryBytesCollector
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityMemoryBytesCollector is a prometheus collector that generates
// KubeNodeStatusCapacityMemoryBytesMetrics
type KubeNodeStatusCapacityMemoryBytesCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNodeStatusCapacityMemoryBytesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_node_status_capacity_memory_bytes", "node capacity memory bytes", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNodeStatusCapacityMemoryBytesCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := nsac.KubeClusterCache.GetAllNodes()
	for _, node := range nodes {
		// k8s.io/apimachinery/pkg/api/resource/amount.go and
		// k8s.io/apimachinery/pkg/api/resource/quantity.go for
		// details on the "amount" API. See
		// https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-types
		// for the units of memory and CPU.
		memoryBytes := node.Status.Capacity.Memory().Value()

		m := newKubeNodeStatusCapacityMemoryBytesMetric(node.GetName(), memoryBytes, "kube_node_status_capacity_memory_bytes", nil, nil)
		ch <- m
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
//  KubeNodeStatusCapacityCPUCoresCollector
//--------------------------------------------------------------------------

// KubeNodeStatusCapacityCPUCoresCollector is a prometheus collector that generates
// KubeNodeStatusCapacityCPUCoresMetrics
type KubeNodeStatusCapacityCPUCoresCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNodeStatusCapacityCPUCoresCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_node_status_capacity_cpu_cores", "node capacity cpu cores", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNodeStatusCapacityCPUCoresCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := nsac.KubeClusterCache.GetAllNodes()
	for _, node := range nodes {
		// k8s.io/apimachinery/pkg/api/resource/amount.go and
		// k8s.io/apimachinery/pkg/api/resource/quantity.go for
		// details on the "amount" API. See
		// https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-types
		// for the units of memory and CPU.
		cpuCores := float64(node.Status.Capacity.Cpu().MilliValue()) / 1000

		m := newKubeNodeStatusCapacityCPUCoresMetric(node.GetName(), cpuCores, "kube_node_status_capacity_cpu_cores", nil, nil)
		ch <- m
	}
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
//  KubePodLabelsMetric
//--------------------------------------------------------------------------

// KubePodLabelsMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_pod_labels
type KubePodLabelsMetric struct {
	fqName      string
	help        string
	labelNames  []string
	labelValues []string
	pod         string
	namespace   string
	uid         string
}

// Creates a new KubePodLabelsMetric, implementation of prometheus.Metric
func newKubePodLabelsMetric(pod string, namespace string, uid string, fqname string, labelNames []string, labelValues []string) KubePodLabelsMetric {
	return KubePodLabelsMetric{
		fqName:      fqname,
		labelNames:  labelNames,
		labelValues: labelValues,
		help:        "kube_pod_labels all labels for each pod prefixed with label_",
		pod:         pod,
		namespace:   namespace,
		uid:         uid,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (nam KubePodLabelsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       nam.pod,
		"namespace": nam.namespace,
		"uid":       nam.uid,
	}
	return prometheus.NewDesc(nam.fqName, nam.help, nam.labelNames, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (nam KubePodLabelsMetric) Write(m *dto.Metric) error {
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

	podString := "pod"
	namespaceString := "namespace"
	uidString := "uid"
	labels = append(labels,
		&dto.LabelPair{
			Name:  &podString,
			Value: &nam.pod,
		},
		&dto.LabelPair{
			Name:  &namespaceString,
			Value: &nam.namespace,
		}, &dto.LabelPair{
			Name:  &uidString,
			Value: &nam.uid,
		},
	)
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

// KubeNodeLabelsCollector is a prometheus collector that generates
// KubeNodeLabelsMetrics
type KubeNodeLabelsCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (nsac KubeNodeLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_node_labels", "all labels for each node prefixed with label_", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (nsac KubeNodeLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := nsac.KubeClusterCache.GetAllNodes()
	for _, node := range nodes {

		labelNames, labelValues := prom.KubePrependQualifierToLabels(node.GetLabels(), "label_")

		m := newKubeNodeLabelsMetric(
			node.GetName(),
			"kube_node_labels",
			labelNames,
			labelValues,
		)
		ch <- m
	}
}

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
//  KubePodContainerStatusRestartsTotalMetric
//--------------------------------------------------------------------------

// KubePodContainerStatusRestartsTotalMetric is a prometheus.Metric emitting container restarts metrics.
type KubePodContainerStatusRestartsTotalMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	value     float64
}

// Creates a new KubePodContainerStatusRestartsTotalMetric, implementation of prometheus.Metric
func newKubePodContainerStatusRestartsTotalMetric(fqname, pod, namespace, uid, container string, value float64) KubePodContainerStatusRestartsTotalMetric {
	return KubePodContainerStatusRestartsTotalMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_restarts_total total container restarts",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodContainerStatusRestartsTotalMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       kpcs.pod,
		"namespace": kpcs.namespace,
		"uid":       kpcs.uid,
		"container": kpcs.container,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcs KubePodContainerStatusRestartsTotalMetric) Write(m *dto.Metric) error {
	m.Counter = &dto.Counter{
		Value: &kpcs.value,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("container"),
			Value: &kpcs.container,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerStatusTerminatedReasonMetric
//--------------------------------------------------------------------------

// KubePodContainerStatusTerminatedReasonMetric is a prometheus.Metric emitting container termination reasons.
type KubePodContainerStatusTerminatedReasonMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	reason    string
}

// Creates a new KubePodContainerStatusRestartsTotalMetric, implementation of prometheus.Metric
func newKubePodContainerStatusTerminatedReasonMetric(fqname, pod, namespace, uid, container, reason string) KubePodContainerStatusTerminatedReasonMetric {
	return KubePodContainerStatusTerminatedReasonMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_terminated_reason Describes the reason the container is currently in terminated state.",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		reason:    reason,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodContainerStatusTerminatedReasonMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       kpcs.pod,
		"namespace": kpcs.namespace,
		"uid":       kpcs.uid,
		"container": kpcs.container,
		"reason":    kpcs.reason,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcs KubePodContainerStatusTerminatedReasonMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("container"),
			Value: &kpcs.container,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
		&dto.LabelPair{
			Name:  toStringPtr("reason"),
			Value: &kpcs.reason,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodStatusPhaseMetric
//--------------------------------------------------------------------------

// KubePodStatusPhaseMetric is a prometheus.Metric emitting all phases for a pod
type KubePodStatusPhaseMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	uid       string
	phase     string
	value     float64
}

// Creates a new KubePodContainerStatusRestartsTotalMetric, implementation of prometheus.Metric
func newKubePodStatusPhaseMetric(fqname, pod, namespace, uid, phase string, value float64) KubePodStatusPhaseMetric {
	return KubePodStatusPhaseMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_terminated_reason Describes the reason the container is currently in terminated state.",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		phase:     phase,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodStatusPhaseMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       kpcs.pod,
		"namespace": kpcs.namespace,
		"uid":       kpcs.uid,
		"phase":     kpcs.phase,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data transmission object.
func (kpcs KubePodStatusPhaseMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcs.value,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
		&dto.LabelPair{
			Name:  toStringPtr("phase"),
			Value: &kpcs.phase,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerStatusRunningMetric
//--------------------------------------------------------------------------

// KubePodLabelsMetric is a prometheus.Metric used to encode
// a duplicate of the deprecated kube-state-metrics metric
// kube_pod_labels
type KubePodContainerStatusRunningMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
}

// Creates a new KubePodContainerStatusRunningMetric, implementation of prometheus.Metric
func newKubePodContainerStatusRunningMetric(fqname string, pod string, namespace string, uid string, container string) KubePodContainerStatusRunningMetric {
	return KubePodContainerStatusRunningMetric{
		fqName:    fqname,
		help:      "kube_pod_container_status_running pods container status",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcs KubePodContainerStatusRunningMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       kpcs.pod,
		"namespace": kpcs.namespace,
		"uid":       kpcs.uid,
		"container": kpcs.container,
	}
	return prometheus.NewDesc(kpcs.fqName, kpcs.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcs KubePodContainerStatusRunningMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}

	var labels []*dto.LabelPair
	labels = append(labels,
		&dto.LabelPair{
			Name:  toStringPtr("pod"),
			Value: &kpcs.pod,
		},
		&dto.LabelPair{
			Name:  toStringPtr("namespace"),
			Value: &kpcs.namespace,
		},
		&dto.LabelPair{
			Name:  toStringPtr("container"),
			Value: &kpcs.container,
		},
		&dto.LabelPair{
			Name:  toStringPtr("uid"),
			Value: &kpcs.uid,
		},
	)
	m.Label = labels
	return nil
}

//--------------------------------------------------------------------------
//  KubePodContainerResourceRequestMetric
//--------------------------------------------------------------------------

// KubePodContainerResourceRequestsMetric is a prometheus.Metric
type KubePodContainerResourceRequestsMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	resource  string
	unit      string
	node      string
	value     float64
}

// Creates a new newKubePodContainerResourceRequestsMetric, implementation of prometheus.Metric
func newKubePodContainerResourceRequestsMetric(fqname, pod, namespace, uid, container, node, resource, unit string, value float64) KubePodContainerResourceRequestsMetric {
	return KubePodContainerResourceRequestsMetric{
		fqName:    fqname,
		help:      "kube_pod_container_resource_requests pods container resource requests",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		node:      node,
		resource:  resource,
		unit:      unit,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePodContainerResourceRequestsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       kpcrr.pod,
		"namespace": kpcrr.namespace,
		"uid":       kpcrr.uid,
		"container": kpcrr.container,
		"node":      kpcrr.node,
		"resource":  kpcrr.resource,
		"unit":      kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePodContainerResourceRequestsMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("pod"),
			Value: &kpcrr.pod,
		},
		{
			Name:  toStringPtr("namespace"),
			Value: &kpcrr.namespace,
		},
		{
			Name:  toStringPtr("container"),
			Value: &kpcrr.container,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpcrr.uid,
		},
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

//--------------------------------------------------------------------------
//  KubePodContainerResourceLimitsMetric
//--------------------------------------------------------------------------

// KubePodContainerResourceLimitsMetric is a prometheus.Metric
type KubePodContainerResourceLimitsMetric struct {
	fqName    string
	help      string
	pod       string
	namespace string
	container string
	uid       string
	resource  string
	unit      string
	node      string
	value     float64
}

// Creates a new KubePodContainerResourceLimitsMetric, implementation of prometheus.Metric
func newKubePodContainerResourceLimitsMetric(fqname, pod, namespace, uid, container, node, resource, unit string, value float64) KubePodContainerResourceLimitsMetric {
	return KubePodContainerResourceLimitsMetric{
		fqName:    fqname,
		help:      "kube_pod_container_resource_limits pods container resource limits",
		pod:       pod,
		namespace: namespace,
		uid:       uid,
		container: container,
		node:      node,
		resource:  resource,
		unit:      unit,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePodContainerResourceLimitsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"pod":       kpcrr.pod,
		"namespace": kpcrr.namespace,
		"uid":       kpcrr.uid,
		"container": kpcrr.container,
		"node":      kpcrr.node,
		"resource":  kpcrr.resource,
		"unit":      kpcrr.unit,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePodContainerResourceLimitsMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("pod"),
			Value: &kpcrr.pod,
		},
		{
			Name:  toStringPtr("namespace"),
			Value: &kpcrr.namespace,
		},
		{
			Name:  toStringPtr("container"),
			Value: &kpcrr.container,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpcrr.uid,
		},
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

//--------------------------------------------------------------------------
//  KubePVCapacityBytesCollector
//--------------------------------------------------------------------------

// KubePVCapacityBytesCollector is a prometheus collector that generates
// KubePVCapacityBytesMetric
type KubePVCapacityBytesCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpvcb KubePVCapacityBytesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_persistentvolume_capacity_bytes", "The pv storage capacity in bytes", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpvcb KubePVCapacityBytesCollector) Collect(ch chan<- prometheus.Metric) {
	pvs := kpvcb.KubeClusterCache.GetAllPersistentVolumes()
	for _, pv := range pvs {
		storage := pv.Spec.Capacity[v1.ResourceStorage]
		m := newKubePVCapacityBytesMetric("kube_persistentvolume_capacity_bytes", pv.Name, float64(storage.Value()))

		ch <- m
	}
}

//--------------------------------------------------------------------------
//  KubePVCapacityBytesMetric
//--------------------------------------------------------------------------

// KubePVCapacityBytesMetric is a prometheus.Metric
type KubePVCapacityBytesMetric struct {
	fqName string
	help   string
	pv     string
	value  float64
}

// Creates a new KubePVCapacityBytesMetric, implementation of prometheus.Metric
func newKubePVCapacityBytesMetric(fqname, pv string, value float64) KubePVCapacityBytesMetric {
	return KubePVCapacityBytesMetric{
		fqName: fqname,
		help:   "kube_persistentvolume_capacity_bytes pv storage capacity in bytes",
		pv:     pv,
		value:  value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpcrr KubePVCapacityBytesMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"persistentvolume": kpcrr.pv,
	}
	return prometheus.NewDesc(kpcrr.fqName, kpcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpcrr KubePVCapacityBytesMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("persistentvolume"),
			Value: &kpcrr.pv,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePVCResourceRequestsStorageBytesCollector
//--------------------------------------------------------------------------

// KubePVCResourceRequestsStorageBytesCollector is a prometheus collector that generates
// KubePVCResourceRequestsStorageBytesMetric
type KubePVCResourceRequestsStorageBytesCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpvcb KubePVCResourceRequestsStorageBytesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_persistentvolumeclaim_resource_requests_storage_bytes", "The pvc storage resource requests in bytes", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpvcb KubePVCResourceRequestsStorageBytesCollector) Collect(ch chan<- prometheus.Metric) {
	pvcs := kpvcb.KubeClusterCache.GetAllPersistentVolumeClaims()
	for _, pvc := range pvcs {
		if storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			m := newKubePVCResourceRequestsStorageBytesMetric("kube_persistentvolumeclaim_resource_requests_storage_bytes", pvc.Name, pvc.Namespace, float64(storage.Value()))
			ch <- m
		}
	}
}

//--------------------------------------------------------------------------
//  KubePVCResourceRequestsStorageBytesMetric
//--------------------------------------------------------------------------

// KubePVCResourceRequestsStorageBytesMetric is a prometheus.Metric
type KubePVCResourceRequestsStorageBytesMetric struct {
	fqName    string
	help      string
	namespace string
	pvc       string
	value     float64
}

// Creates a new KubePVCResourceRequestsStorageBytesMetric, implementation of prometheus.Metric
func newKubePVCResourceRequestsStorageBytesMetric(fqname, pvc, namespace string, value float64) KubePVCResourceRequestsStorageBytesMetric {
	return KubePVCResourceRequestsStorageBytesMetric{
		fqName:    fqname,
		help:      "kube_persistentvolumeclaim_resource_requests_storage_bytes pvc storage resource requests in bytes",
		pvc:       pvc,
		namespace: namespace,
		value:     value,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpvcrr KubePVCResourceRequestsStorageBytesMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"persistentvolumeclaim": kpvcrr.pvc,
		"namespace":             kpvcrr.namespace,
	}
	return prometheus.NewDesc(kpvcrr.fqName, kpvcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpvcrr KubePVCResourceRequestsStorageBytesMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpvcrr.value,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("persistentvolumeclaim"),
			Value: &kpvcrr.pvc,
		},
		{
			Name:  toStringPtr("namespace"),
			Value: &kpvcrr.namespace,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePVCInfoCollector
//--------------------------------------------------------------------------

// KubePVCInfoCollector is a prometheus collector that generates KubePVCInfoMetric
type KubePVCInfoCollector struct {
	KubeClusterCache clustercache.ClusterCache
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpvci KubePVCInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("kube_persistentvolumeclaim_info", "The pvc storage resource requests in bytes", []string{}, nil)
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpvci KubePVCInfoCollector) Collect(ch chan<- prometheus.Metric) {
	pvcs := kpvci.KubeClusterCache.GetAllPersistentVolumeClaims()
	for _, pvc := range pvcs {
		storageClass := getPersistentVolumeClaimClass(pvc)
		volume := pvc.Spec.VolumeName

		m := newKubePVCInfoMetric("kube_persistentvolumeclaim_info", pvc.Name, pvc.Namespace, volume, storageClass)
		ch <- m
	}
}

//--------------------------------------------------------------------------
//  KubePVCInfoMetric
//--------------------------------------------------------------------------

// KubePVCInfoMetric is a prometheus.Metric
type KubePVCInfoMetric struct {
	fqName       string
	help         string
	namespace    string
	pvc          string
	storageclass string
	volume       string
}

// Creates a new KubePVCInfoMetric, implementation of prometheus.Metric
func newKubePVCInfoMetric(fqname, pvc, namespace, storageclass, volume string) KubePVCInfoMetric {
	return KubePVCInfoMetric{
		fqName:       fqname,
		help:         "kube_persistentvolumeclaim_info pvc storage resource requests in bytes",
		pvc:          pvc,
		namespace:    namespace,
		storageclass: storageclass,
		volume:       volume,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpvcrr KubePVCInfoMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"persistentvolumeclaim": kpvcrr.pvc,
		"namespace":             kpvcrr.namespace,
		"storageclass":          kpvcrr.storageclass,
		"volumename":            kpvcrr.volume,
	}
	return prometheus.NewDesc(kpvcrr.fqName, kpvcrr.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpvci KubePVCInfoMetric) Write(m *dto.Metric) error {
	v := float64(1.0)
	m.Gauge = &dto.Gauge{
		Value: &v,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpvci.namespace,
		},
		{
			Name:  toStringPtr("persistentvolumeclaim"),
			Value: &kpvci.pvc,
		},
		{
			Name:  toStringPtr("storageclass"),
			Value: &kpvci.storageclass,
		},
		{
			Name:  toStringPtr("volumename"),
			Value: &kpvci.volume,
		},
	}
	return nil
}

//--------------------------------------------------------------------------
//  KubePodOwnerMetric
//--------------------------------------------------------------------------

// KubePodOwnerMetric is a prometheus.Metric
type KubePodOwnerMetric struct {
	fqName            string
	help              string
	namespace         string
	pod               string
	ownerIsController bool
	ownerName         string
	ownerKind         string
}

// Creates a new KubePodOwnerMetric, implementation of prometheus.Metric
func newKubePodOwnerMetric(fqname, namespace, pod, ownerName, ownerKind string, ownerIsController bool) KubePodOwnerMetric {
	return KubePodOwnerMetric{
		fqName:            fqname,
		help:              "kube_pod_owner Information about the Pod's owner",
		namespace:         namespace,
		pod:               pod,
		ownerName:         ownerName,
		ownerKind:         ownerKind,
		ownerIsController: ownerIsController,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpo KubePodOwnerMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"namespace":           kpo.namespace,
		"pod":                 kpo.pod,
		"owner_name":          kpo.ownerName,
		"owner_kind":          kpo.ownerKind,
		"owner_is_controller": fmt.Sprintf("%t", kpo.ownerIsController),
	}
	return prometheus.NewDesc(kpo.fqName, kpo.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpo KubePodOwnerMetric) Write(m *dto.Metric) error {
	v := float64(1.0)
	m.Gauge = &dto.Gauge{
		Value: &v,
	}

	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpo.namespace,
		},
		{
			Name:  toStringPtr("pod"),
			Value: &kpo.pod,
		},
		{
			Name:  toStringPtr("owner_name"),
			Value: &kpo.ownerName,
		},
		{
			Name:  toStringPtr("owner_kind"),
			Value: &kpo.ownerKind,
		},
		{
			Name:  toStringPtr("owner_is_controller"),
			Value: toStringPtr(fmt.Sprintf("%t", kpo.ownerIsController)),
		},
	}
	return nil
}

// getPersistentVolumeClaimClass returns StorageClassName. If no storage class was
// requested, it returns "".
func getPersistentVolumeClaimClass(claim *v1.PersistentVolumeClaim) string {
	// Use beta annotation first
	if class, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	// Special non-empty string to indicate absence of storage class.
	return "<none>"
}

// toResourceUnitValue accepts a resource name and quantity and returns the sanitized resource, the unit, and the value in the units.
// Returns an empty string for resource and unit if there was a failure.
func toResourceUnitValue(resourceName v1.ResourceName, quantity resource.Quantity) (resource string, unit string, value float64) {
	resource = prom.SanitizeLabelName(string(resourceName))

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

// boolFloat64 converts a boolean input into a 1 or 0
func boolFloat64(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// toStringPtr is used to create a new string pointer from iteration vars
func toStringPtr(s string) *string { return &s }

var kubeMetricInit sync.Once

//--------------------------------------------------------------------------
//  Kube Metric Registration
//--------------------------------------------------------------------------

// KubeMetricsOpts represents our Kubernetes metrics emission options.
type KubeMetricsOpts struct {
	EmitKubecostControllerMetrics bool
	EmitNamespaceAnnotations      bool
	EmitPodAnnotations            bool
	EmitKubeStateMetrics          bool
}

// DefaultKubeMetricsOpts returns KubeMetricsOpts with default values set
func DefaultKubeMetricsOpts() *KubeMetricsOpts {
	return &KubeMetricsOpts{
		EmitKubecostControllerMetrics: true,
		EmitNamespaceAnnotations:      false,
		EmitPodAnnotations:            false,
		EmitKubeStateMetrics:          true,
	}
}

// InitKubeMetrics initializes kubernetes metric emission using the provided options.
func InitKubeMetrics(clusterCache clustercache.ClusterCache, opts *KubeMetricsOpts) {
	if opts == nil {
		opts = DefaultKubeMetricsOpts()
	}

	kubeMetricInit.Do(func() {
		if opts.EmitKubecostControllerMetrics {
			prometheus.MustRegister(ServiceCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(DeploymentCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(StatefulsetCollector{
				KubeClusterCache: clusterCache,
			})
		}

		if opts.EmitNamespaceAnnotations {
			prometheus.MustRegister(NamespaceAnnotationCollector{
				KubeClusterCache: clusterCache,
			})
		}

		if opts.EmitKubeStateMetrics {
			prometheus.MustRegister(KubePodMetricCollector{
				KubeClusterCache:   clusterCache,
				emitPodAnnotations: opts.EmitPodAnnotations,
			})

			prometheus.MustRegister(KubeNodeStatusCapacityMemoryBytesCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(KubeNodeStatusCapacityCPUCoresCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(KubeNodeLabelsCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(KubePVCapacityBytesCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(KubePVCResourceRequestsStorageBytesCollector{
				KubeClusterCache: clusterCache,
			})
			prometheus.MustRegister(KubePVCInfoCollector{
				KubeClusterCache: clusterCache,
			})

		}
	})
}
