package metrics

import (
	"fmt"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
)

//--------------------------------------------------------------------------
//  KubePodCollector
//--------------------------------------------------------------------------

// KubePodMetricCollector is a prometheus collector that emits pod metrics
type KubePodCollector struct {
	KubeClusterCache   clustercache.ClusterCache
	emitPodAnnotations bool
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpmc KubePodCollector) Describe(ch chan<- *prometheus.Desc) {
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
func (kpmc KubePodCollector) Collect(ch chan<- prometheus.Metric) {
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
