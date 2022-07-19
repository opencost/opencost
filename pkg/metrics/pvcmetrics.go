package metrics

import (
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	v1 "k8s.io/api/core/v1"
)

//--------------------------------------------------------------------------
//  KubePVCCollector
//--------------------------------------------------------------------------

// KubePVCCollector is a prometheus collector that generates pvc sourced metrics
type KubePVCCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics collected by this Collector.
func (kpvc KubePVCCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kpvc.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_persistentvolumeclaim_resource_requests_storage_bytes"]; !disabled {
		ch <- prometheus.NewDesc("kube_persistentvolumeclaim_resource_requests_storage_bytes", "The pvc storage resource requests in bytes", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_persistentvolumeclaim_info"]; !disabled {
		ch <- prometheus.NewDesc("kube_persistentvolumeclaim_info", "The pvc storage resource requests in bytes", []string{}, nil)
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpvc KubePVCCollector) Collect(ch chan<- prometheus.Metric) {
	pvcs := kpvc.KubeClusterCache.GetAllPersistentVolumeClaims()
	disabledMetrics := kpvc.metricsConfig.GetDisabledMetricsMap()

	for _, pvc := range pvcs {
		storageClass := getPersistentVolumeClaimClass(pvc)
		volume := pvc.Spec.VolumeName

		if _, disabled := disabledMetrics["kube_persistentvolumeclaim_info"]; !disabled {
			ch <- newKubePVCInfoMetric("kube_persistentvolumeclaim_info", pvc.Name, pvc.Namespace, storageClass, volume)
		}

		if storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]; ok {
			if _, disabled := disabledMetrics["kube_persistentvolumeclaim_resource_requests_storage_bytes"]; !disabled {
				ch <- newKubePVCResourceRequestsStorageBytesMetric("kube_persistentvolumeclaim_resource_requests_storage_bytes", pvc.Name, pvc.Namespace, float64(storage.Value()))
			}
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
