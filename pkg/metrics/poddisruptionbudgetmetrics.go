package metrics

import (
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

//--------------------------------------------------------------------------
//  KubePodDisruptionBudgetCollector
//--------------------------------------------------------------------------

// KubePodDisruptionBudgetCollector is a prometheus collector that generates pod disruption budget metrics
type KubePodDisruptionBudgetCollector struct {
	KubeClusterCache clustercache.ClusterCache
	metricsConfig    MetricsConfig
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
func (kpdb KubePodDisruptionBudgetCollector) Describe(ch chan<- *prometheus.Desc) {
	disabledMetrics := kpdb.metricsConfig.GetDisabledMetricsMap()

	if _, disabled := disabledMetrics["kube_poddisruptionbudget_info"]; !disabled {
		ch <- prometheus.NewDesc("kube_poddisruptionbudget_info", "Information about pod disruption budget including UID", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_poddisruptionbudget_status_expected_pods"]; !disabled {
		ch <- prometheus.NewDesc("kube_poddisruptionbudget_status_expected_pods", "Expected number of pods", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_poddisruptionbudget_status_current_healthy"]; !disabled {
		ch <- prometheus.NewDesc("kube_poddisruptionbudget_status_current_healthy", "Current number of healthy pods", []string{}, nil)
	}
	if _, disabled := disabledMetrics["kube_poddisruptionbudget_status_desired_healthy"]; !disabled {
		ch <- prometheus.NewDesc("kube_poddisruptionbudget_status_desired_healthy", "Desired number of healthy pods", []string{}, nil)
	}
}

// Collect is called by the Prometheus registry when collecting metrics.
func (kpdb KubePodDisruptionBudgetCollector) Collect(ch chan<- prometheus.Metric) {
	podDisruptionBudgets := kpdb.KubeClusterCache.GetAllPodDisruptionBudgets()
	disabledMetrics := kpdb.metricsConfig.GetDisabledMetricsMap()

	for _, pdb := range podDisruptionBudgets {
		pdbName := pdb.Name
		pdbNS := pdb.Namespace
		pdbUID := string(pdb.UID)

		// Pod disruption budget info metric with UID
		if _, disabled := disabledMetrics["kube_poddisruptionbudget_info"]; !disabled {
			ch <- newKubePodDisruptionBudgetInfoMetric("kube_poddisruptionbudget_info", pdbName, pdbNS, pdbUID)
		}

		// Expected pods
		if _, disabled := disabledMetrics["kube_poddisruptionbudget_status_expected_pods"]; !disabled {
			expectedPods := pdb.Status.ExpectedPods
			ch <- newKubePodDisruptionBudgetExpectedPodsMetric("kube_poddisruptionbudget_status_expected_pods", pdbName, pdbNS, pdbUID, expectedPods)
		}

		// Current healthy pods
		if _, disabled := disabledMetrics["kube_poddisruptionbudget_status_current_healthy"]; !disabled {
			currentHealthy := pdb.Status.CurrentHealthy
			ch <- newKubePodDisruptionBudgetCurrentHealthyMetric("kube_poddisruptionbudget_status_current_healthy", pdbName, pdbNS, pdbUID, currentHealthy)
		}

		// Desired healthy pods
		if _, disabled := disabledMetrics["kube_poddisruptionbudget_status_desired_healthy"]; !disabled {
			desiredHealthy := pdb.Status.DesiredHealthy
			ch <- newKubePodDisruptionBudgetDesiredHealthyMetric("kube_poddisruptionbudget_status_desired_healthy", pdbName, pdbNS, pdbUID, desiredHealthy)
		}
	}
}

//--------------------------------------------------------------------------
//  KubePodDisruptionBudgetInfoMetric
//--------------------------------------------------------------------------

// KubePodDisruptionBudgetInfoMetric is a prometheus.Metric used to encode pod disruption budget info with UID
type KubePodDisruptionBudgetInfoMetric struct {
	fqName                  string
	help                    string
	podDisruptionBudgetName string
	namespace               string
	uid                     string
}

// Creates a new KubePodDisruptionBudgetInfoMetric, implementation of prometheus.Metric
func newKubePodDisruptionBudgetInfoMetric(fqname, podDisruptionBudgetName, namespace, uid string) KubePodDisruptionBudgetInfoMetric {
	return KubePodDisruptionBudgetInfoMetric{
		fqName:                  fqname,
		help:                    "Information about pod disruption budget including UID",
		podDisruptionBudgetName: podDisruptionBudgetName,
		namespace:               namespace,
		uid:                     uid,
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpdbim KubePodDisruptionBudgetInfoMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"poddisruptionbudget": kpdbim.podDisruptionBudgetName,
		"namespace":           kpdbim.namespace,
		"uid":                 kpdbim.uid,
	}
	return prometheus.NewDesc(kpdbim.fqName, kpdbim.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpdbim KubePodDisruptionBudgetInfoMetric) Write(m *dto.Metric) error {
	h := float64(1)
	m.Gauge = &dto.Gauge{
		Value: &h,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpdbim.namespace,
		},
		{
			Name:  toStringPtr("poddisruptionbudget"),
			Value: &kpdbim.podDisruptionBudgetName,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpdbim.uid,
		},
	}

	return nil
}

//--------------------------------------------------------------------------
//  KubePodDisruptionBudgetExpectedPodsMetric
//--------------------------------------------------------------------------

// KubePodDisruptionBudgetExpectedPodsMetric is a prometheus.Metric used to encode pod disruption budget expected pods
type KubePodDisruptionBudgetExpectedPodsMetric struct {
	fqName                  string
	help                    string
	podDisruptionBudgetName string
	namespace               string
	uid                     string
	expectedPods            float64
}

// Creates a new KubePodDisruptionBudgetExpectedPodsMetric, implementation of prometheus.Metric
func newKubePodDisruptionBudgetExpectedPodsMetric(fqname, podDisruptionBudgetName, namespace, uid string, expectedPods int32) KubePodDisruptionBudgetExpectedPodsMetric {
	return KubePodDisruptionBudgetExpectedPodsMetric{
		fqName:                  fqname,
		help:                    "kube_poddisruptionbudget_status_expected_pods Expected number of pods",
		podDisruptionBudgetName: podDisruptionBudgetName,
		namespace:               namespace,
		uid:                     uid,
		expectedPods:            float64(expectedPods),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpdbepm KubePodDisruptionBudgetExpectedPodsMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"poddisruptionbudget": kpdbepm.podDisruptionBudgetName,
		"namespace":           kpdbepm.namespace,
		"uid":                 kpdbepm.uid,
	}
	return prometheus.NewDesc(kpdbepm.fqName, kpdbepm.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpdbepm KubePodDisruptionBudgetExpectedPodsMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpdbepm.expectedPods,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpdbepm.namespace,
		},
		{
			Name:  toStringPtr("poddisruptionbudget"),
			Value: &kpdbepm.podDisruptionBudgetName,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpdbepm.uid,
		},
	}

	return nil
}

//--------------------------------------------------------------------------
//  KubePodDisruptionBudgetCurrentHealthyMetric
//--------------------------------------------------------------------------

// KubePodDisruptionBudgetCurrentHealthyMetric is a prometheus.Metric used to encode pod disruption budget current healthy
type KubePodDisruptionBudgetCurrentHealthyMetric struct {
	fqName                  string
	help                    string
	podDisruptionBudgetName string
	namespace               string
	uid                     string
	currentHealthy          float64
}

// Creates a new KubePodDisruptionBudgetCurrentHealthyMetric, implementation of prometheus.Metric
func newKubePodDisruptionBudgetCurrentHealthyMetric(fqname, podDisruptionBudgetName, namespace, uid string, currentHealthy int32) KubePodDisruptionBudgetCurrentHealthyMetric {
	return KubePodDisruptionBudgetCurrentHealthyMetric{
		fqName:                  fqname,
		help:                    "kube_poddisruptionbudget_status_current_healthy Current number of healthy pods",
		podDisruptionBudgetName: podDisruptionBudgetName,
		namespace:               namespace,
		uid:                     uid,
		currentHealthy:          float64(currentHealthy),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpdbchm KubePodDisruptionBudgetCurrentHealthyMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"poddisruptionbudget": kpdbchm.podDisruptionBudgetName,
		"namespace":           kpdbchm.namespace,
		"uid":                 kpdbchm.uid,
	}
	return prometheus.NewDesc(kpdbchm.fqName, kpdbchm.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpdbchm KubePodDisruptionBudgetCurrentHealthyMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpdbchm.currentHealthy,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpdbchm.namespace,
		},
		{
			Name:  toStringPtr("poddisruptionbudget"),
			Value: &kpdbchm.podDisruptionBudgetName,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpdbchm.uid,
		},
	}

	return nil
}

//--------------------------------------------------------------------------
//  KubePodDisruptionBudgetDesiredHealthyMetric
//--------------------------------------------------------------------------

// KubePodDisruptionBudgetDesiredHealthyMetric is a prometheus.Metric used to encode pod disruption budget desired healthy
type KubePodDisruptionBudgetDesiredHealthyMetric struct {
	fqName                  string
	help                    string
	podDisruptionBudgetName string
	namespace               string
	uid                     string
	desiredHealthy          float64
}

// Creates a new KubePodDisruptionBudgetDesiredHealthyMetric, implementation of prometheus.Metric
func newKubePodDisruptionBudgetDesiredHealthyMetric(fqname, podDisruptionBudgetName, namespace, uid string, desiredHealthy int32) KubePodDisruptionBudgetDesiredHealthyMetric {
	return KubePodDisruptionBudgetDesiredHealthyMetric{
		fqName:                  fqname,
		help:                    "kube_poddisruptionbudget_status_desired_healthy Desired number of healthy pods",
		podDisruptionBudgetName: podDisruptionBudgetName,
		namespace:               namespace,
		uid:                     uid,
		desiredHealthy:          float64(desiredHealthy),
	}
}

// Desc returns the descriptor for the Metric. This method idempotently
// returns the same descriptor throughout the lifetime of the Metric.
func (kpdbdhm KubePodDisruptionBudgetDesiredHealthyMetric) Desc() *prometheus.Desc {
	l := prometheus.Labels{
		"poddisruptionbudget": kpdbdhm.podDisruptionBudgetName,
		"namespace":           kpdbdhm.namespace,
		"uid":                 kpdbdhm.uid,
	}
	return prometheus.NewDesc(kpdbdhm.fqName, kpdbdhm.help, []string{}, l)
}

// Write encodes the Metric into a "Metric" Protocol Buffer data
// transmission object.
func (kpdbdhm KubePodDisruptionBudgetDesiredHealthyMetric) Write(m *dto.Metric) error {
	m.Gauge = &dto.Gauge{
		Value: &kpdbdhm.desiredHealthy,
	}
	m.Label = []*dto.LabelPair{
		{
			Name:  toStringPtr("namespace"),
			Value: &kpdbdhm.namespace,
		},
		{
			Name:  toStringPtr("poddisruptionbudget"),
			Value: &kpdbdhm.podDisruptionBudgetName,
		},
		{
			Name:  toStringPtr("uid"),
			Value: &kpdbdhm.uid,
		},
	}

	return nil
}
