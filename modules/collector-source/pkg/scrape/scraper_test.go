package scrape

import (
	"testing"

	"github.com/stretchr/testify/assert"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// staticScraper is a test double that returns a fixed set of updates.
type staticScraper struct {
	updates []metric.Update
}

func (s *staticScraper) Scrape() []metric.Update { return s.updates }

func updates(names ...string) []metric.Update {
	out := make([]metric.Update, len(names))
	for i, n := range names {
		out[i] = metric.Update{Name: n}
	}
	return out
}

func updateNames(us []metric.Update) []string {
	names := make([]string, len(us))
	for i, u := range us {
		names[i] = u.Name
	}
	return names
}

func TestWithFilter_EmptyFilterReturnsOriginalScraper(t *testing.T) {
	s := &staticScraper{}
	wrapped := withFilter(s, MetricFilter{})
	assert.Same(t, s, wrapped, "empty filter should return the original scraper unchanged")
}

func TestWithFilter_NilFilterReturnsOriginalScraper(t *testing.T) {
	s := &staticScraper{}
	wrapped := withFilter(s, nil)
	assert.Same(t, s, wrapped, "nil filter should return the original scraper unchanged")
}

func TestWithFilter_NonEmptyFilterWraps(t *testing.T) {
	s := &staticScraper{}
	f := MetricFilter{"some_metric": {}}
	wrapped := withFilter(s, f)
	_, isFiltered := wrapped.(*filteredScraper)
	assert.True(t, isFiltered, "non-empty filter should wrap in a filteredScraper")
}

func TestFilteredScraper_AllowsMetricsNotInFilter(t *testing.T) {
	s := &staticScraper{updates: updates("cpu_usage", "memory_usage")}
	f := MetricFilter{"kube_pod_annotations": {}}
	wrapped := withFilter(s, f)

	got := wrapped.Scrape()
	assert.Equal(t, []string{"cpu_usage", "memory_usage"}, updateNames(got))
}

func TestFilteredScraper_BlocksDeniedMetrics(t *testing.T) {
	s := &staticScraper{updates: updates("cpu_usage", metric.KubePodAnnotations, "memory_usage")}
	f := MetricFilter{metric.KubePodAnnotations: {}}
	wrapped := withFilter(s, f)

	got := wrapped.Scrape()
	assert.Equal(t, []string{"cpu_usage", "memory_usage"}, updateNames(got))
}

func TestFilteredScraper_BlocksMultipleDeniedMetrics(t *testing.T) {
	s := &staticScraper{updates: updates(
		metric.KubePodAnnotations,
		"cpu_usage",
		metric.KubeNamespaceAnnotations,
		"memory_usage",
	)}
	f := MetricFilter{
		metric.KubePodAnnotations:       {},
		metric.KubeNamespaceAnnotations: {},
	}
	wrapped := withFilter(s, f)

	got := wrapped.Scrape()
	assert.Equal(t, []string{"cpu_usage", "memory_usage"}, updateNames(got))
}

func TestFilteredScraper_EmptyResultsPassThrough(t *testing.T) {
	s := &staticScraper{updates: []metric.Update{}}
	f := MetricFilter{metric.KubePodAnnotations: {}}
	wrapped := withFilter(s, f)

	got := wrapped.Scrape()
	assert.Empty(t, got)
}

func TestFilteredScraper_AllMetricsDenied(t *testing.T) {
	s := &staticScraper{updates: updates(metric.KubePodAnnotations, metric.KubeNamespaceAnnotations)}
	f := MetricFilter{
		metric.KubePodAnnotations:       {},
		metric.KubeNamespaceAnnotations: {},
	}
	wrapped := withFilter(s, f)

	got := wrapped.Scrape()
	assert.Empty(t, got)
}

func TestGetDefaultMetricFilter_PodAnnotationsDisabled(t *testing.T) {
	t.Setenv(coreenv.EmitPodAnnotationsMetricEnvVar, "false")
	t.Setenv(coreenv.EmitNamespaceAnnotationsMetricEnvVar, "true")

	f := getDefaultMetricFilter()

	_, podDenied := f[metric.KubePodAnnotations]
	_, nsDenied := f[metric.KubeNamespaceAnnotations]
	assert.True(t, podDenied, "pod annotations should be denied when env var is false")
	assert.False(t, nsDenied, "namespace annotations should be allowed when env var is true")
}

func TestGetDefaultMetricFilter_NamespaceAnnotationsDisabled(t *testing.T) {
	t.Setenv(coreenv.EmitPodAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitNamespaceAnnotationsMetricEnvVar, "false")

	f := getDefaultMetricFilter()

	_, podDenied := f[metric.KubePodAnnotations]
	_, nsDenied := f[metric.KubeNamespaceAnnotations]
	assert.False(t, podDenied, "pod annotations should be allowed when env var is true")
	assert.True(t, nsDenied, "namespace annotations should be denied when env var is false")
}

func TestGetDefaultMetricFilter_BothDisabledByDefault(t *testing.T) {
	t.Setenv(coreenv.EmitPodAnnotationsMetricEnvVar, "")
	t.Setenv(coreenv.EmitNamespaceAnnotationsMetricEnvVar, "")

	f := getDefaultMetricFilter()

	_, podDenied := f[metric.KubePodAnnotations]
	_, nsDenied := f[metric.KubeNamespaceAnnotations]
	assert.True(t, podDenied, "pod annotations should be denied by default")
	assert.True(t, nsDenied, "namespace annotations should be denied by default")
}

func TestGetDefaultMetricFilter_AllEnabled(t *testing.T) {
	t.Setenv(coreenv.EmitPodAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitNamespaceAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitDeploymentLabelsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitDeploymentAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitStatefulSetLabelsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitStatefulSetAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitDaemonSetLabelsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitDaemonSetAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitJobLabelsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitJobAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitCronJobLabelsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitCronJobAnnotationsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitReplicaSetLabelsMetricEnvVar, "true")
	t.Setenv(coreenv.EmitReplicaSetAnnotationsMetricEnvVar, "true")

	f := getDefaultMetricFilter()

	assert.Empty(t, f, "filter should be empty when all metrics are enabled")
}

func TestGetDefaultMetricFilter_AnnotationsDefaultFalse(t *testing.T) {
	// Unset all env vars to exercise pure defaults.
	for _, v := range []string{
		coreenv.EmitPodAnnotationsMetricEnvVar,
		coreenv.EmitNamespaceAnnotationsMetricEnvVar,
		coreenv.EmitDeploymentAnnotationsMetricEnvVar,
		coreenv.EmitStatefulSetAnnotationsMetricEnvVar,
		coreenv.EmitDaemonSetAnnotationsMetricEnvVar,
		coreenv.EmitJobAnnotationsMetricEnvVar,
		coreenv.EmitCronJobAnnotationsMetricEnvVar,
		coreenv.EmitReplicaSetAnnotationsMetricEnvVar,
	} {
		t.Setenv(v, "")
	}

	f := getDefaultMetricFilter()

	for _, name := range []string{
		metric.KubePodAnnotations,
		metric.KubeNamespaceAnnotations,
		metric.DeploymentAnnotations,
		metric.StatefulSetAnnotations,
		metric.DaemonSetAnnotations,
		metric.JobAnnotations,
		metric.CronJobAnnotations,
		metric.ReplicaSetAnnotations,
	} {
		_, denied := f[name]
		assert.True(t, denied, "%s should be denied by default", name)
	}
}

func TestGetDefaultMetricFilter_LabelsDefaultTrue(t *testing.T) {
	// Unset all label env vars to exercise pure defaults.
	for _, v := range []string{
		coreenv.EmitDeploymentLabelsMetricEnvVar,
		coreenv.EmitStatefulSetLabelsMetricEnvVar,
		coreenv.EmitDaemonSetLabelsMetricEnvVar,
		coreenv.EmitJobLabelsMetricEnvVar,
		coreenv.EmitCronJobLabelsMetricEnvVar,
		coreenv.EmitReplicaSetLabelsMetricEnvVar,
	} {
		t.Setenv(v, "")
	}

	f := getDefaultMetricFilter()

	for _, name := range []string{
		metric.DeploymentLabels,
		metric.StatefulSetLabels,
		metric.DaemonSetLabels,
		metric.JobLabels,
		metric.CronJobLabels,
		metric.ReplicaSetLabels,
	} {
		_, denied := f[name]
		assert.False(t, denied, "%s should be allowed by default", name)
	}
}
