package env

const (
	EmitPodAnnotationsMetricEnvVar       = "EMIT_POD_ANNOTATIONS_METRIC"
	EmitNamespaceAnnotationsMetricEnvVar = "EMIT_NAMESPACE_ANNOTATIONS_METRIC"
	EmitDeprecatedMetrics                = "EMIT_DEPRECATED_METRICS"

	EmitKsmV1MetricsEnvVar = "EMIT_KSM_V1_METRICS"
	EmitKsmV1MetricsOnly   = "EMIT_KSM_V1_METRICS_ONLY"

	EmitDeploymentLabelsMetricEnvVar      = "EMIT_DEPLOYMENT_LABELS_METRIC"
	EmitDeploymentAnnotationsMetricEnvVar = "EMIT_DEPLOYMENT_ANNOTATIONS_METRIC"

	EmitStatefulSetLabelsMetricEnvVar      = "EMIT_STATEFULSET_LABELS_METRIC"
	EmitStatefulSetAnnotationsMetricEnvVar = "EMIT_STATEFULSET_ANNOTATIONS_METRIC"

	EmitDaemonSetLabelsMetricEnvVar      = "EMIT_DAEMONSET_LABELS_METRIC"
	EmitDaemonSetAnnotationsMetricEnvVar = "EMIT_DAEMONSET_ANNOTATIONS_METRIC"

	EmitJobLabelsMetricEnvVar      = "EMIT_JOB_LABELS_METRIC"
	EmitJobAnnotationsMetricEnvVar = "EMIT_JOB_ANNOTATIONS_METRIC"

	EmitCronJobLabelsMetricEnvVar      = "EMIT_CRONJOB_LABELS_METRIC"
	EmitCronJobAnnotationsMetricEnvVar = "EMIT_CRONJOB_ANNOTATIONS_METRIC"

	EmitReplicaSetLabelsMetricEnvVar      = "EMIT_REPLICASET_LABELS_METRIC"
	EmitReplicaSetAnnotationsMetricEnvVar = "EMIT_REPLICASET_ANNOTATIONS_METRIC"
)

// IsEmitNamespaceAnnotationsMetric returns true if cost-model is configured to emit the kube_namespace_annotations metric
// containing the namespace annotations
func IsEmitNamespaceAnnotationsMetric() bool {
	return GetBool(EmitNamespaceAnnotationsMetricEnvVar, false)
}

// IsEmitPodAnnotationsMetric returns true if cost-model is configured to emit the kube_pod_annotations metric containing
// pod annotations.
func IsEmitPodAnnotationsMetric() bool {
	return GetBool(EmitPodAnnotationsMetricEnvVar, false)
}

// IsEmitKsmV1Metrics returns true if cost-model is configured to emit all necessary KSM v1
// metrics that were removed in KSM v2
func IsEmitKsmV1Metrics() bool {
	return GetBool(EmitKsmV1MetricsEnvVar, true)
}

func IsEmitKsmV1MetricsOnly() bool {
	return GetBool(EmitKsmV1MetricsOnly, false)
}

func IsEmitDeprecatedMetrics() bool {
	return GetBool(EmitDeprecatedMetrics, false)
}

func IsEmitDeploymentLabelsMetric() bool {
	return GetBool(EmitDeploymentLabelsMetricEnvVar, true)
}

func IsEmitDeploymentAnnotationsMetric() bool {
	return GetBool(EmitDeploymentAnnotationsMetricEnvVar, false)
}

func IsEmitStatefulSetLabelsMetric() bool {
	return GetBool(EmitStatefulSetLabelsMetricEnvVar, true)
}

func IsEmitStatefulSetAnnotationsMetric() bool {
	return GetBool(EmitStatefulSetAnnotationsMetricEnvVar, false)
}

func IsEmitDaemonSetLabelsMetric() bool {
	return GetBool(EmitDaemonSetLabelsMetricEnvVar, true)
}

func IsEmitDaemonSetAnnotationsMetric() bool {
	return GetBool(EmitDaemonSetAnnotationsMetricEnvVar, false)
}

func IsEmitJobLabelsMetric() bool {
	return GetBool(EmitJobLabelsMetricEnvVar, true)
}

func IsEmitJobAnnotationsMetric() bool {
	return GetBool(EmitJobAnnotationsMetricEnvVar, false)
}

func IsEmitCronJobLabelsMetric() bool {
	return GetBool(EmitCronJobLabelsMetricEnvVar, true)
}

func IsEmitCronJobAnnotationsMetric() bool {
	return GetBool(EmitCronJobAnnotationsMetricEnvVar, false)
}

func IsEmitReplicaSetLabelsMetric() bool {
	return GetBool(EmitReplicaSetLabelsMetricEnvVar, true)
}

func IsEmitReplicaSetAnnotationsMetric() bool {
	return GetBool(EmitReplicaSetAnnotationsMetricEnvVar, false)
}
