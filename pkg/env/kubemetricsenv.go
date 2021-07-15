package env

const (
	KubecostMetricsPodEnabledEnvVar = "KUBECOST_METRICS_POD_ENABLED"
	KubecostMetricsPodPortEnvVar    = "KUBECOST_METRICS_PORT"
)

func GetKubecostMetricsPort() int {
	return GetInt(KubecostMetricsPodPortEnvVar, 9005)
}

// IsKubecostMetricsPodEnabled returns true if the kubecost metrics pod is deployed
func IsKubecostMetricsPodEnabled() bool {
	return GetBool(KubecostMetricsPodEnabledEnvVar, true)
}
