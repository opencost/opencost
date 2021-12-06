package env

const (
	KubecostMetricsPodEnabledEnvVar = "KUBECOST_METRICS_POD_ENABLED"
	KubecostMetricsPodPortEnvVar    = "KUBECOST_METRICS_PORT"
	ExportClusterCacheEnabledEnvVar = "EXPORT_CLUSTER_CACHE_ENABLED"
)

func GetKubecostMetricsPort() int {
	return GetInt(KubecostMetricsPodPortEnvVar, 9005)
}

// IsKubecostMetricsPodEnabled returns true if the kubecost metrics pod is deployed
func IsKubecostMetricsPodEnabled() bool {
	return GetBool(KubecostMetricsPodEnabledEnvVar, false)
}

// IsExportClusterCacheEnabled is set to true if the metrics pod should export the cluster cache
// data to a target file location
func IsExportClusterCacheEnabled() bool {
	return GetBool(ExportClusterCacheEnabledEnvVar, false)
}
