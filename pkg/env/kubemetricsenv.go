package env

import "github.com/opencost/opencost/core/pkg/env"

const (
	KubecostMetricsPodEnabledEnvVar = "KUBECOST_METRICS_POD_ENABLED"
	KubecostMetricsPodPortEnvVar    = "KUBECOST_METRICS_PORT"
	ExportClusterInfoEnabledEnvVar  = "EXPORT_CLUSTER_INFO_ENABLED"
)

func GetKubecostMetricsPort() int {
	return env.GetInt(KubecostMetricsPodPortEnvVar, 9005)
}

// IsKubecostMetricsPodEnabled returns true if the kubecost metrics pod is deployed
func IsKubecostMetricsPodEnabled() bool {
	return env.GetBool(KubecostMetricsPodEnabledEnvVar, false)
}

// IsExportClusterInfoEnabled is set to true if the metrics pod should export its own cluster info
// data to a target file location
func IsExportClusterInfoEnabled() bool {
	return env.GetBool(ExportClusterInfoEnabledEnvVar, false)
}
