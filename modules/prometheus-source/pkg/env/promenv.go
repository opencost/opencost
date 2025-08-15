package env

import (
	"fmt"
	"runtime"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/source"
)

const (
	PrometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"

	PromClusterIDLabelEnvVar       = "PROM_CLUSTER_ID_LABEL"
	PromNamespaceLabelEnvVar       = "PROM_NAMESPACE_LABEL"
	PromNodeLabelEnvVar            = "PROM_NODE_LABEL"
	PromInstanceLabelEnvVar        = "PROM_INSTANCE_LABEL"
	PromInstanceTypeLabelEnvVar    = "PROM_INSTANCE_TYPE_LABEL"
	PromContainerLabelEnvVar       = "PROM_CONTAINER_LABEL"
	PromPodLabelEnvVar             = "PROM_POD_LABEL"
	PromProviderIDLabelEnvVar      = "PROM_PROVIDER_ID_LABEL"
	PromDeviceLabelEnvVar          = "PROM_DEVICE_LABEL"
	PromPVCLabelEnvVar             = "PROM_PVC_LABEL"
	PromPVLabelEnvVar              = "PROM_PV_LABEL"
	PromStorageClassLabelEnvVar    = "PROM_STORAGE_CLASS_LABEL"
	PromVolumeNameLabelEnvVar      = "PROM_VOLUME_NAME_LABEL"
	PromServiceLabelEnvVar         = "PROM_SERVICE_LABEL"
	PromServiceNameLabelEnvVar     = "PROM_SERVICE_NAME_LABEL"
	PromIngressIPLabelEnvVar       = "PROM_INGRESS_IP_LABEL"
	PromProvisionerNameLabelEnvVar = "PROM_PROVISIONER_NAME_LABEL"
	PromUIDLabelEnvVar             = "PROM_UID_LABEL"
	PromKubernetesNodeLabelEnvVar  = "PROM_KUBERNETES_NODE_LABEL"
	PromModeLabelEnvVar            = "PROM_MODE_LABEL"
	PromModelNameLabelEnvVar       = "PROM_MODEL_NAME_LABEL"
	PromUUIDLabelEnvVar            = "PROM_UUID_LABEL"
	PromResourceLabelEnvVar        = "PROM_RESOURCE_LABEL"
	PromDeploymentLabelEnvVar      = "PROM_DEPLOYMENT_LABEL"
	PromStatefulSetLabelEnvVar     = "PROM_STATEFUL_SET_LABEL"
	PromReplicaSetLabelEnvVar      = "PROM_REPLICA_SET_LABEL"
	PromOwnerNameLabelEnvVar       = "PROM_OWNER_NAME_LABEL"
	PromOwnerKindLabelEnvVar       = "PROM_OWNER_KIND_LABEL"
	PromUnitLabelEnvVar            = "PROM_UNIT_LABEL"
	PromInternetLabelEnvVar        = "PROM_INTERNET_LABEL"
	PromSameZoneLabelEnvVar        = "PROM_SAME_ZONE_LABEL"
	PromSameRegionLabelEnvVar      = "PROM_SAME_REGION_LABEL"

	PrometheusRetryOnRateLimitResponseEnvVar    = "PROMETHEUS_RETRY_ON_RATE_LIMIT"
	PrometheusRetryOnRateLimitMaxRetriesEnvVar  = "PROMETHEUS_RETRY_ON_RATE_LIMIT_MAX_RETRIES"
	PrometheusRetryOnRateLimitDefaultWaitEnvVar = "PROMETHEUS_RETRY_ON_RATE_LIMIT_DEFAULT_WAIT"

	PrometheusQueryTimeoutEnvVar        = "PROMETHEUS_QUERY_TIMEOUT"
	PrometheusKeepAliveEnvVar           = "PROMETHEUS_KEEP_ALIVE"
	PrometheusTLSHandshakeTimeoutEnvVar = "PROMETHEUS_TLS_HANDSHAKE_TIMEOUT"
	PrometheusScrapeJobNameEnvVar       = "PROMETHEUS_SCRAPE_JOB_NAME"
	PrometheusScrapeIntervalEnvVar      = "PROMETHEUS_SCRAPE_INTERVAL"

	PrometheusMaxQueryDurationMinutesEnvVar = "PROMETHEUS_MAX_QUERY_DURATION_MINUTES"
	PrometheusQueryResolutionSecondsEnvVar  = "PROMETHEUS_QUERY_RESOLUTION_SECONDS"

	MaxQueryConcurrencyEnvVar = "MAX_QUERY_CONCURRENCY"

	PrometheusHeaderXScopeOrgIdEnvVar = "PROMETHEUS_HEADER_X_SCOPE_ORGID"
	InsecureSkipVerifyEnvVar          = "INSECURE_SKIP_VERIFY"
	KubeRbacProxyEnabledEnvVar        = "KUBE_RBAC_PROXY_ENABLED"

	DBBasicAuthUsername = "DB_BASIC_AUTH_USERNAME"
	DBBasicAuthPassword = "DB_BASIC_AUTH_PW"
	DBBearerToken       = "DB_BEARER_TOKEN"

	CurrentClusterIdFilterEnabledVar = "CURRENT_CLUSTER_ID_FILTER_ENABLED"

	// Deprecated env vars that we can use to fallback on temporarily
	DeprecatedScrapeIntervalEnvVar = "KUBECOST_SCRAPE_INTERVAL"
	DeprecatedJobNameEnvVar        = "KUBECOST_JOB_NAME"
)

// GetPrometheusServerEndpoint returns the environment variable value for PrometheusServerEndpointEnvVar which
// represents the prometheus server endpoint used to execute prometheus queries.
//
// In sharded Prometheus setups, PROMETHEUS_SERVER_ENDPOINT should point to a global query endpoint (e.g., Thanos Query, Cortex, or Mimir)
// to ensure OpenCost receives complete data. Pointing to a single Prometheus pod may result in incomplete or intermittent export results.
func GetPrometheusServerEndpoint() string {
	return env.Get(PrometheusServerEndpointEnvVar, "")
}

// IsPrometheusRetryOnRateLimitResponse will attempt to retry if a 429 response is received OR a 400 with a body containing
// ThrottleException (common in AWS services like AMP)
func IsPrometheusRetryOnRateLimitResponse() bool {
	return env.GetBool(PrometheusRetryOnRateLimitResponseEnvVar, true)
}

// GetPrometheusRetryOnRateLimitMaxRetries returns the maximum number of retries that should be attempted prior to failing.
// Only used if IsPrometheusRetryOnRateLimitResponse() is true.
func GetPrometheusRetryOnRateLimitMaxRetries() int {
	return env.GetInt(PrometheusRetryOnRateLimitMaxRetriesEnvVar, 5)
}

// GetPrometheusRetryOnRateLimitDefaultWait returns the default wait time for a retriable rate limit response without a
// Retry-After header.
func GetPrometheusRetryOnRateLimitDefaultWait() time.Duration {
	return env.GetDuration(PrometheusRetryOnRateLimitDefaultWaitEnvVar, 100*time.Millisecond)
}

// GetPrometheusHeaderXScopeOrgId returns the default value for X-Scope-OrgID header used for requests in Mimir/Cortex-Tenant API.
// To use Mimir(or Cortex-Tenant) instead of Prometheus add variable from cluster settings:
// "PROMETHEUS_HEADER_X_SCOPE_ORGID": "my-cluster-name"
// Then set Prometheus URL to prometheus API endpoint:
// "PROMETHEUS_SERVER_ENDPOINT": "http://mimir-url/prometheus/"
func GetPrometheusHeaderXScopeOrgId() string {
	return env.Get(PrometheusHeaderXScopeOrgIdEnvVar, "")
}

// GetScrapeInterval returns the environment variable for PrometheusScrapeIntervalEnvVar, specifying the scrape interval for Prometheus,
// should opencost not be able to deduce the configuration automatically.
func GetScrapeInterval() time.Duration {
	// return the current scrape interval env var, fallback on deprecated env var, default to 60s
	// (as per the helm installation defaults)
	return env.GetDuration(PrometheusScrapeIntervalEnvVar, env.GetDuration(DeprecatedScrapeIntervalEnvVar, 60*time.Second))
}

// GetJobName returns the environment variable value for PrometheusScrapeJobNameEnvVar, specifying which job name
// is used for prometheus to scrape the provided metrics.
func GetJobName() string {
	// return the current job name env var, fallback on deprecated env var, default to "opencost"
	// (as per the helm installation defaults)
	return env.Get(PrometheusScrapeJobNameEnvVar, env.Get(DeprecatedJobNameEnvVar, "opencost"))
}

func GetPrometheusQueryTimeout() time.Duration {
	return env.GetDuration(PrometheusQueryTimeoutEnvVar, 120*time.Second)
}

func GetPrometheusKeepAlive() time.Duration {
	return env.GetDuration(PrometheusKeepAliveEnvVar, 120*time.Second)
}

func GetPrometheusTLSHandshakeTimeout() time.Duration {
	return env.GetDuration(PrometheusTLSHandshakeTimeoutEnvVar, 10*time.Second)
}

func IsInsecureSkipVerify() bool {
	return env.GetBool(InsecureSkipVerifyEnvVar, false)
}

func IsKubeRbacProxyEnabled() bool {
	return env.GetBool(KubeRbacProxyEnabledEnvVar, false)
}

// GetPrometheusQueryResolution determines the resolution of prom queries. The smaller the
// duration, the higher the resolution; the higher the resolution, the more
// accurate the query results, but the more computationally expensive.
func GetPrometheusQueryResolution() time.Duration {
	// Use the configured query resolution, or default to
	// 5m (i.e. 300s)
	secs := time.Duration(env.GetInt64(PrometheusQueryResolutionSecondsEnvVar, 300))
	return secs * time.Second
}

// GetMaxQueryConcurrency returns the environment variable value for MaxQueryConcurrencyEnvVar
func GetMaxQueryConcurrency() int {
	maxQueryConcurrency := env.GetInt(MaxQueryConcurrencyEnvVar, 5)
	if maxQueryConcurrency <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return maxQueryConcurrency
}

func GetDBBasicAuthUsername() string {
	return env.Get(DBBasicAuthUsername, "")
}

func GetDBBasicAuthUserPassword() string {
	return env.Get(DBBasicAuthPassword, "")
}

func GetDBBearerToken() string {
	return env.Get(DBBearerToken, "")
}

func GetPrometheusMaxQueryDuration() time.Duration {
	dayMins := 60 * 24
	mins := time.Duration(env.GetInt64(PrometheusMaxQueryDurationMinutesEnvVar, int64(dayMins)))
	return mins * time.Minute
}

// GetPromClusterFilter returns environment variable value CurrentClusterIdFilterEnabledVar which
// represents additional prometheus filter for all metrics for current cluster id
func GetPromClusterFilter() string {
	if env.GetBool(CurrentClusterIdFilterEnabledVar, false) {
		return fmt.Sprintf("%s=\"%s\"", GetPromClusterLabel(), env.GetClusterID())
	}
	return ""
}

// GetPromClusterLabel returns the environment variable value for PromClusterIDLabel: `PROM_CLUSTER_ID_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the ClusterID
func GetPromClusterLabel() string {
	return env.Get(PromClusterIDLabelEnvVar, source.ClusterIDLabel)
}

// GetPromNamespaceLabel returns the value set by PromNamespaceLabelEnvVar: `PROM_NAMESPACE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Namespace.
func GetPromNamespaceLabel() []string {
	return GetListWithDefaults(PromNamespaceLabelEnvVar, source.NamespaceLabel)
}

// GetPromNodeLabel returns the value set by PromNodeLabelEnvVar: `PROM_NODE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Node.
func GetPromNodeLabel() []string {
	return GetListWithDefaults(PromNodeLabelEnvVar, source.NodeLabel)
}

// GetPromInstanceLabel returns the value set by PromInstanceLabelEnvVar: `PROM_INSTANCE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Instance.
func GetPromInstanceLabel() []string {
	return GetListWithDefaults(PromInstanceLabelEnvVar, source.InstanceLabel)
}

// GetPromInstanceTypeLabel returns the value set by PromInstanceTypeLabelEnvVar: `PROM_INSTANCE_TYPE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the InstanceType.
func GetPromInstanceTypeLabel() []string {
	return GetListWithDefaults(PromInstanceTypeLabelEnvVar, source.InstanceTypeLabel)
}

// GetPromContainerLabel returns the value set by PromContainerLabelEnvVar: `PROM_CONTAINER_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Container.
func GetPromContainerLabel() []string {
	return GetListWithDefaults(PromContainerLabelEnvVar, source.ContainerLabel, source.ContainerNameLabel)
}

// GetPromPodLabel returns the value set by PromPodLabelEnvVar: `PROM_POD_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Pod.
func GetPromPodLabel() []string {
	return GetListWithDefaults(PromPodLabelEnvVar, source.PodLabel, source.PodNameLabel)
}

// GetPromProviderIDLabel returns the value set by PromProviderIDLabelEnvVar: `PROM_PROVIDER_ID_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the ProviderID.
func GetPromProviderIDLabel() []string {
	return GetListWithDefaults(PromProviderIDLabelEnvVar, source.ProviderIDLabel)
}

// GetPromDeviceLabel returns the value set by PromDeviceLabelEnvVar: `PROM_DEVICE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Device.
func GetPromDeviceLabel() []string {
	return GetListWithDefaults(PromDeviceLabelEnvVar, source.DeviceLabel)
}

// GetPromPVCLabel returns the value set by PromPVCLabelEnvVar: `PROM_PVC_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the PVC.
func GetPromPVCLabel() []string {
	return GetListWithDefaults(PromPVCLabelEnvVar, source.PVCLabel)
}

// GetPromPVLabel returns the value set by PromPVLabelEnvVar: `PROM_PV_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the PV.
func GetPromPVLabel() []string {
	return GetListWithDefaults(PromPVLabelEnvVar, source.PVLabel)
}

// GetPromStorageClassLabel returns the value set by PromStorageClassLabelEnvVar: `PROM_STORAGE_CLASS_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the StorageClass.
func GetPromStorageClassLabel() []string {
	return GetListWithDefaults(PromStorageClassLabelEnvVar, source.StorageClassLabel)
}

// GetPromVolumeNameLabel returns the value set by PromVolumeNameLabelEnvVar: `PROM_VOLUME_NAME_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the VolumeName.
func GetPromVolumeNameLabel() []string {
	return GetListWithDefaults(PromVolumeNameLabelEnvVar, source.VolumeNameLabel)
}

// GetPromServiceLabel returns the value set by PromServiceLabelEnvVar: `PROM_SERVICE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Service.
func GetPromServiceLabel() []string {
	return GetListWithDefaults(PromServiceLabelEnvVar, source.ServiceLabel, source.ServiceNameLabel)
}

// GetPromIngressIPLabel returns the value set by PromIngressIPLabelEnvVar: `PROM_INGRESS_IP_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the IngressIP.
func GetPromIngressIPLabel() []string {
	return GetListWithDefaults(PromIngressIPLabelEnvVar, source.IngressIPLabel)
}

// GetPromProvisionerNameLabel returns the value set by PromProvisionerNameLabelEnvVar: `PROM_PROVISIONER_NAME_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the ProvisionerName.
func GetPromProvisionerNameLabel() []string {
	return GetListWithDefaults(PromProvisionerNameLabelEnvVar, source.ProvisionerNameLabel)
}

// GetPromUIDLabel returns the value set by PromUIDLabelEnvVar: `PROM_UID_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the UID.
func GetPromUIDLabel() []string {
	return GetListWithDefaults(PromUIDLabelEnvVar, source.UIDLabel)
}

// GetPromKubernetesNodeLabel returns the value set by PromKubernetesNodeLabelEnvVar: `PROM_KUBERNETES_NODE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the KubernetesNode.
func GetPromKubernetesNodeLabel() []string {
	return GetListWithDefaults(PromKubernetesNodeLabelEnvVar, source.KubernetesNodeLabel)
}

// GetPromModeLabel returns the value set by PromModeLabelEnvVar: `PROM_MODE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Mode.
func GetPromModeLabel() []string {
	return GetListWithDefaults(PromModeLabelEnvVar, source.ModeLabel)
}

// GetPromModelNameLabel returns the value set by PromModelNameLabelEnvVar: `PROM_MODEL_NAME_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the ModelName.
func GetPromModelNameLabel() []string {
	return GetListWithDefaults(PromModelNameLabelEnvVar, source.ModelNameLabel)
}

// GetPromUUIDLabel returns the value set by PromUUIDLabelEnvVar: `PROM_UUID_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the UUID.
func GetPromUUIDLabel() []string {
	return GetListWithDefaults(PromUUIDLabelEnvVar, source.UUIDLabel)
}

// GetPromResourceLabel returns the value set by PromResourceLabelEnvVar: `PROM_RESOURCE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Resource.
func GetPromResourceLabel() []string {
	return GetListWithDefaults(PromResourceLabelEnvVar, source.ResourceLabel)
}

// GetPromDeploymentLabel returns the value set by PromDeploymentLabelEnvVar: `PROM_DEPLOYMENT_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Deployment.
func GetPromDeploymentLabel() []string {
	return GetListWithDefaults(PromDeploymentLabelEnvVar, source.DeploymentLabel)
}

// GetPromStatefulSetLabel returns the value set by PromStatefulSetLabelEnvVar: `PROM_STATEFUL_SET_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the StatefulSet.
func GetPromStatefulSetLabel() []string {
	return GetListWithDefaults(PromStatefulSetLabelEnvVar, source.StatefulSetLabel)
}

// GetPromReplicaSetLabel returns the value set by PromReplicaSetLabelEnvVar: `PROM_REPLICA_SET_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the ReplicaSet.
func GetPromReplicaSetLabel() []string {
	return GetListWithDefaults(PromReplicaSetLabelEnvVar, source.ReplicaSetLabel)
}

// GetPromOwnerNameLabel returns the value set by PromOwnerNameLabelEnvVar: `PROM_OWNER_NAME_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the OwnerName.
func GetPromOwnerNameLabel() []string {
	return GetListWithDefaults(PromOwnerNameLabelEnvVar, source.OwnerNameLabel)
}

// GetPromOwnerKindLabel returns the value set by PromOwnerKindLabelEnvVar: `PROM_OWNER_KIND_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the OwnerKind.
func GetPromOwnerKindLabel() []string {
	return GetListWithDefaults(PromOwnerKindLabelEnvVar, source.OwnerKindLabel)
}

// GetPromUnitLabel returns the value set by PromUnitLabelEnvVar: `PROM_UNIT_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Unit.
func GetPromUnitLabel() []string {
	return GetListWithDefaults(PromUnitLabelEnvVar, source.UnitLabel)
}

// GetPromInternetLabel returns the value set by PromInternetLabelEnvVar: `PROM_INTERNET_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the Internet.
func GetPromInternetLabel() []string {
	return GetListWithDefaults(PromInternetLabelEnvVar, source.InternetLabel)
}

// GetPromSameZoneLabel returns the value set by PromSameZoneLabelEnvVar: `PROM_SAME_ZONE_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the SameZone.
func GetPromSameZoneLabel() []string {
	return GetListWithDefaults(PromSameZoneLabelEnvVar, source.SameZoneLabel)
}

// GetPromSameRegionLabel returns the value set by PromSameRegionLabelEnvVar: `PROM_SAME_REGION_LABEL`
// Prometheus query formatting and results parsers will use this label to determine the SameRegion.
func GetPromSameRegionLabel() []string {
	return GetListWithDefaults(PromSameRegionLabelEnvVar, source.SameRegionLabel)
}

func GetListWithDefaults(key string, defaultValues ...string) []string {
	list := env.GetList(key, ",")
	if len(list) == 0 {
		return defaultValues
	}
	return list
}
