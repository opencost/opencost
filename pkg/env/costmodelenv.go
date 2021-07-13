package env

import (
	"regexp"
	"strconv"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
)

const (
	AppVersionEnvVar = "APP_VERSION"

	AWSAccessKeyIDEnvVar     = "AWS_ACCESS_KEY_ID"
	AWSAccessKeySecretEnvVar = "AWS_SECRET_ACCESS_KEY"
	AWSClusterIDEnvVar       = "AWS_CLUSTER_ID"

	AzureStorageAccessKeyEnvVar     = "AZURE_STORAGE_ACCESS_KEY"
	AzureStorageAccountNameEnvVar   = "AZURE_STORAGE_ACCOUNT"
	AzureStorageContainerNameEnvVar = "AZURE_STORAGE_CONTAINER"

	KubecostNamespaceEnvVar        = "KUBECOST_NAMESPACE"
	ClusterIDEnvVar                = "CLUSTER_ID"
	ClusterProfileEnvVar           = "CLUSTER_PROFILE"
	PrometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"
	MaxQueryConcurrencyEnvVar      = "MAX_QUERY_CONCURRENCY"
	QueryLoggingFileEnvVar         = "QUERY_LOGGING_FILE"
	RemoteEnabledEnvVar            = "REMOTE_WRITE_ENABLED"
	RemotePWEnvVar                 = "REMOTE_WRITE_PASSWORD"
	SQLAddressEnvVar               = "SQL_ADDRESS"
	UseCSVProviderEnvVar           = "USE_CSV_PROVIDER"
	CSVRegionEnvVar                = "CSV_REGION"
	CSVEndpointEnvVar              = "CSV_ENDPOINT"
	CSVPathEnvVar                  = "CSV_PATH"
	ConfigPathEnvVar               = "CONFIG_PATH"
	CloudProviderAPIKeyEnvVar      = "CLOUD_PROVIDER_API_KEY"

	EmitPodAnnotationsMetricEnvVar       = "EMIT_POD_ANNOTATIONS_METRIC"
	EmitNamespaceAnnotationsMetricEnvVar = "EMIT_NAMESPACE_ANNOTATIONS_METRIC"

	EmitKsmV1MetricsEnvVar = "EMIT_KSM_V1_METRICS"

	ThanosEnabledEnvVar      = "THANOS_ENABLED"
	ThanosQueryUrlEnvVar     = "THANOS_QUERY_URL"
	ThanosOffsetEnvVar       = "THANOS_QUERY_OFFSET"
	ThanosMaxSourceResEnvVar = "THANOS_MAX_SOURCE_RESOLUTION"

	LogCollectionEnabledEnvVar    = "LOG_COLLECTION_ENABLED"
	ProductAnalyticsEnabledEnvVar = "PRODUCT_ANALYTICS_ENABLED"
	ErrorReportingEnabledEnvVar   = "ERROR_REPORTING_ENABLED"
	ValuesReportingEnabledEnvVar  = "VALUES_REPORTING_ENABLED"

	DBBasicAuthUsername = "DB_BASIC_AUTH_USERNAME"
	DBBasicAuthPassword = "DB_BASIC_AUTH_PW"
	DBBearerToken       = "DB_BEARER_TOKEN"

	MultiClusterBasicAuthUsername = "MC_BASIC_AUTH_USERNAME"
	MultiClusterBasicAuthPassword = "MC_BASIC_AUTH_PW"
	MultiClusterBearerToken       = "MC_BEARER_TOKEN"

	InsecureSkipVerify = "INSECURE_SKIP_VERIFY"

	KubeConfigPathEnvVar = "KUBECONFIG_PATH"

	UTCOffsetEnvVar = "UTC_OFFSET"

	CacheWarmingEnabledEnvVar    = "CACHE_WARMING_ENABLED"
	ETLEnabledEnvVar             = "ETL_ENABLED"
	ETLMaxBatchHours             = "ETL_MAX_BATCH_HOURS"
	ETLResolutionSeconds         = "ETL_RESOLUTION_SECONDS"
	LegacyExternalAPIDisabledVar = "LEGACY_EXTERNAL_API_DISABLED"

	PromClusterIDLabelEnvVar = "PROM_CLUSTER_ID_LABEL"
)

// GetAWSAccessKeyID returns the environment variable value for AWSAccessKeyIDEnvVar which represents
// the AWS access key for authentication
func GetAppVersion() string {
	return Get(AppVersionEnvVar, "1.83.0")
}

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

// GetAWSAccessKeyID returns the environment variable value for AWSAccessKeyIDEnvVar which represents
// the AWS access key for authentication
func GetAWSAccessKeyID() string {
	return Get(AWSAccessKeyIDEnvVar, "")
}

// GetAWSAccessKeySecret returns the environment variable value for AWSAccessKeySecretEnvVar which represents
// the AWS access key secret for authentication
func GetAWSAccessKeySecret() string {
	return Get(AWSAccessKeySecretEnvVar, "")
}

// GetAWSClusterID returns the environment variable value for AWSClusterIDEnvVar which represents
// an AWS specific cluster identifier.
func GetAWSClusterID() string {
	return Get(AWSClusterIDEnvVar, "")
}

func GetAzureStorageAccessKey() string {
	return Get(AzureStorageAccessKeyEnvVar, "")
}

func GetAzureStorageAccountName() string {
	return Get(AzureStorageAccountNameEnvVar, "")
}

func GetAzureStorageContainerName() string {
	return Get(AzureStorageContainerNameEnvVar, "")
}

// GetKubecostNamespace returns the environment variable value for KubecostNamespaceEnvVar which
// represents the namespace the cost model exists in.
func GetKubecostNamespace() string {
	return Get(KubecostNamespaceEnvVar, "kubecost")
}

// GetClusterProfile returns the environment variable value for ClusterProfileEnvVar which
// represents the cluster profile configured for
func GetClusterProfile() string {
	return Get(ClusterProfileEnvVar, "development")
}

// GetClusterID returns the environment variable value for ClusterIDEnvVar which represents the
// configurable identifier used for multi-cluster metric emission.
func GetClusterID() string {
	return Get(ClusterIDEnvVar, "")
}

// GetPrometheusServerEndpoint returns the environment variable value for PrometheusServerEndpointEnvVar which
// represents the prometheus server endpoint used to execute prometheus queries.
func GetPrometheusServerEndpoint() string {
	return Get(PrometheusServerEndpointEnvVar, "")
}

func GetInsecureSkipVerify() bool {
	return GetBool(InsecureSkipVerify, false)
}

// IsRemoteEnabled returns the environment variable value for RemoteEnabledEnvVar which represents whether
// or not remote write is enabled for prometheus for use with SQL backed persistent storage.
func IsRemoteEnabled() bool {
	return GetBool(RemoteEnabledEnvVar, false)
}

// GetRemotePW returns the environment variable value for RemotePWEnvVar which represents the remote
// persistent storage password.
func GetRemotePW() string {
	return Get(RemotePWEnvVar, "")
}

// GetSQLAddress returns the environment variable value for SQLAddressEnvVar which represents the SQL
// database address used with remote persistent storage.
func GetSQLAddress() string {
	return Get(SQLAddressEnvVar, "")
}

// IsUseCSVProvider returns the environment variable value for UseCSVProviderEnvVar which represents
// whether or not the use of a CSV cost provider is enabled.
func IsUseCSVProvider() bool {
	return GetBool(UseCSVProviderEnvVar, false)
}

// GetCSVRegion returns the environment variable value for CSVRegionEnvVar which represents the
// region configured for a CSV provider.
func GetCSVRegion() string {
	return Get(CSVRegionEnvVar, "")
}

// GetCSVEndpoint returns the environment variable value for CSVEndpointEnvVar which represents the
// endpoint configured for a S3 CSV provider another than AWS S3.
func GetCSVEndpoint() string {
	return Get(CSVEndpointEnvVar, "")
}

// GetCSVPath returns the environment variable value for CSVPathEnvVar which represents the key path
// configured for a CSV provider.
func GetCSVPath() string {
	return Get(CSVPathEnvVar, "")
}

// GetConfigPath returns the environment variable value for ConfigPathEnvVar which represents the cost
// model configuration path
func GetConfigPath() string {
	return Get(ConfigPathEnvVar, "")
}

// GetConfigPath returns the environment variable value for ConfigPathEnvVar which represents the cost
// model configuration path
func GetConfigPathWithDefault(defaultValue string) string {
	return Get(ConfigPathEnvVar, defaultValue)
}

// GetCloudProviderAPI returns the environment variable value for CloudProviderAPIEnvVar which represents
// the API key provided for the cloud provider.
func GetCloudProviderAPIKey() string {
	return Get(CloudProviderAPIKeyEnvVar, "")
}

// IsThanosEnabled returns the environment variable value for ThanosEnabledEnvVar which represents whether
// or not thanos is enabled.
func IsThanosEnabled() bool {
	return GetBool(ThanosEnabledEnvVar, false)
}

// GetThanosQueryUrl returns the environment variable value for ThanosQueryUrlEnvVar which represents the
// target query endpoint for hitting thanos.
func GetThanosQueryUrl() string {
	return Get(ThanosQueryUrlEnvVar, "")
}

// GetThanosOffset returns the environment variable value for ThanosOffsetEnvVar which represents the total
// amount of time to offset all queries made to thanos.
func GetThanosOffset() string {
	return Get(ThanosOffsetEnvVar, "3h")
}

// GetThanosMaxSourceResolution returns the environment variable value for ThanosMaxSourceResEnvVar which represents
// the max source resolution to use when querying thanos.
func GetThanosMaxSourceResolution() string {
	res := Get(ThanosMaxSourceResEnvVar, "raw")

	switch res {
	case "raw":
		return "0s"
	case "0s":
		fallthrough
	case "5m":
		fallthrough
	case "1h":
		return res
	default:
		return "0s"
	}
}

// IsLogCollectionEnabled returns the environment variable value for LogCollectionEnabledEnvVar which represents
// whether or not log collection has been enabled for kubecost deployments.
func IsLogCollectionEnabled() bool {
	return GetBool(LogCollectionEnabledEnvVar, true)
}

// IsProductAnalyticsEnabled returns the environment variable value for ProductAnalyticsEnabledEnvVar
func IsProductAnalyticsEnabled() bool {
	return GetBool(ProductAnalyticsEnabledEnvVar, true)
}

// IsErrorReportingEnabled returns the environment variable value for ErrorReportingEnabledEnvVar
func IsErrorReportingEnabled() bool {
	return GetBool(ErrorReportingEnabledEnvVar, true)
}

// IsValuesReportingEnabled returns the environment variable value for ValuesReportingEnabledEnvVar
func IsValuesReportingEnabled() bool {
	return GetBool(ValuesReportingEnabledEnvVar, true)
}

// GetMaxQueryConcurrency returns the environment variable value for MaxQueryConcurrencyEnvVar
func GetMaxQueryConcurrency() int {
	return GetInt(MaxQueryConcurrencyEnvVar, 5)
}

// GetQueryLoggingFile returns a file location if query logging is enabled. Otherwise, empty string
func GetQueryLoggingFile() string {
	return Get(QueryLoggingFileEnvVar, "")
}

func GetDBBasicAuthUsername() string {
	return Get(DBBasicAuthUsername, "")
}

func GetDBBasicAuthUserPassword() string {
	return Get(DBBasicAuthPassword, "")

}

func GetDBBearerToken() string {
	return Get(DBBearerToken, "")
}

// GetMultiClusterBasicAuthUsername returns the environemnt variable value for MultiClusterBasicAuthUsername
func GetMultiClusterBasicAuthUsername() string {
	return Get(MultiClusterBasicAuthUsername, "")
}

// GetMultiClusterBasicAuthPassword returns the environemnt variable value for MultiClusterBasicAuthPassword
func GetMultiClusterBasicAuthPassword() string {
	return Get(MultiClusterBasicAuthPassword, "")
}

func GetMultiClusterBearerToken() string {
	return Get(MultiClusterBearerToken, "")
}

// GetKubeConfigPath returns the environment variable value for KubeConfigPathEnvVar
func GetKubeConfigPath() string {
	return Get(KubeConfigPathEnvVar, "")
}

// GetUTCOffset returns the environemnt variable value for UTCOffset
func GetUTCOffset() string {
	return Get(UTCOffsetEnvVar, "")
}

// GetParsedUTCOffset returns the duration of the configured UTC offset
func GetParsedUTCOffset() time.Duration {
	offset := time.Duration(0)

	if offsetStr := GetUTCOffset(); offsetStr != "" {
		regex := regexp.MustCompile(`^(\+|-)(\d\d):(\d\d)$`)
		match := regex.FindStringSubmatch(offsetStr)
		if match == nil {
			log.Warningf("Illegal UTC offset: %s", offsetStr)
			return offset
		}

		sig := 1
		if match[1] == "-" {
			sig = -1
		}

		hrs64, _ := strconv.ParseInt(match[2], 10, 64)
		hrs := sig * int(hrs64)

		mins64, _ := strconv.ParseInt(match[3], 10, 64)
		mins := sig * int(mins64)

		offset = time.Duration(hrs)*time.Hour + time.Duration(mins)
	}

	return offset
}

func IsCacheWarmingEnabled() bool {
	return GetBool(CacheWarmingEnabledEnvVar, true)
}

func IsETLEnabled() bool {
	return GetBool(ETLEnabledEnvVar, true)
}

// GetETLMaxBatchDuration limits the window duration of the most expensive ETL
// queries to a maximum batch size, such that queries can be tuned to avoid
// timeout for large windows; e.g. if a 24h query is expected to timeout, but
// a 6h query is expected to complete in 1m, then 6h could be a good value.
func GetETLMaxBatchDuration() time.Duration {
	// Default to 6h
	hrs := time.Duration(GetInt64(ETLMaxBatchHours, 6))
	return hrs * time.Hour
}

// GetETLResolution determines the resolution of ETL queries. The smaller the
// duration, the higher the resolution; the higher the resolution, the more
// accurate the query results, but the more computationally expensive. This
// value is always 1m for Prometheus, but is configurable for Thanos.
func GetETLResolution() time.Duration {
	// If Thanos is not enabled, hard-code to 1m resolution
	if !IsThanosEnabled() {
		return 60 * time.Second
	}

	// Thanos is enabled, so use the configured ETL resolution, or default to
	// 5m (i.e. 300s)
	secs := time.Duration(GetInt64(ETLResolutionSeconds, 300))
	return secs * time.Second
}

func LegacyExternalCostsAPIDisabled() bool {
	return GetBool(LegacyExternalAPIDisabledVar, false)
}

// GetPromClusterLabel returns the environemnt variable value for PromClusterIDLabel
func GetPromClusterLabel() string {
	return Get(PromClusterIDLabelEnvVar, "cluster_id")
}
