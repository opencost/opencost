package env

import (
	"time"

	"github.com/opencost/opencost/core/pkg/env"
)

// FilePaths
const (
	ClusterInfoFile = "cluster-info.json"
	ClusterCacheFile
	GCPAuthSecretFile        = "key.json"
	MetricConfigFile         = "metrics.json"
	DefaultLocalCollectorDir = "collector"
)

// Env Variables
const (
	// Open configs

	// We assume that Kubernetes is enabled if there is a KUBERNETES_PORT environment variable present
	KubernetesEnabledEnvVar = "KUBERNETES_PORT"

	// Cloud Provider
	AWSAccessKeyIDEnvVar     = "AWS_ACCESS_KEY_ID"
	AWSAccessKeySecretEnvVar = "AWS_SECRET_ACCESS_KEY"
	AWSClusterIDEnvVar       = "AWS_CLUSTER_ID"
	AWSPricingURL            = "AWS_PRICING_URL"
	AWSECSPricingURLOverride = "AWS_ECS_PRICING_URL"

	AlibabaAccessKeyIDEnvVar     = "ALIBABA_ACCESS_KEY_ID"
	AlibabaAccessKeySecretEnvVar = "ALIBABA_SECRET_ACCESS_KEY"

	AzureOfferIDEnvVar        = "AZURE_OFFER_ID"
	AzureBillingAccountEnvVar = "AZURE_BILLING_ACCOUNT"
	
	// Azure rate card filter environment variables
	AzureLocaleEnvVar     = "AZURE_LOCALE"
	AzureCurrencyEnvVar   = "AZURE_CURRENCY"
	AzureRegionInfoEnvVar = "AZURE_REGION_INFO"

	// Currently being used for OCI and DigitalOcean
	ProviderPricingURL = "PROVIDER_PRICING_URL"

	ClusterProfileEnvVar    = "CLUSTER_PROFILE"
	RemoteEnabledEnvVar     = "REMOTE_WRITE_ENABLED"
	RemotePWEnvVar          = "REMOTE_WRITE_PASSWORD"
	SQLAddressEnvVar        = "SQL_ADDRESS"
	UseCSVProviderEnvVar    = "USE_CSV_PROVIDER"
	UseCustomProviderEnvVar = "USE_CUSTOM_PROVIDER"
	CSVRegionEnvVar         = "CSV_REGION"
	CSVEndpointEnvVar       = "CSV_ENDPOINT"
	CSVPathEnvVar           = "CSV_PATH"

	CloudProviderAPIKeyEnvVar        = "CLOUD_PROVIDER_API_KEY"
	CollectorDataSourceEnabledEnvVar = "COLLECTOR_DATA_SOURCE_ENABLED"
	LocalCollectorDirectoryEnvVar    = "LOCAL_COLLECTOR_DIRECTORY"

	EmitPodAnnotationsMetricEnvVar       = "EMIT_POD_ANNOTATIONS_METRIC"
	EmitNamespaceAnnotationsMetricEnvVar = "EMIT_NAMESPACE_ANNOTATIONS_METRIC"
	EmitDeprecatedMetrics                = "EMIT_DEPRECATED_METRICS"

	EmitKsmV1MetricsEnvVar = "EMIT_KSM_V1_METRICS"
	EmitKsmV1MetricsOnly   = "EMIT_KSM_V1_METRICS_ONLY"

	LogCollectionEnabledEnvVar    = "LOG_COLLECTION_ENABLED"
	ProductAnalyticsEnabledEnvVar = "PRODUCT_ANALYTICS_ENABLED"
	ErrorReportingEnabledEnvVar   = "ERROR_REPORTING_ENABLED"
	ValuesReportingEnabledEnvVar  = "VALUES_REPORTING_ENABLED"

	PricingConfigmapName = "PRICING_CONFIGMAP_NAME"
	MetricsConfigmapName = "METRICS_CONFIGMAP_NAME"

	ClusterInfoFileEnabledEnvVar = "CLUSTER_INFO_FILE_ENABLED"

	IngestPodUIDEnvVar = "INGEST_POD_UID"

	AllocationNodeLabelsEnabled = "ALLOCATION_NODE_LABELS_ENABLED"

	AssetIncludeLocalDiskCostEnvVar = "ASSET_INCLUDE_LOCAL_DISK_COST"

	regionOverrideList = "REGION_OVERRIDE_LIST"

	ExportCSVFile       = "EXPORT_CSV_FILE"
	ExportCSVLabelsList = "EXPORT_CSV_LABELS_LIST"
	ExportCSVLabelsAll  = "EXPORT_CSV_LABELS_ALL"
	ExportCSVMaxDays    = "EXPORT_CSV_MAX_DAYS"

	CarbonEstimatesEnabledEnvVar = "CARBON_ESTIMATES_ENABLED"

	KubernetesResourceAccessEnvVar = "KUBERNETES_RESOURCE_ACCESS"
	UseCacheV1                     = "USE_CACHE_V1"

	// Cloud provider override
	CloudProviderVar = "CLOUD_PROVIDER"

	// MCP Server
	MCPServerEnabledEnvVar = "MCP_SERVER_ENABLED"
	MCPHTTPPortEnvVar      = "MCP_HTTP_PORT"

	// Metrics Emitter
	MetricsEmitterQueryWindowEnvVar = "METRICS_EMITTER_QUERY_WINDOW"
)

func GetGCPAuthSecretFilePath() string {
	return env.GetPathFromConfig(GCPAuthSecretFile)
}

func GetExportCSVFile() string {
	return env.Get(ExportCSVFile, "")
}

func GetExportCSVLabelsAll() bool {
	return env.GetBool(ExportCSVLabelsAll, false)
}

func GetExportCSVLabelsList() []string {
	return env.GetList(ExportCSVLabelsList, ",")
}

func GetExportCSVMaxDays() int {
	return env.GetInt(ExportCSVMaxDays, 90)
}

// IsClusterInfoFileEnabled returns true if the cluster info is read from a file or pulled from the local
// cloud provider and kubernetes.
func IsClusterInfoFileEnabled() bool {
	return env.GetBool(ClusterInfoFileEnabledEnvVar, false)
}

func GetClusterInfoFilePath() string {
	return env.GetPathFromConfig(ClusterInfoFile)
}

func GetClusterCacheFilePath() string {
	return env.GetPathFromConfig(ClusterCacheFile)
}

func GetPricingConfigmapName() string {
	return env.Get(PricingConfigmapName, "pricing-configs")
}

func GetMetricsConfigmapName() string {
	return env.Get(MetricsConfigmapName, "metrics-config")
}

// IsEmitNamespaceAnnotationsMetric returns true if cost-model is configured to emit the kube_namespace_annotations metric
// containing the namespace annotations
func IsEmitNamespaceAnnotationsMetric() bool {
	return env.GetBool(EmitNamespaceAnnotationsMetricEnvVar, false)
}

// IsEmitPodAnnotationsMetric returns true if cost-model is configured to emit the kube_pod_annotations metric containing
// pod annotations.
func IsEmitPodAnnotationsMetric() bool {
	return env.GetBool(EmitPodAnnotationsMetricEnvVar, false)
}

// IsEmitKsmV1Metrics returns true if cost-model is configured to emit all necessary KSM v1
// metrics that were removed in KSM v2
func IsEmitKsmV1Metrics() bool {
	return env.GetBool(EmitKsmV1MetricsEnvVar, true)
}

func IsEmitKsmV1MetricsOnly() bool {
	return env.GetBool(EmitKsmV1MetricsOnly, false)
}

func IsEmitDeprecatedMetrics() bool {
	return env.GetBool(EmitDeprecatedMetrics, false)
}

// GetAWSAccessKeyID returns the environment variable value for AWSAccessKeyIDEnvVar which represents
// the AWS access key for authentication
func GetAWSAccessKeyID() string {
	return env.Get(AWSAccessKeyIDEnvVar, "")
}

// GetAWSAccessKeySecret returns the environment variable value for AWSAccessKeySecretEnvVar which represents
// the AWS access key secret for authentication
func GetAWSAccessKeySecret() string {
	return env.Get(AWSAccessKeySecretEnvVar, "")
}

// GetAWSClusterID returns the environment variable value for AWSClusterIDEnvVar which represents
// an AWS specific cluster identifier.
func GetAWSClusterID() string {
	return env.Get(AWSClusterIDEnvVar, "")
}

// GetAWSPricingURL returns an optional alternative URL to fetch AWS pricing data from; for use in airgapped environments
func GetAWSPricingURL() string {
	return env.Get(AWSPricingURL, "")
}

// GetAWSECSPricingURLOverride returns an optional alternative URL to fetch AmazonECS pricing data from; for use in airgapped environments
func GetAWSECSPricingURLOverride() string {
	return env.Get(AWSECSPricingURLOverride, "")
}

// GetAlibabaAccessKeyID returns the environment variable value for AlibabaAccessKeyIDEnvVar which represents
// the Alibaba access key for authentication
func GetAlibabaAccessKeyID() string {
	return env.Get(AlibabaAccessKeyIDEnvVar, "")
}

// GetAlibabaAccessKeySecret returns the environment variable value for AlibabaAccessKeySecretEnvVar which represents
// the Alibaba access key secret for authentication
func GetAlibabaAccessKeySecret() string {
	return env.Get(AlibabaAccessKeySecretEnvVar, "")
}

// GetAzureOfferID returns the environment variable value for AzureOfferIDEnvVar which represents
// the Azure offer ID for determining prices.
func GetAzureOfferID() string {
	return env.Get(AzureOfferIDEnvVar, "")
}

// GetAzureBillingAccount returns the environment variable value for
// AzureBillingAccountEnvVar which represents the Azure billing
// account for determining prices. If this is specified
// customer-specific prices will be downloaded from the consumption
// price sheet API.
func GetAzureBillingAccount() string {
	return env.Get(AzureBillingAccountEnvVar, "")
}

// GetAzureLocale returns the environment variable value for AzureLocaleEnvVar which represents
// the Azure rate card locale filter. Defaults to "en-US" if not specified.
func GetAzureLocale() string {
	return env.Get(AzureLocaleEnvVar, "en-US")
}

// GetAzureCurrency returns the environment variable value for AzureCurrencyEnvVar which represents
// the Azure rate card currency filter. This overrides the default currency from config if specified.
func GetAzureCurrency() string {
	return env.Get(AzureCurrencyEnvVar, "")
}

// GetAzureRegionInfo returns the environment variable value for AzureRegionInfoEnvVar which represents
// the Azure rate card region filter. This overrides the default region from config if specified.
func GetAzureRegionInfo() string {
	return env.Get(AzureRegionInfoEnvVar, "")
}

// IsAzureDownloadBillingDataToDisk returns the environment variable value for
// AzureDownloadBillingDataToDiskEnvVar which indicates whether the Azure
// Billing Data should be held in memory or written to disk.
func IsAzureDownloadBillingDataToDisk() bool {
	return env.GetBool(AzureDownloadBillingDataToDiskEnvVar, true)
}

// GetClusterProfile returns the environment variable value for ClusterProfileEnvVar which
// represents the cluster profile configured for
func GetClusterProfile() string {
	return env.Get(ClusterProfileEnvVar, "development")
}

// IsRemoteEnabled returns the environment variable value for RemoteEnabledEnvVar which represents whether
// or not remote write is enabled for prometheus for use with SQL backed persistent storage.
func IsRemoteEnabled() bool {
	return env.GetBool(RemoteEnabledEnvVar, false)
}

// GetRemotePW returns the environment variable value for RemotePWEnvVar which represents the remote
// persistent storage password.
func GetRemotePW() string {
	return env.Get(RemotePWEnvVar, "")
}

// GetSQLAddress returns the environment variable value for SQLAddressEnvVar which represents the SQL
// database address used with remote persistent storage.
func GetSQLAddress() string {
	return env.Get(SQLAddressEnvVar, "")
}

// IsUseCSVProvider returns the environment variable value for UseCSVProviderEnvVar which represents
// whether or not the use of a CSV cost provider is enabled.
func IsUseCSVProvider() bool {
	return env.GetBool(UseCSVProviderEnvVar, false)
}

// IsUseCustomProvider returns the environment variable value for UseCustomProviderEnvVar which represents
// whether or not the use of a custom cost provider is enabled.
func IsUseCustomProvider() bool {
	return env.GetBool(UseCustomProviderEnvVar, false)
}

// GetCSVRegion returns the environment variable value for CSVRegionEnvVar which represents the
// region configured for a CSV provider.
func GetCSVRegion() string {
	return env.Get(CSVRegionEnvVar, "")
}

// GetCSVEndpoint returns the environment variable value for CSVEndpointEnvVar which represents the
// endpoint configured for a S3 CSV provider another than AWS S3.
func GetCSVEndpoint() string {
	return env.Get(CSVEndpointEnvVar, "")
}

// GetCSVPath returns the environment variable value for CSVPathEnvVar which represents the key path
// configured for a CSV provider.
func GetCSVPath() string {
	return env.Get(CSVPathEnvVar, "")
}

// GetCloudProviderAPI returns the environment variable value for CloudProviderAPIEnvVar which represents
// the API key provided for the cloud provider.
func GetCloudProviderAPIKey() string {
	return env.Get(CloudProviderAPIKeyEnvVar, "")
}

// IsCollectorDataSourceEnabeled returns the environment variable which enables a source.OpencostDatasource which does not use uses Prometheus
func IsCollectorDataSourceEnabled() bool {
	return env.GetBool(CollectorDataSourceEnabledEnvVar, false)
}

// IsLogCollectionEnabled returns the environment variable value for LogCollectionEnabledEnvVar which represents
// whether or not log collection has been enabled for kubecost deployments.
func IsLogCollectionEnabled() bool {
	return env.GetBool(LogCollectionEnabledEnvVar, true)
}

// IsProductAnalyticsEnabled returns the environment variable value for ProductAnalyticsEnabledEnvVar
func IsProductAnalyticsEnabled() bool {
	return env.GetBool(ProductAnalyticsEnabledEnvVar, true)
}

// IsErrorReportingEnabled returns the environment variable value for ErrorReportingEnabledEnvVar
func IsErrorReportingEnabled() bool {
	return env.GetBool(ErrorReportingEnabledEnvVar, true)
}

// IsValuesReportingEnabled returns the environment variable value for ValuesReportingEnabledEnvVar
func IsValuesReportingEnabled() bool {
	return env.GetBool(ValuesReportingEnabledEnvVar, true)
}

// IsIngestingPodUID returns the env variable from ingestPodUID, which alters the
// contents of podKeys in Allocation
func IsIngestingPodUID() bool {
	return env.GetBool(IngestPodUIDEnvVar, false)
}

func IsAllocationNodeLabelsEnabled() bool {
	return env.GetBool(AllocationNodeLabelsEnabled, true)
}

func IsAssetIncludeLocalDiskCost() bool {
	return env.GetBool(AssetIncludeLocalDiskCostEnvVar, true)
}

func GetRegionOverrideList() []string {
	regionList := env.GetList(regionOverrideList, ",")

	if regionList == nil {
		return []string{}
	}

	return regionList
}

func IsKubernetesEnabled() bool {
	return env.Get(KubernetesEnabledEnvVar, "") != ""
}

func GetOCIPricingURL() string {
	return env.Get(ProviderPricingURL, "https://apexapps.oracle.com/pls/apex/cetools/api/v1/products")
}

func IsCarbonEstimatesEnabled() bool {
	return env.GetBool(CarbonEstimatesEnabledEnvVar, false)
}

// HasKubernetesResourceAccess can be set to false if Opencost is run without access to the kubernetes resources
func HasKubernetesResourceAccess() bool { return env.GetBool(KubernetesResourceAccessEnvVar, true) }

// GetUseCacheV1 is a temporary flag to allow users to opt-in to using the old cache
// Mainly for comparison purposes
func GetUseCacheV1() bool {
	return env.GetBool(UseCacheV1, false)
}

// GetCloudProvider returns the explicitly set cloud provider from environment variable
func GetCloudProvider() string {
	return env.Get(CloudProviderVar, "")
}

func GetMetricConfigFile() string {
	return env.GetPathFromConfig(MetricConfigFile)
}

func GetLocalCollectorDirectory() string {
	dir := env.Get(LocalCollectorDirectoryEnvVar, DefaultLocalCollectorDir)
	return env.GetPathFromConfig(dir)
}

func GetDOKSPricingURL() string {
	return env.Get(ProviderPricingURL, "https://api.digitalocean.com/v2/billing/pricing")
}

// IsMCPServerEnabled returns the environment variable value for MCPServerEnabledEnvVar which represents
// whether or not the MCP server is enabled.
func IsMCPServerEnabled() bool {
	return env.GetBool(MCPServerEnabledEnvVar, true)
}

// GetMCPHTTPPort returns the environment variable value for MCPHTTPPortEnvVar which represents
// the HTTP port for the MCP server.
func GetMCPHTTPPort() int {
	return env.GetInt(MCPHTTPPortEnvVar, 8081)
}

// GetMetricsEmitterQueryWindow returns the time window for the metrics emitter
// to query historical data. This controls the time range used in ComputeCostData queries.
// Default is 2m.
func GetMetricsEmitterQueryWindow() time.Duration {
	return env.GetDuration(MetricsEmitterQueryWindowEnvVar, 2*time.Minute)
}
