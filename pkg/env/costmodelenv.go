package env

const (
	AWSAccessKeyIDEnvVar     = "AWS_ACCESS_KEY_ID"
	AWSAccessKeySecretEnvVar = "AWS_SECRET_ACCESS_KEY"
	AWSClusterIDEnvVar       = "AWS_CLUSTER_ID"

	KubecostNamespaceEnvVar        = "KUBECOST_NAMESPACE"
	ClusterIDEnvVar                = "CLUSTER_ID"
	ClusterProfileEnvVar           = "CLUSTER_PROFILE"
	PrometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"
	MaxQueryConcurrencyEnvVar      = "MAX_QUERY_CONCURRENCY"
	RemoteEnabledEnvVar            = "REMOTE_WRITE_ENABLED"
	RemotePWEnvVar                 = "REMOTE_WRITE_PASSWORD"
	SQLAddressEnvVar               = "SQL_ADDRESS"
	UseCSVProviderEnvVar           = "USE_CSV_PROVIDER"
	CSVRegionEnvVar                = "CSV_REGION"
	CSVPathEnvVar                  = "CSV_PATH"
	ConfigPathEnvVar               = "CONFIG_PATH"
	CloudProviderAPIKeyEnvVar      = "CLOUD_PROVIDER_API_KEY"

	ThanosEnabledEnvVar  = "THANOS_ENABLED"
	ThanosQueryUrlEnvVar = "THANOS_QUERY_URL"
	ThanosOffsetEnvVar   = "THANOS_QUERY_OFFSET"

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
)

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
