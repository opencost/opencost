package env

import (
	"fmt"
	"runtime"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
)

const (
	PrometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"

	PrometheusRetryOnRateLimitResponseEnvVar    = "PROMETHEUS_RETRY_ON_RATE_LIMIT"
	PrometheusRetryOnRateLimitMaxRetriesEnvVar  = "PROMETHEUS_RETRY_ON_RATE_LIMIT_MAX_RETRIES"
	PrometheusRetryOnRateLimitDefaultWaitEnvVar = "PROMETHEUS_RETRY_ON_RATE_LIMIT_DEFAULT_WAIT"

	PrometheusQueryTimeoutEnvVar        = "PROMETHEUS_QUERY_TIMEOUT"
	PrometheusKeepAliveEnvVar           = "PROMETHEUS_KEEP_ALIVE"
	PrometheusTLSHandshakeTimeoutEnvVar = "PROMETHEUS_TLS_HANDSHAKE_TIMEOUT"
	ScrapeIntervalEnvVar                = "KUBECOST_SCRAPE_INTERVAL"

	PrometheusMaxQueryDurationMinutesEnvVar = "PROMETHEUS_MAX_QUERY_DURATION_MINUTES"
	PrometheusQueryResolutionSecondsEnvVar  = "PROMETHEUS_QUERY_RESOLUTION_SECONDS"

	MaxQueryConcurrencyEnvVar = "MAX_QUERY_CONCURRENCY"
	PromClusterIDLabelEnvVar  = "PROM_CLUSTER_ID_LABEL"

	PrometheusHeaderXScopeOrgIdEnvVar = "PROMETHEUS_HEADER_X_SCOPE_ORGID"
	InsecureSkipVerifyEnvVar          = "INSECURE_SKIP_VERIFY"
	KubeRbacProxyEnabledEnvVar        = "KUBE_RBAC_PROXY_ENABLED"

	DBBasicAuthUsername = "DB_BASIC_AUTH_USERNAME"
	DBBasicAuthPassword = "DB_BASIC_AUTH_PW"
	DBBearerToken       = "DB_BEARER_TOKEN"

	PromMtlsAuthCAFile  = "PROM_MTLS_AUTH_CA_FILE"
	PromMtlsAuthCrtFile = "PROM_MTLS_AUTH_CRT_FILE"
	PromMtlsAuthKeyFile = "PROM_MTLS_AUTH_KEY_FILE"

	CurrentClusterIdFilterEnabledVar = "CURRENT_CLUSTER_ID_FILTER_ENABLED"

	KubecostJobNameEnvVar = "KUBECOST_JOB_NAME"
)

// In sharded Prometheus setups, PROMETHEUS_SERVER_ENDPOINT should point to a global query endpoint (e.g., Thanos Query, Cortex, or Mimir)
// to ensure OpenCost receives complete data. Pointing to a single Prometheus pod may result in incomplete or intermittent export results.

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

// GetPrometheusServerEndpoint returns the environment variable value for PrometheusServerEndpointEnvVar which
// represents the prometheus server endpoint used to execute prometheus queries.
func GetPrometheusServerEndpoint() string {
	return env.Get(PrometheusServerEndpointEnvVar, "")
}

func GetScrapeInterval() time.Duration {
	return env.GetDuration(ScrapeIntervalEnvVar, 0)
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

// GetJobName returns the environment variable value for JobNameEnvVar, specifying which job name
// is used for prometheus to scrape the provided metrics.
func GetJobName() string {
	return env.Get(KubecostJobNameEnvVar, "kubecost")
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

func IsPromMtlsAuthEnabled() bool {
	if GetPromMtlsAuthCAFile() == "" {
		return false
	}
	if GetPromMtlsAuthCrtFile() == "" {
		return false
	}
	if GetPromMtlsAuthKeyFile() == "" {
		return false
	}
	return true
}

func GetPromMtlsAuthCAFile() string {
	return env.Get(PromMtlsAuthCAFile, "")
}

func GetPromMtlsAuthCrtFile() string {
	return env.Get(PromMtlsAuthCrtFile, "")
}

func GetPromMtlsAuthKeyFile() string {
	return env.Get(PromMtlsAuthKeyFile, "")
}

func GetPrometheusMaxQueryDuration() time.Duration {
	dayMins := 60 * 24
	mins := time.Duration(env.GetInt64(PrometheusMaxQueryDurationMinutesEnvVar, int64(dayMins)))
	return mins * time.Minute
}

// GetPromClusterLabel returns the environment variable value for PromClusterIDLabel
func GetPromClusterLabel() string {
	return env.Get(PromClusterIDLabelEnvVar, "cluster_id")
}

// GetPromClusterFilter returns environment variable value CurrentClusterIdFilterEnabledVar which
// represents additional prometheus filter for all metrics for current cluster id
func GetPromClusterFilter() string {
	if env.GetBool(CurrentClusterIdFilterEnabledVar, false) {
		return fmt.Sprintf("%s=\"%s\"", GetPromClusterLabel(), env.GetClusterID())
	}
	return ""
}
