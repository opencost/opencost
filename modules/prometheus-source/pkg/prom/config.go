package prom

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"

	restclient "k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
)

const (
	ServiceCA = `/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt`
)

type OpenCostPrometheusConfig struct {
	ServerEndpoint        string
	Version               string
	IsOffsetResolution    bool
	ClientConfig          *PrometheusClientConfig
	ScrapeInterval        time.Duration
	JobName               string
	Offset                string
	QueryOffset           time.Duration
	MaxQueryDuration      time.Duration
	ClusterLabel          string
	ClusterID             string
	ClusterFilter         string
	DataResolution        time.Duration
	DataResolutionMinutes int
}

func (ocpc *OpenCostPrometheusConfig) IsRateLimitRetryEnabled() bool {
	return ocpc.ClientConfig.RateLimitRetryOpts != nil
}

// NewOpenCostPrometheusConfigFromEnv creates a new OpenCostPrometheusConfig from environment variables.
func NewOpenCostPrometheusConfigFromEnv() (*OpenCostPrometheusConfig, error) {
	serverEndpoint := env.GetPrometheusServerEndpoint()
	if serverEndpoint == "" {
		return nil, fmt.Errorf("no address for prometheus set in $%s", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	log.Infof("Prometheus Client Max Concurrency set to %d", queryConcurrency)

	timeout := env.GetPrometheusQueryTimeout()
	keepAlive := env.GetPrometheusKeepAlive()
	tlsHandshakeTimeout := env.GetPrometheusTLSHandshakeTimeout()

	jobName := env.GetJobName()
	scrapeInterval := env.GetScrapeInterval()

	maxQueryDuration := env.GetPrometheusMaxQueryDuration()

	clusterId := coreenv.GetClusterID()
	clusterLabel := env.GetPromClusterLabel()
	clusterFilter := env.GetPromClusterFilter()

	var rateLimitRetryOpts *RateLimitRetryOpts = nil
	if env.IsPrometheusRetryOnRateLimitResponse() {
		rateLimitRetryOpts = &RateLimitRetryOpts{
			MaxRetries:       env.GetPrometheusRetryOnRateLimitMaxRetries(),
			DefaultRetryWait: env.GetPrometheusRetryOnRateLimitDefaultWait(),
		}
	}

	auth := &ClientAuth{
		Username:    env.GetDBBasicAuthUsername(),
		Password:    env.GetDBBasicAuthUserPassword(),
		BearerToken: env.GetDBBearerToken(),
	}

	// We will use the service account token and service-ca.crt to authenticate with the Prometheus server via kube-rbac-proxy.
	// We need to ensure that the service account has the necessary permissions to access the Prometheus server by binding it to the appropriate role.
	var tlsCaCert *x509.CertPool
	var tlsClientCertificates []tls.Certificate
	if env.IsKubeRbacProxyEnabled() {
		restConfig, err := restclient.InClusterConfig()
		if err != nil {
			log.Errorf("%s was set to true but failed to get in-cluster config: %s", env.KubeRbacProxyEnabledEnvVar, err)
		}
		auth.BearerToken = restConfig.BearerToken
		tlsCaCert, err = certutil.NewPool(ServiceCA)
		if err != nil {
			log.Errorf("%s was set to true but failed to load service-ca.crt: %s", env.KubeRbacProxyEnabledEnvVar, err)
		}
	} else if env.IsPromMtlsAuthEnabled() {
		tlsCaCert = x509.NewCertPool()
		// The /etc/ssl/cert.pem location is correct for Alpine Linux, the container base used here
		systemCa, err := os.ReadFile("/etc/ssl/cert.pem")
		if err != nil {
			log.Errorf("mTLS options were set but failed to load system CAs: %s", err)
		} else {
			tlsCaCert.AppendCertsFromPEM(systemCa)
		}
		mTlsCa, err := os.ReadFile(env.GetPromMtlsAuthCAFile())
		if err != nil {
			log.Errorf("mTLS options were set but failed to load PROM_MTLS_AUTH_CA_FILE: %s", err)
		} else {
			tlsCaCert.AppendCertsFromPEM(mTlsCa)
		}
		mTlsKeyPair, err := tls.LoadX509KeyPair(env.GetPromMtlsAuthCrtFile(), env.GetPromMtlsAuthKeyFile())
		if err != nil {
			log.Errorf("mTLS options were set but failed to load PROM_MTLS_AUTH_CRT_FILE or PROM_MTLS_AUTH_KEY_FILE: %s", err)
		} else {
			tlsClientCertificates = []tls.Certificate{mTlsKeyPair}
		}
	}

	dataResolution := env.GetPrometheusQueryResolution()

	// Ensuring if data resolution is less than 60s default it to 1m
	resolutionMinutes := int(dataResolution.Minutes())
	if resolutionMinutes == 0 {
		resolutionMinutes = 1
	}

	clientConfig := &PrometheusClientConfig{
		Timeout:               timeout,
		KeepAlive:             keepAlive,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		TLSInsecureSkipVerify: env.IsInsecureSkipVerify(),
		RootCAs:               tlsCaCert,
		ClientCertificates:    tlsClientCertificates,
		RateLimitRetryOpts:    rateLimitRetryOpts,
		Auth:                  auth,
		QueryConcurrency:      queryConcurrency,
		QueryLogFile:          "",
		HeaderXScopeOrgId:     env.GetPrometheusHeaderXScopeOrgId(),
	}

	return &OpenCostPrometheusConfig{
		ServerEndpoint:        serverEndpoint,
		Version:               "0.0.0",
		IsOffsetResolution:    false,
		ClientConfig:          clientConfig,
		ScrapeInterval:        scrapeInterval,
		JobName:               jobName,
		Offset:                "",
		QueryOffset:           time.Duration(0),
		MaxQueryDuration:      maxQueryDuration,
		ClusterLabel:          clusterLabel,
		ClusterID:             clusterId,
		ClusterFilter:         clusterFilter,
		DataResolution:        dataResolution,
		DataResolutionMinutes: resolutionMinutes,
	}, nil
}
