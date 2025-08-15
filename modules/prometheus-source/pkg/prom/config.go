package prom

import (
	"crypto/x509"
	"fmt"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"

	restclient "k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
)

const (
	ServiceCA = `/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt`
)

func NewPrometheusLabelMappingFromEnv() source.FieldMapper {
	check := func(err error) {
		if err != nil {
			panic(fmt.Sprintf("Failed to create PrometheusLabelMapping from environment: %s", err))
		}
	}

	rfm := source.NewReverseFieldMapper()
	check(rfm.Set(source.ClusterIDLabel, env.GetPromClusterLabel()))
	check(rfm.Set(source.NamespaceLabel, env.GetPromNamespaceLabel()...))
	check(rfm.Set(source.NodeLabel, env.GetPromNodeLabel()...))
	check(rfm.Set(source.InstanceLabel, env.GetPromInstanceLabel()...))
	check(rfm.Set(source.InstanceTypeLabel, env.GetPromInstanceTypeLabel()...))
	check(rfm.Set(source.ContainerLabel, env.GetPromContainerLabel()...))
	check(rfm.Set(source.PodLabel, env.GetPromPodLabel()...))
	check(rfm.Set(source.ProviderIDLabel, env.GetPromProviderIDLabel()...))
	check(rfm.Set(source.DeviceLabel, env.GetPromDeviceLabel()...))
	check(rfm.Set(source.PVCLabel, env.GetPromPVCLabel()...))
	check(rfm.Set(source.PVLabel, env.GetPromPVLabel()...))
	check(rfm.Set(source.StorageClassLabel, env.GetPromStorageClassLabel()...))
	check(rfm.Set(source.VolumeNameLabel, env.GetPromVolumeNameLabel()...))
	check(rfm.Set(source.ServiceLabel, env.GetPromServiceLabel()...))
	check(rfm.Set(source.IngressIPLabel, env.GetPromIngressIPLabel()...))
	check(rfm.Set(source.ProvisionerNameLabel, env.GetPromProvisionerNameLabel()...))
	check(rfm.Set(source.UIDLabel, env.GetPromUIDLabel()...))
	check(rfm.Set(source.KubernetesNodeLabel, env.GetPromKubernetesNodeLabel()...))
	check(rfm.Set(source.ModeLabel, env.GetPromModeLabel()...))
	check(rfm.Set(source.ModelNameLabel, env.GetPromModelNameLabel()...))
	check(rfm.Set(source.UUIDLabel, env.GetPromUUIDLabel()...))
	check(rfm.Set(source.ResourceLabel, env.GetPromResourceLabel()...))
	check(rfm.Set(source.DeploymentLabel, env.GetPromDeploymentLabel()...))
	check(rfm.Set(source.StatefulSetLabel, env.GetPromStatefulSetLabel()...))
	check(rfm.Set(source.ReplicaSetLabel, env.GetPromReplicaSetLabel()...))
	check(rfm.Set(source.OwnerNameLabel, env.GetPromOwnerNameLabel()...))
	check(rfm.Set(source.OwnerKindLabel, env.GetPromOwnerKindLabel()...))
	check(rfm.Set(source.UnitLabel, env.GetPromUnitLabel()...))
	check(rfm.Set(source.InternetLabel, env.GetPromInternetLabel()...))
	check(rfm.Set(source.SameZoneLabel, env.GetPromSameZoneLabel()...))
	check(rfm.Set(source.SameRegionLabel, env.GetPromSameRegionLabel()...))
	return rfm
}

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
	LabelMapping          source.FieldMapper
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
	log.Debugf("[Prometheus]: Client Max Concurrency set to: %d", queryConcurrency)

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
	}

	dataResolution := env.GetPrometheusQueryResolution()

	// Ensuring if data resolution is less than 60s default it to 1m
	resolutionMinutes := int(dataResolution.Minutes())
	if resolutionMinutes == 0 {
		resolutionMinutes = 1
	}

	labelMapping := NewPrometheusLabelMappingFromEnv()

	clientConfig := &PrometheusClientConfig{
		Timeout:               timeout,
		KeepAlive:             keepAlive,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		TLSInsecureSkipVerify: env.IsInsecureSkipVerify(),
		RootCAs:               tlsCaCert,
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
		LabelMapping:          labelMapping,
	}, nil
}
