package prom

import (
	"context"
	"fmt"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/julienschmidt/httprouter"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"

	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
)

// creates a new help error which indicates the caller can retry and is non-fatal.
func newHelpRetryError(format string, args ...any) error {
	formatWithHelp := format + "\nTroubleshooting help available at: %s"
	args = append(args, PrometheusTroubleshootingURL)

	cause := fmt.Errorf(formatWithHelp, args...)
	return source.NewHelpRetryError(cause)
}

// PrometheusDataSource is the OpenCost data source implementation leveraging Prometheus. Prometheus provides longer retention periods and
// more detailed metrics than the OpenCost Collector, which is useful for historical analysis and cost forecasting.
type PrometheusDataSource struct {
	promConfig   *OpenCostPrometheusConfig
	promClient   prometheus.Client
	promContexts *ContextFactory

	metricsQuerier *PrometheusMetricsQuerier
	clusterMap     clusters.ClusterMap
	clusterInfo    clusters.ClusterInfoProvider
}

// NewDefaultPrometheusDataSource creates and initializes a new `PrometheusDataSource` with configuration
// parsed from environment variables. This function will block until a connection to prometheus is established,
// or fails. It is recommended to run this function in a goroutine on a retry cycle.
func NewDefaultPrometheusDataSource(clusterInfoProvider clusters.ClusterInfoProvider) (*PrometheusDataSource, error) {
	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus config from env: %w", err)
	}

	return NewPrometheusDataSource(clusterInfoProvider, config)
}

// NewPrometheusDataSource initializes clients for Prometheus and Thanos, and returns a new PrometheusDataSource.
func NewPrometheusDataSource(infoProvider clusters.ClusterInfoProvider, promConfig *OpenCostPrometheusConfig) (*PrometheusDataSource, error) {
	promClient, err := NewPrometheusClient(promConfig.ServerEndpoint, promConfig.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build prometheus client: %w", err)
	}

	// validation of the prometheus client

	m, err := Validate(promClient, promConfig)
	if err != nil || !m.Running {
		if err != nil {
			return nil, newHelpRetryError("failed to query prometheus at %s: %w", promConfig.ServerEndpoint, err)
		} else if !m.Running {
			return nil, newHelpRetryError("prometheus at %s is not running", promConfig.ServerEndpoint)
		}
	} else {
		log.Infof("Success: retrieved the 'up' query against prometheus at: %s", promConfig.ServerEndpoint)
	}

	// we don't consider this a fatal error, but we log for visibility
	api := prometheusAPI.NewAPI(promClient)
	bi, err := api.Buildinfo(context.Background())

	if err != nil {
		log.Infof("No valid prometheus config file at %s. Error: %s.\nTroubleshooting help available at: %s.\n**Ignore if using cortex/mimir/thanos here**", promConfig.ServerEndpoint, err.Error(), PrometheusTroubleshootingURL)
	} else {
		log.Infof("Retrieved a prometheus config file from: %s", promConfig.ServerEndpoint)
		promConfig.Version = bi.Version

		// for versions of prometheus >= 3.0.0, we need to offset the resolution for range queries
		// due to a breaking change in prometheus lookback and range query alignment
		v, err := semver.NewVersion(promConfig.Version)
		if err != nil {
			log.Warnf("Failed to parse prometheus version %s. Error: %s", promConfig.Version, err.Error())
		} else {
			promConfig.IsOffsetResolution = v.Major() >= 3
		}
	}

	// Fix scrape interval if zero by attempting to lookup the interval for the configured job
	if promConfig.ScrapeInterval == 0 {
		promConfig.ScrapeInterval = time.Minute

		// Lookup scrape interval for kubecost job, update if found
		si, err := ScrapeIntervalFor(promClient, promConfig.JobName)
		if err == nil {
			promConfig.ScrapeInterval = si
		}
	}

	log.Infof("Using scrape interval of %f", promConfig.ScrapeInterval.Seconds())

	promContexts := NewContextFactory(promClient, promConfig)

	// metadata creation for cluster info
	metadata := map[string]string{
		clusters.ClusterInfoThanosEnabledKey: "false",
	}

	// cluster info provider
	clusterInfoProvider := clusters.NewClusterInfoDecorator(infoProvider, metadata)
	clusterMap := NewPrometheusClusterMap(promContexts, clusterInfoProvider, 5*time.Minute)

	// create metrics querier implementation for prometheus and thanos
	metricsQuerier := newPrometheusMetricsQuerier(
		promConfig,
		promClient,
		promContexts,
	)

	return &PrometheusDataSource{
		promConfig:     promConfig,
		promClient:     promClient,
		promContexts:   promContexts,
		metricsQuerier: metricsQuerier,
		clusterMap:     clusterMap,
		clusterInfo:    clusterInfoProvider,
	}, nil
}

func (pds *PrometheusDataSource) PrometheusClient() prometheus.Client {
	return pds.promClient
}

func (pds *PrometheusDataSource) PrometheusConfig() *OpenCostPrometheusConfig {
	return pds.promConfig
}

func (pds *PrometheusDataSource) PrometheusContexts() *ContextFactory {
	return pds.promContexts
}

func (pds *PrometheusDataSource) RegisterEndPoints(_ *httprouter.Router) {}

// RegisterDiagnostics registers any custom data source diagnostics with the `DiagnosticService` that can
// be used to report externally.
func (pds *PrometheusDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {
	const PrometheusDiagnosticCategory = "prometheus"

	for _, dd := range diagnosticDefinitions {
		err := diagService.Register(dd.ID, dd.Description, PrometheusDiagnosticCategory, func(ctx context.Context) (map[string]any, error) {
			promDiag := dd.NewDiagnostic(pds.promConfig.ClusterFilter, "")

			promContext := pds.promContexts.NewNamedContext(DiagnosticContextName)
			e := promDiag.executePrometheusDiagnosticQuery(promContext)
			if e != nil {
				return nil, fmt.Errorf("failed to execute prometheus diagnostic: %s - %w", dd.ID, e)
			}

			return promDiag.AsMap(), nil
		})

		if err != nil {
			log.Warnf("Failed to register prometheus diagnostic %s: %s", dd.ID, err.Error())
		}
	}
}

func (pds *PrometheusDataSource) RefreshInterval() time.Duration {
	return pds.promConfig.ScrapeInterval
}

func (pds *PrometheusDataSource) Metrics() source.MetricsQuerier {
	return pds.metricsQuerier
}

func (pds *PrometheusDataSource) ClusterMap() clusters.ClusterMap {
	return pds.clusterMap
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (pds *PrometheusDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return pds.clusterInfo
}

func (pds *PrometheusDataSource) BatchDuration() time.Duration {
	return pds.promConfig.MaxQueryDuration
}

func (pds *PrometheusDataSource) Resolution() time.Duration {
	return pds.promConfig.DataResolution
}
