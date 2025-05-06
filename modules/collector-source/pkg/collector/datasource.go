package collector

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	"k8s.io/client-go/kubernetes"
)

type collectorDataSource struct {
	metricsQuerier *collectorMetricsQuerier
	clusterMap     clusters.ClusterMap
	clusterInfo    clusters.ClusterInfoProvider
	config         CollectorConfig
}

func NewDefaultCollectorDataSource(
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	k8s kubernetes.Interface,
	statSummaryClient util.StatSummaryClient,
) source.OpenCostDataSource {
	config := NewOpenCostCollectorConfigFromEnv()
	return NewCollectorDataSource(
		config,
		clusterInfoProvider,
		clusterCache,
		k8s,
		statSummaryClient,
	)
}

func NewCollectorDataSource(
	config CollectorConfig,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	k8s kubernetes.Interface,
	statSummaryClient util.StatSummaryClient,
) source.OpenCostDataSource {

	var storeFactory metric.MetricStoreFactory
	storeFactory = NewOpenCostMetricStore

	repo := metric.NewMetricRepository(metric.RepositoryConfig{
		Resolutions: config.Resolutions,
	}, storeFactory)

	scrapeController := scrape.NewScrapeController(
		config.ScrapeInterval,
		config.ReleaseName,
		config.NetworkPort,
		repo,
		clusterCache,
		k8s,
		statSummaryClient,
	)
	scrapeController.Start()

	metricQuerier := newCollectorMetricsQuerier(repo, config.Resolutions)

	clusterInfo := clusterInfoProvider

	clusterMap := newCollectorClusterMap(clusterInfo)

	return &collectorDataSource{
		metricsQuerier: metricQuerier,
		clusterInfo:    clusterInfo,
		clusterMap:     clusterMap,
	}
}

func (c *collectorDataSource) RegisterEndPoints(router *httprouter.Router) {
	return
}

func (c *collectorDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {
	return
}

func (c *collectorDataSource) Metrics() source.MetricsQuerier {
	return c.metricsQuerier
}

func (c *collectorDataSource) ClusterMap() clusters.ClusterMap {
	return c.clusterMap
}

func (c *collectorDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return c.clusterInfo
}

// BatchDuration collector data source queries do not need to be broken up
func (c *collectorDataSource) BatchDuration() time.Duration {
	var maxDuration time.Duration = 1<<63 - 1
	return maxDuration
}

func (c *collectorDataSource) Resolution() time.Duration {
	return c.config.ScrapeInterval
}
