package collector

import (
	"os"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
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
	statSummaryClient util.StatSummaryClient,
) source.OpenCostDataSource {
	config := NewOpenCostCollectorConfigFromEnv()
	return NewCollectorDataSource(
		config,
		clusterInfoProvider,
		clusterCache,
		statSummaryClient,
	)
}

func NewCollectorDataSource(
	config CollectorConfig,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	statSummaryClient util.StatSummaryClient,
) source.OpenCostDataSource {

	var storeFactory metric.MetricStoreFactory
	storeFactory = NewOpenCostMetricStore

	resolutions := map[string]*util.Resolution{}
	for _, resConf := range config.Resolutions {
		resolution, err := util.NewResolution(resConf)
		if err != nil {
			log.Errorf("Error creating resolution for: %s", err.Error())
			continue
		}
		resolutions[resConf.Interval] = resolution
	}

	repo := metric.NewMetricRepository(resolutions, storeFactory)

	var store storage.Storage
	if config.BucketConfigFile != "" {
		bucketConfig, err := os.ReadFile(config.BucketConfigFile)
		if err != nil {
			log.Errorf("Failed to initialize bucket output storage, please check your configuration and bucket security settings: %s", err)
		} else {
			store, err = storage.NewBucketStorage(bucketConfig)
			if err != nil {
				log.Errorf("Failed to create bucket storage, please check your configuration and bucket security settings: %s", err)
			}
		}
	}

	scrapeController := scrape.NewScrapeController(
		resolutions,
		config.ScrapeInterval,
		config.ClusterID,
		config.NetworkPort,
		repo,
		clusterCache,
		statSummaryClient,
		store,
	)
	scrapeController.Start()

	metricQuerier := newCollectorMetricsQuerier(repo, config.Resolutions)

	// cluster info provider
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
	interval, _ := util.NewInterval(c.config.ScrapeInterval)
	current := interval.Truncate(time.Now().UTC())
	next := interval.Add(current, 1)
	return next.Sub(current)
}
