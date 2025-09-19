package collector

import (
	"context"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/nodestats"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type collectorDataSource struct {
	metricsQuerier    *collectorMetricsQuerier
	clusterMap        clusters.ClusterMap
	clusterInfo       clusters.ClusterInfoProvider
	config            CollectorConfig
	diagnosticsModule *metric.DiagnosticsModule
}

func NewDefaultCollectorDataSource(
	store storage.Storage,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	statSummaryClient nodestats.StatSummaryClient,
) source.OpenCostDataSource {
	config := NewOpenCostCollectorConfigFromEnv()
	return NewCollectorDataSource(
		config,
		store,
		clusterInfoProvider,
		clusterCache,
		statSummaryClient,
	)
}

func NewCollectorDataSource(
	config CollectorConfig,
	store storage.Storage,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	statSummaryClient nodestats.StatSummaryClient,
) source.OpenCostDataSource {
	var resolutions []*util.Resolution
	for _, resconf := range config.Resolutions {
		resolution, err := util.NewResolution(resconf)
		if err != nil {
			log.Errorf("failed to create resolution %s", err.Error())
			continue
		}
		resolutions = append(resolutions, resolution)
	}

	repo := metric.NewMetricRepository(
		resolutions,
		NewOpenCostMetricStore,
	)
	var updater metric.Updater
	updater = repo
	if store != nil {
		wal, err := metric.NewWalinator(
			config.ClusterID,
			config.ApplicationName,
			store,
			resolutions,
			updater,
		)
		if err != nil {
			log.Errorf("failed to initialize the walinator: %s", err.Error())
		} else {
			wal.Start()
			updater = wal
		}
	}

	diagnosticsModule := metric.NewDiagnosticsModule()
	scrapeController := scrape.NewScrapeController(
		config.ScrapeInterval,
		config.NetworkPort,
		updater,
		clusterCache,
		statSummaryClient,
	)
	scrapeController.Start()

	metricQuerier := newCollectorMetricsQuerier(repo, config.Resolutions)

	// cluster info provider
	clusterInfo := clusterInfoProvider

	clusterMap := newCollectorClusterMap(clusterInfo)

	return &collectorDataSource{
		config:            config,
		metricsQuerier:    metricQuerier,
		clusterInfo:       clusterInfo,
		clusterMap:        clusterMap,
		diagnosticsModule: diagnosticsModule,
	}
}

func (c *collectorDataSource) RegisterEndPoints(router *httprouter.Router) {
	return
}

func (c *collectorDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {
	const CollectorDiagnosticCategory = "collector"

	diagnosticDefinitions := c.diagnosticsModule.DiagnosticsDefinitions()

	for _, dd := range diagnosticDefinitions {
		err := diagService.Register(dd.MetricName, dd.Description, CollectorDiagnosticCategory, func(ctx context.Context) (map[string]any, error) {
			details, err := c.diagnosticsModule.DiagnosticsDetails(dd.ID)
			if err != nil {
				return nil, err
			}
			return details, nil
		})
		if err != nil {
			log.Warnf("Failed to register collector diagnostic %s: %s", dd.ID, err.Error())
		}
	}
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
