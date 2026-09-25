package collector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/external"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/nodestats"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/synthetic"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type collectorDataSource struct {
	metricsQuerier    *collectorMetricsQuerier
	clusterMap        clusters.ClusterMap
	clusterInfo       clusters.ClusterInfoProvider
	config            CollectorConfig
	diagnosticsModule *metric.DiagnosticsModule
	wal               *metric.Walinator
}

func NewDefaultCollectorDataSource(
	clusterUID string,
	store storage.Storage,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	statSummaryClient nodestats.StatSummaryClient,
	externalLabelProvider external.LabelProvider,
) source.OpenCostDataSource {
	config := NewOpenCostCollectorConfigFromEnv(clusterUID)
	return NewCollectorDataSource(
		config,
		store,
		clusterInfoProvider,
		clusterCache,
		statSummaryClient,
		externalLabelProvider,
	)
}

func NewCollectorDataSource(
	config CollectorConfig,
	store storage.Storage,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	statSummaryClient nodestats.StatSummaryClient,
	externalLabelProvider external.LabelProvider,
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
	var walinator *metric.Walinator
	if store != nil {
		wal, err := metric.NewWalinator(
			config.ClusterName,
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
			walinator = wal
		}
	}

	// synthesizer collects specific metric types and generates new metrics to pass
	// along with the original metrics into the updater
	metricSynthesizer := synthetic.NewMetricSynthesizers(
		updater,
		synthetic.NewContainerMemoryAllocationSynthesizer(),
		synthetic.NewContainerCpuAllocationSynthesizer(),
	)
	updater = metricSynthesizer

	diagnosticsModule := metric.NewDiagnosticsModule()
	scrapeController := scrape.NewScrapeController(
		config.ClusterUID,
		config.ScrapeInterval,
		config.NetworkPort,
		updater,
		clusterInfoProvider,
		clusterCache,
		statSummaryClient,
		externalLabelProvider,
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
		wal:               walinator,
	}
}

func (c *collectorDataSource) RegisterEndPoints(router *httprouter.Router) {

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

	if c.wal != nil {
		err := diagService.Register(WALDiagnosticName, WALDiagnosticDescription, CollectorDiagnosticCategory, func(ctx context.Context) (map[string]any, error) {
			return walDiagnosticDetails(c.wal.Status())
		})
		if err != nil {
			log.Warnf("Failed to register collector diagnostic %s: %s", WALDiagnosticName, err.Error())
		}
	}
}

const (
	WALDiagnosticName        = "Collector WAL"
	WALDiagnosticDescription = "Collector write-ahead log is persisting scrapes to storage and was fully restored at startup."
)

// walDiagnosticDetails converts a WAL status into diagnostic details, returning an error describing
// the failure when writes are currently failing or the startup restore was incomplete.
func walDiagnosticDetails(status source.WALStatus) (map[string]any, error) {
	var problems []string
	if status.ConsecutiveExportFailures > 0 {
		since := "no successful write since start"
		if !status.LastExportSuccess.IsZero() {
			since = "last successful write at " + status.LastExportSuccess.Format(time.RFC3339)
		}
		problems = append(problems, fmt.Sprintf("%d consecutive write failures, %s (last error: %s)",
			status.ConsecutiveExportFailures, since, status.LastExportError))
	}
	if status.RestoreListError != "" {
		problems = append(problems, fmt.Sprintf("restore could not list objects: %s", status.RestoreListError))
	}
	if status.RestoreErrors > 0 {
		problems = append(problems, fmt.Sprintf("restore failed to read %d of %d objects", status.RestoreErrors, status.RestoreObjectsSeen))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(problems, "; "))
	}

	return map[string]any{
		"lastExportSuccess":      status.LastExportSuccess,
		"exportFailuresTotal":    status.ExportFailuresTotal,
		"restoreCompleted":       status.RestoreCompleted,
		"restoreObjectsApplied":  status.RestoreObjectsApplied,
		"restoreDuration":        status.RestoreDuration.String(),
		"restoreOldest":          status.RestoreOldest,
		"restoreNewest":          status.RestoreNewest,
		"restoreLargestGap":      status.RestoreLargestGap.String(),
		"restoreLargestGapStart": status.RestoreLargestGapStart,
		"restoreTailGap":         status.RestoreTailGap.String(),
	}, nil
}

// WALStatus implements source.WALStatusProvider, reporting the export and restore health of the
// collector's write-ahead log.
func (c *collectorDataSource) WALStatus() source.WALStatus {
	if c.wal == nil {
		return source.WALStatus{}
	}
	return c.wal.Status()
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
