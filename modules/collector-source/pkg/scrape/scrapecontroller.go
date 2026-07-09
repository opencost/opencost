package scrape

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/nodestats"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

// ScrapeController initializes and holds the scrapers in addition to running the loop that triggers scrapes
type ScrapeController struct {
	scrapeInterval util.Interval
	runState       atomic.AtomicRunState
	scrapers       []Scraper
	updater        metric.Updater
}

// getDefaultMetricFilter builds a MetricFilter from environment variable configuration.
// Metrics whose corresponding env var is false (the default) are added to the deny set.
func getDefaultMetricFilter() MetricFilter {
	f := MetricFilter{}
	deny := func(name string) { f[name] = struct{}{} }

	if !coreenv.IsEmitPodAnnotationsMetric() {
		deny(metric.KubePodAnnotations)
	}
	if !coreenv.IsEmitNamespaceAnnotationsMetric() {
		deny(metric.KubeNamespaceAnnotations)
	}
	if !coreenv.IsEmitDeploymentLabelsMetric() {
		deny(metric.DeploymentLabels)
	}
	if !coreenv.IsEmitDeploymentAnnotationsMetric() {
		deny(metric.DeploymentAnnotations)
	}
	if !coreenv.IsEmitStatefulSetLabelsMetric() {
		deny(metric.StatefulSetLabels)
	}
	if !coreenv.IsEmitStatefulSetAnnotationsMetric() {
		deny(metric.StatefulSetAnnotations)
	}
	if !coreenv.IsEmitDaemonSetLabelsMetric() {
		deny(metric.DaemonSetLabels)
	}
	if !coreenv.IsEmitDaemonSetAnnotationsMetric() {
		deny(metric.DaemonSetAnnotations)
	}
	if !coreenv.IsEmitJobLabelsMetric() {
		deny(metric.JobLabels)
	}
	if !coreenv.IsEmitJobAnnotationsMetric() {
		deny(metric.JobAnnotations)
	}
	if !coreenv.IsEmitCronJobLabelsMetric() {
		deny(metric.CronJobLabels)
	}
	if !coreenv.IsEmitCronJobAnnotationsMetric() {
		deny(metric.CronJobAnnotations)
	}
	if !coreenv.IsEmitReplicaSetLabelsMetric() {
		deny(metric.ReplicaSetLabels)
	}
	if !coreenv.IsEmitReplicaSetAnnotationsMetric() {
		deny(metric.ReplicaSetAnnotations)
	}
	return f
}

func NewScrapeController(
	clusterUID string,
	scrapeInterval string,
	networkPort int,
	updater metric.Updater,
	clusterInfoProvider clusters.ClusterInfoProvider,
	clusterCache clustercache.ClusterCache,
	statSummaryClient nodestats.StatSummaryClient,
) *ScrapeController {
	// Start with env-driven defaults, then layer in any caller-supplied entries.
	filter := getDefaultMetricFilter()

	var scrapers []Scraper
	clusterInfoScrapper := withFilter(newClusterInfoScrapper(clusterUID, clusterInfoProvider), filter)
	scrapers = append(scrapers, clusterInfoScrapper)

	clusterCacheScraper := withFilter(newClusterCacheScraper(clusterCache), filter)
	scrapers = append(scrapers, clusterCacheScraper)

	opencostScraper := withFilter(newOpenCostScraper(), filter)
	scrapers = append(scrapers, opencostScraper)

	statSummaryScraper := withFilter(newStatSummaryScraper(statSummaryClient, clusterCache), filter)
	scrapers = append(scrapers, statSummaryScraper)

	networkScraper := withFilter(newNetworkScraper(networkPort, clusterCache), filter)
	scrapers = append(scrapers, networkScraper)

	dcgmScraper := withFilter(newDCGMScrapper(clusterCache), filter)
	scrapers = append(scrapers, dcgmScraper)

	inferenceScraper := withFilter(newInferenceScraper(clusterCache), filter)
	scrapers = append(scrapers, inferenceScraper)

	si, err := util.NewInterval(scrapeInterval)
	if err != nil {
		panic(fmt.Errorf("scrapecontroller failed to create scrape interval: %w", err))
	}

	sc := &ScrapeController{
		scrapeInterval: si,
		scrapers:       scrapers,
		updater:        updater,
	}

	return sc
}

func (sc *ScrapeController) Start() {
	// Before we attempt to start, we must ensure we are not in a stopping state
	sc.runState.WaitForReset()

	// This will atomically check the current state to ensure we can run, then advances the state.
	// If the state is already started, it will return false.
	if !sc.runState.Start() {
		log.Info("metric already running")
		return
	}
	go func() {
		nextScrape := time.Now().UTC()
		timer := time.NewTimer(time.Duration(0))
		for {
			select {
			case <-sc.runState.OnStop():
				sc.runState.Reset()
				timer.Stop()
				return // exit go routine
			case <-timer.C:
				sc.Scrape(nextScrape)
				nextScrape = sc.scrapeInterval.Add(sc.scrapeInterval.Truncate(time.Now().UTC()), 1)
				timer.Reset(time.Until(nextScrape))
			}
		}
	}()
}

func (sc *ScrapeController) Stop() {
	sc.runState.Stop()
}

func (sc *ScrapeController) Scrape(timestamp time.Time) {

	// Run scrapes concurrently to minimize time from call to data collection
	var scrapeFuncs []ScrapeFunc
	for i := range sc.scrapers {
		scraper := sc.scrapers[i]
		scrapeFuncs = append(scrapeFuncs, scraper.Scrape)
	}
	scrapeResults := concurrentScrape(scrapeFuncs...)

	// once all results are returned run updates all at once with the same timestamp
	sc.updater.Update(&metric.UpdateSet{
		Timestamp: timestamp,
		Updates:   scrapeResults,
	})
}
