package scrape

import (
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	"k8s.io/client-go/kubernetes"
)

// ScrapeController initializes and holds the scrapers in addition to running the loop that triggers scrapes
type ScrapeController struct {
	scrapeInterval time.Duration
	runState       atomic.AtomicRunState
	scrapers       []Scraper
}

func NewScrapeController(
	scrapeInterval time.Duration,
	releaseName string,
	networkPort int,
	updater metric.MetricUpdater,
	clusterCache clustercache.ClusterCache,
	k8s kubernetes.Interface,
	statSummaryClient util.StatSummaryClient,
) *ScrapeController {
	var scrapers []Scraper

	clusterCacheScraper := newClusterCacheScraper(clusterCache, updater)
	scrapers = append(scrapers, clusterCacheScraper)

	opencostScraper := newOpenCostScraper(updater)
	scrapers = append(scrapers, opencostScraper)

	statSummaryScraper := newStatSummaryScraper(statSummaryClient, updater)
	scrapers = append(scrapers, statSummaryScraper)

	networkScraper := newNetworkScraper(releaseName, networkPort, k8s, updater)
	scrapers = append(scrapers, networkScraper)

	dcgmScraper := newDCGMScrapper(k8s, updater)
	scrapers = append(scrapers, dcgmScraper)

	sc := &ScrapeController{
		scrapeInterval: scrapeInterval,
		scrapers:       scrapers,
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
	func() {
		for {
			select {
			case <-sc.runState.OnStop():
				sc.runState.Reset()
				return // exit go routine
			default:

			}
			time.Sleep(sc.scrapeInterval)
		}

	}()
}

func (sc *ScrapeController) Stop() {
	sc.runState.Stop()
}
