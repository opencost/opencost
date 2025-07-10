package scrape

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
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

func NewScrapeController(
	scrapeInterval string,
	networkPort int,
	updater metric.Updater,
	clusterCache clustercache.ClusterCache,
	statSummaryClient nodestats.StatSummaryClient,
) *ScrapeController {

	var scrapers []Scraper
	clusterCacheScraper := newClusterCacheScraper(clusterCache)
	scrapers = append(scrapers, clusterCacheScraper)

	opencostScraper := newOpenCostScraper()
	scrapers = append(scrapers, opencostScraper)

	statSummaryScraper := newStatSummaryScraper(statSummaryClient)
	scrapers = append(scrapers, statSummaryScraper)

	networkScraper := newNetworkScraper(networkPort, clusterCache)
	scrapers = append(scrapers, networkScraper)

	dcgmScraper := newDCGMScrapper(clusterCache)
	scrapers = append(scrapers, dcgmScraper)

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
