package scrape

import (
	"encoding/json"
	"fmt"
	path "path"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const ControllerEventName = "controller"

// ScrapeController initializes and holds the scrapers in addition to running the loop that triggers scrapes
type ScrapeController struct {
	scrapeInterval util.Interval
	runState       atomic.AtomicRunState
	scrapers       []Scraper
	repo           *metric.MetricRepository
	exporter       exporter.EventExporter[metric.UpdateSet]
}

func NewScrapeController(
	resolutions map[string]*util.Resolution,
	scrapeInterval string,
	clusterID string,
	networkPort int,
	repo *metric.MetricRepository,
	clusterCache clustercache.ClusterCache,
	statSummaryClient util.StatSummaryClient,
	storage storage.Storage,
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
		repo:           repo,
	}

	if storage != nil {
		pathFormatter, err := pathing.NewEventStoragePathFormatter("", clusterID, ControllerEventName)
		if err != nil {
			log.Errorf("filed to create path formatter for scrape controller: %s", err.Error())
			return sc
		}
		encoder := exporter.NewJSONEncoder[metric.UpdateSet]()
		sc.exporter = exporter.NewEventStorageExporter(
			pathFormatter,
			encoder,
			storage,
		)
		// attempt to restore state from files
		// get path of saved files
		dirPath := path.Dir(pathFormatter.ToFullPath("", time.Time{}, ""))
		files, err := storage.List(dirPath)
		if err != nil {
			log.Errorf("failed to list files in scrape controller: %s", err.Error())
		}
		// find oldest limit
		limit := time.Now().UTC()
		for _, res := range resolutions {
			if limit.After(res.Limit()) {
				limit = res.Limit()
			}
		}

		// find files that are within limit
		var filesToRun []string
		for _, file := range files {
			fileName := path.Base(file.Name)
			timeString := strings.TrimSuffix(fileName, encoder.FileExt())
			timestamp, err := time.Parse(pathing.EventStorageTimeFormat, timeString)
			if err != nil {
				log.Errorf("failed to parse fileName %s: %s", fileName, err.Error())
				continue
			}
			if timestamp.After(limit) {
				filesToRun = append(filesToRun, file.Name)
			}
		}

		// sort files
		sort.Strings(filesToRun)

		// open files and run updates
		for _, fileName := range filesToRun {
			b, err := storage.Read(fileName)
			if err != nil {
				log.Errorf("failed to load file contents for '%s': %s", fileName, err.Error())
				continue
			}
			updateSet := metric.UpdateSet{}
			err = json.Unmarshal(b, &updateSet)
			if err != nil {
				log.Errorf("failed to unmarshal file %s: %s", fileName, err.Error())
				continue
			}
			filePrefix := path.Base(fileName)
			timeString := strings.TrimSuffix(filePrefix, encoder.FileExt())
			timestamp, err := time.Parse(pathing.EventStorageTimeFormat, timeString)
			repo.Update(updateSet.Updates, timestamp)
		}

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
	sc.repo.Update(scrapeResults, timestamp)

	if sc.exporter != nil {
		err := sc.exporter.Export(timestamp, &metric.UpdateSet{
			Updates: scrapeResults,
		})
		if err != nil {
			log.Errorf("failed to export update results: %s", err.Error())
		}
	}
}
