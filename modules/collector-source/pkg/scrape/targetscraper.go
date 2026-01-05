package scrape

import (
	"sync"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

type TargetScraper struct {
	name           string // identifier for the scraper
	targetProvider target.TargetProvider
	metricNames    map[string]struct{} // filter for which metrics will be processed
	includeMetrics bool                // toggle to make metrics an include or exclude list
}

func newTargetScrapper(name string, provider target.TargetProvider, metricNames []string, includeMetrics bool) *TargetScraper {
	metricSet := make(map[string]struct{})
	for _, metricName := range metricNames {
		metricSet[metricName] = struct{}{}
	}
	return &TargetScraper{
		name:           name,
		targetProvider: provider,
		metricNames:    metricSet,
		includeMetrics: includeMetrics,
	}
}

func (s *TargetScraper) Scrape() []metric.Update {
	targets := s.targetProvider.GetTargets()

	var errLock sync.Mutex
	var errors []error

	var scrapeFuncs []ScrapeFunc
	for i := range targets {
		target := targets[i]
		fn := func() []metric.Update {
			var scrapeResults []metric.Update
			f, err := target.Load()
			if err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()

				log.Errorf("failed to scrape target: %s", err.Error())
				return scrapeResults
			}
			results, err := parser.Parse(f)
			if err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()

				log.Errorf("failed to parse target: %s", err.Error())
				return scrapeResults
			}
			for _, result := range results {
				// filter metrics to be processed by name
				if _, ok := s.metricNames[result.Name]; ok != s.includeMetrics {
					continue
				}
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   result.Name,
					Labels: result.Labels,
					Value:  result.Value,
				})
			}
			return scrapeResults
		}
		scrapeFuncs = append(scrapeFuncs, fn)
	}

	updates := concurrentScrape(scrapeFuncs...)

	// dispatch a scrape event for this specific scrape
	events.Dispatch(event.ScrapeEvent{
		ScraperName: s.name,
		Targets:     len(targets),
		Errors:      errors,
	})

	return updates
}
