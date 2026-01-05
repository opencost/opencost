package scrape

import (
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

type Scraper interface {
	// Scrape performs the metrics scrape and returns a slice of `Update` instances to apply.
	Scrape() []metric.Update
}

type ScrapeFunc func() []metric.Update

func concurrentScrape(scrapeFuncs ...ScrapeFunc) []metric.Update {
	resultCh := make(chan []metric.Update)
	defer close(resultCh)
	for _, scrapeFunc := range scrapeFuncs {
		go func() {
			scrapeResults := scrapeFunc()
			resultCh <- scrapeResults
		}()
	}

	var scrapeResults []metric.Update
	for range scrapeFuncs {
		targetResults := <-resultCh
		scrapeResults = append(scrapeResults, targetResults...)
	}
	return scrapeResults
}
