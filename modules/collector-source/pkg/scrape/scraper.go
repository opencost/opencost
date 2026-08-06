package scrape

import (
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

type Scraper interface {
	// Scrape performs the metrics scrape and returns a slice of `Update` instances to apply.
	Scrape() []metric.Update
}

// MetricFilter is a set of metric names to suppress from scrape output.
// An empty or nil filter allows all metrics through.
type MetricFilter map[string]struct{}

// filteredScraper wraps a Scraper and removes any Update whose Name appears in the filter.
type filteredScraper struct {
	inner  Scraper
	filter MetricFilter
}

// withFilter returns s wrapped with f so that denied metric names are stripped from Scrape output.
// If f is empty, s is returned unchanged.
func withFilter(s Scraper, f MetricFilter) Scraper {
	if len(f) == 0 {
		return s
	}
	return &filteredScraper{inner: s, filter: f}
}

func (fs *filteredScraper) Scrape() []metric.Update {
	results := fs.inner.Scrape()
	out := results[:0]
	for _, u := range results {
		if _, denied := fs.filter[u.Name]; !denied {
			out = append(out, u)
		}
	}
	return out
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
