package scrape

type Scraper interface {
	Scrape() []ScrapeResult
}

type ScrapeResult struct {
	Name           string
	Labels         map[string]string
	Value          float64
	AdditionalInfo map[string]string
}

type ScrapeFunc func() []ScrapeResult

func concurrentScrape(scrapeFuncs ...ScrapeFunc) []ScrapeResult {
	resultCh := make(chan []ScrapeResult)
	defer close(resultCh)
	for _, scrapeFunc := range scrapeFuncs {
		go func() {
			scrapeResults := scrapeFunc()
			resultCh <- scrapeResults
		}()
	}

	var scrapeResults []ScrapeResult
	for range scrapeFuncs {
		targetResults := <-resultCh
		scrapeResults = append(scrapeResults, targetResults...)
	}
	return scrapeResults
}
