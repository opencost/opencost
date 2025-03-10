package metrics

import (
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
)

// MetricScraper is a struct that is used to scrape and parse a raw metrics `ScrapeTarget.`
type MetricScraper struct {
	scrapeTarget target.ScrapeTarget
}

func NewMetricScraper(scrapeTarget target.ScrapeTarget) *MetricScraper {
	return &MetricScraper{
		scrapeTarget: scrapeTarget,
	}
}

func (s *MetricScraper) Scrape() ([]*parser.MetricRecord, error) {
	reader, err := s.scrapeTarget.Load()
	if err != nil {
		return nil, err
	}

	metrics, err := parser.Parse(reader)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}
