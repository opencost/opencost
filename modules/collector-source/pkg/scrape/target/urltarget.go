package target

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// 30s = default COLLECTOR_SCRAPE_INTERVAL: a hung target delays at most one
// scrape, and fails before scrapeTimeout (1m) would drop the whole scraper.
var client = &http.Client{Timeout: 30 * time.Second}

type UrlTarget struct {
	url string
}

func NewUrlTarget(url string) *UrlTarget {
	return &UrlTarget{
		url: url,
	}
}

func (t *UrlTarget) Load() (io.Reader, error) {
	resp, err := client.Get(t.url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch URL: %w", err)
	}

	return resp.Body, nil
}
