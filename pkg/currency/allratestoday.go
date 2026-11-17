package currency

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const allRatesTodayBaseURL = "https://allratestoday.com/api/v1/rates"

type allRatesTodayClient struct {
	apiKey     string
	baseURL    string
	httpClient httpClient
}

// allRatesTodayRate is one entry of the allratestoday.com rates response
type allRatesTodayRate struct {
	Source string  `json:"source"`
	Target string  `json:"target"`
	Rate   float64 `json:"rate"`
}

// NewAllRatesTodayProvider returns a RateProvider backed by allratestoday.com.
func NewAllRatesTodayProvider(apiKey string, timeout time.Duration) RateProvider {
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	return &allRatesTodayClient{
		apiKey:  apiKey,
		baseURL: allRatesTodayBaseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// FetchRates implements RateProvider. A request without a target currency
// returns every rate for the base currency in one call.
func (c *allRatesTodayClient) FetchRates(baseCurrency string) (map[string]float64, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	if baseCurrency == "" {
		baseCurrency = "USD"
	}

	req, err := http.NewRequest("GET", c.baseURL+"?source="+url.QueryEscape(baseCurrency), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange rates: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response []allRatesTodayRate
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	rates := make(map[string]float64, len(response)+1)
	for _, r := range response {
		if r.Target != "" && r.Rate > 0 {
			rates[r.Target] = r.Rate
		}
	}

	if len(rates) == 0 {
		return nil, fmt.Errorf("no conversion rates returned")
	}

	// Match exchangerate-api.com, which includes the base currency itself
	rates[baseCurrency] = 1.0

	return rates, nil
}
