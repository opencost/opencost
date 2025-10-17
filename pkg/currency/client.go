package currency

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	apiBaseURL = "https://v6.exchangerate-api.com/v6"
	userAgent  = "opencost-plugins/1.0"
)

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type exchangeRateClient struct {
	apiKey     string
	httpClient httpClient
	timeout    time.Duration
}

func newExchangeRateClient(apiKey string, timeout time.Duration) *exchangeRateClient {
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	return &exchangeRateClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}
}

func (c *exchangeRateClient) fetchRates(baseCurrency string) (*exchangeRateResponse, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	if baseCurrency == "" {
		baseCurrency = "USD"
	}

	url := fmt.Sprintf("%s/%s/latest/%s", apiBaseURL, c.apiKey, baseCurrency)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange rates: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var response exchangeRateResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if response.Result != "success" {
		return nil, fmt.Errorf("API returned error result: %s", response.Result)
	}

	if len(response.ConversionRates) == 0 {
		return nil, fmt.Errorf("no conversion rates returned")
	}

	return &response, nil
}
