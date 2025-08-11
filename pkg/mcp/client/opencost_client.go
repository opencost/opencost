package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/opencost/opencost/pkg/mcp/types"
)

type OpenCostClient interface {
	GetAllocations(window types.Window, filters map[string]string) (interface{}, error)
	GetAssets(window types.Window, filters map[string]string) (interface{}, error)
	HealthCheck() error
}

type openCostClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewOpenCostClient() (OpenCostClient, error) {
	baseURL := os.Getenv("OPENCOST_URL")
	if baseURL == "" {
		baseURL = "http://localhost:9003"
	}

	return &openCostClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (c *openCostClient) GetAllocations(window types.Window, filters map[string]string) (interface{}, error) {
	params := url.Values{}
	params.Set("window", window.String())

	for key, value := range filters {
		params.Set(key, value)
	}

	resp, err := c.httpClient.Get(fmt.Sprintf("%s/allocation/compute?%s", c.baseURL, params.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to get allocations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

func (c *openCostClient) GetAssets(window types.Window, filters map[string]string) (interface{}, error) {
	params := url.Values{}
	params.Set("window", window.String())

	for key, value := range filters {
		params.Set(key, value)
	}

	resp, err := c.httpClient.Get(fmt.Sprintf("%s/assets?%s", c.baseURL, params.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to get assets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

func (c *openCostClient) HealthCheck() error {
	resp, err := c.httpClient.Get(c.baseURL + "/healthz")
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status %d", resp.StatusCode)
	}

	return nil
}