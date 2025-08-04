package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/mcp/utils"
)

// OpenCostClient wraps the OpenCost API with intelligent query building
type OpenCostClient struct {
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration
}

// NewOpenCostClient creates a new OpenCost client
func NewOpenCostClient(baseURL string) (*OpenCostClient, error) {
	if baseURL == "" {
		baseURL = "http://localhost:9090"
	}

	// Validate URL
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	client := &OpenCostClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		timeout: 30 * time.Second,
	}

	return client, nil
}

// QueryAllocations queries allocation data from OpenCost
func (c *OpenCostClient) QueryAllocations(ctx context.Context, params *utils.AllocationQueryParams) (*opencost.AllocationSetRange, error) {
	// Build URL parameters
	urlParams := url.Values{}
	
	// Window and step
	urlParams.Set("window", params.Window)
	if params.Step != "" {
		urlParams.Set("step", params.Step)
	}

	// Aggregation
	if params.Aggregate != "" {
		urlParams.Set("aggregate", params.Aggregate)
	}

	// Accumulate
	if params.Accumulate {
		urlParams.Set("accumulate", "true")
	}

	// Filter
	if params.Filter != "" {
		urlParams.Set("filter", params.Filter)
	}

	// Format
	urlParams.Set("format", "json")

	// Include efficiency data
	urlParams.Set("includeIdle", "true")
	urlParams.Set("includeSharedCostBreakdown", "true")

	// Build full URL
	fullURL := fmt.Sprintf("%s/model/allocation?%s", c.baseURL, urlParams.Encode())

	// Make request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result opencost.AllocationSetRange
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// QueryAssets queries asset data from OpenCost
func (c *OpenCostClient) QueryAssets(ctx context.Context, params *utils.AssetQueryParams) (*opencost.AssetSetRange, error) {
	// Build URL parameters
	urlParams := url.Values{}
	
	// Window and step
	urlParams.Set("window", params.Window)
	if params.Step != "" {
		urlParams.Set("step", params.Step)
	}

	// Aggregation
	if params.Aggregate != "" {
		urlParams.Set("aggregate", params.Aggregate)
	}

	// Accumulate
	if params.Accumulate {
		urlParams.Set("accumulate", "true")
	}

	// Filter
	if params.Filter != "" {
		urlParams.Set("filter", params.Filter)
	}

	// Format
	urlParams.Set("format", "json")

	// Include breakdown details
	urlParams.Set("includeBreakdown", "true")

	// Build full URL
	fullURL := fmt.Sprintf("%s/model/assets?%s", c.baseURL, urlParams.Encode())

	// Make request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result opencost.AssetSetRange
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// QueryCloudCosts queries cloud cost data from OpenCost
func (c *OpenCostClient) QueryCloudCosts(ctx context.Context, params *utils.CloudCostQueryParams) (*opencost.CloudCostSetRange, error) {
	// Build URL parameters
	urlParams := url.Values{}
	
	// Window and step
	urlParams.Set("window", params.Window)
	if params.Step != "" {
		urlParams.Set("step", params.Step)
	}

	// Aggregation
	if params.Aggregate != "" {
		urlParams.Set("aggregate", params.Aggregate)
	}

	// Accumulate
	if params.Accumulate {
		urlParams.Set("accumulate", "true")
	}

	// Filter
	if params.Filter != "" {
		urlParams.Set("filter", params.Filter)
	}

	// Format
	urlParams.Set("format", "json")

	// Build full URL
	fullURL := fmt.Sprintf("%s/model/cloudCosts?%s", c.baseURL, urlParams.Encode())

	// Make request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var result opencost.CloudCostSetRange
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetClusterInfo retrieves cluster information
func (c *OpenCostClient) GetClusterInfo(ctx context.Context) (map[string]interface{}, error) {
	fullURL := fmt.Sprintf("%s/model/clusterInfo", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// GetMetrics retrieves Prometheus metrics
func (c *OpenCostClient) GetMetrics(ctx context.Context, query string, startTime, endTime time.Time, step time.Duration) (interface{}, error) {
	urlParams := url.Values{}
	urlParams.Set("query", query)
	
	if !startTime.IsZero() {
		urlParams.Set("start", strconv.FormatInt(startTime.Unix(), 10))
	}
	
	if !endTime.IsZero() {
		urlParams.Set("end", strconv.FormatInt(endTime.Unix(), 10))
	}
	
	if step > 0 {
		urlParams.Set("step", strconv.FormatFloat(step.Seconds(), 'f', 0, 64))
	}

	fullURL := fmt.Sprintf("%s/api/v1/query_range?%s", c.baseURL, urlParams.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// ValidateConnection tests the connection to OpenCost
func (c *OpenCostClient) ValidateConnection(ctx context.Context) error {
	fullURL := fmt.Sprintf("%s/healthz", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to OpenCost: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OpenCost health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// BuildAllocationQuery intelligently builds allocation query parameters
func (c *OpenCostClient) BuildAllocationQuery(nlQuery string, baseParams map[string]interface{}) (*utils.AllocationQueryParams, error) {
	params := &utils.AllocationQueryParams{
		Window:     "1d",
		Aggregate:  "namespace",
		Step:       "1d",
		Accumulate: true,
	}

	// Extract explicit parameters
	if window, ok := baseParams["window"].(string); ok && window != "" {
		params.Window = window
	}

	if aggregate, ok := baseParams["aggregate"].(string); ok && aggregate != "" {
		params.Aggregate = aggregate
	}

	if step, ok := baseParams["step"].(string); ok && step != "" {
		params.Step = step
	}

	if filter, ok := baseParams["filter"].(string); ok && filter != "" {
		params.Filter = filter
	}

	// Parse natural language query for additional parameters
	queryLower := strings.ToLower(nlQuery)

	// Time window detection
	if strings.Contains(queryLower, "last week") || strings.Contains(queryLower, "past week") {
		params.Window = "7d"
	} else if strings.Contains(queryLower, "last month") || strings.Contains(queryLower, "past month") {
		params.Window = "30d"
	} else if strings.Contains(queryLower, "yesterday") {
		params.Window = "1d"
	} else if strings.Contains(queryLower, "today") {
		params.Window = "today"
	} else if strings.Contains(queryLower, "last hour") || strings.Contains(queryLower, "past hour") {
		params.Window = "1h"
	}

	// Aggregation detection
	if strings.Contains(queryLower, "pod") || strings.Contains(queryLower, "pods") {
		params.Aggregate = "pod"
	} else if strings.Contains(queryLower, "service") || strings.Contains(queryLower, "services") {
		params.Aggregate = "service"
	} else if strings.Contains(queryLower, "deployment") || strings.Contains(queryLower, "deployments") {
		params.Aggregate = "deployment"
	} else if strings.Contains(queryLower, "container") || strings.Contains(queryLower, "containers") {
		params.Aggregate = "container"
	} else if strings.Contains(queryLower, "node") || strings.Contains(queryLower, "nodes") {
		params.Aggregate = "node"
	} else if strings.Contains(queryLower, "cluster") || strings.Contains(queryLower, "clusters") {
		params.Aggregate = "cluster"
	}

	// Filter detection
	filterParts := []string{}

	// Namespace filtering
	if strings.Contains(queryLower, "production") || strings.Contains(queryLower, "prod") {
		filterParts = append(filterParts, `namespace:"production"`)
	} else if strings.Contains(queryLower, "staging") || strings.Contains(queryLower, "stage") {
		filterParts = append(filterParts, `namespace:"staging"`)
	} else if strings.Contains(queryLower, "development") || strings.Contains(queryLower, "dev") {
		filterParts = append(filterParts, `namespace:"development"`)
	}

	// Extract quoted namespace names
	namespaceMatches := findQuotedValues(nlQuery, []string{"namespace", "ns"})
	for _, ns := range namespaceMatches {
		filterParts = append(filterParts, fmt.Sprintf(`namespace:"%s"`, ns))
	}

	// Label filtering
	labelMatches := findLabelFilters(nlQuery)
	filterParts = append(filterParts, labelMatches...)

	if len(filterParts) > 0 {
		if params.Filter != "" {
			params.Filter = fmt.Sprintf("(%s) AND (%s)", params.Filter, strings.Join(filterParts, " AND "))
		} else {
			params.Filter = strings.Join(filterParts, " AND ")
		}
	}

	return params, nil
}

// BuildAssetQuery intelligently builds asset query parameters
func (c *OpenCostClient) BuildAssetQuery(nlQuery string, baseParams map[string]interface{}) (*utils.AssetQueryParams, error) {
	params := &utils.AssetQueryParams{
		Window:     "1d",
		Aggregate:  "type",
		Step:       "1d",
		Accumulate: true,
	}

	// Extract explicit parameters
	if window, ok := baseParams["window"].(string); ok && window != "" {
		params.Window = window
	}

	if aggregate, ok := baseParams["aggregate"].(string); ok && aggregate != "" {
		params.Aggregate = aggregate
	}

	if step, ok := baseParams["step"].(string); ok && step != "" {
		params.Step = step
	}

	if filter, ok := baseParams["filter"].(string); ok && filter != "" {
		params.Filter = filter
	}

	// Parse natural language query
	queryLower := strings.ToLower(nlQuery)

	// Time window detection (similar to allocation)
	if strings.Contains(queryLower, "last week") || strings.Contains(queryLower, "past week") {
		params.Window = "7d"
	} else if strings.Contains(queryLower, "last month") || strings.Contains(queryLower, "past month") {
		params.Window = "30d"
	}

	// Aggregation detection
	if strings.Contains(queryLower, "node") || strings.Contains(queryLower, "nodes") {
		params.Aggregate = "type"
		params.Filter = addFilterCondition(params.Filter, `type:"Node"`)
	} else if strings.Contains(queryLower, "disk") || strings.Contains(queryLower, "volume") || strings.Contains(queryLower, "storage") {
		params.Aggregate = "type"
		params.Filter = addFilterCondition(params.Filter, `type:"Disk"`)
	} else if strings.Contains(queryLower, "network") || strings.Contains(queryLower, "load balancer") {
		params.Aggregate = "type"
		params.Filter = addFilterCondition(params.Filter, `type:"Network"`)
	}

	return params, nil
}

// BuildCloudCostQuery intelligently builds cloud cost query parameters
func (c *OpenCostClient) BuildCloudCostQuery(nlQuery string, baseParams map[string]interface{}) (*utils.CloudCostQueryParams, error) {
	params := &utils.CloudCostQueryParams{
		Window:     "7d",
		Aggregate:  "service",
		Step:       "1d",
		Accumulate: true,
	}

	// Extract explicit parameters
	if window, ok := baseParams["window"].(string); ok && window != "" {
		params.Window = window
	}

	if aggregate, ok := baseParams["aggregate"].(string); ok && aggregate != "" {
		params.Aggregate = aggregate
	}

	if step, ok := baseParams["step"].(string); ok && step != "" {
		params.Step = step
	}

	if filter, ok := baseParams["filter"].(string); ok && filter != "" {
		params.Filter = filter
	}

	// Parse natural language query
	queryLower := strings.ToLower(nlQuery)

	// Provider detection
	filterParts := []string{}
	if strings.Contains(queryLower, "aws") || strings.Contains(queryLower, "amazon") {
		filterParts = append(filterParts, `provider:"AWS"`)
	} else if strings.Contains(queryLower, "gcp") || strings.Contains(queryLower, "google") {
		filterParts = append(filterParts, `provider:"GCP"`)
	} else if strings.Contains(queryLower, "azure") || strings.Contains(queryLower, "microsoft") {
		filterParts = append(filterParts, `provider:"Azure"`)
	}

	// Service detection
	serviceMatches := findQuotedValues(nlQuery, []string{"service"})
	for _, service := range serviceMatches {
		filterParts = append(filterParts, fmt.Sprintf(`service:"%s"`, service))
	}

	// Common service names
	if strings.Contains(queryLower, "ec2") {
		filterParts = append(filterParts, `service:"EC2"`)
	} else if strings.Contains(queryLower, "s3") {
		filterParts = append(filterParts, `service:"S3"`)
	} else if strings.Contains(queryLower, "rds") {
		filterParts = append(filterParts, `service:"RDS"`)
	}

	// Aggregation detection
	if strings.Contains(queryLower, "account") || strings.Contains(queryLower, "accounts") {
		params.Aggregate = "account"
	} else if strings.Contains(queryLower, "region") || strings.Contains(queryLower, "regions") {
		params.Aggregate = "region"
	} else if strings.Contains(queryLower, "provider") || strings.Contains(queryLower, "providers") {
		params.Aggregate = "provider"
	}

	if len(filterParts) > 0 {
		if params.Filter != "" {
			params.Filter = fmt.Sprintf("(%s) AND (%s)", params.Filter, strings.Join(filterParts, " AND "))
		} else {
			params.Filter = strings.Join(filterParts, " AND ")
		}
	}

	return params, nil
}

// Helper functions

func findQuotedValues(text string, prefixes []string) []string {
	values := []string{}
	textLower := strings.ToLower(text)

	for _, prefix := range prefixes {
		// Look for patterns like 'namespace "production"' or 'ns:"staging"'
		patterns := []string{
			prefix + ` "`,
			prefix + `:"`,
			prefix + `='`,
			prefix + `="`,
		}

		for _, pattern := range patterns {
			start := strings.Index(textLower, pattern)
			if start >= 0 {
				start += len(pattern)
				end := start
				quote := text[start-1]
				
				for i := start; i < len(text); i++ {
					if text[i] == quote {
						end = i
						break
					}
				}
				
				if end > start {
					values = append(values, text[start:end])
				}
			}
		}
	}

	return values
}

func findLabelFilters(text string) []string {
	filters := []string{}
	textLower := strings.ToLower(text)

	// Look for patterns like 'label:app="web"' or 'env=production'
	labelPatterns := []string{
		`label:`,
		`labels:`,
	}

	for _, pattern := range labelPatterns {
		index := strings.Index(textLower, pattern)
		if index >= 0 {
			// Extract the label filter
			start := index + len(pattern)
			end := len(text)
			
			// Find the end of the label expression
			for i := start; i < len(text); i++ {
				if text[i] == ' ' || text[i] == ',' || text[i] == ')' {
					end = i
					break
				}
			}
			
			if end > start {
				labelExpr := text[start:end]
				filters = append(filters, fmt.Sprintf("label:%s", labelExpr))
			}
		}
	}

	return filters
}

func addFilterCondition(existing, condition string) string {
	if existing == "" {
		return condition
	}
	return fmt.Sprintf("(%s) AND (%s)", existing, condition)
}