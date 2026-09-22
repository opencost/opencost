package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/usageapi"
)

func TestParseAttributedCost(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	cases := map[string]struct {
		input   *string
		want    float64
		wantErr bool
	}{
		"nil":        {nil, 0, false},
		"empty":      {strPtr(""), 0, false},
		"zero":       {strPtr("0"), 0, false},
		"valid":      {strPtr("1.23"), 1.23, false},
		"negative":   {strPtr("-0.5"), -0.5, false},
		"unparsable": {strPtr("abc"), 0, true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseAttributedCost(c.input)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestUsageAPIIntegration_GetCloudCost(t *testing.T) {
	usageApiConfigPath := os.Getenv("USAGEAPI_CONFIGURATION")
	if usageApiConfigPath == "" {
		t.Skip("skipping integration test, set environment variable USAGEAPI_CONFIGURATION")
	}
	usageApiConfigBin, err := os.ReadFile(usageApiConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var usageApiConfig UsageApiConfiguration
	err = json.Unmarshal(usageApiConfigBin, &usageApiConfig)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}
	testCases := map[string]struct {
		integration *UsageApiIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{
		// No CUR data is expected within 2 days of now
		"too_recent_window": {
			integration: &UsageApiIntegration{
				UsageApiConfiguration: usageApiConfig,
			},
			end:      time.Now(),
			start:    time.Now().Add(-timeutil.Day),
			expected: true,
		},
		// CUR data should be available
		"last week window": {
			integration: &UsageApiIntegration{
				UsageApiConfiguration: usageApiConfig,
			},
			end:      time.Now().Add(-7 * timeutil.Day),
			start:    time.Now().Add(-8 * timeutil.Day),
			expected: false,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := testCase.integration.GetCloudCost(testCase.start, testCase.end)
			if err != nil {
				t.Errorf("Other error during testing %s", err)
			} else if actual.IsEmpty() != testCase.expected {
				t.Errorf("Incorrect result, actual emptiness: %t, expected: %t", actual.IsEmpty(), testCase.expected)
			}
		})
	}
}

type fakeUsageAPIClient struct {
	responses []usageapi.RequestSummarizedUsagesResponse
	requests  []usageapi.RequestSummarizedUsagesRequest
}

func (f *fakeUsageAPIClient) RequestSummarizedUsages(_ context.Context, request usageapi.RequestSummarizedUsagesRequest) (usageapi.RequestSummarizedUsagesResponse, error) {
	f.requests = append(f.requests, request)
	if len(f.responses) == 0 {
		return usageapi.RequestSummarizedUsagesResponse{}, fmt.Errorf("unexpected request")
	}

	response := f.responses[0]
	f.responses = f.responses[1:]
	return response, nil
}

func TestUsageAPIIntegrationGetCloudCostLoadsEachDailyItem(t *testing.T) {
	start := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	client := &fakeUsageAPIClient{
		responses: []usageapi.RequestSummarizedUsagesResponse{
			{
				UsageAggregation: usageapi.UsageAggregation{
					Items: []usageapi.UsageSummary{
						testUsageSummary(start, "resource-1", 1),
						testUsageSummary(start.AddDate(0, 0, 1), "resource-2", 2),
						testUsageSummary(start.AddDate(0, 0, 2), "resource-3", 3),
					},
				},
			},
		},
	}
	integration := &UsageApiIntegration{
		UsageApiConfiguration: UsageApiConfiguration{
			TenancyID: "tenancy-id",
			Region:    "region",
		},
	}

	ccsr, err := integration.getCloudCost(context.Background(), client, start, start.AddDate(0, 0, 3))
	if err != nil {
		t.Fatalf("getCloudCost() error = %v", err)
	}

	if len(ccsr.CloudCostSets) != 3 {
		t.Fatalf("expected 3 daily CloudCostSets, got %d", len(ccsr.CloudCostSets))
	}

	for i, ccs := range ccsr.CloudCostSets {
		if len(ccs.CloudCosts) != 1 {
			t.Fatalf("day %d: expected 1 CloudCost, got %d", i+1, len(ccs.CloudCosts))
		}
		for _, cloudCost := range ccs.CloudCosts {
			wantCost := float64(i + 1)
			if cloudCost.NetCost.Cost != wantCost {
				t.Errorf("day %d: NetCost = %v, want %v", i+1, cloudCost.NetCost.Cost, wantCost)
			}
		}
	}
}

func TestUsageAPIIntegrationGetCloudCostFollowsPagination(t *testing.T) {
	start := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	firstPageItems := make([]usageapi.UsageSummary, 500)
	for i := range firstPageItems {
		firstPageItems[i] = testUsageSummary(start, fmt.Sprintf("resource-%d", i), 1)
	}

	client := &fakeUsageAPIClient{
		responses: []usageapi.RequestSummarizedUsagesResponse{
			{
				UsageAggregation: usageapi.UsageAggregation{Items: firstPageItems},
				OpcNextPage:      common.String("next-page"),
			},
			{
				UsageAggregation: usageapi.UsageAggregation{
					Items: []usageapi.UsageSummary{testUsageSummary(start, "resource-500", 1)},
				},
			},
		},
	}
	integration := &UsageApiIntegration{
		UsageApiConfiguration: UsageApiConfiguration{
			TenancyID: "tenancy-id",
			Region:    "region",
		},
	}

	ccsr, err := integration.getCloudCost(context.Background(), client, start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("getCloudCost() error = %v", err)
	}

	if len(client.requests) != 2 {
		t.Fatalf("expected 2 OCI requests, got %d", len(client.requests))
	}
	if client.requests[0].Page != nil {
		t.Errorf("first request page = %q, want nil", *client.requests[0].Page)
	}
	if client.requests[1].Page == nil || *client.requests[1].Page != "next-page" {
		t.Errorf("second request page = %v, want next-page", client.requests[1].Page)
	}

	if got := len(ccsr.CloudCostSets[0].CloudCosts); got != 501 {
		t.Errorf("expected 501 CloudCosts from both pages, got %d", got)
	}
}

func testUsageSummary(start time.Time, resourceID string, computedAmount float32) usageapi.UsageSummary {
	return usageapi.UsageSummary{
		TimeUsageStarted: &common.SDKTime{Time: start},
		TimeUsageEnded:   &common.SDKTime{Time: start.AddDate(0, 0, 1)},
		ResourceId:       common.String(resourceID),
		Service:          common.String("Compute"),
		ComputedAmount:   common.Float32(computedAmount),
		AttributedCost:   common.String(fmt.Sprintf("%v", computedAmount)),
	}
}
