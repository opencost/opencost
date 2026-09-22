package ibm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/usagereportsv4"
	"github.com/opencost/opencost/pkg/cloud"
)

const usageReportsTestAccountID = "b09edf5642ebfad587c594f4d4a354b0"

type capturedUsageRequest struct {
	method string
	path   string
	query  url.Values
}

func TestUsageIntegrationGetCloudCostPaginatesAndMapsResponses(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []capturedUsageRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		mu.Lock()
		requests = append(requests, capturedUsageRequest{method: r.Method, path: r.URL.EscapedPath(), query: query})
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		response := map[string]any{
			"count": 1,
			"limit": 1,
		}
		switch query.Get("_start") {
		case "":
			response["next"] = map[string]any{
				"href": "https://unused.test/resource-usage?_start=page-2",
			}
			response["resources"] = []any{
				usageReportsTestResource("instance-1", "is.instance", 31, 62, "env:test"),
			}
		case "page-2":
			response["resources"] = []any{
				usageReportsTestResource("instance-2", "cloud-object-storage", 62, 93, "team:storage"),
			}
		default:
			http.Error(w, `{"error":"unexpected page token"}`, http.StatusBadRequest)
			return
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	integration := newUsageReportsTestIntegration(t, server.URL)
	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	result, err := integration.getCloudCost(start, end, asOf)
	if err != nil {
		t.Fatalf("getCloudCost: %v", err)
	}
	if integration.ConnectionStatus != cloud.SuccessfulConnection {
		t.Fatalf("ConnectionStatus = %s, want %s", integration.ConnectionStatus, cloud.SuccessfulConnection)
	}

	mu.Lock()
	gotRequests := append([]capturedUsageRequest(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 {
		t.Fatalf("request count = %d, want 2", len(gotRequests))
	}
	wantPath := "/v4/accounts/" + usageReportsTestAccountID + "/resource_instances/usage/2026-01"
	for i, request := range gotRequests {
		if request.method != http.MethodGet {
			t.Errorf("request %d method = %q, want GET", i+1, request.method)
		}
		if request.path != wantPath {
			t.Errorf("request %d path = %q, want %q", i+1, request.path, wantPath)
		}
		for key, want := range map[string]string{
			"_limit": "200",
			"_names": "true",
			"_tags":  "true",
		} {
			if got := request.query.Get(key); got != want {
				t.Errorf("request %d query %s = %q, want %q", i+1, key, got, want)
			}
		}
	}
	if got := gotRequests[0].query.Get("_start"); got != "" {
		t.Errorf("first request _start = %q, want empty", got)
	}
	if got := gotRequests[1].query.Get("_start"); got != "page-2" {
		t.Errorf("second request _start = %q, want page-2", got)
	}

	if len(result.CloudCostSets) != 31 {
		t.Fatalf("CloudCostSet count = %d, want 31", len(result.CloudCostSets))
	}
	accumulated, err := result.AccumulateAll()
	if err != nil {
		t.Fatalf("accumulating result: %v", err)
	}
	if accumulated.Length() != 2 {
		t.Fatalf("mapped CloudCost count = %d, want 2", accumulated.Length())
	}

	byProviderID := map[string]float64{}
	for _, cost := range accumulated.CloudCosts {
		byProviderID[cost.Properties.ProviderID] = cost.NetCost.Cost
		if cost.Properties.AccountID != usageReportsTestAccountID {
			t.Errorf("AccountID = %q, want normalized %q", cost.Properties.AccountID, usageReportsTestAccountID)
		}
		switch cost.Properties.ProviderID {
		case "instance-1":
			if cost.Properties.Service != "is.instance" || cost.Properties.Labels["env"] != "test" {
				t.Errorf("instance-1 mapping = service %q, labels %#v", cost.Properties.Service, cost.Properties.Labels)
			}
		case "instance-2":
			if cost.Properties.Service != "cloud-object-storage" || cost.Properties.Labels["team"] != "storage" {
				t.Errorf("instance-2 mapping = service %q, labels %#v", cost.Properties.Service, cost.Properties.Labels)
			}
		default:
			t.Errorf("unexpected ProviderID %q", cost.Properties.ProviderID)
		}
	}
	if got := byProviderID["instance-1"]; got != 31 {
		t.Errorf("instance-1 NetCost = %v, want 31", got)
	}
	if got := byProviderID["instance-2"]; got != 62 {
		t.Errorf("instance-2 NetCost = %v, want 62", got)
	}
}

func TestUsageIntegrationGetCloudCostMarksMissingData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"count":     0,
			"limit":     200,
			"resources": []any{},
		}); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	integration := newUsageReportsTestIntegration(t, server.URL)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	result, err := integration.getCloudCost(start, end, end)
	if err != nil {
		t.Fatalf("getCloudCost: %v", err)
	}
	if !result.IsEmpty() {
		t.Fatal("expected an empty CloudCostSetRange")
	}
	if integration.ConnectionStatus != cloud.MissingData {
		t.Errorf("ConnectionStatus = %s, want %s", integration.ConnectionStatus, cloud.MissingData)
	}
}

func TestUsageIntegrationGetCloudCostMarksFailedConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errors":[{"code":"server_error","message":"test failure"}]}`))
	}))
	t.Cleanup(server.Close)

	integration := newUsageReportsTestIntegration(t, server.URL)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	result, err := integration.getCloudCost(start, end, end)
	if err == nil {
		t.Fatal("getCloudCost error = nil, want request failure")
	}
	if result != nil {
		t.Fatal("getCloudCost result is non-nil on request failure")
	}
	if !strings.Contains(err.Error(), "querying IBM resource usage for 2026-01") {
		t.Errorf("error = %q, want month-scoped query context", err)
	}
	if integration.ConnectionStatus != cloud.FailedConnection {
		t.Errorf("ConnectionStatus = %s, want %s", integration.ConnectionStatus, cloud.FailedConnection)
	}
}

func newUsageReportsTestIntegration(t *testing.T, serviceURL string) *UsageIntegration {
	t.Helper()
	client, err := usagereportsv4.NewUsageReportsV4(&usagereportsv4.UsageReportsV4Options{
		URL:           serviceURL,
		Authenticator: &core.NoAuthAuthenticator{},
	})
	if err != nil {
		t.Fatalf("creating Usage Reports test client: %v", err)
	}

	return &UsageIntegration{
		UsageConfiguration: UsageConfiguration{AccountID: "a/" + usageReportsTestAccountID},
		clientFactory: func() (*usagereportsv4.UsageReportsV4, error) {
			return client, nil
		},
	}
}

func usageReportsTestResource(providerID, service string, cost, ratedCost float64, tag string) map[string]any {
	return map[string]any{
		"account_id":           "a/" + usageReportsTestAccountID,
		"resource_instance_id": providerID,
		"resource_id":          service,
		"resource_name":        service + " display name",
		"pricing_country":      "USA",
		"currency_code":        "USD",
		"currency_rate":        1,
		"billable":             true,
		"plan_id":              "test-plan",
		"month":                "2026-01",
		"tags":                 []any{tag},
		"usage": []any{
			map[string]any{
				"metric":     "VCPU_HOURS",
				"quantity":   1,
				"cost":       cost,
				"rated_cost": ratedCost,
				"discounts":  []any{},
			},
		},
	}
}
