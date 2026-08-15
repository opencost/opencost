package huawei

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/env"
)

func strPtr(s string) *string { return &s }

func TestDimensionValue(t *testing.T) {
	dims := []bssintlmodel.DimensionGroup{
		{Key: strPtr("RESOURCE_ID"), Value: strPtr("res-1")},
		{Key: strPtr("REGION_CODE"), Value: strPtr("la-south-2")},
	}

	if got := dimensionValue(&dims, "RESOURCE_ID"); got != "res-1" {
		t.Fatalf("expected res-1, got %q", got)
	}
	if got := dimensionValue(&dims, "REGION_CODE"); got != "la-south-2" {
		t.Fatalf("expected la-south-2, got %q", got)
	}
	if got := dimensionValue(&dims, "MISSING"); got != "" {
		t.Fatalf("expected empty string for missing key, got %q", got)
	}
	if got := dimensionValue(nil, "RESOURCE_ID"); got != "" {
		t.Fatalf("expected empty string for nil dimensions, got %q", got)
	}
}

func TestParseCostDay(t *testing.T) {
	fallback := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	day, ok := parseCostDay(strPtr("2026-01-15"), fallback)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	expected := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !day.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, day)
	}

	day, ok = parseCostDay(nil, fallback)
	if !ok || !day.Equal(fallback) {
		t.Fatalf("expected fallback for nil value, got %v", day)
	}

	day, ok = parseCostDay(strPtr(""), fallback)
	if !ok || !day.Equal(fallback) {
		t.Fatalf("expected fallback for empty value, got %v", day)
	}

	day, ok = parseCostDay(strPtr("not-a-date"), fallback)
	if !ok || !day.Equal(fallback) {
		t.Fatalf("expected fallback for unparsable value, got %v", day)
	}
}

func TestParseCostAmount(t *testing.T) {
	amount, err := parseCostAmount(strPtr("12.34"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if amount != 12.34 {
		t.Fatalf("expected 12.34, got %v", amount)
	}

	amount, err = parseCostAmount(nil)
	if err != nil || amount != 0 {
		t.Fatalf("expected 0/nil for nil input, got %v/%v", amount, err)
	}

	amount, err = parseCostAmount(strPtr(""))
	if err != nil || amount != 0 {
		t.Fatalf("expected 0/nil for empty input, got %v/%v", amount, err)
	}

	if _, err := parseCostAmount(strPtr("not-a-number")); err == nil {
		t.Fatalf("expected error for unparsable amount")
	}
}

func TestSelectHuaweiCategory(t *testing.T) {
	cases := []struct {
		serviceType string
		want        string
	}{
		{"Elastic Cloud Server", "Compute"},
		{"Cloud Container Engine", "Compute"},
		{"Elastic Volume Service", "Storage"},
		{"Object Storage Service", "Storage"},
		{"Elastic Load Balance", "Network"},
		{"Virtual Private Cloud", "Network"},
		{"Relational Database Service", "Storage"},
		{"Distributed Cache Service", "Compute"},
		{"Data Encryption Workshop", "Storage"},
		{"Some Unrecognized Service", "Other"},
	}
	for _, c := range cases {
		if got := selectHuaweiCategory(c.serviceType); got != c.want {
			t.Errorf("selectHuaweiCategory(%q) = %q, want %q", c.serviceType, got, c.want)
		}
	}
}

// TestDescribeResource covers the composite RESOURCE_ID values BSS actually
// returns, taken from a live query against the account.
func TestDescribeResource(t *testing.T) {
	cases := []struct {
		name       string
		resourceID string
		want       bssResource
	}{
		{
			name:       "database instance",
			resourceID: "hws.service.type.rds:hws.resource.type.rds.instance:57907bc4in01:rds-mlops-mysql",
			want: bssResource{
				ID:   "hws.service.type.rds:hws.resource.type.rds.instance:57907bc4in01:rds-mlops-mysql",
				Type: "rds.instance",
				Name: "rds-mlops-mysql",
			},
		},
		{
			name:       "cluster node",
			resourceID: "hws.service.type.ec2:hws.resource.type.vm:08099e97-4178:cce-mlops-np-training-cpu-52qrp",
			want: bssResource{
				ID:   "hws.service.type.ec2:hws.resource.type.vm:08099e97-4178:cce-mlops-np-training-cpu-52qrp",
				Type: "vm",
				Name: "cce-mlops-np-training-cpu-52qrp",
			},
		},
		{
			// A bucket has no name of its own: its ID is what it is called.
			name:       "unnamed resource, name field empty",
			resourceID: "hws.service.type.obs:hws.resource.type.obs:obs-mlops-build-29074b:",
			want: bssResource{
				ID:   "hws.service.type.obs:hws.resource.type.obs:obs-mlops-build-29074b:",
				Type: "obs",
				Name: "obs-mlops-build-29074b",
			},
		},
		{
			// The same resource as above, as BSS also spells it. Both must
			// describe one resource under one ID, or its cost is split in two.
			name:       "unnamed resource, name field null",
			resourceID: "hws.service.type.obs:hws.resource.type.obs:obs-mlops-build-29074b:null",
			want: bssResource{
				ID:   "hws.service.type.obs:hws.resource.type.obs:obs-mlops-build-29074b:",
				Type: "obs",
				Name: "obs-mlops-build-29074b",
			},
		},
		{
			name:       "no resource id, named",
			resourceID: "hws.service.type.obs:hws.resource.type.obs::ListAllMyBucketsOperation",
			want: bssResource{
				ID:   "hws.service.type.obs:hws.resource.type.obs::ListAllMyBucketsOperation",
				Type: "obs",
				Name: "ListAllMyBucketsOperation",
			},
		},
		{
			name:       "not in composite form",
			resourceID: "res-1",
			want:       bssResource{ID: "res-1"},
		},
		{
			name:       "empty",
			resourceID: "",
			want:       bssResource{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := describeResource(c.resourceID); got != c.want {
				t.Errorf("describeResource(%q) = %+v, want %+v", c.resourceID, got, c.want)
			}
		})
	}
}

// TestCostIntegration_GetCloudCost_ResourceDetails checks that what a resource
// is and what it is called reach the CloudCost, and that the group-by stays
// within the three dimensions BSS allows.
func TestCostIntegration_GetCloudCost_ResourceDetails(t *testing.T) {
	var requestedGroupby []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("unexpected error decoding request: %v", err)
		}
		requestedGroupby = nil
		for _, g := range body.Groupby {
			requestedGroupby = append(requestedGroupby, g.Key)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"total_count": 1,
			"cost_data": [
				{
					"dimensions": [
						{"key": "RESOURCE_ID", "value": "hws.service.type.rds:hws.resource.type.rds.instance:57907bc4in01:rds-mlops-mysql"},
						{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.rds"},
						{"key": "REGION_CODE", "value": "la-south-2"}
					],
					"costs": [
						{
							"time_dimension_value": "2026-01-15",
							"time_measure_id": 1,
							"amount": "1.230000",
							"official_amount": "2.000000"
						}
					]
				}
			]
		}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{
		CostConfiguration: CostConfiguration{ProjectID: "test-project", Region: "la-south-2"},
	}

	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// BSS rejects a group-by of more than three dimensions with CBC.0100.
	if len(requestedGroupby) > 3 {
		t.Errorf("group-by exceeds the three dimensions BSS allows: %v", requestedGroupby)
	}
	if !slices.Equal(requestedGroupby, costQueryDimensions) {
		t.Errorf("group-by = %v, want %v", requestedGroupby, costQueryDimensions)
	}

	found := false
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			if cc.Properties == nil || cc.Properties.Service != "hws.service.type.rds" {
				continue
			}
			found = true
			if cc.Properties.Category != opencost.StorageCategory {
				t.Errorf("expected category %q, got %q", opencost.StorageCategory, cc.Properties.Category)
			}
			if got := cc.Properties.Labels[opencost.AssetResourceTypeLabel]; got != "rds.instance" {
				t.Errorf("expected resource type label %q, got %q", "rds.instance", got)
			}
			if got := cc.Properties.Labels[opencost.AssetResourceNameLabel]; got != "rds-mlops-mysql" {
				t.Errorf("expected resource name label %q, got %q", "rds-mlops-mysql", got)
			}
		}
	}
	if !found {
		t.Fatalf("expected a CloudCost entry for the RDS resource, got none in %+v", ccsr)
	}
}

// TestCostIntegration_GetCloudCost_LiveBSS drives GetCloudCost end-to-end against an
// httptest server standing in for the Huawei Cloud BSS cost-analysed-bills endpoint.
func TestCostIntegration_GetCloudCost_LiveBSS(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v4/costs/cost-analysed-bills/query" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"total_count": 1,
			"cost_data": [
				{
					"dimensions": [
						{"key": "RESOURCE_ID", "value": "res-1"},
						{"key": "CLOUD_SERVICE_TYPE", "value": "Elastic Cloud Server"},
						{"key": "REGION_CODE", "value": "la-south-2"}
					],
					"costs": [
						{
							"time_dimension_value": "2026-01-15",
							"time_measure_id": 1,
							"amount": "1.230000",
							"official_amount": "2.000000"
						}
					]
				}
			]
		}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{
		CostConfiguration: CostConfiguration{ProjectID: "test-project", Region: "la-south-2"},
	}

	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)

	ccsr, err := ci.GetCloudCost(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ccsr == nil {
		t.Fatalf("expected non-nil CloudCostSetRange")
	}

	if ci.GetStatus() != cloud.SuccessfulConnection {
		t.Fatalf("expected SuccessfulConnection status, got %v", ci.GetStatus())
	}

	found := false
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			if cc.Properties == nil || cc.Properties.ProviderID != "res-1" {
				continue
			}
			found = true
			if cc.Properties.Service != "Elastic Cloud Server" {
				t.Fatalf("expected service 'Elastic Cloud Server', got %q", cc.Properties.Service)
			}
			if cc.Properties.RegionID != "la-south-2" {
				t.Fatalf("expected region la-south-2, got %q", cc.Properties.RegionID)
			}
			if cc.NetCost.Cost != 1.23 {
				t.Fatalf("expected net cost 1.23, got %v", cc.NetCost.Cost)
			}
			if cc.ListCost.Cost != 2.0 {
				t.Fatalf("expected list cost 2.0, got %v", cc.ListCost.Cost)
			}
		}
	}
	if !found {
		t.Fatalf("expected a CloudCost entry for res-1, got none in %+v", ccsr)
	}
}

func TestCostIntegration_GetCloudCost_NoData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"total_count": 0}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{
		CostConfiguration: CostConfiguration{ProjectID: "test-project", Region: "la-south-2"},
	}

	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)

	ccsr, err := ci.GetCloudCost(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ccsr == nil {
		t.Fatalf("expected non-nil CloudCostSetRange even with no data")
	}
	if ci.GetStatus() != cloud.MissingData {
		t.Fatalf("expected MissingData status, got %v", ci.GetStatus())
	}
}

func TestCostIntegration_GetCloudCost_MissingCredentials(t *testing.T) {
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "")
	t.Setenv(env.HuaweiDomainIDEnvVar, "")

	ci := &CostIntegration{
		CostConfiguration: CostConfiguration{ProjectID: "test-project", Region: "la-south-2"},
	}

	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)

	_, err := ci.GetCloudCost(start, end)
	if err == nil {
		t.Fatalf("expected error for missing credentials")
	}
	if !strings.Contains(err.Error(), "getting huawei cloud cost data") {
		t.Fatalf("expected wrapped error, got: %v", err)
	}
	if ci.GetStatus() != cloud.FailedConnection {
		t.Fatalf("expected FailedConnection status, got %v", ci.GetStatus())
	}
}

func TestCostIntegration_RefreshStatus(t *testing.T) {
	ci := &CostIntegration{ConnectionStatus: cloud.SuccessfulConnection}
	if got := ci.RefreshStatus(); got != cloud.SuccessfulConnection {
		t.Fatalf("expected RefreshStatus to return current status unchanged, got %v", got)
	}
}
