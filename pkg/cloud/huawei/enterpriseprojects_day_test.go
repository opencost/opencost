package huawei

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/env"
)

// TestCostIntegration_GetCloudCost_EnterpriseProjectChangesMidWindow reproduces
// two Sentinel nodes billed to the default Enterprise Project for some days
// and to ep-sentinel afterwards: each day must go to the Enterprise Project
// it was billed to that day, not all to the last one read.
func TestCostIntegration_GetCloudCost_EnterpriseProjectChangesMidWindow(t *testing.T) {
	const node = "hws.service.type.ec2:hws.resource.type.vm:ff018b03-ab42-4f50-a76d-984667eebb12:np-sentinel-prod-general-az1"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if len(body.Groupby) == 2 {
			// The ep-sentinel row first: a "last one wins" map would get day 22 wrong.
			w.Write([]byte(`{"total_count": 2, "cost_data": [
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-sentinel-id"}],
				 "costs": [{"time_dimension_value": "2026-09-23", "amount": "4"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "0"}],
				 "costs": [{"time_dimension_value": "2026-09-22", "amount": "4"}]}
			]}`))
			return
		}
		w.Write([]byte(`{"total_count": 1, "cost_data": [{"dimensions": [
			{"key": "RESOURCE_ID", "value": "` + node + `"},
			{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.ec2"},
			{"key": "REGION_CODE", "value": "la-south-2"}
		], "costs": [
			{"time_dimension_value": "2026-09-22", "amount": "4", "official_amount": "4"},
			{"time_dimension_value": "2026-09-23", "amount": "4", "official_amount": "4"},
			{"time_dimension_value": "2026-09-24", "amount": "4", "official_amount": "4"}
		]}]}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{CostConfiguration: CostConfiguration{
		ProjectID: "test-project", Region: "la-south-2",
		EnterpriseProjects: []EnterpriseProject{
			{ID: "ep-sentinel-id", Name: "ep-sentinel", Platform: "sentinel-prod"},
			{ID: "0", Name: "default", Platform: "tooling"},
		},
	}}
	start := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 3))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Day 24 has no Enterprise Project record: the latest one (ep-sentinel) applies.
	want := map[string]string{"2026-09-22": "tooling", "2026-09-23": "sentinel-prod", "2026-09-24": "sentinel-prod"}
	seen := 0
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			day := cc.Window.Start().Format("2006-01-02")
			if got := cc.Properties.InvoiceEntityID; got != want[day] {
				t.Errorf("%s: platform %q, want %q", day, got, want[day])
			}
			seen++
		}
	}
	if seen != 3 {
		t.Fatalf("expected one cost per day, got %d", seen)
	}
}
