package huawei

import (
	"encoding/json"
	"math"
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

// TestCostIntegration_GetCloudCost_DaySplitAcrossEnterpriseProjects covers a
// resource billed to two Enterprise Projects on the same day ($10 and $14, as
// when it moved during the day): its daily cost must be split between them in
// that proportion, not all assigned to whichever the response listed last.
func TestCostIntegration_GetCloudCost_DaySplitAcrossEnterpriseProjects(t *testing.T) {
	const node = "hws.service.type.ec2:hws.resource.type.vm:0a1b2c3d-0000-4000-8000-000000000001:node-moved"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if len(body.Groupby) == 2 {
			w.Write([]byte(`{"total_count": 2, "cost_data": [
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-a"}],
				 "costs": [{"time_dimension_value": "2026-09-22", "amount": "10"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-b"}],
				 "costs": [{"time_dimension_value": "2026-09-22", "amount": "14"}]}
			]}`))
			return
		}
		w.Write([]byte(`{"total_count": 1, "cost_data": [{"dimensions": [
			{"key": "RESOURCE_ID", "value": "` + node + `"},
			{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.ec2"},
			{"key": "REGION_CODE", "value": "la-south-2"}
		], "costs": [
			{"time_dimension_value": "2026-09-22", "amount": "24", "official_amount": "30"}
		]}]}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{CostConfiguration: CostConfiguration{
		Region: "la-south-2",
		EnterpriseProjects: []EnterpriseProject{
			{ID: "ep-a", Name: "ep-a", Platform: "platform-a"},
			{ID: "ep-b", Name: "ep-b", Platform: "platform-b"},
		},
	}}
	start := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	type cost struct{ net, list float64 }
	want := map[string]cost{"platform-a": {10, 12.5}, "platform-b": {14, 17.5}}
	got := map[string]cost{}
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			c := got[cc.Properties.InvoiceEntityID]
			c.net += cc.NetCost.Cost
			c.list += cc.ListCost.Cost
			got[cc.Properties.InvoiceEntityID] = c
		}
	}
	if len(got) != len(want) {
		t.Fatalf("expected one cost per Enterprise Project, got %+v", got)
	}
	for platform, w := range want {
		if c := got[platform]; math.Abs(c.net-w.net) > 1e-9 || math.Abs(c.list-w.list) > 1e-9 {
			t.Errorf("%s: net %v list %v, want net %v list %v", platform, c.net, c.list, w.net, w.list)
		}
	}
}
