package huawei

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"

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
		if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
			// The ep-sentinel row first: a "last one wins" map would get day 22 wrong.
			w.Write([]byte(`{"total_count": 2, "cost_data": [
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-sentinel-id"}, {"key": "REGION_CODE", "value": "la-south-2"}],
				 "costs": [{"time_dimension_value": "2026-09-23", "amount": "4"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "0"}, {"key": "REGION_CODE", "value": "la-south-2"}],
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
		if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
			w.Write([]byte(`{"total_count": 2, "cost_data": [
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-a"}, {"key": "REGION_CODE", "value": "la-south-2"}],
				 "costs": [{"time_dimension_value": "2026-09-22", "amount": "10"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-b"}, {"key": "REGION_CODE", "value": "la-south-2"}],
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

// TestCostIntegration_GetCloudCost_ZeroSumAcrossEnterpriseProjects covers a
// day whose Enterprise Project amounts cancel out: a $10 charge in one and a
// $10 adjustment in another. With no proportion to split by, each must keep
// its own signed amount (scaled by the price factor), not an even share of
// the $0 total.
func TestCostIntegration_GetCloudCost_ZeroSumAcrossEnterpriseProjects(t *testing.T) {
	const node = "hws.service.type.ec2:hws.resource.type.vm:0a1b2c3d-0000-4000-8000-000000000002:node-adjusted"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
			w.Write([]byte(`{"total_count": 2, "cost_data": [
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-a"}, {"key": "REGION_CODE", "value": "la-south-2"}],
				 "costs": [{"time_dimension_value": "2026-09-22", "amount": "10", "official_amount": "10"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + node + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "ep-b"}, {"key": "REGION_CODE", "value": "la-south-2"}],
				 "costs": [{"time_dimension_value": "2026-09-22", "amount": "-10", "official_amount": "0"}]}
			]}`))
			return
		}
		w.Write([]byte(`{"total_count": 1, "cost_data": [{"dimensions": [
			{"key": "RESOURCE_ID", "value": "` + node + `"},
			{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.ec2"},
			{"key": "REGION_CODE", "value": "la-south-2"}
		], "costs": [
			{"time_dimension_value": "2026-09-22", "amount": "0", "official_amount": "10"}
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
		PriceFactors: []PriceFactor{{Service: "ec2", Factor: 0.5}},
	}}
	start := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	type cost struct{ net, list float64 }
	want := map[string]cost{"platform-a": {5, 10}, "platform-b": {-5, 0}}
	got := map[string]cost{}
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			c := got[cc.Properties.InvoiceEntityID]
			c.net += cc.NetCost.Cost
			c.list += cc.ListCost.Cost
			got[cc.Properties.InvoiceEntityID] = c
		}
	}
	for platform, w := range want {
		if c := got[platform]; math.Abs(c.net-w.net) > 1e-9 || math.Abs(c.list-w.list) > 1e-9 {
			t.Errorf("%s: net %v list %v, want net %v list %v", platform, c.net, c.list, w.net, w.list)
		}
	}
}

func TestAllocate(t *testing.T) {
	// amounts returns what each of the projects a, b, c... was billed.
	amounts := func(values ...string) func(string) decimal.Decimal {
		return func(id string) decimal.Decimal {
			return decimal.RequireFromString(values[id[0]-'a'])
		}
	}
	cases := []struct {
		name             string
		ids              []string
		total            float64
		billed, fallback func(string) decimal.Decimal
		want             []float64
	}{
		{"proportional", []string{"a", "b"}, 24, amounts("10", "14"), nil, []float64{10, 14}},
		{"proportional, scaled", []string{"a", "b"}, 12, amounts("10", "14"), nil, []float64{5, 7}},
		{"zero-sum keeps signed amounts", []string{"a", "b"}, 0, amounts("10", "-10"), nil, []float64{10, -10}},
		{"zero-sum, unaccounted total split evenly", []string{"a", "b"}, 2, amounts("10", "-10"), nil, []float64{11, -9}},
		// In floating point 0.10 + 0.20 - 0.30 leaves a residue, which would
		// take the proportional branch and give every project 0.
		{"fractional zero-sum across three projects", []string{"a", "b", "c"}, 0, amounts("0.10", "0.20", "-0.30"), nil, []float64{0.10, 0.20, -0.30}},
		{"nothing billed uses the fallback", []string{"a", "b"}, 30, amounts("0", "0"), amounts("1", "2"), []float64{10, 20}},
		{"nothing billed, no fallback: even", []string{"a", "b"}, 30, amounts("0", "0"), nil, []float64{15, 15}},
	}
	for _, tc := range cases {
		got := allocate(tc.total, tc.ids, tc.billed, tc.fallback)
		for i := range got {
			if math.Abs(got[i]-tc.want[i]) > 1e-9 {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				break
			}
		}
	}
}

// epBillingServer stands in for BSS: the Enterprise Project query (grouped by
// resource and project) gets epRows, the main cost query gets costRows.
func epBillingServer(t *testing.T, epRows, costRows string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
			w.Write([]byte(`{"total_count": 1, "cost_data": [` + epRows + `]}`))
			return
		}
		w.Write([]byte(`{"total_count": 1, "cost_data": [` + costRows + `]}`))
	}))
	t.Cleanup(server.Close)
	bssEndpointOverride = server.URL
	t.Cleanup(func() { bssEndpointOverride = "" })
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")
}

func epRow(resourceID, epID, day, amount string) string {
	return epRowIn("la-south-2", resourceID, epID, day, amount)
}

func epRowIn(region, resourceID, epID, day, amount string) string {
	return `{"dimensions": [{"key": "RESOURCE_ID", "value": "` + resourceID + `"}, {"key": "ENTERPRISE_PROJECT_ID", "value": "` + epID + `"},
		{"key": "REGION_CODE", "value": "` + region + `"}],
		"costs": [{"time_dimension_value": "` + day + `", "amount": "` + amount + `", "official_amount": "` + amount + `"}]}`
}

func mainCostRow(resourceID, day, amount string) string {
	return mainCostRowIn("la-south-2", resourceID, day, amount)
}

func mainCostRowIn(region, resourceID, day, amount string) string {
	return `{"dimensions": [{"key": "RESOURCE_ID", "value": "` + resourceID + `"},
		{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.obs"}, {"key": "REGION_CODE", "value": "` + region + `"}],
		"costs": [{"time_dimension_value": "` + day + `", "amount": "` + amount + `", "official_amount": "` + amount + `"}]}`
}

// netByPlatform runs GetCloudCost over one day and sums the net cost by
// invoice entity (platform); costs outside any project sum under "".
func netByPlatform(t *testing.T, day time.Time) map[string]float64 {
	t.Helper()
	ci := &CostIntegration{CostConfiguration: CostConfiguration{
		Region: "la-south-2",
		EnterpriseProjects: []EnterpriseProject{
			{ID: "ep-a", Name: "ep-a", Platform: "platform-a"},
			{ID: "ep-b", Name: "ep-b", Platform: "platform-b"},
		},
	}}
	ccsr, err := ci.GetCloudCost(day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := map[string]float64{}
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			got[cc.Properties.InvoiceEntityID] += cc.NetCost.Cost
		}
	}
	return got
}

func assertNet(t *testing.T, got, want map[string]float64) {
	t.Helper()
	for platform, w := range want {
		if math.Abs(got[platform]-w) > 1e-9 {
			t.Errorf("platform %q: net %v, want %v (all: %v)", platform, got[platform], w, got)
		}
	}
}

// TestCostIntegration_GetCloudCost_ResourceSpellingsNotPooled covers one bucket
// BSS spells two ways (an empty last field and "null"), +$10 billed to ep-a
// under one spelling and -$10 to ep-b under the other: each cost row is split
// by its own spelling's amounts, so the zero-sum split is not applied twice.
func TestCostIntegration_GetCloudCost_ResourceSpellingsNotPooled(t *testing.T) {
	const bucket = "hws.service.type.obs:hws.resource.type.bucket:my-bucket"
	epBillingServer(t,
		epRow(bucket+":", "ep-a", "2026-09-22", "10")+","+epRow(bucket+":null", "ep-b", "2026-09-22", "-10"),
		mainCostRow(bucket+":", "2026-09-22", "10")+","+mainCostRow(bucket+":null", "2026-09-22", "-10"))

	got := netByPlatform(t, time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC))
	assertNet(t, got, map[string]float64{"platform-a": 10, "platform-b": -10})
}

// TestCostIntegration_GetCloudCost_UnassignedEnterpriseProjectCharges covers a
// day with $10 billed to ep-a and $14 billed to no Enterprise Project: the
// $14 stays at the account level instead of going to ep-a.
func TestCostIntegration_GetCloudCost_UnassignedEnterpriseProjectCharges(t *testing.T) {
	const bucket = "hws.service.type.obs:hws.resource.type.bucket:shared-bucket:shared-bucket"
	epBillingServer(t,
		epRow(bucket, "ep-a", "2026-09-22", "10")+","+epRow(bucket, "null", "2026-09-22", "14"),
		mainCostRow(bucket, "2026-09-22", "24"))

	got := netByPlatform(t, time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC))
	assertNet(t, got, map[string]float64{"platform-a": 10, "": 14})
}

// TestCostIntegration_GetCloudCost_LatestProjectAfterAdjustment covers a
// latest billed day with ep-a +$10, ep-b +$9 and then ep-a -$5 (rows of two
// spellings of one bucket): ep-b ends the day ahead, so a later day with no
// Enterprise Project record goes to ep-b, not to the running maximum ep-a.
func TestCostIntegration_GetCloudCost_LatestProjectAfterAdjustment(t *testing.T) {
	const bucket = "hws.service.type.obs:hws.resource.type.bucket:busy-bucket"
	epBillingServer(t,
		epRow(bucket+":", "ep-a", "2026-09-22", "10")+","+
			epRow(bucket+":", "ep-b", "2026-09-22", "9")+","+
			epRow(bucket+":null", "ep-a", "2026-09-22", "-5"),
		mainCostRow(bucket+":", "2026-09-23", "7"))

	got := netByPlatform(t, time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC))
	assertNet(t, got, map[string]float64{"platform-b": 7, "platform-a": 0})
}

// TestCostIntegration_GetCloudCost_RegionsNotPooled covers a RESOURCE_ID
// shared across regions -- a charge with no resource of its own -- with +$10
// billed to ep-a in one region and -$10 to ep-b in another: each region's
// cost row is split by its own region's amounts, so the zero-sum split is
// not applied twice.
func TestCostIntegration_GetCloudCost_RegionsNotPooled(t *testing.T) {
	const charge = "obs-request-charges"
	epBillingServer(t,
		epRowIn("la-south-2", charge, "ep-a", "2026-09-22", "10")+","+epRowIn("la-north-2", charge, "ep-b", "2026-09-22", "-10"),
		mainCostRowIn("la-south-2", charge, "2026-09-22", "10")+","+mainCostRowIn("la-north-2", charge, "2026-09-22", "-10"))

	got := netByPlatform(t, time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC))
	assertNet(t, got, map[string]float64{"platform-a": 10, "platform-b": -10})
}
