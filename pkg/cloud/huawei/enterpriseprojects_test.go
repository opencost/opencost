package huawei

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	ocjson "github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"
)

const (
	testRDSResourceID  = "hws.service.type.rds:hws.resource.type.rds.instance:57907bc4in01:rds-dify"
	testMaaSResourceID = "hws.service.type.modelarts:hws.resource.type.modelarts.tokens:3c6c5b0d-ca93-4964-b286-fd1913560ede:GLM-5.2-Resident"
	testSimResourceID  = "hws.service.type.ec2:hws.resource.type.vm:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:ecs-sim"
)

func TestCostConfiguration_OptionalListsRoundTrip(t *testing.T) {
	c := CostConfiguration{
		Region: "la-south-2",
		EnterpriseProjects: []EnterpriseProject{
			{ID: "ep-1", Name: "ep-web-prod", Platform: "web-prod"},
			{ID: "0", Platform: "tooling"},
		},
		PriceFactors: []PriceFactor{{Service: "modelarts", ResourceType: "modelarts.tokens", Factor: 0.63}},
	}

	data, err := ocjson.Marshal(c)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	var got CostConfiguration
	if err := ocjson.Unmarshal(data, &got); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if !c.Equals(&got) {
		t.Fatalf("round-tripped config %+v does not equal original %+v", got, c)
	}

	changed := got
	changed.PriceFactors = []PriceFactor{{Service: "modelarts", Factor: 0.5}}
	if c.Equals(&changed) {
		t.Fatalf("expected different price factors to not be equal")
	}

	sanitized := c.Sanitize().(*CostConfiguration)
	if !c.Equals(sanitized) {
		t.Fatalf("Sanitize should keep the optional lists, got %+v", sanitized)
	}
}

func TestCostConfiguration_UnmarshalJSON_WithoutOptionalLists(t *testing.T) {
	var c CostConfiguration
	if err := json.Unmarshal([]byte(`{"projectID":"","region":"la-south-2"}`), &c); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.EnterpriseProjects != nil || c.PriceFactors != nil {
		t.Fatalf("expected no optional lists, got %+v", c)
	}
}

func TestCostConfiguration_ValidateOptionalLists(t *testing.T) {
	cases := map[string]CostConfiguration{
		"enterprise project without id": {Region: "la-south-2", EnterpriseProjects: []EnterpriseProject{{Name: "x"}}},
		"zero factor":                   {Region: "la-south-2", PriceFactors: []PriceFactor{{Service: "modelarts"}}},
		"negative factor":               {Region: "la-south-2", PriceFactors: []PriceFactor{{Factor: -1}}},
	}
	for name, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestCostConfiguration_EnterpriseProject(t *testing.T) {
	c := CostConfiguration{EnterpriseProjects: []EnterpriseProject{
		{ID: "ep-1", Name: "ep-web-prod", Platform: "web-prod"},
		{ID: "ep-2", Platform: "sentinel-prod"},
	}}

	cases := map[string]EnterpriseProject{
		"ep-1":    {ID: "ep-1", Name: "ep-web-prod", Platform: "web-prod"},
		"ep-2":    {ID: "ep-2", Name: "ep-2", Platform: "sentinel-prod"},
		"0":       {ID: "0", Name: "default"},
		"unknown": {ID: "unknown", Name: "unknown"},
	}
	for id, want := range cases {
		if got := c.enterpriseProject(id); got != want {
			t.Errorf("enterpriseProject(%q) = %+v, want %+v", id, got, want)
		}
	}
}

func TestCostConfiguration_PriceFactor(t *testing.T) {
	c := CostConfiguration{PriceFactors: []PriceFactor{
		{Service: "hws.service.type.modelarts", ResourceType: "modelarts.tokens", Factor: 0.63},
		{Service: "RDS", Factor: 0.9},
	}}

	cases := []struct {
		service, resourceType string
		want                  float64
	}{
		{"hws.service.type.modelarts", "modelarts.tokens", 0.63},
		{"hws.service.type.modelarts", "modelarts.training", 1},
		{"hws.service.type.rds", "rds.instance", 0.9},
		{"hws.service.type.ec2", "vm", 1},
		// CLOUD_SERVICE_TYPE as an English name, for a bare RESOURCE_ID
		// that carries no service code of its own.
		{"Relational Database Service", "", 0.9},
		{"Elastic Cloud Server", "", 1},
		{"", "", 1},
	}
	for _, tc := range cases {
		if got := c.priceFactor(tc.service, tc.resourceType); got != tc.want {
			t.Errorf("priceFactor(%q, %q) = %v, want %v", tc.service, tc.resourceType, got, tc.want)
		}
	}

	// A factor may name the service by an alias or by its English name.
	aliases := CostConfiguration{PriceFactors: []PriceFactor{
		{Service: "ecs", Factor: 0.8},
		{Service: "Elastic Volume Service", Factor: 0.7},
	}}
	if got := aliases.priceFactor("hws.service.type.ec2", "vm"); got != 0.8 {
		t.Errorf("expected the ecs alias to match ec2, got %v", got)
	}
	if got := aliases.priceFactor("hws.service.type.ebs", "volume"); got != 0.7 {
		t.Errorf("expected the EVS name to match ebs, got %v", got)
	}
}

// TestCostConfiguration_PriceFactorStaysWithItsService checks that a factor
// for one service never applies to another one in the same category: the
// services table once grouped them (ECS with BMS, SFS with SFS Turbo...), and
// using such a group as the identity scaled unrelated charges.
func TestCostConfiguration_PriceFactorStaysWithItsService(t *testing.T) {
	cases := []struct {
		factorFor, billed string
		want              float64
	}{
		{"bms", "hws.service.type.bms", 0.5},
		{"bms", "Bare Metal Server", 0.5},
		{"bms", "hws.service.type.ec2", 1},
		{"bms", "Elastic Cloud Server", 1},
		{"sfsturbo", "hws.service.type.sfsturbo", 0.5},
		{"sfsturbo", "hws.service.type.sfs", 1},
		{"sfs", "SFS Turbo", 1},
		{"taurusdb", "hws.service.type.gaussdbformysql", 0.5},
		{"taurusdb", "hws.service.type.gaussdb", 1},
		{"gaussdb", "GaussDB(for MySQL)", 1},
		{"kms", "Data Encryption Workshop", 0.5},
		{"kms", "hws.service.type.csms", 1},
		{"aad", "hws.service.type.antiddos", 1},
		{"scm", "hws.service.type.ccm", 1},
		{"servicestage", "hws.service.type.cse", 1},
	}
	for _, tc := range cases {
		c := CostConfiguration{PriceFactors: []PriceFactor{{Service: tc.factorFor, Factor: 0.5}}}
		if got := c.priceFactor(tc.billed, ""); got != tc.want {
			t.Errorf("factor for %q on %q = %v, want %v", tc.factorFor, tc.billed, got, tc.want)
		}
	}
}

// TestCostConfiguration_PriceFactorIgnoresCategoryOverrides checks that a
// HUAWEICLOUD_SERVICE_CATEGORIES entry grouping services under one category
// does not merge their identities for price factors.
func TestCostConfiguration_PriceFactorIgnoresCategoryOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "categories.json")
	if err := os.WriteFile(path, []byte(`[{"category": "Shared", "codes": ["ec2", "bms"]}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(env.HuaweiServiceCategoriesEnvVar, path)
	serviceTableOnce = sync.Once{}
	defer func() {
		serviceTableOnce = sync.Once{}
		activeServiceTable = nil
	}()

	if got := selectHuaweiCategory("hws.service.type.bms"); got != opencost.SharedCategory {
		t.Fatalf("expected the override to apply, got %q", got)
	}
	c := CostConfiguration{PriceFactors: []PriceFactor{{Service: "bms", Factor: 0.5}}}
	if got := c.priceFactor("hws.service.type.ec2", ""); got != 1 {
		t.Errorf("a BMS factor must not apply to ECS, got %v", got)
	}
}

// TestCostIntegration_GetCloudCost_PriceFactorForBareResourceID covers a row
// whose RESOURCE_ID is not composite, so it carries no service code: the
// service-scoped factor must still apply, from CLOUD_SERVICE_TYPE, in either
// form BSS returns it.
func TestCostIntegration_GetCloudCost_PriceFactorForBareResourceID(t *testing.T) {
	for _, serviceType := range []string{"hws.service.type.modelarts", "ModelArts"} {
		t.Run(serviceType, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Groupby []struct {
						Key string `json:"key"`
					} `json:"groupby"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("unexpected error decoding request: %v", err)
				}
				w.Header().Set("Content-Type", "application/json")
				if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
					w.Write([]byte(`{"total_count": 0, "cost_data": []}`))
					return
				}
				w.Write([]byte(`{"total_count": 1, "cost_data": [{"dimensions": [
					{"key": "RESOURCE_ID", "value": "bare-resource-1"},
					{"key": "CLOUD_SERVICE_TYPE", "value": "` + serviceType + `"},
					{"key": "REGION_CODE", "value": "la-north-2"}
				], "costs": [{"time_dimension_value": "2026-09-22", "amount": "100", "official_amount": "100"}]}]}`))
			}))
			defer server.Close()

			bssEndpointOverride = server.URL
			defer func() { bssEndpointOverride = "" }()

			t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
			t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
			t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

			ci := &CostIntegration{CostConfiguration: CostConfiguration{
				Region:       "la-north-2",
				PriceFactors: []PriceFactor{{Service: "hws.service.type.modelarts", Factor: 0.63}},
			}}
			start := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
			ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			found := false
			for _, ccs := range ccsr.CloudCostSets {
				for _, cc := range ccs.CloudCosts {
					if cc.Properties.ProviderID != "bare-resource-1" {
						continue
					}
					found = true
					if cc.NetCost.Cost != 63 || cc.ListCost.Cost != 100 {
						t.Fatalf("expected net 63 and list 100, got net %v list %v", cc.NetCost.Cost, cc.ListCost.Cost)
					}
				}
			}
			if !found {
				t.Fatalf("expected a CloudCost for bare-resource-1")
			}
		})
	}
}

// TestCostIntegration_GetCloudCost_EnterpriseProjectsAndPriceFactors drives
// GetCloudCost against a stand-in BSS that answers the cost query and the
// Enterprise Project query, and checks each resource gets its Enterprise
// Project, platform and price factor.
func TestCostIntegration_GetCloudCost_EnterpriseProjectsAndPriceFactors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("unexpected error decoding request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")

		if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
			w.Write([]byte(`{"total_count": 3, "cost_data": [
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + testRDSResourceID + `"}, {"key": "` + enterpriseProjectDimension + `", "value": "ep-prod-id"}, {"key": "REGION_CODE", "value": "la-north-2"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + testMaaSResourceID + `"}, {"key": "` + enterpriseProjectDimension + `", "value": "0"}, {"key": "REGION_CODE", "value": "la-north-2"}]},
				{"dimensions": [{"key": "RESOURCE_ID", "value": "` + testSimResourceID + `"}, {"key": "` + enterpriseProjectDimension + `", "value": "ep-sim-id"}, {"key": "REGION_CODE", "value": "la-north-2"}]}
			]}`))
			return
		}

		row := func(resourceID, service, amount string) string {
			return `{"dimensions": [
				{"key": "RESOURCE_ID", "value": "` + resourceID + `"},
				{"key": "CLOUD_SERVICE_TYPE", "value": "` + service + `"},
				{"key": "REGION_CODE", "value": "la-north-2"}
			], "costs": [{"time_dimension_value": "2026-09-22", "amount": "` + amount + `", "official_amount": "` + amount + `"}]}`
		}
		w.Write([]byte(`{"total_count": 3, "cost_data": [` + strings.Join([]string{
			row(testRDSResourceID, "hws.service.type.rds", "20"),
			row(testMaaSResourceID, "hws.service.type.modelarts", "100"),
			row(testSimResourceID, "hws.service.type.ec2", "5"),
		}, ",") + `]}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{CostConfiguration: CostConfiguration{
		ProjectID: "test-project",
		Region:    "la-north-2",
		EnterpriseProjects: []EnterpriseProject{
			{ID: "ep-prod-id", Name: "ep-web-prod", Platform: "web-prod"},
			{ID: "0", Name: "default", Platform: "tooling"},
		},
		PriceFactors: []PriceFactor{{Service: "modelarts", ResourceType: "modelarts.tokens", Factor: 0.63}},
	}}

	start := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	type expectation struct {
		epID, epName, platform string
		list, net              float64
	}
	want := map[string]expectation{
		"rds-dify":         {"ep-prod-id", "ep-web-prod", "web-prod", 20, 20},
		"GLM-5.2-Resident": {"0", "default", "tooling", 100, 63},
		// Not configured: labeled with its ID, no platform.
		"ecs-sim": {"ep-sim-id", "ep-sim-id", "", 5, 5},
	}
	seen := 0
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			labels := cc.Properties.Labels
			w, ok := want[labels[ResourceNameLabel]]
			if !ok {
				t.Fatalf("unexpected resource %q", labels[ResourceNameLabel])
			}
			seen++
			// The UI breaks down by account (Enterprise Project) and invoice
			// entity (platform), not by label.
			if cc.Properties.AccountID != w.epName || cc.Properties.InvoiceEntityID != w.platform {
				t.Errorf("%s: account %q invoice entity %q, want %q / %q", labels[ResourceNameLabel],
					cc.Properties.AccountID, cc.Properties.InvoiceEntityID, w.epName, w.platform)
			}
			if labels[EnterpriseProjectIDLabel] != w.epID || labels[EnterpriseProjectLabel] != w.epName || labels[PlatformLabel] != w.platform {
				t.Errorf("%s: labels %v, want ep %q/%q platform %q", labels[ResourceNameLabel], labels, w.epID, w.epName, w.platform)
			}
			if cc.ListCost.Cost != w.list || math.Abs(cc.NetCost.Cost-w.net) > 1e-9 || math.Abs(cc.InvoicedCost.Cost-w.net) > 1e-9 {
				t.Errorf("%s: list %v net %v invoiced %v, want list %v net %v", labels[ResourceNameLabel], cc.ListCost.Cost, cc.NetCost.Cost, cc.InvoicedCost.Cost, w.list, w.net)
			}
		}
	}
	if seen != len(want) {
		t.Fatalf("expected %d cloud costs, got %d", len(want), seen)
	}
}

// TestCostIntegration_GetCloudCost_EnterpriseProjectLookupFails checks that the
// bill is still reported, without Enterprise Projects, when their lookup fails.
func TestCostIntegration_GetCloudCost_EnterpriseProjectLookupFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if len(body.Groupby) > 1 && body.Groupby[1].Key == enterpriseProjectDimension {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error_code": "CBC.0100", "error_msg": "invalid parameter"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"total_count": 1, "cost_data": [{"dimensions": [
			{"key": "RESOURCE_ID", "value": "` + testRDSResourceID + `"},
			{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.rds"},
			{"key": "REGION_CODE", "value": "la-north-2"}
		], "costs": [{"time_dimension_value": "2026-09-22", "amount": "20", "official_amount": "20"}]}]}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{CostConfiguration: CostConfiguration{ProjectID: "test-project", Region: "la-north-2"}}
	start := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("expected the bill without Enterprise Projects, got error: %v", err)
	}
	found := false
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			found = true
			if _, ok := cc.Properties.Labels[EnterpriseProjectIDLabel]; ok {
				t.Errorf("expected no Enterprise Project label, got %v", cc.Properties.Labels)
			}
		}
	}
	if !found {
		t.Fatalf("expected the RDS cost to be reported")
	}
}
