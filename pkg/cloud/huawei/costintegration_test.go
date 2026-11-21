package huawei

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	var requestedGroupbys [][]string
	var requestedCostTypes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
			CostType string `json:"cost_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("unexpected error decoding request: %v", err)
		}
		var groupby []string
		for _, g := range body.Groupby {
			groupby = append(groupby, g.Key)
		}
		requestedGroupbys = append(requestedGroupbys, groupby)
		requestedCostTypes = append(requestedCostTypes, body.CostType)

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

	// BSS rejects a group-by of more than three dimensions with CBC.0100. The
	// costs come from one query and the Enterprise Projects from another, and
	// each is asked for the original and then for the amortized costs.
	if len(requestedGroupbys) != 4 {
		t.Fatalf("expected the cost and Enterprise Project queries for both cost types, got %v", requestedGroupbys)
	}
	for _, groupby := range requestedGroupbys {
		if len(groupby) > 3 {
			t.Errorf("group-by exceeds the three dimensions BSS allows: %v", groupby)
		}
	}
	epDimensions := []string{"RESOURCE_ID", enterpriseProjectDimension, "REGION_CODE"}
	wantQueries := []struct {
		groupby  []string
		costType string
	}{
		{costQueryDimensions, originalCostType},
		{costQueryDimensions, amortizedCostType},
		{epDimensions, originalCostType},
		{epDimensions, amortizedCostType},
	}
	for i, want := range wantQueries {
		if !slices.Equal(requestedGroupbys[i], want.groupby) || requestedCostTypes[i] != want.costType {
			t.Errorf("query %d: group-by %v cost_type %q, want %v %q", i, requestedGroupbys[i], requestedCostTypes[i], want.groupby, want.costType)
		}
	}

	found := false
	for _, ccs := range ccsr.CloudCostSets {
		for _, cc := range ccs.CloudCosts {
			if cc.Properties == nil || cc.Properties.Service != "RDS" {
				continue
			}
			found = true
			if got := cc.Properties.Labels[ServiceCodeLabel]; got != "hws.service.type.rds" {
				t.Errorf("expected service code label %q, got %q", "hws.service.type.rds", got)
			}
			if cc.Properties.Category != opencost.StorageCategory {
				t.Errorf("expected category %q, got %q", opencost.StorageCategory, cc.Properties.Category)
			}
			if got := cc.Properties.Labels[ResourceTypeLabel]; got != "rds.instance" {
				t.Errorf("expected resource type label %q, got %q", "rds.instance", got)
			}
			if got := cc.Properties.Labels[ResourceNameLabel]; got != "rds-mlops-mysql" {
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

	// No projectID: the bill is account-wide, so none is needed (and none is
	// resolved from instance metadata, which a test process cannot reach).
	ci := &CostIntegration{
		CostConfiguration: CostConfiguration{Region: "la-south-2"},
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
			if cc.Properties.AccountID != "test-domain" {
				t.Fatalf("expected the account (domain ID) as AccountID, got %q", cc.Properties.AccountID)
			}
			// The row names the service in English; it must be reported as
			// it is when BSS gives the code (hws.service.type.ec2), so one
			// service is not split in two by the form a row uses.
			if cc.Properties.Service != "ECS" {
				t.Fatalf("expected service 'ECS', got %q", cc.Properties.Service)
			}
			if got := cc.Properties.Labels[ServiceCodeLabel]; got != "hws.service.type.ec2" {
				t.Fatalf("expected service code hws.service.type.ec2, got %q", got)
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
	withoutHuaweiCredentials(t)

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
	ci := &CostIntegration{}
	if got := ci.RefreshStatus(); got != cloud.InitialStatus {
		t.Fatalf("expected RefreshStatus to return the initial status, got %v", got)
	}
	ci.setStatus(cloud.SuccessfulConnection)
	if got := ci.RefreshStatus(); got != cloud.SuccessfulConnection {
		t.Fatalf("expected RefreshStatus to return current status unchanged, got %v", got)
	}
}

// TestCostIntegration_StatusIsSynchronized mirrors the ingestor: its build
// and run goroutines call GetCloudCost while the status endpoint calls
// GetStatus. Run with -race, unsynchronized status access fails it.
func TestCostIntegration_StatusIsSynchronized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"total_count": 0, "cost_data": []}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")

	ci := &CostIntegration{CostConfiguration: CostConfiguration{Region: "la-south-2"}}
	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := ci.GetCloudCost(start, end); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			ci.GetStatus()
			ci.RefreshStatus()
		}()
	}
	wg.Wait()

	if got := ci.GetStatus(); got != cloud.MissingData {
		t.Fatalf("expected MissingData after empty bills, got %v", got)
	}
}

// amortizedBillServer stands in for BSS cost-analysed-bills, answering each
// query by its cost_type and by whether it is grouped by Enterprise Project.
// A nil body for a query answers it with a BSS error.
func amortizedBillServer(t *testing.T, bodies map[string]string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Groupby []struct {
				Key string `json:"key"`
			} `json:"groupby"`
			CostType string `json:"cost_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("unexpected error decoding request: %v", err)
		}
		query := body.CostType
		for _, g := range body.Groupby {
			if g.Key == enterpriseProjectDimension {
				query += "/EP"
			}
		}
		w.Header().Set("Content-Type", "application/json")
		answer, ok := bodies[query]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error_code": "CBC.0100", "error_msg": "rejected"}`))
			return
		}
		w.Write([]byte(answer))
	}))
	t.Cleanup(server.Close)

	bssEndpointOverride = server.URL
	t.Cleanup(func() { bssEndpointOverride = "" })
	t.Setenv(env.HuaweiAccessKeyIDEnvVar, "test-ak")
	t.Setenv(env.HuaweiAccessKeySecretEnvVar, "test-sk")
	t.Setenv(env.HuaweiDomainIDEnvVar, "test-domain")
}

// billRowJSON is one cost-analysed-bills row: a resource in a region, with its
// Enterprise Project when epID is not empty, and an amount per day.
func billRowJSON(resourceID, epID string, amounts map[string]string) string {
	dims := `{"key": "RESOURCE_ID", "value": "` + resourceID + `"}, `
	if epID != "" {
		dims += `{"key": "` + enterpriseProjectDimension + `", "value": "` + epID + `"}, `
	} else {
		dims += `{"key": "CLOUD_SERVICE_TYPE", "value": "hws.service.type.ec2"}, `
	}
	dims += `{"key": "REGION_CODE", "value": "la-south-2"}`
	days := make([]string, 0, len(amounts))
	for day := range amounts {
		days = append(days, day)
	}
	slices.Sort(days)
	costs := make([]string, 0, len(days))
	for _, day := range days {
		costs = append(costs, `{"time_dimension_value": "`+day+`", "time_measure_id": 1, "amount": "`+amounts[day]+`", "official_amount": "`+amounts[day]+`"}`)
	}
	return `{"dimensions": [` + dims + `], "costs": [` + strings.Join(costs, ", ") + `]}`
}

func billRowsJSON(rows ...string) string {
	return `{"total_count": ` + strconv.Itoa(len(rows)) + `, "cost_data": [` + strings.Join(rows, ", ") + `]}`
}

// dailyCost returns the CloudCost of the resource on the day, or nil.
func dailyCost(ccsr *opencost.CloudCostSetRange, providerID string, day time.Time) *opencost.CloudCost {
	for _, ccs := range ccsr.CloudCostSets {
		if !ccs.Window.Start().Equal(day) {
			continue
		}
		for _, cc := range ccs.CloudCosts {
			if cc.Properties != nil && cc.Properties.ProviderID == providerID {
				return cc
			}
		}
	}
	return nil
}

// TestCostIntegration_GetCloudCost_AmortizedCosts covers a prepaid resource:
// paid on its first day, the original bill has it all on that day, while the
// amortized bill spreads it a day at a time. The original amounts fill
// ListCost, NetCost and InvoicedCost and the amortized ones AmortizedCost and
// AmortizedNetCost, both split across Enterprise Projects by their own
// records. A resource prepaid before the window has amortized days only, and
// keeps its Enterprise Project on them.
func TestCostIntegration_GetCloudCost_AmortizedCosts(t *testing.T) {
	const (
		prepaid = "hws.service.type.ec2:hws.resource.type.vm:aaaa:prepaid-node"
		earlier = "hws.service.type.ec2:hws.resource.type.vm:bbbb:earlier-node"
	)
	days := map[string]string{"2026-01-01": "2.40", "2026-01-02": "2.40", "2026-01-03": "2.40"}
	amortizedBillServer(t, map[string]string{
		originalCostType:          billRowsJSON(billRowJSON(prepaid, "", map[string]string{"2026-01-01": "72"})),
		amortizedCostType:         billRowsJSON(billRowJSON(prepaid, "", days), billRowJSON(earlier, "", days)),
		originalCostType + "/EP":  billRowsJSON(billRowJSON(prepaid, "ep-a", map[string]string{"2026-01-01": "72"})),
		amortizedCostType + "/EP": billRowsJSON(billRowJSON(prepaid, "ep-a", days), billRowJSON(earlier, "ep-b", days)),
	})

	ci := &CostIntegration{CostConfiguration: CostConfiguration{Region: "la-south-2"}}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 3))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 3; i++ {
		day := start.AddDate(0, 0, i)
		wantOriginal := 0.0
		if i == 0 {
			wantOriginal = 72
		}
		for _, r := range []struct {
			id, ep   string
			original float64
		}{{prepaid, "ep-a", wantOriginal}, {earlier, "ep-b", 0}} {
			cc := dailyCost(ccsr, r.id, day)
			if cc == nil {
				t.Fatalf("%s: no CloudCost for %s", day.Format(bssDateLayout), r.id)
			}
			if got := cc.Properties.Labels[EnterpriseProjectIDLabel]; got != r.ep {
				t.Errorf("%s %s: Enterprise Project %q, want %q", day.Format(bssDateLayout), r.id, got, r.ep)
			}
			if cc.ListCost.Cost != r.original || cc.NetCost.Cost != r.original || cc.InvoicedCost.Cost != r.original {
				t.Errorf("%s %s: list/net/invoiced %v/%v/%v, want %v", day.Format(bssDateLayout), r.id,
					cc.ListCost.Cost, cc.NetCost.Cost, cc.InvoicedCost.Cost, r.original)
			}
			if cc.AmortizedNetCost.Cost != 2.40 || cc.AmortizedCost.Cost != 2.40 {
				t.Errorf("%s %s: amortized net/amortized %v/%v, want 2.40", day.Format(bssDateLayout), r.id,
					cc.AmortizedNetCost.Cost, cc.AmortizedCost.Cost)
			}
		}
	}
	if ci.GetStatus() != cloud.SuccessfulConnection {
		t.Errorf("expected SuccessfulConnection, got %v", ci.GetStatus())
	}
}

// TestCostIntegration_GetCloudCost_AmortizedQueryFails checks that a failed
// amortized query fails the window rather than reporting the original costs
// as the amortized ones: the ingestor stores what GetCloudCost returns as
// covered, so only an error lets it retry the window once BSS answers.
func TestCostIntegration_GetCloudCost_AmortizedQueryFails(t *testing.T) {
	const resource = "hws.service.type.ec2:hws.resource.type.vm:aaaa:prepaid-node"
	amortizedBillServer(t, map[string]string{
		originalCostType:         billRowsJSON(billRowJSON(resource, "", map[string]string{"2026-01-01": "72"})),
		originalCostType + "/EP": billRowsJSON(billRowJSON(resource, "ep-a", map[string]string{"2026-01-01": "72"})),
	})

	ci := &CostIntegration{CostConfiguration: CostConfiguration{Region: "la-south-2"}}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ccsr, err := ci.GetCloudCost(start, start.AddDate(0, 0, 1))
	if err == nil {
		t.Fatalf("expected an error when the amortized cost query fails")
	}
	if ccsr != nil {
		t.Errorf("expected no result alongside the error, got %+v", ccsr)
	}
	if ci.GetStatus() != cloud.FailedConnection {
		t.Errorf("expected FailedConnection, got %v", ci.GetStatus())
	}
}
