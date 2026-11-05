package ibm

import (
	"testing"
	"time"

	"github.com/IBM/platform-services-go-sdk/usagereportsv4"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestMonthsOverlapping(t *testing.T) {
	start := time.Date(2026, 1, 28, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	got := monthsOverlapping(start, end)
	want := []string{"2026-01", "2026-02", "2026-03"}
	if len(got) != len(want) {
		t.Fatalf("monthsOverlapping len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("monthsOverlapping[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMonthsOverlappingSameMonth(t *testing.T) {
	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC)
	got := monthsOverlapping(start, end)
	if len(got) != 1 || got[0] != "2026-02" {
		t.Errorf("got %v, want [2026-02]", got)
	}
}

func TestDaysInMonth(t *testing.T) {
	if got := daysInMonth(2026, 2); got != 28 {
		t.Errorf("Feb 2026 days = %d, want 28", got)
	}
	if got := daysInMonth(2024, 2); got != 29 {
		t.Errorf("Feb 2024 days = %d, want 29", got)
	}
	if got := daysInMonth(2026, 1); got != 31 {
		t.Errorf("Jan 2026 days = %d, want 31", got)
	}
}

func TestProrationDays(t *testing.T) {
	monthStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := prorationDays(monthStart, time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)); got != 31 {
		t.Errorf("completed month = %d, want 31", got)
	}
	if got := prorationDays(monthStart, time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)); got != 5 {
		t.Errorf("MTD day 5 = %d, want 5", got)
	}
	if got := prorationDays(monthStart, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)); got != 31 {
		t.Errorf("last day of month = %d, want 31", got)
	}
}

func TestSelectIBMCategory(t *testing.T) {
	tests := []struct {
		service string
		want    string
	}{
		{"is.instance", opencost.ComputeCategory},
		{"is.bare-metal-server", opencost.ComputeCategory},
		{"is.dedicated-host", opencost.ComputeCategory},
		{"codeengine", opencost.ComputeCategory},
		{"containers-kubernetes", opencost.ComputeCategory},
		{"is.volume", opencost.StorageCategory},
		{"is.snapshot", opencost.StorageCategory},
		{"is.share", opencost.StorageCategory},
		{"cloud-object-storage", opencost.StorageCategory},
		{"databases-for-postgresql", opencost.StorageCategory},
		{"databases-for-redis", opencost.StorageCategory},
		{"is.load-balancer", opencost.NetworkCategory},
		{"is.floating-ip", opencost.NetworkCategory},
		{"is.public-gateway", opencost.NetworkCategory},
		{"is.vpn", opencost.NetworkCategory},
		{"transit", opencost.NetworkCategory},
		{"internet-svcs", opencost.NetworkCategory},
		// Prefix arms removed: these must not silently widen beyond the locked table.
		{"is.vpn-server", opencost.OtherCategory},
		{"transit-gateway", opencost.OtherCategory},
		{"codeengine-app", opencost.OtherCategory},
		{"is.vpc", opencost.OtherCategory},
		{"dns", opencost.OtherCategory},
		{"cdn", opencost.OtherCategory},
		{"", opencost.OtherCategory},
		{"unknown-service", opencost.OtherCategory},
	}
	for _, tt := range tests {
		if got := selectIBMCategory(tt.service); got != tt.want {
			t.Errorf("selectIBMCategory(%q) = %q, want %q", tt.service, got, tt.want)
		}
	}
}

func TestParseTags(t *testing.T) {
	tags := parseTags([]any{
		"env:prod",
		"staging",
		map[string]any{"key": "owner", "value": "platform"},
		map[string]any{"Key": "team", "Value": "sre"},
		map[string]any{"key": "empty", "value": ""},
	})
	if tags["env"] != "prod" {
		t.Errorf("env tag = %q, want prod", tags["env"])
	}
	if tags["owner"] != "platform" {
		t.Errorf("owner tag = %q, want platform", tags["owner"])
	}
	if tags["team"] != "sre" {
		t.Errorf("team tag = %q, want sre", tags["team"])
	}
	if _, ok := tags["staging"]; ok {
		t.Error("non key:value string tag should be omitted")
	}
	if _, ok := tags["empty"]; ok {
		t.Error("empty tag value should be omitted")
	}
}

func TestInstanceUsageFromSDK_SkipsNonBillable(t *testing.T) {
	billable := false
	cost := 10.0
	item := usagereportsv4.InstanceUsage{
		Billable: &billable,
		Usage: []usagereportsv4.Metric{
			{Cost: &cost, RatedCost: &cost},
		},
	}
	if _, ok := instanceUsageFromSDK(item); ok {
		t.Fatal("expected non-billable instance to be skipped")
	}
}

func TestInstanceUsageFromSDK_CurrencyConversion(t *testing.T) {
	billable := true
	cost := 10.0
	rated := 20.0
	rate := 1.5
	month := "2026-01"
	account := "acct"
	resourceID := "is.instance"
	item := usagereportsv4.InstanceUsage{
		Billable:     &billable,
		AccountID:    &account,
		ResourceID:   &resourceID,
		Month:        &month,
		CurrencyRate: &rate,
		Usage: []usagereportsv4.Metric{
			{Cost: &cost, RatedCost: &rated},
		},
		Tags:        []any{"env:test"},
		ServiceTags: []any{"svc:iks"},
	}
	record, ok := instanceUsageFromSDK(item)
	if !ok {
		t.Fatal("expected billable instance")
	}
	if record.Cost != 15.0 {
		t.Errorf("Cost = %v, want 15", record.Cost)
	}
	if record.RatedCost != 30.0 {
		t.Errorf("RatedCost = %v, want 30", record.RatedCost)
	}
	labels := parseTags(record.Tags)
	if labels["env"] != "test" || labels["svc"] != "iks" {
		t.Errorf("merged tags = %#v", labels)
	}
}

func TestInstanceUsageFromSDK_NormalizesPrefixedAccountID(t *testing.T) {
	billable := true
	month := "2026-01"
	account := "a/b09edf5642ebfad587c594f4d4a354b0"
	resourceID := "is.instance"
	cost := 1.0
	item := usagereportsv4.InstanceUsage{
		Billable:   &billable,
		AccountID:  &account,
		ResourceID: &resourceID,
		Month:      &month,
		Usage: []usagereportsv4.Metric{
			{Cost: &cost, RatedCost: &cost},
		},
	}
	record, ok := instanceUsageFromSDK(item)
	if !ok {
		t.Fatal("expected billable instance")
	}
	if record.AccountID != "b09edf5642ebfad587c594f4d4a354b0" {
		t.Errorf("AccountID = %q, want bare hex", record.AccountID)
	}
}

func TestInstanceUsageFromSDK_SkipsNonChargeableMetrics(t *testing.T) {
	billable := true
	cost := 5.0
	rated := 5.0
	info := true
	month := "2026-01"
	item := usagereportsv4.InstanceUsage{
		Billable: &billable,
		Month:    &month,
		Usage: []usagereportsv4.Metric{
			{Cost: &cost, RatedCost: &rated, NonChargeable: &info},
			{Cost: &cost, RatedCost: &rated},
		},
	}
	record, ok := instanceUsageFromSDK(item)
	if !ok {
		t.Fatal("expected billable instance")
	}
	if record.Cost != 5.0 || record.RatedCost != 5.0 {
		t.Errorf("got cost=%v rated=%v, want 5/5", record.Cost, record.RatedCost)
	}
}

func TestCloudCostsFromInstance(t *testing.T) {
	start := time.Date(2026, 1, 30, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)

	item := instanceUsageRecord{
		AccountID:          "b09edf5642ebfad587c594f4d4a354b0",
		ResourceInstanceID: "crn:v1:bluemix:public:containers-kubernetes:us-south:a/b09edf5642ebfad587c594f4d4a354b0:8042b2a8af6a4a5cbf6dbe09e07311d2:worker:kube-w1",
		ResourceID:         "containers-kubernetes",
		ResourceName:       "IBM Cloud Kubernetes Service",
		Region:             "us-south",
		Month:              "2026-01",
		Cost:               31.0,
		RatedCost:          62.0,
		Tags:               []any{"team:sre"},
	}

	costs := cloudCostsFromInstance(item, start, end, asOf)
	// Every day the month total was prorated across is emitted, not just the days inside the
	// window, so the whole of the completed January is returned.
	if len(costs) != 31 {
		t.Fatalf("got %d cloud costs, want 31 (whole prorated January)", len(costs))
	}

	dailyNet := 31.0 / 31.0
	dailyList := 62.0 / 31.0
	for _, cc := range costs {
		if cc.Properties.Provider != opencost.IBMProvider {
			t.Errorf("provider = %q", cc.Properties.Provider)
		}
		if cc.Properties.AccountID != "b09edf5642ebfad587c594f4d4a354b0" {
			t.Errorf("account = %q", cc.Properties.AccountID)
		}
		if cc.Properties.InvoiceEntityID != cc.Properties.AccountID {
			t.Errorf("InvoiceEntityID = %q, want AccountID", cc.Properties.InvoiceEntityID)
		}
		if cc.Properties.Service != "containers-kubernetes" {
			t.Errorf("service = %q, want ResourceID", cc.Properties.Service)
		}
		if cc.Properties.Category != opencost.ComputeCategory {
			t.Errorf("category = %q", cc.Properties.Category)
		}
		if cc.Properties.Labels["team"] != "sre" {
			t.Errorf("labels = %#v", cc.Properties.Labels)
		}
		if cc.Properties.Labels["ibm_resource_name"] != "IBM Cloud Kubernetes Service" {
			t.Errorf("ibm_resource_name = %q", cc.Properties.Labels["ibm_resource_name"])
		}
		if cc.NetCost.Cost != dailyNet {
			t.Errorf("net cost = %v, want %v", cc.NetCost.Cost, dailyNet)
		}
		if cc.ListCost.Cost != dailyList {
			t.Errorf("list cost = %v, want %v", cc.ListCost.Cost, dailyList)
		}
		if cc.AmortizedCost.Cost != dailyList {
			t.Errorf("amortized cost = %v, want list/rated %v", cc.AmortizedCost.Cost, dailyList)
		}
	}
}

func TestCloudCostsFromInstance_RatedCostZeroKept(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	item := instanceUsageRecord{
		AccountID:          "acct",
		ResourceInstanceID: "inst",
		ResourceID:         "is.instance",
		Month:              "2026-01",
		Cost:               31.0,
		RatedCost:          0,
	}
	costs := cloudCostsFromInstance(item, start, end, asOf)
	if len(costs) != 31 {
		t.Fatalf("got %d costs, want 31 (whole prorated month)", len(costs))
	}
	if costs[0].ListCost.Cost != 0 {
		t.Errorf("ListCost = %v, want 0 (rated_cost zero is valid)", costs[0].ListCost.Cost)
	}
	if costs[0].NetCost.Cost != 1.0 {
		t.Errorf("NetCost = %v, want 1", costs[0].NetCost.Cost)
	}
}

func TestCloudCostsFromInstance_MTDProration(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	item := instanceUsageRecord{
		AccountID:          "acct",
		ResourceInstanceID: "inst",
		ResourceID:         "is.instance",
		Month:              "2026-01",
		Cost:               50.0,
		RatedCost:          50.0,
	}
	costs := cloudCostsFromInstance(item, start, end, asOf)
	if len(costs) != 5 {
		t.Fatalf("got %d costs, want 5 MTD days", len(costs))
	}
	if costs[0].NetCost.Cost != 10.0 {
		t.Errorf("daily net = %v, want 10 (50/5)", costs[0].NetCost.Cost)
	}
}

func TestServiceUsesResourceID(t *testing.T) {
	item := instanceUsageRecord{
		AccountID:          "acct",
		ResourceInstanceID: "inst",
		ResourceID:         "is.instance",
		ResourceName:       "Virtual Server for VPC",
		Month:              "2026-02",
		Cost:               28.0,
		RatedCost:          28.0,
	}
	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	costs := cloudCostsFromInstance(item, start, end, asOf)
	if len(costs) != 28 {
		t.Fatalf("got %d costs, want 28 (whole prorated February)", len(costs))
	}
	if costs[0].Properties.Service != "is.instance" {
		t.Errorf("service = %q, want is.instance", costs[0].Properties.Service)
	}
	if costs[0].Properties.Labels["ibm_resource_name"] != "Virtual Server for VPC" {
		t.Errorf("display name label = %q", costs[0].Properties.Labels["ibm_resource_name"])
	}
}

func TestNormalizeAccountID(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"b09edf5642ebfad587c594f4d4a354b0", "b09edf5642ebfad587c594f4d4a354b0"},
		{"a/b09edf5642ebfad587c594f4d4a354b0", "b09edf5642ebfad587c594f4d4a354b0"},
		{"  a/acct  ", "acct"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := normalizeAccountID(tt.in); got != tt.want {
			t.Errorf("normalizeAccountID(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestUsageConfigurationKeySanitizesSlash(t *testing.T) {
	cfg := &UsageConfiguration{AccountID: "a/b09edf5642ebfad587c594f4d4a354b0"}
	if got := cfg.Key(); got != "b09edf5642ebfad587c594f4d4a354b0" {
		t.Errorf("Key() = %q, want bare hex", got)
	}
}

// The billing-export producer (cloudability/kubecost-saas, bingen/ibm) marks every IKS/ROKS row
// as fully Kubernetes. Both producers write into the same aggregation, so if this path leaves
// KubernetesPercent at zero, migrating a customer from the export to an API key silently zeroes
// their entire IBM Kubernetes spend.
func TestKubernetesPercentMatchesBillingExportProducer(t *testing.T) {
	const workerCRN = "crn:v1:bluemix:public:containers-kubernetes:jp-tok:a/f8ce6d5aa4cf4:" +
		"ch9goilt0elehhb3utbg:worker:kube-ch9goilt0elehhb3utbg-ccmdefault-0000017d"

	tests := []struct {
		name       string
		resourceID string
		providerID string
		want       float64
	}{
		{"kubernetes by service id", "containers-kubernetes", "crn:v1:bluemix:public:containers-kubernetes:us-south:a/x:y::", 1.0},
		{"kubernetes by crn service segment", "", workerCRN, 1.0},
		{"service id is case insensitive", "Containers-Kubernetes", "", 1.0},
		{"unrelated service", "is.volume", "crn:v1:bluemix:public:is:us-south:a/x::volume:r006-1", 0.0},
	}

	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			item := instanceUsageRecord{
				AccountID:          "acct",
				ResourceInstanceID: tt.providerID,
				ResourceID:         tt.resourceID,
				Month:              "2026-02",
				Cost:               28.0,
				RatedCost:          56.0,
			}
			costs := cloudCostsFromInstance(item, start, end, asOf)
			if len(costs) != 28 {
				t.Fatalf("got %d costs, want 28 (whole prorated February)", len(costs))
			}
			cc := costs[0]
			for metric, got := range map[string]float64{
				"ListCost":         cc.ListCost.KubernetesPercent,
				"NetCost":          cc.NetCost.KubernetesPercent,
				"AmortizedNetCost": cc.AmortizedNetCost.KubernetesPercent,
				"AmortizedCost":    cc.AmortizedCost.KubernetesPercent,
				"InvoicedCost":     cc.InvoicedCost.KubernetesPercent,
			} {
				if got != tt.want {
					t.Errorf("%s.KubernetesPercent = %v, want %v", metric, got, tt.want)
				}
			}
		})
	}
}

// bingen/ibm sets AccountName and InvoiceEntityName to the same normalized account id, because IBM
// billing data carries no account display name. Both are user-selectable aggregation properties, so
// leaving them empty here would split rows when a customer switches producers.
func TestAccountNamesMatchBillingExportProducer(t *testing.T) {
	item := instanceUsageRecord{
		AccountID:          "4756fbd48cbc4b6d968340888c27fd05",
		ResourceInstanceID: "inst",
		ResourceID:         "is.instance",
		Month:              "2026-02",
		Cost:               28.0,
		RatedCost:          28.0,
	}
	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	costs := cloudCostsFromInstance(item, start, end, asOf)
	if len(costs) != 28 {
		t.Fatalf("got %d costs, want 28 (whole prorated February)", len(costs))
	}
	props := costs[0].Properties
	if props.AccountName != item.AccountID {
		t.Errorf("AccountName = %q, want %q", props.AccountName, item.AccountID)
	}
	if props.InvoiceEntityName != item.AccountID {
		t.Errorf("InvoiceEntityName = %q, want %q", props.InvoiceEntityName, item.AccountID)
	}
	// Pinned so a later edit cannot populate it and split rows against the other producer.
	if props.AvailabilityZone != "" {
		t.Errorf("AvailabilityZone = %q, want empty", props.AvailabilityZone)
	}
}

// A month total is divided by a divisor derived from asOf, so the resulting flat daily rate is only
// self-consistent if every day it covers is written in the same pass. The ingestor's Put replaces
// whole day-sets, so emitting only the days inside the caller's window leaves the rest of the month
// holding rates computed at an earlier asOf, and the stored month sums to neither total.
//
// The integration already fetches the whole month from IBM regardless of the window, so covering
// every prorated day costs no extra API calls.
func TestCloudCostsFromInstanceCoversWholeProratedMonth(t *testing.T) {
	// Stock ingestor shape: a 7-day window that does not reach the first of the month.
	start := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)

	item := instanceUsageRecord{
		AccountID:          "acct",
		ResourceInstanceID: "inst",
		ResourceID:         "is.instance",
		Month:              "2026-10",
		Cost:               110.0,
		RatedCost:          110.0,
	}

	costs := cloudCostsFromInstance(item, start, end, asOf)

	// asOf is the 11th, so the month-to-date total spreads over days 1..11.
	if len(costs) != 11 {
		t.Fatalf("got %d costs, want 11 (every prorated day of the month)", len(costs))
	}

	var total float64
	for _, cc := range costs {
		total += cc.NetCost.Cost
	}
	if diff := total - item.Cost; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("emitted total = %v, want %v (IBM's reported month total)", total, item.Cost)
	}

	// First emitted day must be the first of the month, not the window start.
	if got := costs[0].Window.Start().UTC(); !got.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("first day = %s, want 2026-10-01", got.Format("2006-01-02"))
	}
}
