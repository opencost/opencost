package cloudcost

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

type mockConverter struct {
	rate         float64
	failValues   map[float64]bool
	getRateFail  bool
	getRateCalls int
	calls        int
}

func (m *mockConverter) Convert(amount float64, from, to string) (float64, error) {
	m.calls++
	if from != "USD" {
		return 0, fmt.Errorf("mock only converts from USD, got %q", from)
	}
	if m.failValues[amount] {
		return 0, fmt.Errorf("simulated converter failure for amount %v", amount)
	}
	return amount * m.rate, nil
}

func (m *mockConverter) GetRate(from, to string) (float64, error) {
	m.getRateCalls++
	if m.getRateFail {
		return 0, fmt.Errorf("simulated rate lookup failure USD->%s", to)
	}
	return m.rate, nil
}

func newCCSR(t *testing.T) *opencost.CloudCostSetRange {
	t.Helper()
	start := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	set := opencost.NewCloudCostSet(start, end)
	cc := opencost.NewCloudCost(
		start, end,
		&opencost.CloudCostProperties{ProviderID: "cc-a"},
		0.5, // kubernetesPercent -- must remain untouched
		100, // listCost
		90,  // netCost
		80,  // amortizedNetCost
		110, // invoicedCost
		85,  // amortizedCost
	)
	set.Insert(cc)
	return &opencost.CloudCostSetRange{
		CloudCostSets: []*opencost.CloudCostSet{set},
		Window:        opencost.NewWindow(&start, &end),
	}
}

func TestConvertCloudCostSetRange_USDIsNoOp(t *testing.T) {
	ccsr := newCCSR(t)
	m := &mockConverter{rate: 2.0}
	if err := convertCloudCostSetRange(ccsr, m, "USD"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.calls != 0 {
		t.Fatalf("USD must not call converter, got %d calls", m.calls)
	}
}

func TestConvertCloudCostSetRange_NilGuards(t *testing.T) {
	if err := convertCloudCostSetRange(nil, &mockConverter{rate: 2.0}, "EUR"); err != nil {
		t.Fatalf("nil range: %v", err)
	}
	ccsr := newCCSR(t)
	if err := convertCloudCostSetRange(ccsr, nil, "EUR"); err != nil {
		t.Fatalf("nil converter: %v", err)
	}
}

func TestConvertCloudCostSetRange_MutatesAllCostMetrics(t *testing.T) {
	ccsr := newCCSR(t)
	m := &mockConverter{rate: 2.0}
	if err := convertCloudCostSetRange(ccsr, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var cc *opencost.CloudCost
	for _, v := range ccsr.CloudCostSets[0].CloudCosts {
		cc = v
		break
	}
	if cc == nil {
		t.Fatal("CloudCost missing from set")
	}

	checks := map[string]struct{ got, want float64 }{
		"ListCost.Cost":         {cc.ListCost.Cost, 200},
		"NetCost.Cost":          {cc.NetCost.Cost, 180},
		"AmortizedNetCost.Cost": {cc.AmortizedNetCost.Cost, 160},
		"InvoicedCost.Cost":     {cc.InvoicedCost.Cost, 220},
		"AmortizedCost.Cost":    {cc.AmortizedCost.Cost, 170},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v want %v", name, c.got, c.want)
		}
	}

	// KubernetesPercent must be preserved exactly.
	if cc.ListCost.KubernetesPercent != 0.5 {
		t.Errorf("KubernetesPercent must not be touched: got %v want 0.5", cc.ListCost.KubernetesPercent)
	}
}

func TestConvertCloudCostSetRange_GetRateFailsReturnsError(t *testing.T) {
	ccsr := newCCSR(t)
	m := &mockConverter{rate: 2.0, getRateFail: true}
	err := convertCloudCostSetRange(ccsr, m, "XXX")
	if err == nil {
		t.Fatal("expected error when GetRate fails")
	}
	if m.calls != 0 {
		t.Errorf("no Convert calls expected on precheck failure; got %d", m.calls)
	}
	for _, v := range ccsr.CloudCostSets[0].CloudCosts {
		if v.ListCost.Cost != 100 {
			t.Errorf("data must not be mutated on precheck failure; ListCost=%v", v.ListCost.Cost)
		}
	}
}

func TestConvertCloudCostSetRange_USDNormalized(t *testing.T) {
	for _, target := range []string{"usd", " USD ", ""} {
		ccsr := newCCSR(t)
		m := &mockConverter{rate: 2.0}
		if err := convertCloudCostSetRange(ccsr, m, target); err != nil {
			t.Fatalf("target %q: %v", target, err)
		}
		if m.calls != 0 || m.getRateCalls != 0 {
			t.Errorf("target %q: expected zero converter activity", target)
		}
	}
}

func TestConvertCloudCostSetRange_BestEffortOnPartialFailure(t *testing.T) {
	ccsr := newCCSR(t)
	// Fail only NetCost (90), leave others to convert.
	m := &mockConverter{
		rate:       2.0,
		failValues: map[float64]bool{90: true},
	}
	if err := convertCloudCostSetRange(ccsr, m, "EUR"); err != nil {
		t.Fatalf("best-effort helper must not return error; got %v", err)
	}

	var cc *opencost.CloudCost
	for _, v := range ccsr.CloudCostSets[0].CloudCosts {
		cc = v
		break
	}
	if cc.NetCost.Cost != 90 {
		t.Errorf("failed field must retain USD: NetCost got %v want 90", cc.NetCost.Cost)
	}
	if cc.ListCost.Cost != 200 {
		t.Errorf("other fields must still convert: ListCost got %v want 200", cc.ListCost.Cost)
	}
}
