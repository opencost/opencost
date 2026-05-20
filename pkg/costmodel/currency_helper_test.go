package costmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// mockConverter is a test double for currency.Converter. Rate is applied
// uniformly unless failFields contains the specific source amount, in
// which case Convert returns an error (useful for testing partial-
// failure / best-effort behavior).
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

func newFullAllocation() *opencost.Allocation {
	start := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	return &opencost.Allocation{
		Name:                         "ns/pod/container",
		Start:                        start,
		End:                          end,
		CPUCost:                      10,
		CPUCostAdjustment:            1,
		CPUCostIdle:                  2,
		GPUCost:                      20,
		GPUCostAdjustment:            3,
		GPUCostIdle:                  4,
		NetworkCost:                  30,
		NetworkCrossZoneCost:         5,
		NetworkCrossRegionCost:       6,
		NetworkInternetCost:          7,
		NetworkCostAdjustment:        8,
		NetworkNatGatewayEgressCost:  11,
		NetworkNatGatewayIngressCost: 12,
		LoadBalancerCost:             40,
		LoadBalancerCostAdjustment:   9,
		PVCostAdjustment:             13,
		RAMCost:                      50,
		RAMCostAdjustment:            14,
		RAMCostIdle:                  15,
		SharedCost:                   60,
		ExternalCost:                 70,
		UnmountedPVCost:              16,
		PVs: opencost.PVAllocations{
			opencost.PVKey{Cluster: "c1", Name: "pv-a"}: {Cost: 100, Adjustment: 5, ByteHours: 1},
		},
		LoadBalancers: opencost.LbAllocations{
			"lb-a": {Service: "svc-a", Cost: 80, Adjustment: 3},
		},
		SharedCostBreakdown: opencost.SharedCostBreakdowns{
			"shared-a": {
				Name:         "shared-a",
				TotalCost:    200,
				CPUCost:      40,
				GPUCost:      41,
				RAMCost:      42,
				PVCost:       43,
				NetworkCost:  44,
				LBCost:       45,
				ExternalCost: 46,
			},
		},
	}
}

func TestConvertAllocation_USDIsNoOp(t *testing.T) {
	a := newFullAllocation()
	before := *a
	m := &mockConverter{rate: 2.0}
	if err := ConvertAllocation(a, m, "USD"); err != nil {
		t.Fatalf("ConvertAllocation(USD) returned error: %v", err)
	}
	if m.calls != 0 {
		t.Fatalf("expected zero converter calls for USD, got %d", m.calls)
	}
	if a.CPUCost != before.CPUCost || a.RAMCost != before.RAMCost {
		t.Fatalf("USD path mutated allocation")
	}
}

func TestConvertAllocation_NilSafe(t *testing.T) {
	if err := ConvertAllocation(nil, &mockConverter{rate: 2.0}, "EUR"); err != nil {
		t.Fatalf("nil Allocation: unexpected error %v", err)
	}
	a := newFullAllocation()
	if err := ConvertAllocation(a, nil, "EUR"); err != nil {
		t.Fatalf("nil Converter: unexpected error %v", err)
	}
	// nil converter must not mutate
	if a.CPUCost != 10 {
		t.Fatalf("nil converter mutated CPUCost: got %v want 10", a.CPUCost)
	}
}

func TestConvertAllocation_MutatesAllCostFields(t *testing.T) {
	a := newFullAllocation()
	m := &mockConverter{rate: 2.0}

	if err := ConvertAllocation(a, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checks := map[string]struct{ got, want float64 }{
		"CPUCost":                      {a.CPUCost, 20},
		"CPUCostAdjustment":            {a.CPUCostAdjustment, 2},
		"CPUCostIdle":                  {a.CPUCostIdle, 4},
		"GPUCost":                      {a.GPUCost, 40},
		"GPUCostAdjustment":            {a.GPUCostAdjustment, 6},
		"GPUCostIdle":                  {a.GPUCostIdle, 8},
		"NetworkCost":                  {a.NetworkCost, 60},
		"NetworkCrossZoneCost":         {a.NetworkCrossZoneCost, 10},
		"NetworkCrossRegionCost":       {a.NetworkCrossRegionCost, 12},
		"NetworkInternetCost":          {a.NetworkInternetCost, 14},
		"NetworkCostAdjustment":        {a.NetworkCostAdjustment, 16},
		"NetworkNatGatewayEgressCost":  {a.NetworkNatGatewayEgressCost, 22},
		"NetworkNatGatewayIngressCost": {a.NetworkNatGatewayIngressCost, 24},
		"LoadBalancerCost":             {a.LoadBalancerCost, 80},
		"LoadBalancerCostAdjustment":   {a.LoadBalancerCostAdjustment, 18},
		"PVCostAdjustment":             {a.PVCostAdjustment, 26},
		"RAMCost":                      {a.RAMCost, 100},
		"RAMCostAdjustment":            {a.RAMCostAdjustment, 28},
		"RAMCostIdle":                  {a.RAMCostIdle, 30},
		"SharedCost":                   {a.SharedCost, 120},
		"ExternalCost":                 {a.ExternalCost, 140},
		"UnmountedPVCost":              {a.UnmountedPVCost, 32},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v want %v", name, c.got, c.want)
		}
	}
}

func TestConvertAllocation_NestedStructures(t *testing.T) {
	a := newFullAllocation()
	m := &mockConverter{rate: 2.0}

	if err := ConvertAllocation(a, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pv := a.PVs[opencost.PVKey{Cluster: "c1", Name: "pv-a"}]
	if pv == nil {
		t.Fatal("PV was lost")
	}
	if pv.Cost != 200 {
		t.Errorf("PV.Cost: got %v want 200", pv.Cost)
	}
	if pv.Adjustment != 10 {
		t.Errorf("PV.Adjustment: got %v want 10", pv.Adjustment)
	}
	if pv.ByteHours != 1 {
		t.Errorf("PV.ByteHours must not be touched: got %v want 1", pv.ByteHours)
	}

	lb := a.LoadBalancers["lb-a"]
	if lb == nil {
		t.Fatal("LB was lost")
	}
	if lb.Cost != 160 {
		t.Errorf("LB.Cost: got %v want 160", lb.Cost)
	}
	if lb.Adjustment != 6 {
		t.Errorf("LB.Adjustment: got %v want 6", lb.Adjustment)
	}
	if lb.Service != "svc-a" {
		t.Errorf("LB.Service must not be touched: got %q", lb.Service)
	}

	scb, ok := a.SharedCostBreakdown["shared-a"]
	if !ok {
		t.Fatal("SharedCostBreakdown entry was lost")
	}
	if scb.TotalCost != 400 {
		t.Errorf("SCB.TotalCost: got %v want 400", scb.TotalCost)
	}
	if scb.CPUCost != 80 {
		t.Errorf("SCB.CPUCost: got %v want 80", scb.CPUCost)
	}
	if scb.ExternalCost != 92 {
		t.Errorf("SCB.ExternalCost: got %v want 92", scb.ExternalCost)
	}
	if scb.Name != "shared-a" {
		t.Errorf("SCB.Name must not be touched: got %q", scb.Name)
	}
}

func TestConvertAllocation_BestEffortOnPartialFailure(t *testing.T) {
	a := newFullAllocation()
	// Inject a failure for the exact CPUCost value. All other fields
	// should still convert.
	m := &mockConverter{
		rate:       2.0,
		failValues: map[float64]bool{10: true},
	}

	if err := ConvertAllocation(a, m, "EUR"); err != nil {
		t.Fatalf("best-effort helper must not return error; got %v", err)
	}

	if a.CPUCost != 10 {
		t.Errorf("failed field must retain USD value: CPUCost got %v want 10", a.CPUCost)
	}
	if a.RAMCost != 100 {
		t.Errorf("unrelated field must convert: RAMCost got %v want 100", a.RAMCost)
	}
}

func TestConvertAllocation_ZeroValuesSkipped(t *testing.T) {
	a := &opencost.Allocation{Name: "zero"} // everything is 0
	m := &mockConverter{rate: 2.0}

	if err := ConvertAllocation(a, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.calls != 0 {
		t.Errorf("zero-valued fields must not call the converter; got %d calls", m.calls)
	}
}

func TestConvertAllocationSet(t *testing.T) {
	start := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	set := opencost.NewAllocationSet(start, end)
	set.Insert(&opencost.Allocation{Name: "a1", Start: start, End: end, CPUCost: 5})
	set.Insert(&opencost.Allocation{Name: "a2", Start: start, End: end, CPUCost: 7})

	m := &mockConverter{rate: 3.0}
	if err := ConvertAllocationSet(set, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if set.Allocations["a1"].CPUCost != 15 {
		t.Errorf("a1 CPUCost: got %v want 15", set.Allocations["a1"].CPUCost)
	}
	if set.Allocations["a2"].CPUCost != 21 {
		t.Errorf("a2 CPUCost: got %v want 21", set.Allocations["a2"].CPUCost)
	}
}

func TestConvertAllocationSetRange(t *testing.T) {
	start := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	set1 := opencost.NewAllocationSet(start, end)
	set1.Insert(&opencost.Allocation{Name: "a1", Start: start, End: end, CPUCost: 5})
	set2 := opencost.NewAllocationSet(end, end.Add(time.Hour))
	set2.Insert(&opencost.Allocation{Name: "a2", Start: end, End: end.Add(time.Hour), CPUCost: 9})
	asr := opencost.NewAllocationSetRange(set1, set2)

	m := &mockConverter{rate: 4.0}
	if err := ConvertAllocationSetRange(asr, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := set1.Allocations["a1"].CPUCost; got != 20 {
		t.Errorf("set1.a1.CPUCost: got %v want 20", got)
	}
	if got := set2.Allocations["a2"].CPUCost; got != 36 {
		t.Errorf("set2.a2.CPUCost: got %v want 36", got)
	}
}

func TestConvertAllocationSetRange_NilGuards(t *testing.T) {
	if err := ConvertAllocationSetRange(nil, &mockConverter{rate: 2}, "EUR"); err != nil {
		t.Fatalf("nil range: %v", err)
	}
	asr := opencost.NewAllocationSetRange()
	if err := ConvertAllocationSetRange(asr, nil, "EUR"); err != nil {
		t.Fatalf("nil converter: %v", err)
	}
}

// TestConvertAllocation_USDNormalized exercises the normalize-before-
// USD-check fix: lowercase and whitespace-padded "USD" must be treated
// as a no-op without invoking the converter.
func TestConvertAllocation_USDNormalized(t *testing.T) {
	for _, target := range []string{"usd", " USD ", "Usd", ""} {
		a := newFullAllocation()
		m := &mockConverter{rate: 2.0}
		if err := ConvertAllocation(a, m, target); err != nil {
			t.Fatalf("target %q: unexpected error %v", target, err)
		}
		if m.calls != 0 || m.getRateCalls != 0 {
			t.Errorf("target %q: expected zero converter activity, got convert=%d rate=%d",
				target, m.calls, m.getRateCalls)
		}
		if a.CPUCost != 10 {
			t.Errorf("target %q: mutated CPUCost to %v", target, a.CPUCost)
		}
	}
}

// TestConvertAllocation_GetRateFailsReturnsErrorNoMutation exercises the
// upfront rate probe: when GetRate fails, the helper returns an error
// and does NOT attempt any per-field conversion (prevents log-storms
// when the converter is down or the currency code is invalid).
func TestConvertAllocation_GetRateFailsReturnsErrorNoMutation(t *testing.T) {
	a := newFullAllocation()
	m := &mockConverter{rate: 2.0, getRateFail: true}

	err := ConvertAllocation(a, m, "XXX")
	if err == nil {
		t.Fatal("expected error when GetRate fails")
	}
	if m.calls != 0 {
		t.Errorf("expected zero Convert calls when GetRate fails; got %d", m.calls)
	}
	if a.CPUCost != 10 {
		t.Errorf("allocation must not be mutated when precheck fails: CPUCost got %v want 10", a.CPUCost)
	}
}

func TestConvertAllocationSetRange_GetRateFailsReturnsError(t *testing.T) {
	start := time.Date(2026, 4, 16, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	set := opencost.NewAllocationSet(start, end)
	set.Insert(&opencost.Allocation{Name: "a1", Start: start, End: end, CPUCost: 5})
	asr := opencost.NewAllocationSetRange(set)

	m := &mockConverter{rate: 2.0, getRateFail: true}
	err := ConvertAllocationSetRange(asr, m, "XXX")
	if err == nil {
		t.Fatal("expected error when GetRate fails")
	}
	if m.calls != 0 {
		t.Errorf("no Convert calls expected when precheck fails; got %d", m.calls)
	}
	if set.Allocations["a1"].CPUCost != 5 {
		t.Errorf("data must not be mutated on precheck failure")
	}
}
