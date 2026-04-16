package customcost

import (
	"fmt"
	"math"
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
)

type mockConverter struct {
	rate       float64
	failValues map[float64]bool
	calls      int
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
	return m.rate, nil
}

func newCostResponse() *CostResponse {
	return &CostResponse{
		Window:        opencost.NewWindow(nil, nil),
		TotalCost:     150,
		TotalCostType: CostTypeBlended,
		CustomCosts: []*CustomCost{
			{Id: "cc-a", Cost: 100, ListUnitPrice: 10, UsageQuantity: 7},
			{Id: "cc-b", Cost: 50, ListUnitPrice: 5, UsageQuantity: 3},
		},
	}
}

// fpEq compares float32s with a small tolerance to absorb the
// float64->float32 round-trip the helper performs.
func fpEq(got, want float32) bool {
	const tol = 1e-5
	return math.Abs(float64(got-want)) < tol
}

func TestConvertCustomCostResponse_USDIsNoOp(t *testing.T) {
	resp := newCostResponse()
	m := &mockConverter{rate: 2.0}
	if err := convertCustomCostResponse(resp, m, "USD"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.calls != 0 {
		t.Fatalf("USD must not call converter, got %d calls", m.calls)
	}
	if resp.TotalCost != 150 {
		t.Fatalf("USD path mutated TotalCost: got %v", resp.TotalCost)
	}
}

func TestConvertCustomCostResponse_NilGuards(t *testing.T) {
	if err := convertCustomCostResponse(nil, &mockConverter{rate: 2.0}, "EUR"); err != nil {
		t.Fatalf("nil response: %v", err)
	}
	resp := newCostResponse()
	if err := convertCustomCostResponse(resp, nil, "EUR"); err != nil {
		t.Fatalf("nil converter: %v", err)
	}
	if resp.TotalCost != 150 {
		t.Fatalf("nil converter mutated TotalCost: got %v", resp.TotalCost)
	}
}

func TestConvertCustomCostResponse_MutatesAllCostFields(t *testing.T) {
	resp := newCostResponse()
	m := &mockConverter{rate: 2.0}
	if err := convertCustomCostResponse(resp, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fpEq(resp.TotalCost, 300) {
		t.Errorf("TotalCost: got %v want 300", resp.TotalCost)
	}
	if !fpEq(resp.CustomCosts[0].Cost, 200) {
		t.Errorf("CustomCosts[0].Cost: got %v want 200", resp.CustomCosts[0].Cost)
	}
	if !fpEq(resp.CustomCosts[0].ListUnitPrice, 20) {
		t.Errorf("CustomCosts[0].ListUnitPrice: got %v want 20", resp.CustomCosts[0].ListUnitPrice)
	}
	if !fpEq(resp.CustomCosts[1].Cost, 100) {
		t.Errorf("CustomCosts[1].Cost: got %v want 100", resp.CustomCosts[1].Cost)
	}
	if !fpEq(resp.CustomCosts[1].ListUnitPrice, 10) {
		t.Errorf("CustomCosts[1].ListUnitPrice: got %v want 10", resp.CustomCosts[1].ListUnitPrice)
	}
	// UsageQuantity must NOT be touched -- it is a quantity, not a cost.
	if resp.CustomCosts[0].UsageQuantity != 7 {
		t.Errorf("UsageQuantity must not be touched: got %v want 7", resp.CustomCosts[0].UsageQuantity)
	}
}

func TestConvertCustomCostResponse_BestEffortOnPartialFailure(t *testing.T) {
	resp := newCostResponse()
	// Fail only the ListUnitPrice for cc-a (value 10).
	m := &mockConverter{
		rate:       2.0,
		failValues: map[float64]bool{10: true},
	}
	if err := convertCustomCostResponse(resp, m, "EUR"); err != nil {
		t.Fatalf("best-effort helper must not return error; got %v", err)
	}
	if !fpEq(resp.CustomCosts[0].ListUnitPrice, 10) {
		t.Errorf("failed field must retain USD: got %v want 10", resp.CustomCosts[0].ListUnitPrice)
	}
	if !fpEq(resp.CustomCosts[0].Cost, 200) {
		t.Errorf("other fields must still convert: got %v want 200", resp.CustomCosts[0].Cost)
	}
}

func TestConvertCustomCostTimeseriesResponse(t *testing.T) {
	ts := &CostTimeseriesResponse{
		Window:     opencost.NewWindow(nil, nil),
		Timeseries: []*CostResponse{newCostResponse(), newCostResponse()},
	}
	m := &mockConverter{rate: 3.0}
	if err := convertCustomCostTimeseriesResponse(ts, m, "EUR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, cr := range ts.Timeseries {
		if !fpEq(cr.TotalCost, 450) {
			t.Errorf("ts[%d].TotalCost: got %v want 450", i, cr.TotalCost)
		}
	}
}

func TestConvertCustomCostTimeseriesResponse_NilGuards(t *testing.T) {
	if err := convertCustomCostTimeseriesResponse(nil, &mockConverter{rate: 2.0}, "EUR"); err != nil {
		t.Fatalf("nil response: %v", err)
	}
	ts := &CostTimeseriesResponse{}
	if err := convertCustomCostTimeseriesResponse(ts, nil, "EUR"); err != nil {
		t.Fatalf("nil converter: %v", err)
	}
}
