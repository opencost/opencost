package mcp

import (
	"encoding/json"
	"math"
	"testing"
)

// TestSanitizeNonFiniteFloatsAssetResponseMarshals reproduces the integration
// failure (TestMCPAssetVsHTTP: "marshaling output: json: unsupported value:
// NaN") and verifies the sanitizer fixes it: encoding/json must reject the
// response before sanitization and accept it after, with non-finite floats
// zeroed and finite ones preserved.
func TestSanitizeNonFiniteFloatsAssetResponseMarshals(t *testing.T) {
	usedBytes := math.NaN()
	resp := &AssetResponse{
		Assets: map[string]*AssetSet{
			"assets": {
				Name: "assets",
				Assets: []*Asset{
					{
						Type:          "Node",
						Minutes:       math.NaN(),
						Adjustment:    math.Inf(1),
						TotalCost:     math.Inf(-1),
						CPUCost:       math.NaN(),
						GPUCost:       5.0, // finite, must be preserved
						ByteHoursUsed: &usedBytes,
						Overhead:      &NodeOverhead{OverheadCostFraction: math.NaN()},
						CPUBreakdown:  &AssetBreakdown{Idle: math.NaN()},
					},
				},
			},
		},
	}

	if _, err := json.Marshal(resp); err == nil {
		t.Fatal("expected json.Marshal to fail before sanitization (NaN/Inf present)")
	}

	sanitizeNonFiniteFloats(resp)

	if _, err := json.Marshal(resp); err != nil {
		t.Fatalf("expected json.Marshal to succeed after sanitization, got %v", err)
	}

	a := resp.Assets["assets"].Assets[0]
	if a.Minutes != 0 || a.Adjustment != 0 || a.TotalCost != 0 || a.CPUCost != 0 {
		t.Fatalf("expected non-finite base floats zeroed, got %+v", a)
	}
	if a.GPUCost != 5.0 {
		t.Fatalf("expected finite GPUCost preserved, got %v", a.GPUCost)
	}
	if a.ByteHoursUsed == nil || *a.ByteHoursUsed != 0 {
		t.Fatalf("expected non-finite *float64 zeroed, got %v", a.ByteHoursUsed)
	}
	if a.Overhead.OverheadCostFraction != 0 {
		t.Fatalf("expected nested overhead fraction zeroed, got %v", a.Overhead.OverheadCostFraction)
	}
	if a.CPUBreakdown.Idle != 0 {
		t.Fatalf("expected nested breakdown value zeroed, got %v", a.CPUBreakdown.Idle)
	}
}

func TestSanitizeNonFiniteFloatsNilSafe(t *testing.T) {
	sanitizeNonFiniteFloats(nil)
	var p *AssetResponse
	sanitizeNonFiniteFloats(p)
	sanitizeNonFiniteFloats(&AssetResponse{})
}
