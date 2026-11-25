package costmodel

import (
	"testing"
)

func TestProductionDebtCostGate_CleanPassesReadiness(t *testing.T) {
	gate := NewProductionDebtCostGate(true, 12.0)

	report, err := gate.EvaluateCostAllocation(
		"kubecost-production-ai-workloads",
		16000.0,
		16800.0,
		1.2,
		0,
		0,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !report.IsProductionReady {
		t.Errorf("expected clean allocation to be production ready")
	}
	if report.CDIScore > 12.0 {
		t.Errorf("expected CDI score <= 12.0, got %.2f", report.CDIScore)
	}
	if len(report.CriticalSmells) != 0 {
		t.Errorf("expected 0 critical smells, got %d", len(report.CriticalSmells))
	}
	if report.ReceiptHash == "" {
		t.Errorf("expected non-empty receipt hash")
	}
}

func TestProductionDebtCostGate_DegradedFailsDebt(t *testing.T) {
	gate := NewProductionDebtCostGate(true, 12.0)

	report, err := gate.EvaluateCostAllocation(
		"uncalibrated-dev-namespace",
		16000.0,
		45000.0, // 2.81x allocation sprawl
		28.0,    // High query latency
		3,       // 3 idle allocation stalls
		2,       // 2 un-gated mutations
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if report.IsProductionReady {
		t.Errorf("expected degraded allocation to fail production readiness")
	}
	if report.CDIScore <= 50.0 {
		t.Errorf("expected CDI score > 50.0, got %.2f", report.CDIScore)
	}

	hasAllocationSprawl := false
	hasQueryLatency := false
	hasIdleStalls := false
	hasUngatedMutations := false

	for _, smell := range report.CriticalSmells {
		if smell == "HIGH_COST_ALLOCATION_SPRAWL_2.81X" {
			hasAllocationSprawl = true
		}
		if smell == "HIGH_COST_QUERY_LATENCY_28.0MS" {
			hasQueryLatency = true
		}
		if smell == "DETECTED_3_IDLE_ALLOCATION_STALLS" {
			hasIdleStalls = true
		}
		if smell == "DETECTED_2_UNGATED_COST_MODEL_MUTATIONS" {
			hasUngatedMutations = true
		}
	}

	if !hasAllocationSprawl || !hasQueryLatency || !hasIdleStalls || !hasUngatedMutations {
		t.Errorf("missing expected critical smells in report: %v", report.CriticalSmells)
	}
}

func TestTechnicalDueDiligenceLedger_Integrity(t *testing.T) {
	gate := NewProductionDebtCostGate(true, 12.0)

	_, _ = gate.EvaluateCostAllocation("ns-1", 1000.0, 1050.0, 1.0, 0, 0)
	_, _ = gate.EvaluateCostAllocation("ns-2", 2000.0, 2100.0, 1.1, 0, 0)
	_, _ = gate.EvaluateCostAllocation("ns-3", 3000.0, 3150.0, 1.2, 0, 0)

	entries := gate.Ledger.GetEntries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 ledger entries, got %d", len(entries))
	}

	if entries[0].PrevHash != GenesisHash {
		t.Errorf("expected genesis hash as prev_hash for entry 0, got %s", entries[0].PrevHash)
	}
	if entries[1].PrevHash != entries[0].CurrHash {
		t.Errorf("hash chain broken between entry 0 and 1")
	}
	if entries[2].PrevHash != entries[1].CurrHash {
		t.Errorf("hash chain broken between entry 1 and 2")
	}

	if !gate.Ledger.VerifyIntegrity() {
		t.Errorf("expected ledger integrity verification to pass")
	}
}
