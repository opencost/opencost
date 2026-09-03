package costmodel

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
	"time"
)

const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// CostDebtReport captures the production readiness and FinOps allocation debt scorecard.
type CostDebtReport struct {
	NamespaceID              string   `json:"namespace_id"`
	CDIScore                 float64  `json:"cdi_score"`
	AllocationMultiplier     float64  `json:"allocation_multiplier"`
	QueryLatencyMs           float64  `json:"query_latency_ms"`
	MutationSafetyScore      float64  `json:"mutation_safety_score"`
	ProductionReadinessIndex float64  `json:"production_readiness_index"`
	IsProductionReady        bool     `json:"is_production_ready"`
	CriticalSmells           []string `json:"critical_smells"`
	ReceiptHash              string   `json:"receipt_hash"`
}

// LedgerEntry represents an immutable record in the TechnicalDueDiligenceLedger.
type LedgerEntry struct {
	Index          int                    `json:"index"`
	Timestamp      string                 `json:"timestamp"`
	NamespaceID    string                 `json:"namespace_id"`
	EventType      string                 `json:"event_type"`
	ReadinessIndex float64                `json:"readiness_index"`
	CriticalSmells []string               `json:"critical_smells"`
	PrevHash       string                 `json:"prev_hash"`
	CurrHash       string                 `json:"curr_hash"`
	Metadata       map[string]interface{} `json:"metadata"`
}

// TechnicalDueDiligenceLedger maintains a cryptographically verifiable SHA-256 hash chain of cost allocation events.
type TechnicalDueDiligenceLedger struct {
	mu       sync.RWMutex
	entries  []LedgerEntry
	lastHash string
}

func NewTechnicalDueDiligenceLedger() *TechnicalDueDiligenceLedger {
	return &TechnicalDueDiligenceLedger{
		entries:  make([]LedgerEntry, 0),
		lastHash: GenesisHash,
	}
}

func (l *TechnicalDueDiligenceLedger) RecordCostEvent(
	namespaceID string,
	eventType string,
	readinessIndex float64,
	criticalSmells []string,
	metadata map[string]interface{},
) LedgerEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	index := len(l.entries)

	metaBytes, _ := json.Marshal(metadata)
	metaHash := sha256.Sum256(metaBytes)

	canonical := fmt.Sprintf("%d|%s|%s|%s|%.2f|%s|%s",
		index, l.lastHash, namespaceID, eventType, readinessIndex, timestamp, hex.EncodeToString(metaHash[:]))

	currHashBytes := sha256.Sum256([]byte(canonical))
	currHash := hex.EncodeToString(currHashBytes[:])

	entry := LedgerEntry{
		Index:          index,
		Timestamp:      timestamp,
		NamespaceID:    namespaceID,
		EventType:      eventType,
		ReadinessIndex: readinessIndex,
		CriticalSmells: criticalSmells,
		PrevHash:       l.lastHash,
		CurrHash:       currHash,
		Metadata:       metadata,
	}

	l.entries = append(l.entries, entry)
	l.lastHash = currHash
	return entry
}

func (l *TechnicalDueDiligenceLedger) GetEntries() []LedgerEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	copied := make([]LedgerEntry, len(l.entries))
	copy(copied, l.entries)
	return copied
}

func (l *TechnicalDueDiligenceLedger) VerifyIntegrity() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	prev := GenesisHash
	for _, entry := range l.entries {
		if entry.PrevHash != prev {
			return false
		}
		prev = entry.CurrHash
	}
	return true
}

// ProductionDebtCostGate evaluates Kubernetes cluster cost allocation drift and idle GPU/CPU waste.
type ProductionDebtCostGate struct {
	NeverEquateIntentToApproval bool
	MaxAcceptableCDI            float64
	Ledger                      *TechnicalDueDiligenceLedger
}

func NewProductionDebtCostGate(neverEquateIntentToApproval bool, maxAcceptableCDI float64) *ProductionDebtCostGate {
	if maxAcceptableCDI <= 0 {
		maxAcceptableCDI = 12.0
	}
	return &ProductionDebtCostGate{
		NeverEquateIntentToApproval: neverEquateIntentToApproval,
		MaxAcceptableCDI:            maxAcceptableCDI,
		Ledger:                      NewTechnicalDueDiligenceLedger(),
	}
}

func (g *ProductionDebtCostGate) CheckKillSwitch() bool {
	if val := os.Getenv("AAG_KILL_SWITCH"); val == "true" || val == "1" || val == "yes" {
		return true
	}
	if _, err := os.Stat("artifacts/KILL"); err == nil {
		return true
	}
	if _, err := os.Stat("/tmp/KILL"); err == nil {
		return true
	}
	return false
}

func (g *ProductionDebtCostGate) EvaluateCostAllocation(
	namespaceID string,
	allocatedCostUSD float64,
	utilizedCostUSD float64,
	queryLatencyMs float64,
	idleAllocationStalls int,
	unGatedMutations int,
) (*CostDebtReport, error) {
	if g.CheckKillSwitch() {
		g.Ledger.RecordCostEvent(
			namespaceID,
			"cost_allocation_halted_kill_switch",
			0.0,
			[]string{"EMERGENCY_KILL_SWITCH_ENGAGED"},
			map[string]interface{}{"reason": "AAG_KILL_SWITCH is set"},
		)
		return nil, fmt.Errorf("A2Z SOC ActionGate: Emergency kill switch is engaged. OpenCost execution halted")
	}

	criticalSmells := make([]string, 0)

	// KPI 2: Allocation Multiplier
	allocRatio := utilizedCostUSD / math.Max(1.0, allocatedCostUSD)
	if allocRatio > 1.8 {
		criticalSmells = append(criticalSmells, fmt.Sprintf("HIGH_COST_ALLOCATION_SPRAWL_%.2fX", allocRatio))
	}

	// KPI 3: Latency Ceiling
	if queryLatencyMs > 15.0 {
		criticalSmells = append(criticalSmells, fmt.Sprintf("HIGH_COST_QUERY_LATENCY_%.1fMS", queryLatencyMs))
	}

	// Idle Allocation Stalls
	if idleAllocationStalls > 0 {
		criticalSmells = append(criticalSmells, fmt.Sprintf("DETECTED_%d_IDLE_ALLOCATION_STALLS", idleAllocationStalls))
	}

	// KPI 4: Mutation Safety
	if unGatedMutations > 0 {
		criticalSmells = append(criticalSmells, fmt.Sprintf("DETECTED_%d_UNGATED_COST_MODEL_MUTATIONS", unGatedMutations))
	}

	// KPI 1: Cost Debt Index (0 = Clean, 100 = Catastrophic)
	cdi := math.Max(0.0, (allocRatio-1.0)*20.0) +
		math.Max(0.0, (queryLatencyMs-1.5)*0.5) +
		float64(idleAllocationStalls*25) +
		float64(unGatedMutations*30)
	cdiScore := math.Round(math.Min(100.0, cdi)*100) / 100

	readiness := math.Max(0.0, 100.0-cdiScore)
	isProductionReady := cdiScore <= g.MaxAcceptableCDI && len(criticalSmells) == 0

	entry := g.Ledger.RecordCostEvent(
		namespaceID,
		map[bool]string{true: "cost_allocation_authorized", false: "cost_allocation_flagged_debt"}[isProductionReady],
		readiness,
		criticalSmells,
		map[string]interface{}{
			"cdi_score":                       cdiScore,
			"alloc_ratio":                     allocRatio,
			"allocated_cost_usd":              allocatedCostUSD,
			"utilized_cost_usd":               utilizedCostUSD,
			"query_latency_ms":                queryLatencyMs,
			"idle_allocation_stalls":          idleAllocationStalls,
			"un_gated_mutations":              unGatedMutations,
			"never_equate_intent_to_approval": g.NeverEquateIntentToApproval,
		},
	)

	mutationSafety := 100.0
	if unGatedMutations > 0 {
		mutationSafety = math.Max(0.0, 100.0-float64(unGatedMutations*30))
	}

	return &CostDebtReport{
		NamespaceID:              namespaceID,
		CDIScore:                 cdiScore,
		AllocationMultiplier:     math.Round(allocRatio*100) / 100,
		QueryLatencyMs:           math.Round(queryLatencyMs*100) / 100,
		MutationSafetyScore:      mutationSafety,
		ProductionReadinessIndex: readiness,
		IsProductionReady:        isProductionReady,
		CriticalSmells:           criticalSmells,
		ReceiptHash:              entry.CurrHash,
	}, nil
}
