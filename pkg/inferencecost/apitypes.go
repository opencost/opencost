package inferencecost

import (
	"github.com/opencost/opencost/core/pkg/opencost"
)

// InferenceCostResponse is the flat, per-cost-basis API representation of
// inference costs for a single model/namespace in a time window. It is
// projected from the internal InferenceCost struct (which stores costs keyed
// by CostBasis) so that the JSON output matches the design doc shape exactly.
type InferenceCostResponse struct {
	Properties InferenceCostAPIProperties `json:"properties"`
	Window     opencost.Window            `json:"window"`

	// CostBasis identifies whether these costs are usage-based or
	// allocation-based. Set from the QueryRequest.
	CostBasis CostBasis `json:"costBasis"`

	// Total infrastructure cost for the window under the chosen cost basis.
	TotalCost float64 `json:"totalCost"`

	// Token counts from vLLM metrics.
	PromptTokens     float64 `json:"promptTokens"`
	GenerationTokens float64 `json:"generationTokens"`
	TotalTokens      float64 `json:"totalTokens"`

	// Blended cost per 1M delivered tokens (input + output together).
	CostPerMillionTokens float64 `json:"costPerMillionTokens"`

	// Input/output cost split. InputCost and OutputCost sum to TotalCost if the cost basis is allocation.
	InputCost  float64 `json:"inputCost"`
	OutputCost float64 `json:"outputCost"`

	// Per-million cost metrics for differentiated pricing.
	// InputCostPerMillionTokens uses PromptTokens as the denominator (all delivered
	// input tokens, including those served from KV cache).
	InputCostPerMillionTokens  float64 `json:"inputCostPerMillionTokens"`
	OutputCostPerMillionTokens float64 `json:"outputCostPerMillionTokens"`

	// CacheSavingsFraction is the fraction of prompt tokens served from the KV
	// cache (CachedTokens / PromptTokens, range 0–1). Zero when prefix caching is
	// disabled (see allocationMethod) or when no cache hits occurred in the window.
	CacheSavingsFraction float64 `json:"cacheSavingsFraction"`

	// cachedTokens is carried for aggregation recomputation of CacheSavingsFraction
	// and is not included in the JSON output.
	cachedTokens float64

	// AllocationMethod records which input/output cost-split path was used.
	// Informational; omitted when empty.
	AllocationMethod AllocationMethod `json:"allocationMethod,omitempty"`
}

// InferenceCostAPIProperties is the JSON-facing properties struct for API
// responses. It mirrors InferenceCostProperties but with explicit JSON tags
// matching the design doc field names.
type InferenceCostAPIProperties struct {
	ModelName      string `json:"modelName"`
	ModelVersion   string `json:"modelVersion,omitempty"`
	Namespace      string `json:"namespace"`
	Cluster        string `json:"cluster,omitempty"`
	Pod            string `json:"pod,omitempty"`
	Controller     string `json:"controller,omitempty"`
	ControllerKind string `json:"controllerKind,omitempty"`
	Container      string `json:"container,omitempty"`
}

// InferenceCostSet holds a collection of InferenceCostResponses for a single
// time window, keyed by aggregation key.
type InferenceCostSet struct {
	InferenceCosts map[string]*InferenceCostResponse `json:"inferenceCosts"`
	Window         opencost.Window                   `json:"window"`
}

// InferenceCostSetRange holds multiple InferenceCostSets covering a broader
// time range. Used for the /timeseries endpoint.
type InferenceCostSetRange struct {
	InferenceCostSets []*InferenceCostSet `json:"inferenceCostSets"`
	Window            opencost.Window     `json:"window"`
}

// newInferenceCostResponse projects a single InferenceCost into the flat
// per-basis API response type for the given window.
func newInferenceCostResponse(ic *InferenceCost, basis CostBasis, win opencost.Window) *InferenceCostResponse {
	var totalCost float64
	if basis == CostBasisUsage {
		totalCost = ic.UsageTotalCost
	} else {
		totalCost = ic.AllocationTotalCost
	}

	cpmt := ic.CostPerMillionTokens[basis]
	icpmt := ic.InputCostPerMillionTokens[basis]
	ocpmt := ic.OutputCostPerMillionTokens[basis]
	inputCost := ic.InputCost[basis]
	outputCost := ic.OutputCost[basis]

	return &InferenceCostResponse{
		Properties: InferenceCostAPIProperties{
			ModelName:      ic.Properties.ModelName,
			ModelVersion:   ic.Properties.ModelVersion,
			Namespace:      ic.Properties.Namespace,
			Cluster:        ic.Properties.Cluster,
			Pod:            ic.Properties.Pod,
			Controller:     ic.Properties.Controller,
			ControllerKind: ic.Properties.ControllerKind,
			Container:      ic.Properties.Container,
		},
		Window:                     win,
		CostBasis:                  basis,
		TotalCost:                  totalCost,
		PromptTokens:               ic.PromptTokens,
		GenerationTokens:           ic.GenerationTokens,
		TotalTokens:                ic.TotalTokens,
		CostPerMillionTokens:       cpmt,
		InputCost:                  inputCost,
		OutputCost:                 outputCost,
		InputCostPerMillionTokens:  icpmt,
		OutputCostPerMillionTokens: ocpmt,
		CacheSavingsFraction:       ic.CacheSavingsFraction,
		cachedTokens:               ic.CachedTokens,
		AllocationMethod:           ic.AllocationMethod,
	}
}

// newInferenceCostSet creates an empty InferenceCostSet for the given window.
func newInferenceCostSet(win opencost.Window) *InferenceCostSet {
	return &InferenceCostSet{
		InferenceCosts: make(map[string]*InferenceCostResponse),
		Window:         win,
	}
}
