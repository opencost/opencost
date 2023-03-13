package kubecost

import (
	"math"
	"time"
)

// SummaryAllocationResponse is a sanitized version of SummaryAllocation, which
// formats fields and protects against issues like mashaling NaNs.
type SummaryAllocationResponse struct {
	Name                   string    `json:"name"`
	Start                  time.Time `json:"start"`
	End                    time.Time `json:"end"`
	CPUCoreRequestAverage  *float64  `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage    *float64  `json:"cpuCoreUsageAverage"`
	CPUCost                *float64  `json:"cpuCost"`
	GPUCost                *float64  `json:"gpuCost"`
	NetworkCost            *float64  `json:"networkCost"`
	LoadBalancerCost       *float64  `json:"loadBalancerCost"`
	PVCost                 *float64  `json:"pvCost"`
	RAMBytesRequestAverage *float64  `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage   *float64  `json:"ramByteUsageAverage"`
	RAMCost                *float64  `json:"ramCost"`
	SharedCost             *float64  `json:"sharedCost"`
	ExternalCost           *float64  `json:"externalCost"`
}

// ToResponse converts a SummaryAllocation to a SummaryAllocationResponse,
// protecting against NaN and null values.
func (sa *SummaryAllocation) ToResponse() *SummaryAllocationResponse {
	if sa == nil {
		return nil
	}

	return &SummaryAllocationResponse{
		Name:                   sa.Name,
		Start:                  sa.Start,
		End:                    sa.End,
		CPUCoreRequestAverage:  float64ToResponse(sa.CPUCoreRequestAverage),
		CPUCoreUsageAverage:    float64ToResponse(sa.CPUCoreUsageAverage),
		CPUCost:                float64ToResponse(sa.CPUCost),
		GPUCost:                float64ToResponse(sa.GPUCost),
		NetworkCost:            float64ToResponse(sa.NetworkCost),
		LoadBalancerCost:       float64ToResponse(sa.LoadBalancerCost),
		PVCost:                 float64ToResponse(sa.PVCost),
		RAMBytesRequestAverage: float64ToResponse(sa.RAMBytesRequestAverage),
		RAMBytesUsageAverage:   float64ToResponse(sa.RAMBytesUsageAverage),
		RAMCost:                float64ToResponse(sa.RAMCost),
		SharedCost:             float64ToResponse(sa.SharedCost),
		ExternalCost:           float64ToResponse(sa.ExternalCost),
	}
}

func float64ToResponse(f float64) *float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}

	return &f
}

// SummaryAllocationSetResponse is a sanitized version of SummaryAllocationSet,
// which formats fields and protects against issues like marshaling NaNs.
type SummaryAllocationSetResponse struct {
	SummaryAllocations map[string]*SummaryAllocationResponse `json:"allocations"`
	Window             Window                                `json:"window"`
}

// ToResponse converts a SummaryAllocationSet to a SummaryAllocationSetResponse,
// protecting against NaN and null values.
func (sas *SummaryAllocationSet) ToResponse() *SummaryAllocationSetResponse {
	if sas == nil {
		return nil
	}

	sars := make(map[string]*SummaryAllocationResponse, len(sas.SummaryAllocations))
	for k, v := range sas.SummaryAllocations {
		sars[k] = v.ToResponse()
	}

	return &SummaryAllocationSetResponse{
		SummaryAllocations: sars,
		Window:             sas.Window.Clone(),
	}
}

// SummaryAllocationSetRangeResponse is a sanitized version of SummaryAllocationSetRange,
// which formats fields and protects against issues like marshaling NaNs.
type SummaryAllocationSetRangeResponse struct {
	Step                  time.Duration                   `json:"step"`
	SummaryAllocationSets []*SummaryAllocationSetResponse `json:"sets"`
	Window                Window                          `json:"window"`
}

// ToResponse converts a SummaryAllocationSet to a SummaryAllocationSetResponse,
// protecting against NaN and null values.
func (sasr *SummaryAllocationSetRange) ToResponse() *SummaryAllocationSetRangeResponse {
	if sasr == nil {
		return nil
	}

	sasrr := make([]*SummaryAllocationSetResponse, len(sasr.SummaryAllocationSets))
	for i, v := range sasr.SummaryAllocationSets {
		sasrr[i] = v.ToResponse()
	}

	return &SummaryAllocationSetRangeResponse{
		Step:                  sasr.Step,
		SummaryAllocationSets: sasrr,
		Window:                sasr.Window.Clone(),
	}
}

func EmptySummaryAllocationSetRangeResponse() *SummaryAllocationSetRangeResponse {
	return &SummaryAllocationSetRangeResponse{}
}
