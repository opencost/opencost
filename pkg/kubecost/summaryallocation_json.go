package kubecost

import (
	"time"

	"github.com/opencost/opencost/pkg/util/formatutil"
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
	TotalEfficiency        *float64  `json:"totalEfficiency"`
	TotalCost              *float64  `json:"totalCost"`
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
		CPUCoreRequestAverage:  formatutil.Float64ToResponse(sa.CPUCoreRequestAverage),
		CPUCoreUsageAverage:    formatutil.Float64ToResponse(sa.CPUCoreUsageAverage),
		CPUCost:                formatutil.Float64ToResponse(sa.CPUCost),
		GPUCost:                formatutil.Float64ToResponse(sa.GPUCost),
		NetworkCost:            formatutil.Float64ToResponse(sa.NetworkCost),
		LoadBalancerCost:       formatutil.Float64ToResponse(sa.LoadBalancerCost),
		PVCost:                 formatutil.Float64ToResponse(sa.PVCost),
		RAMBytesRequestAverage: formatutil.Float64ToResponse(sa.RAMBytesRequestAverage),
		RAMBytesUsageAverage:   formatutil.Float64ToResponse(sa.RAMBytesUsageAverage),
		RAMCost:                formatutil.Float64ToResponse(sa.RAMCost),
		SharedCost:             formatutil.Float64ToResponse(sa.SharedCost),
		ExternalCost:           formatutil.Float64ToResponse(sa.ExternalCost),
		TotalEfficiency:        formatutil.Float64ToResponse(sa.TotalEfficiency()),
		TotalCost:              formatutil.Float64ToResponse(sa.TotalCost()),
	}
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
