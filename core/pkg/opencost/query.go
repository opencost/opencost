package opencost

import (
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
)

// Querier is an aggregate interface which has the ability to query each Kubecost store type
type Querier interface {
	AllocationQuerier
	SummaryAllocationQuerier
	AssetQuerier
}

// AllocationQuerier interface defining api for requesting Allocation data
type AllocationQuerier interface {
	QueryAllocation(start, end time.Time, opts *AllocationQueryOptions) (*AllocationSetRange, error)
}

// SummaryAllocationQuerier interface defining api for requesting SummaryAllocation data
type SummaryAllocationQuerier interface {
	QuerySummaryAllocation(start, end time.Time, opts *AllocationQueryOptions) (*SummaryAllocationSetRange, error)
}

// AssetQuerier interface defining api for requesting Asset data
type AssetQuerier interface {
	QueryAsset(start, end time.Time, opts *AssetQueryOptions) (*AssetSetRange, error)
}

// AllocationQueryOptions defines optional parameters for querying an Allocation Store
type AllocationQueryOptions struct {
	Accumulate        AccumulateOption
	AggregateBy       []string
	Compute           bool
	Filter            filter.Filter
	IdleByNode        bool
	IncludeExternal   bool
	IncludeIdle       bool
	LabelConfig       *LabelConfig
	MergeUnallocated  bool
	Reconcile         bool
	ReconcileNetwork  bool
	ShareFuncs        []AllocationMatchFunc
	SharedHourlyCosts map[string]float64
	ShareIdle         string
	ShareSplit        string
	ShareTenancyCosts bool
	SplitIdle         bool
	Step              time.Duration
}

type AccumulateOption string

const (
	AccumulateOptionNone    AccumulateOption = ""
	AccumulateOptionAll     AccumulateOption = "all"
	AccumulateOptionHour    AccumulateOption = "hour"
	AccumulateOptionDay     AccumulateOption = "day"
	AccumulateOptionWeek    AccumulateOption = "week"
	AccumulateOptionMonth   AccumulateOption = "month"
	AccumulateOptionQuarter AccumulateOption = "quarter"
)

// ParseAccumulate converts a string to an AccumulateOption
func ParseAccumulate(acc string) AccumulateOption {
	var opt AccumulateOption
	switch strings.ToLower(acc) {
	case "quarter":
		opt = AccumulateOptionQuarter
	case "month":
		opt = AccumulateOptionMonth
	case "week":
		opt = AccumulateOptionWeek
	case "day":
		opt = AccumulateOptionDay
	case "hour":
		opt = AccumulateOptionHour
	case "true":
		opt = AccumulateOptionAll
	default:
		opt = AccumulateOptionNone
	}
	return opt
}

// AssetQueryOptions defines optional parameters for querying an Asset Store
type AssetQueryOptions struct {
	Accumulate         bool
	AggregateBy        []string
	Compute            bool
	DisableAdjustments bool
	Filter             filter.Filter
	IncludeCloud       bool
	SharedHourlyCosts  map[string]float64
	Step               time.Duration
	LabelConfig        *LabelConfig
}

// QueryAllocationAsync provide a functions for retrieving results from any AllocationQuerier Asynchronously
func QueryAllocationAsync(allocationQuerier AllocationQuerier, start, end time.Time, opts *AllocationQueryOptions) (chan *AllocationSetRange, chan error) {
	asrCh := make(chan *AllocationSetRange)
	errCh := make(chan error)

	go func(asrCh chan *AllocationSetRange, errCh chan error) {
		defer close(asrCh)
		defer close(errCh)

		asr, err := allocationQuerier.QueryAllocation(start, end, opts)
		if err != nil {
			errCh <- err
			return
		}

		asrCh <- asr
	}(asrCh, errCh)

	return asrCh, errCh
}

// QuerySummaryAllocationAsync provide a functions for retrieving results from any SummaryAllocationQuerier Asynchronously
func QuerySummaryAllocationAsync(summaryAllocationQuerier SummaryAllocationQuerier, start, end time.Time, opts *AllocationQueryOptions) (chan *SummaryAllocationSetRange, chan error) {
	asrCh := make(chan *SummaryAllocationSetRange)
	errCh := make(chan error)

	go func(asrCh chan *SummaryAllocationSetRange, errCh chan error) {
		defer close(asrCh)
		defer close(errCh)

		asr, err := summaryAllocationQuerier.QuerySummaryAllocation(start, end, opts)
		if err != nil {
			errCh <- err
			return
		}

		asrCh <- asr
	}(asrCh, errCh)

	return asrCh, errCh
}

// QueryAsseetAsync provide a functions for retrieving results from any AssetQuerier Asynchronously
func QueryAssetAsync(assetQuerier AssetQuerier, start, end time.Time, opts *AssetQueryOptions) (chan *AssetSetRange, chan error) {
	asrCh := make(chan *AssetSetRange)
	errCh := make(chan error)

	go func(asrCh chan *AssetSetRange, errCh chan error) {
		defer close(asrCh)
		defer close(errCh)

		asr, err := assetQuerier.QueryAsset(start, end, opts)
		if err != nil {
			errCh <- err
			return
		}

		asrCh <- asr
	}(asrCh, errCh)

	return asrCh, errCh
}
