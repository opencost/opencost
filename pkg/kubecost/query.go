package kubecost

import "time"

type Querier interface {
	AllocationQuerier
	SummaryAllocationQuerier
}

type AllocationQuerier interface {
	QueryAllocation(start, end time.Time, opts *AllocationQueryOptions) (chan *AllocationSetRange, chan error)
	QueryAllocationSync(start, end time.Time, opts *AllocationQueryOptions) (*AllocationSetRange, error)
}

type SummaryAllocationQuerier interface {
	QuerySummaryAllocation(start, end time.Time, opts *AllocationQueryOptions) (chan *SummaryAllocationSetRange, chan error)
	QuerySummaryAllocationSync(start, end time.Time, opts *AllocationQueryOptions) (*SummaryAllocationSetRange, error)
}

type AllocationQueryOptions struct {
	Accumulate        bool
	AccumulateBy      time.Duration
	AggregateBy       []string
	Compute           bool
	FilterFuncs       []AllocationMatchFunc
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
