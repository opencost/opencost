package kubecost

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
)

// SummaryAllocation summarizes Allocation, keeping only the fields necessary
// for providing a high-level view of identifying the Allocation (Name) over a
// defined period of time (Start, End) and inspecting per-resource costs
// (subtotal with adjustment), total cost, and efficiency.
//
// TODO remove:
// Diff: 25 primitive 4 structs => 15 primitive, 1 struct
//
type SummaryAllocation struct {
	Name                   string                `json:"name"`
	Properties             *AllocationProperties `json:"-"`
	Start                  time.Time             `json:"start"`
	End                    time.Time             `json:"end"`
	CPUCoreRequestAverage  float64               `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage    float64               `json:"cpuCoreUsageAverage"`
	CPUCost                float64               `json:"cpuCost"`
	GPUCost                float64               `json:"gpuCost"`
	NetworkCost            float64               `json:"networkCost"`
	LoadBalancerCost       float64               `json:"loadBalancerCost"`
	PVCost                 float64               `json:"pvCost"`
	RAMBytesRequestAverage float64               `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage   float64               `json:"ramByteUsageAverage"`
	RAMCost                float64               `json:"ramCost"`
	SharedCost             float64               `json:"sharedCost"`
	ExternalCost           float64               `json:"externalCost"`
	Share                  bool                  `json:"-"`
}

func NewSummaryAllocation(alloc *Allocation, reconcile, reconcileNetwork bool) *SummaryAllocation {
	if alloc == nil {
		return nil
	}

	// TODO evaluate performance penalties for "cloning" here

	sa := &SummaryAllocation{
		Name:                   alloc.Name,
		Properties:             alloc.Properties.Clone(), // TODO blerg
		Start:                  alloc.Start,
		End:                    alloc.End,
		CPUCoreRequestAverage:  alloc.CPUCoreRequestAverage,
		CPUCoreUsageAverage:    alloc.CPUCoreUsageAverage,
		CPUCost:                alloc.CPUCost + alloc.CPUCostAdjustment,
		GPUCost:                alloc.GPUCost + alloc.GPUCostAdjustment,
		NetworkCost:            alloc.NetworkCost + alloc.NetworkCostAdjustment,
		LoadBalancerCost:       alloc.LoadBalancerCost + alloc.LoadBalancerCostAdjustment,
		PVCost:                 alloc.PVCost() + alloc.PVCostAdjustment,
		RAMBytesRequestAverage: alloc.RAMBytesRequestAverage,
		RAMBytesUsageAverage:   alloc.RAMBytesUsageAverage,
		RAMCost:                alloc.RAMCost + alloc.RAMCostAdjustment,
		SharedCost:             alloc.SharedCost,
		ExternalCost:           alloc.ExternalCost,
	}

	// Revert adjustments if reconciliation is off. If only network
	// reconciliation is off, only revert network adjustment.
	if !reconcile {
		sa.CPUCost -= alloc.CPUCostAdjustment
		sa.GPUCost -= alloc.GPUCostAdjustment
		sa.NetworkCost -= alloc.NetworkCostAdjustment
		sa.LoadBalancerCost -= alloc.LoadBalancerCostAdjustment
		sa.PVCost -= alloc.PVCostAdjustment
		sa.RAMCost -= alloc.RAMCostAdjustment
	} else if !reconcileNetwork {
		sa.NetworkCost -= alloc.NetworkCostAdjustment
	}

	return sa
}

func (sa *SummaryAllocation) Add(that *SummaryAllocation) {
	if sa == nil {
		log.Warningf("SummaryAllocation.Add: trying to add a nil receiver")
		return
	}

	// TODO do we need to maintain this?
	// Once Added, a SummaryAllocation has no Properties
	sa.Properties = nil

	// Sum non-cumulative fields by turning them into cumulative, adding them,
	// and then converting them back into averages after minutes have been
	// combined (just below).
	cpuReqCoreMins := sa.CPUCoreRequestAverage * sa.Minutes()
	cpuReqCoreMins += that.CPUCoreRequestAverage * that.Minutes()

	cpuUseCoreMins := sa.CPUCoreUsageAverage * sa.Minutes()
	cpuUseCoreMins += that.CPUCoreUsageAverage * that.Minutes()

	ramReqByteMins := sa.RAMBytesRequestAverage * sa.Minutes()
	ramReqByteMins += that.RAMBytesRequestAverage * that.Minutes()

	ramUseByteMins := sa.RAMBytesUsageAverage * sa.Minutes()
	ramUseByteMins += that.RAMBytesUsageAverage * that.Minutes()

	// Expand Start and End to be the "max" of among the given Allocations
	if that.Start.Before(sa.Start) {
		sa.Start = that.Start
	}
	if that.End.After(sa.End) {
		sa.End = that.End
	}

	// Convert cumulative request and usage back into rates
	if sa.Minutes() > 0 {
		sa.CPUCoreRequestAverage = cpuReqCoreMins / sa.Minutes()
		sa.CPUCoreUsageAverage = cpuUseCoreMins / sa.Minutes()
		sa.RAMBytesRequestAverage = ramReqByteMins / sa.Minutes()
		sa.RAMBytesUsageAverage = ramUseByteMins / sa.Minutes()
	} else {
		sa.CPUCoreRequestAverage = 0.0
		sa.CPUCoreUsageAverage = 0.0
		sa.RAMBytesRequestAverage = 0.0
		sa.RAMBytesUsageAverage = 0.0
	}

	// Sum all cumulative cost fields
	sa.CPUCost += that.CPUCost
	sa.ExternalCost += that.ExternalCost
	sa.GPUCost += that.GPUCost
	sa.LoadBalancerCost += that.LoadBalancerCost
	sa.NetworkCost += that.NetworkCost
	sa.PVCost += that.PVCost
	sa.RAMCost += that.RAMCost
	sa.SharedCost += that.SharedCost
}

// CPUEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (sa *SummaryAllocation) CPUEfficiency() float64 {
	if sa.CPUCoreRequestAverage > 0 {
		return sa.CPUCoreUsageAverage / sa.CPUCoreRequestAverage
	}

	if sa.CPUCoreUsageAverage == 0.0 || sa.CPUCost == 0.0 {
		return 0.0
	}

	return 1.0
}

func (sa *SummaryAllocation) generateKey(aggregateBy []string, labelConfig *LabelConfig) string {
	if sa == nil {
		return ""
	}

	return sa.Properties.GenerateKey(aggregateBy, labelConfig)
}

// IsExternal is true if the given Allocation represents external costs.
func (sa *SummaryAllocation) IsExternal() bool {
	return strings.Contains(sa.Name, ExternalSuffix)
}

// IsIdle is true if the given Allocation represents idle costs.
func (sa *SummaryAllocation) IsIdle() bool {
	return strings.Contains(sa.Name, IdleSuffix)
}

// IsUnallocated is true if the given Allocation represents unallocated costs.
func (sa *SummaryAllocation) IsUnallocated() bool {
	return strings.Contains(sa.Name, UnallocatedSuffix)
}

// IsUnmounted is true if the given Allocation represents unmounted volume costs.
func (sa *SummaryAllocation) IsUnmounted() bool {
	return strings.Contains(sa.Name, UnmountedSuffix)
}

// Minutes returns the number of minutes the SummaryAllocation represents, as
// defined by the difference between the end and start times.
func (sa *SummaryAllocation) Minutes() float64 {
	return sa.End.Sub(sa.Start).Minutes()
}

// RAMEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (sa *SummaryAllocation) RAMEfficiency() float64 {
	if sa.RAMBytesRequestAverage > 0 {
		return sa.RAMBytesUsageAverage / sa.RAMBytesRequestAverage
	}

	if sa.RAMBytesUsageAverage == 0.0 || sa.RAMCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// TotalCost is the total cost of the SummaryAllocation
func (sa *SummaryAllocation) TotalCost() float64 {
	return sa.CPUCost + sa.GPUCost + sa.RAMCost + sa.PVCost + sa.NetworkCost + sa.LoadBalancerCost + sa.SharedCost + sa.ExternalCost
}

// TotalEfficiency is the cost-weighted average of CPU and RAM efficiency. If
// there is no cost at all, then efficiency is zero.
func (sa *SummaryAllocation) TotalEfficiency() float64 {
	if sa.RAMCost+sa.CPUCost > 0 {
		ramCostEff := sa.RAMEfficiency() * sa.RAMCost
		cpuCostEff := sa.CPUEfficiency() * sa.CPUCost
		return (ramCostEff + cpuCostEff) / (sa.CPUCost + sa.RAMCost)
	}

	return 0.0
}

// SummaryAllocationSet stores a set of SummaryAllocations, each with a unique
// name, that share a window. An AllocationSet is mutable, so treat it like a
// threadsafe map.
type SummaryAllocationSet struct {
	sync.RWMutex
	externalKeys       map[string]bool
	idleKeys           map[string]bool
	SummaryAllocations map[string]*SummaryAllocation `json:"allocations"`
	Window             Window                        `json:"window"`
}

func NewSummaryAllocationSet(as *AllocationSet, ffs, sfs []AllocationMatchFunc, reconcile, reconcileNetwork bool) *SummaryAllocationSet {
	if as == nil {
		return nil
	}

	// TODO comment in function
	var sasMap map[string]*SummaryAllocation
	if len(ffs) == 0 {
		// No filters, so make the map of summary allocations exactly the size
		// of the origin allocation set.
		sasMap = make(map[string]*SummaryAllocation, len(as.allocations))
	} else {
		// There are filters, so start with a standard map
		sasMap = make(map[string]*SummaryAllocation)
	}

	sas := &SummaryAllocationSet{
		SummaryAllocations: sasMap,
		Window:             as.Window.Clone(),
	}

	for _, alloc := range as.allocations {
		// First, detect if the allocation should be shared. If so, mark it as
		// such, insert it, and continue.
		shouldShare := false
		for _, sf := range sfs {
			if sf(alloc) {
				shouldShare = true
				break
			}
		}
		if shouldShare {
			sa := NewSummaryAllocation(alloc, reconcile, reconcileNetwork)
			sa.Share = true
			sas.Insert(sa)
			continue
		}

		// If the allocation does not pass any of the given filter functions,
		// do not insert it into the set.
		shouldFilter := false
		for _, ff := range ffs {
			if !ff(alloc) {
				shouldFilter = true
				break
			}
		}
		if shouldFilter {
			continue

		}

		err := sas.Insert(NewSummaryAllocation(alloc, reconcile, reconcileNetwork))
		if err != nil {
			log.Errorf("SummaryAllocation: error inserting summary of %s", alloc.Name)
		}
	}

	for key := range as.externalKeys {
		sas.externalKeys[key] = true
	}

	for key := range as.idleKeys {
		sas.idleKeys[key] = true
	}

	return sas
}

func (sas *SummaryAllocationSet) Add(that *SummaryAllocationSet) (*SummaryAllocationSet, error) {
	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return that, nil
	}

	if that == nil || len(that.SummaryAllocations) == 0 {
		return sas, nil
	}

	// Set start, end to min(start), max(end)
	start := *sas.Window.Start()
	end := *sas.Window.End()
	if that.Window.Start().Before(start) {
		start = *that.Window.Start()
	}
	if that.Window.End().After(end) {
		end = *that.Window.End()
	}

	acc := &SummaryAllocationSet{
		SummaryAllocations: make(map[string]*SummaryAllocation, len(sas.SummaryAllocations)),
		Window:             NewClosedWindow(start, end),
	}

	sas.RLock()
	defer sas.RUnlock()

	that.RLock()
	defer that.RUnlock()

	for _, alloc := range sas.SummaryAllocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	for _, alloc := range that.SummaryAllocations {
		err := acc.Insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// AggregateBy aggregates the Allocations in the given AllocationSet by the given
// AllocationProperty. This will only be legal if the AllocationSet is divisible by the
// given AllocationProperty; e.g. Containers can be divided by Namespace, but not vice-a-versa.
func (sas *SummaryAllocationSet) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	// The order of operations for aggregating allocations is as follows:
	//
	//  #. Partition external, idle, and shared allocations into separate sets.
	//     Also, create the aggSet into which the results will be aggregated.
	//
	//  #. Retrieve pre-computed allocation resource totals, which are to be
	//     used to compute idle coefficients, based on the ratio of any given
	//     allocation's per-resource cost to the allocated per-resource totals.
	//
	//  #. Distribute idle allocations according to the idle coefficients
	//
	//  #. Generate aggregation key and insert allocation into the output set
	//
	//  #. If idle is shared and other allocations are shared, it's probable
	//     that some amount of idle cost will be shared with a shared resource.
	//     Distribute that idle allocation, if it exists, to the respective
	//     shared allocations before sharing them with the aggregated
	//     allocations.
	//
	//  #. Apply idle filtration
	//
	//  #. Distribute shared allocations
	//
	//  #. Distribute shared tenancy costs
	//
	//  TODO
	//  #. If there are external allocations that can be aggregated into
	//     the output (i.e. they can be used to generate a valid key for
	//     the given properties) then aggregate; otherwise... ignore them?
	//
	//  #. If the merge idle option is enabled, merge any remaining idle
	//     allocations into a single idle allocation. If there was any idle
	//	   whose costs were not distributed because there was no usage of a
	//     specific resource type, re-add the idle to the aggregation with
	//     only that type.
	//
	//  TODO
	//  #. Distribute any undistributed idle, in the case that idle
	//     coefficients end up being zero and some idle is not shared.

	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return nil
	}

	if sas.Window.IsOpen() {
		return errors.New("cannot aggregate a SummaryAllocationSet with an open window")
	}

	if options == nil {
		options = &AllocationAggregationOptions{}
	}

	if options.LabelConfig == nil {
		options.LabelConfig = NewLabelConfig()
	}

	// Check if we have any work to do; if not, then early return. If
	// aggregateBy is nil, we don't aggregate anything. On the other hand,
	// an empty slice implies that we should aggregate everything. (See
	// generateKey for why that makes sense.)
	shouldAggregate := aggregateBy != nil
	shouldShare := len(options.SharedHourlyCosts) > 0 || len(options.ShareFuncs) > 0
	if !shouldAggregate && !shouldShare {
		return nil
	}

	// aggSet will collect the aggregated allocations
	aggSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// externalSet will collect external allocations
	externalSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// idleSet will be shared among aggSet after initial aggregation
	// is complete
	idleSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	// shareSet will be shared among aggSet after initial aggregation
	// is complete
	shareSet := &SummaryAllocationSet{
		Window: sas.Window.Clone(),
	}

	sas.Lock()
	defer sas.Unlock()

	// TODO comment
	sharedResourceTotals := map[string]*ResourceTotals{}
	totalUnmountedCost := 0.0

	// TODO comment
	for _, sa := range sas.SummaryAllocations {
		// Identify and separate shared allocations into their own set.
		if sa.Share {
			var key string
			if options.IdleByNode {
				key = sa.Properties.Node
			} else {
				key = sa.Properties.Cluster
			}

			if _, ok := sharedResourceTotals[key]; !ok {
				sharedResourceTotals[key] = &ResourceTotals{}
			}
			sharedResourceTotals[key].CPUCost += sa.CPUCost
			sharedResourceTotals[key].GPUCost += sa.GPUCost
			sharedResourceTotals[key].LoadBalancerCost += sa.LoadBalancerCost
			sharedResourceTotals[key].NetworkCost += sa.NetworkCost
			sharedResourceTotals[key].PersistentVolumeCost += sa.PVCost
			sharedResourceTotals[key].RAMCost += sa.RAMCost

			// TODO with shared tenancy costs, do we need to account for
			// shared cost here too?

			shareSet.Insert(sa)
			delete(sas.SummaryAllocations, sa.Name)

			continue
		}

		// External allocations get aggregated post-hoc (see step 6) and do
		// not necessarily contain complete sets of properties, so they are
		// moved to a separate AllocationSet.
		if sa.IsExternal() {
			delete(sas.externalKeys, sa.Name)
			delete(sas.SummaryAllocations, sa.Name)
			externalSet.Insert(sa)
			continue
		}

		// Idle allocations should be separated into idleSet if they are to be
		// shared later on. If they are not to be shared, then add them to the
		// aggSet like any other allocation.
		if sa.IsIdle() {
			delete(sas.idleKeys, sa.Name)
			delete(sas.SummaryAllocations, sa.Name)

			if options.ShareIdle == ShareEven || options.ShareIdle == ShareWeighted {
				idleSet.Insert(sa)
			} else {
				aggSet.Insert(sa)
			}

			continue
		}

		// Track total unmounted cost because it must be taken out of total
		// allocated costs for sharing coefficients.
		if sa.IsUnmounted() {
			totalUnmountedCost += sa.TotalCost()
		}
	}

	// TODO do we need to handle case where len(SummaryAllocations) == 0?

	// (#) Retrieve pre-computed allocation resource totals, which are to be
	// used to compute idle coefficients, based on the ratio of any given
	// allocation's per-resource cost to the allocated per-resource totals.
	var allocTotals map[string]*ResourceTotals
	if options.IdleByNode {
		if options.AllocationResourceTotalsStore != nil {
			allocTotals = options.AllocationResourceTotalsStore.GetResourceTotalsByNode(*sas.Window.Start(), *sas.Window.End())
			if allocTotals == nil {
				// TODO
				log.Warningf("SummaryAllocation: nil allocTotals by node for %s", sas.Window)
			}
		} else {
			// TODO ?
		}
	} else {
		if options.AllocationResourceTotalsStore != nil {
			allocTotals = options.AllocationResourceTotalsStore.GetResourceTotalsByCluster(*sas.Window.Start(), *sas.Window.End())
			if allocTotals == nil {
				// TODO
				log.Warningf("SummaryAllocation: nil allocTotals by cluster for %s", sas.Window)
			}
		} else {
			// TODO ?
		}
	}

	// Instantiate filtered totals map if filters have been applied and there
	// are un-shared idle allocations in the result set. These will be
	// populated in step (#) and used to "filter" out the correct proportion
	// of idle costs, given that non-idle allocations have been filtered.
	var filteredTotals map[string]*ResourceTotals
	if len(aggSet.idleKeys) > 0 && len(options.FilterFuncs) > 0 {
		filteredTotals = make(map[string]*ResourceTotals, len(aggSet.idleKeys))

		// If costs are shared, record those resource totals here so that idle
		// for the shared resources gets included.
		for key, rt := range sharedResourceTotals {
			if _, ok := filteredTotals[key]; !ok {
				filteredTotals[key] = &ResourceTotals{}
			}

			// TODO do we need PV, Network, LoadBalancer here?

			filteredTotals[key].CPUCost += rt.CPUCost
			filteredTotals[key].GPUCost += rt.GPUCost
			filteredTotals[key].RAMCost += rt.RAMCost
		}
	}

	sharingCoeffs := map[string]float64{}

	// (#) Distribute idle cost, if sharing
	// (#) Distribute tenancy costs, if sharing
	// (#) Aggregate
	// TODO better comment
	for _, sa := range sas.SummaryAllocations {
		// Generate key to use for aggregation-by-key and allocation name
		key := sa.generateKey(aggregateBy, options.LabelConfig)

		// TODO comment
		// Do this before adding idle cost
		if options.ShareSplit == ShareEven {
			sharingCoeffs[key] += 1.0
		} else {
			sharingCoeffs[key] += sa.TotalCost()
		}

		// Distribute idle allocations according to the idle coefficients
		// NOTE: if idle allocation is off (i.e. ShareIdle == ShareNone) then
		// all idle allocations will be in the aggSet at this point, so idleSet
		// will be empty and we won't enter this block.
		if len(idleSet.SummaryAllocations) > 0 {
			// Distribute idle allocations by coefficient per-idleId, per-allocation
			for _, idle := range idleSet.SummaryAllocations {
				var key string

				// Only share idle allocation with current allocation (sa) if
				// the relevant property matches (i.e. Cluster or Node,
				// depending on which idle sharing option is selected)
				if options.IdleByNode {
					if idle.Properties.Node != sa.Properties.Node {
						continue
					}

					key = idle.Properties.Node
				} else {
					if idle.Properties.Cluster != sa.Properties.Cluster {
						continue
					}

					key = idle.Properties.Cluster
				}

				cpuCoeff, gpuCoeff, ramCoeff := ComputeIdleCoefficients(options.ShareIdle, key, sa.CPUCost, sa.GPUCost, sa.RAMCost, allocTotals)

				sa.CPUCost += idle.CPUCost * cpuCoeff
				sa.GPUCost += idle.GPUCost * gpuCoeff
				sa.RAMCost += idle.RAMCost * ramCoeff
			}
		}

		// The key becomes the allocation's name, which is used as the key by
		// which the allocation is inserted into the set.
		sa.Name = key

		// If merging unallocated allocations, rename all unallocated
		// allocations as simply __unallocated__
		if options.MergeUnallocated && sa.IsUnallocated() {
			sa.Name = UnallocatedSuffix
		}

		// TODO do we need to be going off of "cluster/node" for "node" in all these places?

		// Record filtered resource totals for idle allocation filtration, if
		// necessary
		if filteredTotals != nil {
			var key string
			if options.IdleByNode {
				key = sa.Properties.Node
			} else {
				key = sa.Properties.Cluster
			}

			if _, ok := filteredTotals[key]; ok {
				filteredTotals[key].CPUCost += sa.CPUCost
				filteredTotals[key].GPUCost += sa.GPUCost
				filteredTotals[key].RAMCost += sa.RAMCost
			} else {
				filteredTotals[key] = &ResourceTotals{
					CPUCost: sa.CPUCost,
					GPUCost: sa.GPUCost,
					RAMCost: sa.RAMCost,
				}
			}
		}

		// Inserting the allocation with the generated key for a name will
		// perform the actual aggregation step.
		aggSet.Insert(sa)
	}

	// (#) If idle is shared and other allocations are shared, it's probable
	// that some amount of idle cost will be shared with a shared resource.
	// Distribute that idle allocation, if it exists, to the respective shared
	// allocations before sharing them with the aggregated allocations.
	if len(idleSet.SummaryAllocations) > 0 && len(shareSet.SummaryAllocations) > 0 {
		for _, sa := range shareSet.SummaryAllocations {
			for _, idle := range idleSet.SummaryAllocations {
				var key string

				// Only share idle allocation with current allocation (sa) if
				// the relevant property matches (i.e. Cluster or Node,
				// depending on which idle sharing option is selected)
				if options.IdleByNode {
					if idle.Properties.Node != sa.Properties.Node {
						continue
					}

					key = idle.Properties.Node
				} else {
					if idle.Properties.Cluster != sa.Properties.Cluster {
						continue
					}

					key = idle.Properties.Cluster
				}

				cpuCoeff, gpuCoeff, ramCoeff := ComputeIdleCoefficients(options.ShareIdle, key, sa.CPUCost, sa.GPUCost, sa.RAMCost, allocTotals)

				sa.CPUCost += idle.CPUCost * cpuCoeff
				sa.GPUCost += idle.GPUCost * gpuCoeff
				sa.RAMCost += idle.RAMCost * ramCoeff
			}
		}
	}

	// (#) Apply idle filtration
	if filteredTotals != nil {
		for idleKey := range aggSet.idleKeys {
			ia := aggSet.SummaryAllocations[idleKey]

			var key string
			if options.IdleByNode {
				key = ia.Properties.Node
			} else {
				key = ia.Properties.Cluster
			}

			// Percentage of idle that should remain after filters are applied,
			// which equals the proportion of filtered-to-actual cost.
			cpuFilterCoeff := 0.0
			if allocTotals[key].CPUCost > 0.0 {
				cpuFilterCoeff = filteredTotals[key].CPUCost / allocTotals[key].CPUCost
			}

			gpuFilterCoeff := 0.0
			if allocTotals[key].RAMCost > 0.0 {
				gpuFilterCoeff = filteredTotals[key].RAMCost / allocTotals[key].RAMCost
			}

			ramFilterCoeff := 0.0

			if allocTotals[key].RAMCost > 0.0 {
				ramFilterCoeff = filteredTotals[key].RAMCost / allocTotals[key].RAMCost
			}

			ia.CPUCost *= cpuFilterCoeff
			ia.GPUCost *= gpuFilterCoeff
			ia.RAMCost *= ramFilterCoeff
		}
	}

	// (#) Convert shared hourly cost into a cumulative allocation to share
	for name, cost := range options.SharedHourlyCosts {
		if cost > 0.0 {
			hours := sas.Window.Hours()

			// If set ends in the future, adjust hours accordingly
			diff := time.Since(*sas.Window.End())
			if diff < 0.0 {
				hours += diff.Hours()
			}

			totalSharedCost := cost * hours

			shareSet.Insert(&SummaryAllocation{
				Name:       fmt.Sprintf("%s/%s", name, SharedSuffix),
				Start:      *sas.Window.Start(),
				End:        *sas.Window.End(),
				SharedCost: totalSharedCost,
			})
		}
	}

	// (#) Distribute shared resources.
	if len(shareSet.SummaryAllocations) > 0 {
		// TODO deprecate ShareEven or accept new definition?
		// TODO consider sharing by Cluster or Node?

		sharingCoeffDenominator := 0.0
		for _, rt := range allocTotals {
			if options.ShareSplit == ShareEven {
				sharingCoeffDenominator += float64(rt.Count)
			} else {
				sharingCoeffDenominator += rt.TotalCost()
			}
		}

		// Do not include the shared costs, themselves, when determining
		// sharing coefficients.
		for _, rt := range sharedResourceTotals {
			if options.ShareSplit == ShareEven {
				sharingCoeffDenominator -= float64(rt.Count)
			} else {
				sharingCoeffDenominator -= rt.TotalCost()
			}
		}

		// Do not include the unmounted costs when determining sharing
		// coefficients becuase they do not receive shared costs.
		if options.ShareSplit == ShareEven {
			if totalUnmountedCost > 0.0 {
				sharingCoeffDenominator -= 1.0
			}
		} else {
			sharingCoeffDenominator -= totalUnmountedCost
		}

		// Compute sharing coeffs by dividing the thus-far accumulated
		// numerators by the now-finalized denominator.
		for key := range sharingCoeffs {
			sharingCoeffs[key] /= sharingCoeffDenominator
		}

		if sharingCoeffDenominator == 0.0 {
			log.Warningf("SummaryAllocation: sharing coefficient denominator is 0.0")
		} else {
			for key, sa := range aggSet.SummaryAllocations {
				// Idle and unmounted allocations, by definition, do not
				// receive shared cost
				if sa.IsIdle() || sa.IsUnmounted() {
					continue
				}

				sharingCoeff := sharingCoeffs[key]

				// Distribute each shared cost with the current allocation on the
				// basis of the proportion of the allocation's cost (ShareWeighted)
				// or count (ShareEven) to the total aggregated cost or count. This
				// condition should hold in spite of filters because the sharing
				// coefficient denominator is held constant by pre-computed
				// resource totals and the post-aggregation total cost of the
				// remaining allocations will, by definition, not be affected.
				for _, shared := range shareSet.SummaryAllocations {
					sa.SharedCost += shared.TotalCost() * sharingCoeff
				}
			}
		}
	}

	// TODO
	// (#) External allocations
	for _, sa := range externalSet.SummaryAllocations {
		skip := false

		// TODO deal with filters...
		// for _, ff := range options.FilterFuncs {
		// 	if !ff(sa) {
		// 		skip = true
		// 		break
		// 	}
		// }

		if !skip {
			key := sa.generateKey(aggregateBy, options.LabelConfig)

			sa.Name = key
			aggSet.Insert(sa)
		}
	}

	// (#) Combine all idle allocations into a single "__idle__" allocation
	if !options.SplitIdle {
		for _, idleAlloc := range aggSet.IdleAllocations() {
			aggSet.Delete(idleAlloc.Name)
			idleAlloc.Name = IdleSuffix
			aggSet.Insert(idleAlloc)
		}
	}

	// TODO
	// (#) Distribute remaining, undistributed idle

	// Replace the existing set's data with the new, aggregated summary data
	sas.SummaryAllocations = aggSet.SummaryAllocations

	return nil
}

func (sas *SummaryAllocationSet) InsertIdleSummaryAllocations(rts map[string]*ResourceTotals, prop AssetProperty) error {
	if sas == nil {
		return errors.New("cannot compute idle allocation for nil SummaryAllocationSet")
	}

	if len(rts) == 0 {
		return nil
	}

	// TODO argh avoid copy? Not the worst thing at this size... O(clusters) or O(nodes)
	idleTotals := make(map[string]*ResourceTotals, len(rts))
	for key, rt := range rts {
		idleTotals[key] = &ResourceTotals{
			Start:   rt.Start,
			End:     rt.End,
			CPUCost: rt.CPUCost,
			GPUCost: rt.GPUCost,
			RAMCost: rt.RAMCost,
		}
	}

	// Subtract allocated costs from resource totals, leaving only the remaining
	// idle totals for each key (cluster or node).
	sas.Each(func(name string, sa *SummaryAllocation) {
		key := sa.Properties.Cluster
		if prop == AssetNodeProp {
			key = sa.Properties.Node
		}

		if _, ok := idleTotals[key]; !ok {
			// Failed to find totals for the allocation's cluster or node.
			// (Should never happen.)
			log.Warningf("InsertIdleSummaryAllocations: failed to find %s: %s", prop, key)
			return
		}

		idleTotals[key].CPUCost -= sa.CPUCost
		idleTotals[key].GPUCost -= sa.GPUCost
		idleTotals[key].RAMCost -= sa.RAMCost
	})

	// Turn remaining idle totals into idle allocations and insert them.
	for key, rt := range idleTotals {
		idleAlloc := &SummaryAllocation{
			Name: fmt.Sprintf("%s/%s", key, IdleSuffix),
			Properties: &AllocationProperties{
				Cluster: rt.Cluster,
				Node:    rt.Node,
			},
			Start:   rt.Start,
			End:     rt.End,
			CPUCost: rt.CPUCost,
			GPUCost: rt.GPUCost,
			RAMCost: rt.RAMCost,
		}

		sas.Insert(idleAlloc)
	}

	return nil
}

// Delete removes the allocation with the given name from the set
func (sas *SummaryAllocationSet) Delete(name string) {
	if sas == nil {
		return
	}

	sas.Lock()
	defer sas.Unlock()

	delete(sas.externalKeys, name)
	delete(sas.idleKeys, name)
	delete(sas.SummaryAllocations, name)
}

// Each invokes the given function for each SummaryAllocation in the set
func (sas *SummaryAllocationSet) Each(f func(string, *SummaryAllocation)) {
	if sas == nil {
		return
	}

	for k, a := range sas.SummaryAllocations {
		f(k, a)
	}
}

// IdleAllocations returns a map of the idle allocations in the AllocationSet.
func (sas *SummaryAllocationSet) IdleAllocations() map[string]*SummaryAllocation {
	idles := map[string]*SummaryAllocation{}

	if sas == nil || len(sas.SummaryAllocations) == 0 {
		return idles
	}

	sas.RLock()
	defer sas.RUnlock()

	for key := range sas.idleKeys {
		if sa, ok := sas.SummaryAllocations[key]; ok {
			idles[key] = sa // TODO Clone()?
		}
	}

	return idles
}

// Insert aggregates the current entry in the SummaryAllocationSet by the given Allocation,
// but only if the Allocation is valid, i.e. matches the SummaryAllocationSet's window. If
// there is no existing entry, one is created. Nil error response indicates success.
func (sas *SummaryAllocationSet) Insert(sa *SummaryAllocation) error {
	if sas == nil {
		return fmt.Errorf("cannot insert into nil SummaryAllocationSet")
	}

	sas.Lock()
	defer sas.Unlock()

	if sas.SummaryAllocations == nil {
		sas.SummaryAllocations = map[string]*SummaryAllocation{}
	}

	if sas.externalKeys == nil {
		sas.externalKeys = map[string]bool{}
	}

	if sas.idleKeys == nil {
		sas.idleKeys = map[string]bool{}
	}

	// Add the given Allocation to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := sas.SummaryAllocations[sa.Name]; ok {
		sas.SummaryAllocations[sa.Name].Add(sa)
	} else {
		sas.SummaryAllocations[sa.Name] = sa
	}

	// If the given Allocation is an external one, record that
	if sa.IsExternal() {
		sas.externalKeys[sa.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if sa.IsIdle() {
		sas.idleKeys[sa.Name] = true
	}

	return nil
}

// SummaryAllocationSetRange is a thread-safe slice of SummaryAllocationSets.
type SummaryAllocationSetRange struct {
	sync.RWMutex
	Step                  time.Duration           `json:"step"`
	SummaryAllocationSets []*SummaryAllocationSet `json:"sets"`
	Window                Window                  `json:"window"`
}

// NewSummaryAllocationSetRange instantiates a new range composed of the given
// SummaryAllocationSets in the order provided.
func NewSummaryAllocationSetRange(sass ...*SummaryAllocationSet) *SummaryAllocationSetRange {
	var step time.Duration
	window := NewWindow(nil, nil)

	for _, sas := range sass {
		if window.Start() == nil || (sas.Window.Start() != nil && sas.Window.Start().Before(*window.Start())) {
			window.start = sas.Window.Start()
		}
		if window.End() == nil || (sas.Window.End() != nil && sas.Window.End().After(*window.End())) {
			window.end = sas.Window.End()
		}
		if step == 0 {
			step = sas.Window.Duration()
		} else if step != sas.Window.Duration() {
			log.Warningf("instantiating range with step %s using set of step %s is illegal", step, sas.Window.Duration())
		}
	}

	return &SummaryAllocationSetRange{
		Step:                  step,
		SummaryAllocationSets: sass,
		Window:                window,
	}
}

// Accumulate sums each AllocationSet in the given range, returning a single cumulative
// AllocationSet for the entire range.
func (sasr *SummaryAllocationSetRange) Accumulate() (*SummaryAllocationSet, error) {
	var result *SummaryAllocationSet
	var err error

	sasr.RLock()
	defer sasr.RUnlock()

	for _, sas := range sasr.SummaryAllocationSets {
		result, err = result.Add(sas)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// AggregateBy aggregates each AllocationSet in the range by the given
// properties and options.
func (sasr *SummaryAllocationSetRange) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	sasr.Lock()
	defer sasr.Unlock()

	for _, sas := range sasr.SummaryAllocationSets {
		err := sas.AggregateBy(aggregateBy, options)
		if err != nil {
			return err
		}
	}

	return nil
}

// Append appends the given AllocationSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (sasr *SummaryAllocationSetRange) Append(sas *SummaryAllocationSet) error {
	if sasr.Step != 0 && sas.Window.Duration() != sasr.Step {
		return fmt.Errorf("cannot append set with duration %s to range of step %s", sas.Window.Duration(), sasr.Step)
	}

	sasr.Lock()
	defer sasr.Unlock()

	// Append to list of sets
	sasr.SummaryAllocationSets = append(sasr.SummaryAllocationSets, sas)

	// Set step, if not set
	if sasr.Step == 0 {
		sasr.Step = sas.Window.Duration()
	}

	// Adjust window
	if sasr.Window.Start() == nil || (sas.Window.Start() != nil && sas.Window.Start().Before(*sasr.Window.Start())) {
		sasr.Window.start = sas.Window.Start()
	}
	if sasr.Window.End() == nil || (sas.Window.End() != nil && sas.Window.End().After(*sasr.Window.End())) {
		sasr.Window.end = sas.Window.End()
	}

	return nil
}

// Each invokes the given function for each AllocationSet in the range
func (sasr *SummaryAllocationSetRange) Each(f func(int, *SummaryAllocationSet)) {
	if sasr == nil {
		return
	}

	for i, as := range sasr.SummaryAllocationSets {
		f(i, as)
	}
}

// TODO this stinks. Can we do better with external cost so that we can remove?
func (sasr *SummaryAllocationSetRange) InsertExternalAllocations(that *AllocationSetRange) error {
	if sasr == nil {
		return fmt.Errorf("cannot insert range into nil AllocationSetRange")
	}

	// keys maps window to index in asr
	keys := map[string]int{}
	for i, as := range sasr.SummaryAllocationSets {
		if as == nil {
			continue
		}
		keys[as.Window.String()] = i
	}

	// Nothing to merge, so simply return
	if len(keys) == 0 {
		return nil
	}

	var err error
	that.Each(func(j int, thatAS *AllocationSet) {
		if thatAS == nil || err != nil {
			return
		}

		// Find matching AllocationSet in asr
		i, ok := keys[thatAS.Window.String()]
		if !ok {
			err = fmt.Errorf("cannot merge AllocationSet into window that does not exist: %s", thatAS.Window.String())
			return
		}
		sas := sasr.SummaryAllocationSets[i]

		// Insert each Allocation from the given set
		thatAS.Each(func(k string, alloc *Allocation) {
			externalSA := NewSummaryAllocation(alloc, true, true)
			err = sas.Insert(externalSA)
			if err != nil {
				// TODO ?
				log.Errorf("SummaryAllocation: error inserting %s: %s", k, err)
			}
		})
	})

	// err might be nil
	return err
}

// TODO Custom MarshalJSON and UnmarshalJSON for these?
// - Step is uintelligible (microseconds??)
// - TotalCost would be nice
