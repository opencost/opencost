package kubecost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"
)

// TODO Clean-up use of IsEmpty; nil checks should be separated for safety.

// TODO Consider making Allocation an interface, which is fulfilled by structs
// like KubernetesAllocation, IdleAllocation, and ExternalAllocation.

// ExternalSuffix indicates an external allocation
const ExternalSuffix = "__external__"

// IdleSuffix indicates an idle allocation property
const IdleSuffix = "__idle__"

// SharedSuffix indicates an shared allocation property
const SharedSuffix = "__shared__"

// UnallocatedSuffix indicates an unallocated allocation property
const UnallocatedSuffix = "__unallocated__"

// UnmountedSuffix indicated allocation to an unmounted PV
const UnmountedSuffix = "__unmounted__"

// ShareWeighted indicates that a shared resource should be shared as a
// proportion of the cost of the remaining allocations.
const ShareWeighted = "__weighted__"

// ShareEven indicates that a shared resource should be shared evenly across
// all remaining allocations.
const ShareEven = "__even__"

// ShareNone indicates that a shareable resource should not be shared
const ShareNone = "__none__"

// Allocation is a unit of resource allocation and cost for a given window
// of time and for a given kubernetes construct with its associated set of
// properties.
// TODO:CLEANUP make TotalCost a function
type Allocation struct {
	Name                   string     `json:"name"`
	Properties             Properties `json:"properties,omitempty"`
	Window                 Window     `json:"window"`
	Start                  time.Time  `json:"start"`
	End                    time.Time  `json:"end"`
	CPUCoreHours           float64    `json:"cpuCoreHours"`
	CPUCoreRequestAverage  float64    `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage    float64    `json:"cpuCoreUsageAverage"`
	CPUCost                float64    `json:"cpuCost"`
	GPUHours               float64    `json:"gpuHours"`
	GPUCost                float64    `json:"gpuCost"`
	NetworkCost            float64    `json:"networkCost"`
	PVByteHours            float64    `json:"pvByteHours"`
	PVCost                 float64    `json:"pvCost"`
	RAMByteHours           float64    `json:"ramByteHours"`
	RAMBytesRequestAverage float64    `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage   float64    `json:"ramByteUsageAverage"`
	RAMCost                float64    `json:"ramCost"`
	SharedCost             float64    `json:"sharedCost"`
	ExternalCost           float64    `json:"externalCost"`
	// TotalCost              float64    `json:"totalCost"`
}

// AllocationMatchFunc is a function that can be used to match Allocations by
// returning true for any given Allocation if a condition is met.
type AllocationMatchFunc func(*Allocation) bool

// Add returns the result of summing the two given Allocations, which sums the
// summary fields (e.g. costs, resources) and recomputes efficiency. Neither of
// the two original Allocations are mutated in the process.
func (a *Allocation) Add(that *Allocation) (*Allocation, error) {
	if a == nil {
		return that.Clone(), nil
	}

	if that == nil {
		return a.Clone(), nil
	}

	// Note: no need to clone "that", as add only mutates the receiver
	agg := a.Clone()
	agg.add(that)

	return agg, nil
}

// Clone returns a deep copy of the given Allocation
func (a *Allocation) Clone() *Allocation {
	if a == nil {
		return nil
	}

	return &Allocation{
		Name:                   a.Name,
		Properties:             a.Properties.Clone(),
		Window:                 a.Window.Clone(),
		Start:                  a.Start,
		End:                    a.End,
		CPUCoreHours:           a.CPUCoreHours,
		CPUCoreRequestAverage:  a.CPUCoreRequestAverage,
		CPUCoreUsageAverage:    a.CPUCoreUsageAverage,
		CPUCost:                a.CPUCost,
		GPUHours:               a.GPUHours,
		GPUCost:                a.GPUCost,
		NetworkCost:            a.NetworkCost,
		PVByteHours:            a.PVByteHours,
		PVCost:                 a.PVCost,
		RAMByteHours:           a.RAMByteHours,
		RAMBytesRequestAverage: a.RAMBytesRequestAverage,
		RAMBytesUsageAverage:   a.RAMBytesUsageAverage,
		RAMCost:                a.RAMCost,
		SharedCost:             a.SharedCost,
		ExternalCost:           a.ExternalCost,
	}
}

// Equal returns true if the values held in the given Allocation precisely
// match those of the receiving Allocation. nil does not match nil.
func (a *Allocation) Equal(that *Allocation) bool {
	if a == nil || that == nil {
		return false
	}

	if a.Name != that.Name {
		return false
	}
	if !a.Properties.Equal(&that.Properties) {
		return false
	}
	if !a.Window.Equal(that.Window) {
		return false
	}
	if !a.Start.Equal(that.Start) {
		return false
	}
	if !a.End.Equal(that.End) {
		return false
	}
	if !util.IsApproximately(a.CPUCoreHours, that.CPUCoreHours) {
		return false
	}
	if !util.IsApproximately(a.CPUCost, that.CPUCost) {
		return false
	}
	if !util.IsApproximately(a.GPUHours, that.GPUHours) {
		return false
	}
	if !util.IsApproximately(a.GPUCost, that.GPUCost) {
		return false
	}
	if !util.IsApproximately(a.NetworkCost, that.NetworkCost) {
		return false
	}
	if !util.IsApproximately(a.PVByteHours, that.PVByteHours) {
		return false
	}
	if !util.IsApproximately(a.PVCost, that.PVCost) {
		return false
	}
	if !util.IsApproximately(a.RAMByteHours, that.RAMByteHours) {
		return false
	}
	if !util.IsApproximately(a.RAMCost, that.RAMCost) {
		return false
	}
	if !util.IsApproximately(a.SharedCost, that.SharedCost) {
		return false
	}
	if !util.IsApproximately(a.ExternalCost, that.ExternalCost) {
		return false
	}

	return true
}

// TotalCost is the total cost of the Allocation
func (a *Allocation) TotalCost() float64 {
	return a.CPUCost + a.GPUCost + a.RAMCost + a.PVCost + a.NetworkCost + a.SharedCost + a.ExternalCost
}

// CPUEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (a *Allocation) CPUEfficiency() float64 {
	if a.CPUCoreRequestAverage > 0 {
		return a.CPUCoreUsageAverage / a.CPUCoreRequestAverage
	}

	if a.CPUCoreUsageAverage == 0.0 || a.CPUCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// RAMEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (a *Allocation) RAMEfficiency() float64 {
	if a.RAMBytesRequestAverage > 0 {
		return a.RAMBytesUsageAverage / a.RAMBytesRequestAverage
	}

	if a.RAMBytesUsageAverage == 0.0 || a.RAMCost == 0.0 {
		return 0.0
	}

	return 1.0
}

// TotalEfficiency is the cost-weighted average of CPU and RAM efficiency. If
// there is no cost at all, then efficiency is zero.
func (a *Allocation) TotalEfficiency() float64 {
	if a.CPUCost+a.RAMCost > 0 {
		ramCostEff := a.RAMEfficiency() * a.RAMCost
		cpuCostEff := a.CPUEfficiency() * a.CPUCost
		return (ramCostEff + cpuCostEff) / (a.CPUCost + a.RAMCost)
	}

	return 0.0
}

// CPUCores converts the Allocation's CPUCoreHours into average CPUCores
func (a *Allocation) CPUCores() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.CPUCoreHours / (a.Minutes() / 60.0)
}

// RAMBytes converts the Allocation's RAMByteHours into average RAMBytes
func (a *Allocation) RAMBytes() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.RAMByteHours / (a.Minutes() / 60.0)
}

// PVBytes converts the Allocation's PVByteHours into average PVBytes
func (a *Allocation) PVBytes() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.PVByteHours / (a.Minutes() / 60.0)
}

// MarshalJSON implements json.Marshal interface
func (a *Allocation) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "name", a.Name, ",")
	jsonEncode(buffer, "properties", a.Properties, ",")
	jsonEncode(buffer, "window", a.Window, ",")
	jsonEncodeString(buffer, "start", a.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", a.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", a.Minutes(), ",")
	jsonEncodeFloat64(buffer, "cpuCores", a.CPUCores(), ",")
	jsonEncodeFloat64(buffer, "cpuCoreRequestAverage", a.CPUCoreRequestAverage, ",")
	jsonEncodeFloat64(buffer, "cpuCoreUsageAverage", a.CPUCoreUsageAverage, ",")
	jsonEncodeFloat64(buffer, "cpuCoreHours", a.CPUCoreHours, ",")
	jsonEncodeFloat64(buffer, "cpuCost", a.CPUCost, ",")
	jsonEncodeFloat64(buffer, "cpuEfficiency", a.CPUEfficiency(), ",")
	jsonEncodeFloat64(buffer, "gpuHours", a.GPUHours, ",")
	jsonEncodeFloat64(buffer, "gpuCost", a.GPUCost, ",")
	jsonEncodeFloat64(buffer, "networkCost", a.NetworkCost, ",")
	jsonEncodeFloat64(buffer, "pvBytes", a.PVBytes(), ",")
	jsonEncodeFloat64(buffer, "pvByteHours", a.PVByteHours, ",")
	jsonEncodeFloat64(buffer, "pvCost", a.PVCost, ",")
	jsonEncodeFloat64(buffer, "ramBytes", a.RAMBytes(), ",")
	jsonEncodeFloat64(buffer, "ramByteRequestAverage", a.RAMBytesRequestAverage, ",")
	jsonEncodeFloat64(buffer, "ramByteUsageAverage", a.RAMBytesUsageAverage, ",")
	jsonEncodeFloat64(buffer, "ramByteHours", a.RAMByteHours, ",")
	jsonEncodeFloat64(buffer, "ramCost", a.RAMCost, ",")
	jsonEncodeFloat64(buffer, "ramEfficiency", a.RAMEfficiency(), ",")
	jsonEncodeFloat64(buffer, "sharedCost", a.SharedCost, ",")
	jsonEncodeFloat64(buffer, "externalCost", a.ExternalCost, ",")
	jsonEncodeFloat64(buffer, "totalCost", a.TotalCost(), ",")
	jsonEncodeFloat64(buffer, "totalEfficiency", a.TotalEfficiency(), "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// Resolution returns the duration of time covered by the Allocation
func (a *Allocation) Resolution() time.Duration {
	return a.End.Sub(a.Start)
}

// IsAggregated is true if the given Allocation has been aggregated, which we
// define by a lack of Properties.
func (a *Allocation) IsAggregated() bool {
	return a == nil || a.Properties == nil
}

// IsExternal is true if the given Allocation represents external costs.
func (a *Allocation) IsExternal() bool {
	return strings.Contains(a.Name, ExternalSuffix)
}

// IsIdle is true if the given Allocation represents idle costs.
func (a *Allocation) IsIdle() bool {
	return strings.Contains(a.Name, IdleSuffix)
}

// IsUnallocated is true if the given Allocation represents unallocated costs.
func (a *Allocation) IsUnallocated() bool {
	return strings.Contains(a.Name, UnallocatedSuffix)
}

// Minutes returns the number of minutes the Allocation represents, as defined
// by the difference between the end and start times.
func (a *Allocation) Minutes() float64 {
	return a.End.Sub(a.Start).Minutes()
}

// Share works like Add, but converts the entire cost of the given Allocation
// to SharedCost, rather than adding to the individual resource costs.
// TODO unit test
func (a *Allocation) Share(that *Allocation) (*Allocation, error) {
	if that == nil {
		return a.Clone(), nil
	}

	// Convert all costs of shared Allocation to SharedCost, zero out all
	// non-shared costs, then add.
	share := that.Clone()
	share.SharedCost += share.TotalCost()
	share.CPUCost = 0
	share.CPUCoreHours = 0
	share.RAMCost = 0
	share.RAMByteHours = 0
	share.GPUCost = 0
	share.GPUHours = 0
	share.PVCost = 0
	share.PVByteHours = 0
	share.NetworkCost = 0
	share.ExternalCost = 0

	if a == nil {
		return share, nil
	}

	agg := a.Clone()
	agg.add(that)

	return agg, nil
}

// String represents the given Allocation as a string
func (a *Allocation) String() string {
	return fmt.Sprintf("%s%s=%.2f", a.Name, NewWindow(&a.Start, &a.End), a.TotalCost())
}

func (a *Allocation) add(that *Allocation) {
	if a == nil {
		log.Warningf("Allocation.AggregateBy: trying to add a nil receiver")
		return
	}

	aCluster, _ := a.Properties.GetCluster()
	thatCluster, _ := that.Properties.GetCluster()
	aNode, _ := a.Properties.GetNode()
	thatNode, _ := that.Properties.GetNode()

	// reset properties
	a.Properties = nil

	// ensure that we carry cluster ID and/or node over if they're the same
	// required for idle/shared cost allocation
	if aCluster == thatCluster {
		a.Properties = Properties{ClusterProp: aCluster}
	}
	if aNode == thatNode {
		if a.Properties == nil {
			a.Properties = Properties{NodeProp: aNode}
		} else {
			a.Properties.SetNode(aNode)
		}
	}

	// Expand the window to encompass both Allocations
	a.Window = a.Window.Expand(that.Window)

	// Sum non-cumulative fields by turning them into cumulative, adding them,
	// and then converting them back into averages after minutes have been
	// combined (just below).
	cpuReqCoreMins := a.CPUCoreRequestAverage * a.Minutes()
	cpuReqCoreMins += that.CPUCoreRequestAverage * that.Minutes()

	cpuUseCoreMins := a.CPUCoreUsageAverage * a.Minutes()
	cpuUseCoreMins += that.CPUCoreUsageAverage * that.Minutes()

	ramReqByteMins := a.RAMBytesRequestAverage * a.Minutes()
	ramReqByteMins += that.RAMBytesRequestAverage * that.Minutes()

	ramUseByteMins := a.RAMBytesUsageAverage * a.Minutes()
	ramUseByteMins += that.RAMBytesUsageAverage * that.Minutes()

	// Expand Start and End to be the "max" of among the given Allocations
	if that.Start.Before(a.Start) {
		a.Start = that.Start
	}
	if that.End.After(a.End) {
		a.End = that.End
	}

	// Convert cumulatuve request and usage back into rates
	// TODO:CLEANUP write a unit test that fails if this is done incorrectly
	a.CPUCoreRequestAverage = cpuReqCoreMins / a.Minutes()
	a.CPUCoreUsageAverage = cpuUseCoreMins / a.Minutes()
	a.RAMBytesRequestAverage = ramReqByteMins / a.Minutes()
	a.RAMBytesUsageAverage = ramUseByteMins / a.Minutes()

	// Sum all cumulative resource fields
	a.CPUCoreHours += that.CPUCoreHours
	a.GPUHours += that.GPUHours
	a.RAMByteHours += that.RAMByteHours
	a.PVByteHours += that.PVByteHours

	// Sum all cumulative cost fields
	a.CPUCost += that.CPUCost
	a.GPUCost += that.GPUCost
	a.RAMCost += that.RAMCost
	a.PVCost += that.PVCost
	a.NetworkCost += that.NetworkCost
	a.SharedCost += that.SharedCost
	a.ExternalCost += that.ExternalCost
}

// AllocationSet stores a set of Allocations, each with a unique name, that share
// a window. An AllocationSet is mutable, so treat it like a threadsafe map.
type AllocationSet struct {
	sync.RWMutex
	allocations  map[string]*Allocation
	externalKeys map[string]bool
	idleKeys     map[string]bool
	Window       Window
	Warnings     []string
	Errors       []string
}

// NewAllocationSet instantiates a new AllocationSet and, optionally, inserts
// the given list of Allocations
func NewAllocationSet(start, end time.Time, allocs ...*Allocation) *AllocationSet {
	as := &AllocationSet{
		allocations:  map[string]*Allocation{},
		externalKeys: map[string]bool{},
		idleKeys:     map[string]bool{},
		Window:       NewWindow(&start, &end),
	}

	for _, a := range allocs {
		as.Insert(a)
	}

	return as
}

// AllocationAggregationOptions provide advanced functionality to AggregateBy, including
// filtering results and sharing allocations. FilterFuncs are a list of match
// functions such that, if any function fails, the allocation is ignored.
// ShareFuncs are a list of match functions such that, if any function
// succeeds, the allocation is marked as a shared resource. ShareIdle is a
// simple flag for sharing idle resources.
type AllocationAggregationOptions struct {
	FilterFuncs       []AllocationMatchFunc
	SplitIdle         bool
	MergeUnallocated  bool
	ShareFuncs        []AllocationMatchFunc
	ShareIdle         string
	ShareSplit        string
	SharedHourlyCosts map[string]float64
}

// AggregateBy aggregates the Allocations in the given AllocationSet by the given
// Property. This will only be legal if the AllocationSet is divisible by the
// given Property; e.g. Containers can be divided by Namespace, but not vice-a-versa.
func (as *AllocationSet) AggregateBy(properties Properties, options *AllocationAggregationOptions) error {
	// The order of operations for aggregating allocations is as follows:
	// 1. Partition external, idle, and shared allocations into separate sets
	// 2. Compute idle coefficients (if necessary)
	//    a) if idle allocation is to be shared, compute idle coefficients
	//       (do not compute shared coefficients here, see step 5)
	//    b) if idle allocation is NOT shared, but filters are present, compute
	//       idle filtration coefficients for the purpose of only returning the
	//       portion of idle allocation that would have been shared with the
	//       unfiltered results set. (See unit tests 5.a,b,c)
	// 3. Ignore allocation if it fails any of the FilterFuncs
	// 4. Distribute idle allocations among remaining non-idle, non-external
	//    allocations
	// 5. Generate aggregation key and insert allocation into the output set
	// 6. Scale un-aggregated idle coefficients by filtration coefficient
	// 7. If there are shared allocations, compute sharing coefficients on
	//    the aggregated set, then share allocation accordingly
	// 8. If there are external allocations that can be aggregated into
	//    the output (i.e. they can be used to generate a valid key for
	//    the given properties) then aggregate; otherwise... ignore them?
	// 9. If the merge idle option is enabled, merge any remaining idle
	//    allocations into a single idle allocation

	// TODO niko/etl revisit (ShareIdle: ShareEven) case, which is probably wrong
	// (and, frankly, ill-defined; i.e. evenly across clusters? within clusters?)

	if options == nil {
		options = &AllocationAggregationOptions{}
	}

	if as.IsEmpty() {
		return nil
	}

	// aggSet will collect the aggregated allocations
	aggSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// externalSet will collect external allocations
	externalSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// idleSet will be shared among aggSet after initial aggregation
	// is complete
	idleSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// shareSet will be shared among aggSet after initial aggregation
	// is complete
	shareSet := &AllocationSet{
		Window: as.Window.Clone(),
	}

	// Convert SharedHourlyCosts to Allocations in the shareSet
	for name, cost := range options.SharedHourlyCosts {
		if cost > 0.0 {
			hours := as.Resolution().Hours()

			// If set ends in the future, adjust hours accordingly
			diff := time.Now().Sub(as.End())
			if diff < 0.0 {
				hours += diff.Hours()
			}

			totalSharedCost := cost * hours

			shareSet.Insert(&Allocation{
				Name:       fmt.Sprintf("%s/%s", name, SharedSuffix),
				Start:      as.Start(),
				End:        as.End(),
				SharedCost: totalSharedCost,
			})
		}
	}

	as.Lock()
	defer as.Unlock()

	// (1) Loop and find all of the external, idle, and shared allocations. Add
	// them to their respective sets, removing them from the set of allocations
	// to aggregate.
	for _, alloc := range as.allocations {
		// External allocations get aggregated post-hoc (see step 6) and do
		// not necessarily contain complete sets of properties, so they are
		// moved to a separate AllocationSet.
		if alloc.IsExternal() {
			delete(as.externalKeys, alloc.Name)
			delete(as.allocations, alloc.Name)
			externalSet.Insert(alloc)
			continue
		}

		cluster, err := alloc.Properties.GetCluster()
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: missing cluster for allocation: %s", alloc.Name)
			return err
		}

		// Idle allocations should be separated into idleSet if they are to be
		// shared later on. If they are not to be shared, then aggregate them.
		if alloc.IsIdle() {
			delete(as.idleKeys, alloc.Name)
			delete(as.allocations, alloc.Name)

			if options.ShareIdle == ShareEven || options.ShareIdle == ShareWeighted {
				idleSet.Insert(alloc)
			} else {
				aggSet.Insert(alloc)
			}

			continue
		}

		// Shared allocations must be identified and separated prior to
		// aggregation and filtering. That is, if any of the ShareFuncs
		// return true, then move the allocation to shareSet.
		for _, sf := range options.ShareFuncs {
			if sf(alloc) {
				delete(as.idleKeys, alloc.Name)
				delete(as.allocations, alloc.Name)

				alloc.Name = fmt.Sprintf("%s/%s", cluster, SharedSuffix)
				shareSet.Insert(alloc)
				break
			}
		}
	}

	// It's possible that no more un-shared, non-idle, non-external allocations
	// remain at this point. This always results in an emptySet.
	if len(as.allocations) == 0 {
		log.Warningf("ETL: AggregateBy: no allocations to aggregate")
		emptySet := &AllocationSet{
			Window: as.Window.Clone(),
		}
		as.allocations = emptySet.allocations
		return nil
	}

	// (2) In order to correctly apply idle and shared resource coefficients
	// appropriately, we need to determine the coefficients for the full set
	// of data. The ensures that the ratios are maintained through filtering.

	// idleCoefficients are organized by [cluster][allocation][resource]=coeff
	var idleCoefficients map[string]map[string]map[string]float64

	// shareCoefficients are organized by [allocation][resource]=coeff (no cluster)
	var shareCoefficients map[string]float64

	var err error

	// (2a) If there are idle costs and we intend to share them, compute the
	// coefficients for sharing the cost among the non-idle, non-aggregated
	// allocations.
	if idleSet.Length() > 0 && options.ShareIdle != ShareNone {
		idleCoefficients, err = computeIdleCoeffs(properties, options, as)
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: compute idle coeff: %s", err)
			return fmt.Errorf("error computing idle coefficients: %s", err)
		}
	}

	// (2b) If we're not sharing idle and we're filtering, we need to track the
	// amount of each idle allocation to "delete" in order to maintain parity
	// with the idle-allocated results. That is, we want to return only the
	// idle cost that would have been shared with the unfiltered portion of
	// the results, not the full idle cost.
	var idleFiltrationCoefficients map[string]map[string]map[string]float64
	if len(options.FilterFuncs) > 0 && options.ShareIdle == ShareNone {
		idleFiltrationCoefficients, err = computeIdleCoeffs(properties, options, as)
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: compute idle coeff: %s", err)
			return fmt.Errorf("error computing idle filtration coefficients: %s", err)
		}
	}

	// (3-5) Filter, distribute idle cost, and aggregate (in that order)
	for _, alloc := range as.allocations {
		cluster, err := alloc.Properties.GetCluster()
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: missing cluster for allocation: %s", alloc.Name)
			return err
		}

		skip := false

		// (3) If any of the filter funcs fail, immediately skip the allocation.
		for _, ff := range options.FilterFuncs {
			if !ff(alloc) {
				skip = true
				break
			}
		}
		if skip {
			// If we are tracking idle filtration coefficients, delete the
			// entry corresponding to the filtered allocation. (Deleting the
			// entry will result in that proportional amount being removed
			// from the idle allocation at the end of the process.)
			if idleFiltrationCoefficients != nil {
				if ifcc, ok := idleFiltrationCoefficients[cluster]; ok {
					delete(ifcc, alloc.Name)
				}
			}

			continue
		}

		// (4) Split idle allocations and distribute among remaining
		// un-aggregated allocations.
		// NOTE: if idle allocation is off (i.e. ShareIdle == ShareNone) then
		// all idle allocations will be in the aggSet at this point, so idleSet
		// will be empty and we won't enter this block.
		if idleSet.Length() > 0 {
			// Distribute idle allocations by coefficient per-cluster, per-allocation
			for _, idleAlloc := range idleSet.allocations {
				// Only share idle if the cluster matches; i.e. the allocation
				// is from the same cluster as the idle costs
				idleCluster, err := idleAlloc.Properties.GetCluster()
				if err != nil {
					return err
				}
				if idleCluster != cluster {
					continue
				}

				// Make sure idle coefficients exist
				if _, ok := idleCoefficients[cluster]; !ok {
					log.Errorf("ETL: share (idle) allocation: error getting allocation coefficient [no cluster: '%s' in coefficients] for '%s'", cluster, alloc.Name)
					continue
				}
				if _, ok := idleCoefficients[cluster][alloc.Name]; !ok {
					log.Errorf("ETL: share (idle) allocation: error getting allocation coefficienct for '%s'", alloc.Name)
					continue
				}

				alloc.CPUCoreHours += idleAlloc.CPUCoreHours * idleCoefficients[cluster][alloc.Name]["cpu"]
				alloc.GPUHours += idleAlloc.GPUHours * idleCoefficients[cluster][alloc.Name]["gpu"]
				alloc.RAMByteHours += idleAlloc.RAMByteHours * idleCoefficients[cluster][alloc.Name]["ram"]

				idleCPUCost := idleAlloc.CPUCost * idleCoefficients[cluster][alloc.Name]["cpu"]
				idleGPUCost := idleAlloc.GPUCost * idleCoefficients[cluster][alloc.Name]["gpu"]
				idleRAMCost := idleAlloc.RAMCost * idleCoefficients[cluster][alloc.Name]["ram"]
				alloc.CPUCost += idleCPUCost
				alloc.GPUCost += idleGPUCost
				alloc.RAMCost += idleRAMCost
			}
		}

		// (5) generate key to use for aggregation-by-key and allocation name
		key, err := alloc.generateKey(properties)
		if err != nil {
			return err
		}

		alloc.Name = key
		if options.MergeUnallocated && alloc.IsUnallocated() {
			alloc.Name = UnallocatedSuffix
		}

		// Inserting the allocation with the generated key for a name will
		// perform the actual basic aggregation step.
		aggSet.Insert(alloc)
	}

	// clusterIdleFiltrationCoeffs is used to track per-resource idle
	// coefficients on a cluster-by-cluster basis. It is, essentailly, an
	// aggregation of idleFiltrationCoefficients after they have been
	// filtered above (in step 3)
	var clusterIdleFiltrationCoeffs map[string]map[string]float64
	if idleFiltrationCoefficients != nil {
		clusterIdleFiltrationCoeffs = map[string]map[string]float64{}

		for cluster, m := range idleFiltrationCoefficients {
			if _, ok := clusterIdleFiltrationCoeffs[cluster]; !ok {
				clusterIdleFiltrationCoeffs[cluster] = map[string]float64{
					"cpu": 0.0,
					"gpu": 0.0,
					"ram": 0.0,
				}
			}

			for _, n := range m {
				for resource, val := range n {
					clusterIdleFiltrationCoeffs[cluster][resource] += val
				}
			}
		}
	}

	// (6) If we have both un-shared idle allocations and idle filtration
	// coefficients (i.e. we have computed coefficients for scaling idle
	// allocation costs by cluster) then use those coefficients to scale down
	// each idle allocation.
	if len(aggSet.idleKeys) > 0 && clusterIdleFiltrationCoeffs != nil {
		for idleKey := range aggSet.idleKeys {
			idleAlloc := aggSet.Get(idleKey)

			cluster, err := idleAlloc.Properties.GetCluster()
			if err != nil {
				log.Warningf("AggregateBy: idle allocation without cluster: %s", idleAlloc)
			}

			if resourceCoeffs, ok := clusterIdleFiltrationCoeffs[cluster]; ok {
				idleAlloc.CPUCost *= resourceCoeffs["cpu"]
				idleAlloc.CPUCoreHours *= resourceCoeffs["cpu"]
				idleAlloc.RAMCost *= resourceCoeffs["ram"]
				idleAlloc.RAMByteHours *= resourceCoeffs["ram"]
			}

		}
	}

	// (7) Split shared allocations and distribute among aggregated allocations
	if shareSet.Length() > 0 {
		shareCoefficients, err = computeShareCoeffs(properties, options, aggSet)
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: compute shared coeff: missing cluster ID: %s", err)
			return err
		}

		for _, alloc := range aggSet.allocations {
			if alloc.IsIdle() {
				// Skip idle allocations (they do not receive shared allocation)
				continue
			}

			// Distribute shared allocations by coefficient per-allocation
			// NOTE: share coefficients do not partition by cluster, like
			// idle coefficients do.
			for _, sharedAlloc := range shareSet.allocations {
				if _, ok := shareCoefficients[alloc.Name]; !ok {
					log.Errorf("ETL: share allocation: error getting allocation coefficienct for '%s'", alloc.Name)
					continue
				}

				alloc.SharedCost += sharedAlloc.TotalCost() * shareCoefficients[alloc.Name]
			}
		}
	}

	// (8) Aggregate external allocations into aggregated allocations. This may
	// not be possible for every external allocation, but attempt to find an
	// exact key match, given each external allocation's proerties, and
	// aggregate if an exact match is found.
	for _, alloc := range externalSet.allocations {
		skip := false
		for _, ff := range options.FilterFuncs {
			if !ff(alloc) {
				skip = true
				break
			}
		}
		if !skip {
			key, err := alloc.generateKey(properties)
			if err != nil {
				continue
			}

			alloc.Name = key
			aggSet.Insert(alloc)
		}
	}

	// (9) Combine all idle allocations into a single "__idle__" allocation
	if !options.SplitIdle {
		for _, idleAlloc := range aggSet.IdleAllocations() {
			aggSet.Delete(idleAlloc.Name)
			idleAlloc.Name = IdleSuffix
			aggSet.Insert(idleAlloc)
		}
	}

	as.allocations = aggSet.allocations

	return nil
}

// TODO niko/etl deprecate the use of a map of resources here, we only use totals
func computeShareCoeffs(properties Properties, options *AllocationAggregationOptions, as *AllocationSet) (map[string]float64, error) {
	// Compute coeffs by totalling per-allocation, then dividing by the total.
	coeffs := map[string]float64{}

	// Compute totals for all allocations
	total := 0.0

	// ShareEven counts each aggregation with even weight, whereas ShareWeighted
	// counts each aggregation proportionally to its respective costs
	shareType := options.ShareSplit

	// Record allocation values first, then normalize by totals to get percentages
	for name, alloc := range as.allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		if shareType == ShareEven {
			// Not additive - set to 1.0 for even distribution
			coeffs[name] = 1.0
			// Total is always additive
			total += 1.0
		} else {
			// Both are additive for weighted distribution
			coeffs[name] += alloc.TotalCost()
			total += alloc.TotalCost()
		}
	}

	// Normalize coefficients by totals
	for a := range coeffs {
		if coeffs[a] > 0 && total > 0 {
			coeffs[a] /= total
		} else {
			log.Warningf("ETL: invalid values for shared coefficients: %d, %d", coeffs[a], total)
			coeffs[a] = 0.0
		}
	}

	return coeffs, nil
}

func computeIdleCoeffs(properties Properties, options *AllocationAggregationOptions, as *AllocationSet) (map[string]map[string]map[string]float64, error) {
	types := []string{"cpu", "gpu", "ram"}

	// Compute idle coefficients, then save them in AllocationAggregationOptions
	coeffs := map[string]map[string]map[string]float64{}

	// Compute totals per resource for CPU, GPU, RAM, and PV
	totals := map[string]map[string]float64{}

	// ShareEven counts each allocation with even weight, whereas ShareWeighted
	// counts each allocation proportionally to its respective costs
	shareType := options.ShareIdle

	// Record allocation values first, then normalize by totals to get percentages
	for _, alloc := range as.allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		// If any of the share funcs succeed, share the allocation. Do this
		// prior to filtering so that shared namespaces, etc do not get
		// filtered out before we have a chance to share them.
		skip := false
		for _, sf := range options.ShareFuncs {
			if sf(alloc) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		// We need to key the allocations by cluster id
		clusterID, err := alloc.Properties.GetCluster()
		if err != nil {
			return nil, err
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create cluster based tables if they don't exist
		if _, ok := coeffs[clusterID]; !ok {
			coeffs[clusterID] = map[string]map[string]float64{}
		}
		if _, ok := totals[clusterID]; !ok {
			totals[clusterID] = map[string]float64{}
		}

		if _, ok := coeffs[clusterID][name]; !ok {
			coeffs[clusterID][name] = map[string]float64{}
		}

		if shareType == ShareEven {
			for _, r := range types {
				// Not additive - hard set to 1.0
				coeffs[clusterID][name][r] = 1.0

				// totals are additive
				totals[clusterID][r] += 1.0
			}
		} else {
			coeffs[clusterID][name]["cpu"] += alloc.CPUCost
			coeffs[clusterID][name]["gpu"] += alloc.GPUCost
			coeffs[clusterID][name]["ram"] += alloc.RAMCost

			totals[clusterID]["cpu"] += alloc.CPUCost
			totals[clusterID]["gpu"] += alloc.GPUCost
			totals[clusterID]["ram"] += alloc.RAMCost
		}
	}

	// Normalize coefficients by totals
	for c := range coeffs {
		for a := range coeffs[c] {
			for _, r := range types {
				if coeffs[c][a][r] > 0 && totals[c][r] > 0 {
					coeffs[c][a][r] /= totals[c][r]
				}
			}
		}
	}

	return coeffs, nil
}

func (a *Allocation) generateKey(properties Properties) (string, error) {
	// Names will ultimately be joined into a single name, which uniquely
	// identifies allocations.
	names := []string{}

	if properties.HasCluster() {
		cluster, err := a.Properties.GetCluster()
		if err != nil {
			return "", err
		}
		names = append(names, cluster)
	}

	if properties.HasNode() {
		node, err := a.Properties.GetNode()
		if err != nil {
			return "", err
		}
		names = append(names, node)
	}

	if properties.HasNamespace() {
		namespace, err := a.Properties.GetNamespace()
		if err != nil {
			return "", err
		}
		names = append(names, namespace)
	}

	if properties.HasControllerKind() {
		controllerKind, err := a.Properties.GetControllerKind()
		if err != nil {
			// Indicate that allocation has no controller
			controllerKind = UnallocatedSuffix
		}

		if prop, _ := properties.GetControllerKind(); prop != "" && prop != controllerKind {
			// The allocation does not have the specified controller kind
			controllerKind = UnallocatedSuffix
		}
		names = append(names, controllerKind)
	}

	if properties.HasController() {
		if !properties.HasControllerKind() {
			controllerKind, err := a.Properties.GetControllerKind()
			if err == nil {
				names = append(names, controllerKind)
			}
		}

		controller, err := a.Properties.GetController()
		if err != nil {
			// Indicate that allocation has no controller
			controller = UnallocatedSuffix
		}

		names = append(names, controller)
	}

	if properties.HasPod() {
		pod, err := a.Properties.GetPod()
		if err != nil {
			return "", err
		}

		names = append(names, pod)
	}

	if properties.HasContainer() {
		container, err := a.Properties.GetContainer()
		if err != nil {
			return "", err
		}

		names = append(names, container)
	}

	if properties.HasService() {
		services, err := a.Properties.GetServices()
		if err != nil {
			// Indicate that allocation has no services
			names = append(names, UnallocatedSuffix)
		} else {
			// TODO niko/etl support multi-service aggregation
			if len(services) > 0 {
				for _, service := range services {
					names = append(names, service)
					break
				}
			} else {
				// Indicate that allocation has no services
				names = append(names, UnallocatedSuffix)
			}
		}
	}

	if properties.HasAnnotations() {
		annotations, err := a.Properties.GetAnnotations() // annotations that the individual allocation possesses
		if err != nil {
			// Indicate that allocation has no annotations
			names = append(names, UnallocatedSuffix)
		} else {
			annotationNames := []string{}

			aggAnnotations, err := properties.GetAnnotations() // potential annotations to aggregate on supplied by the API caller
			if err != nil {
				// We've already checked HasAnnotation, so this should never occur
				return "", err
			}
			// calvin - support multi-annotation aggregation
			for annotationName := range aggAnnotations {
				if val, ok := annotations[annotationName]; ok {
					annotationNames = append(annotationNames, fmt.Sprintf("%s=%s", annotationName, val))
				} else if indexOf(UnallocatedSuffix, annotationNames) == -1 { // if UnallocatedSuffix not already in names
					annotationNames = append(annotationNames, UnallocatedSuffix)
				}
			}
			// resolve arbitrary ordering. e.g., app=app0/env=env0 is the same agg as env=env0/app=app0
			if len(annotationNames) > 1 {
				sort.Strings(annotationNames)
			}
			unallocatedSuffixIndex := indexOf(UnallocatedSuffix, annotationNames)
			// suffix should be at index 0 if it exists b/c of underscores
			if unallocatedSuffixIndex != -1 {
				annotationNames = append(annotationNames[:unallocatedSuffixIndex], annotationNames[unallocatedSuffixIndex+1:]...)
				annotationNames = append(annotationNames, UnallocatedSuffix) // append to end
			}

			names = append(names, annotationNames...)
		}
	}

	if properties.HasLabel() {
		labels, err := a.Properties.GetLabels() // labels that the individual allocation possesses
		if err != nil {
			// Indicate that allocation has no labels
			names = append(names, UnallocatedSuffix)
		} else {
			labelNames := []string{}

			aggLabels, err := properties.GetLabels() // potential labels to aggregate on supplied by the API caller
			if err != nil {
				// We've already checked HasLabel, so this should never occur
				return "", err
			}
			// calvin - support multi-label aggregation
			for labelName := range aggLabels {
				if val, ok := labels[labelName]; ok {
					labelNames = append(labelNames, fmt.Sprintf("%s=%s", labelName, val))
				} else if indexOf(UnallocatedSuffix, labelNames) == -1 { // if UnallocatedSuffix not already in names
					labelNames = append(labelNames, UnallocatedSuffix)
				}
			}
			// resolve arbitrary ordering. e.g., app=app0/env=env0 is the same agg as env=env0/app=app0
			if len(labelNames) > 1 {
				sort.Strings(labelNames)
			}
			unallocatedSuffixIndex := indexOf(UnallocatedSuffix, labelNames)
			// suffix should be at index 0 if it exists b/c of underscores
			if unallocatedSuffixIndex != -1 {
				labelNames = append(labelNames[:unallocatedSuffixIndex], labelNames[unallocatedSuffixIndex+1:]...)
				labelNames = append(labelNames, UnallocatedSuffix) // append to end
			}

			names = append(names, labelNames...)
		}
	}

	return strings.Join(names, "/"), nil
}

// TODO clean up
// Helper function to check for slice membership. Not sure if repeated elsewhere in our codebase.
func indexOf(v string, arr []string) int {
	for i, s := range arr {
		// This is caseless equivalence
		if strings.EqualFold(v, s) {
			return i
		}
	}
	return -1
}

// Clone returns a new AllocationSet with a deep copy of the given
// AllocationSet's allocations.
func (as *AllocationSet) Clone() *AllocationSet {
	if as == nil {
		return nil
	}

	as.RLock()
	defer as.RUnlock()

	allocs := map[string]*Allocation{}
	for k, v := range as.allocations {
		allocs[k] = v.Clone()
	}

	externalKeys := map[string]bool{}
	for k, v := range as.externalKeys {
		externalKeys[k] = v
	}

	idleKeys := map[string]bool{}
	for k, v := range as.idleKeys {
		idleKeys[k] = v
	}

	return &AllocationSet{
		allocations:  allocs,
		externalKeys: externalKeys,
		idleKeys:     idleKeys,
		Window:       as.Window.Clone(),
	}
}

// ComputeIdleAllocations computes the idle allocations for the AllocationSet,
// given a set of Assets. Ideally, assetSet should contain only Nodes, but if
// it contains other Assets, they will be ignored; only CPU, GPU and RAM are
// considered for idle allocation. If the Nodes have adjustments, then apply
// the adjustments proportionally to each of the resources so that total
// allocation with idle reflects the adjusted node costs. One idle allocation
// per-cluster will be computed and returned, keyed by cluster_id.
func (as *AllocationSet) ComputeIdleAllocations(assetSet *AssetSet) (map[string]*Allocation, error) {
	if as == nil {
		return nil, fmt.Errorf("cannot compute idle allocation for nil AllocationSet")
	}

	if assetSet == nil {
		return nil, fmt.Errorf("cannot compute idle allocation with nil AssetSet")
	}

	if !as.Window.Equal(assetSet.Window) {
		return nil, fmt.Errorf("cannot compute idle allocation for sets with mismatched windows: %s != %s", as.Window, assetSet.Window)
	}

	window := as.Window

	// Build a map of cumulative cluster asset costs, per resource; i.e.
	// cluster-to-{cpu|gpu|ram}-to-cost.
	assetClusterResourceCosts := map[string]map[string]float64{}
	assetSet.Each(func(key string, a Asset) {
		if node, ok := a.(*Node); ok {
			if _, ok := assetClusterResourceCosts[node.Properties().Cluster]; !ok {
				assetClusterResourceCosts[node.Properties().Cluster] = map[string]float64{}
			}

			// adjustmentRate is used to scale resource costs proportionally
			// by the adjustment. This is necessary because we only get one
			// adjustment per Node, not one per-resource-per-Node.
			//
			// e.g. total cost = $90, adjustment = -$10 => 0.9
			// e.g. total cost = $150, adjustment = -$300 => 0.3333
			// e.g. total cost = $150, adjustment = $50 => 1.5
			adjustmentRate := 1.0
			if node.TotalCost()-node.Adjustment() == 0 {
				// If (totalCost - adjustment) is 0.0 then adjustment cancels
				// the entire node cost and we should make everything 0
				// without dividing by 0.
				adjustmentRate = 0.0
			} else if node.Adjustment() != 0.0 {
				// adjustmentRate is the ratio of cost-with-adjustment (i.e. TotalCost)
				// to cost-without-adjustment (i.e. TotalCost - Adjustment).
				adjustmentRate = node.TotalCost() / (node.TotalCost() - node.Adjustment())
			}

			cpuCost := node.CPUCost * (1.0 - node.Discount) * adjustmentRate
			gpuCost := node.GPUCost * (1.0 - node.Discount) * adjustmentRate
			ramCost := node.RAMCost * (1.0 - node.Discount) * adjustmentRate

			assetClusterResourceCosts[node.Properties().Cluster]["cpu"] += cpuCost
			assetClusterResourceCosts[node.Properties().Cluster]["gpu"] += gpuCost
			assetClusterResourceCosts[node.Properties().Cluster]["ram"] += ramCost
		}
	})

	// Determine start, end on a per-cluster basis
	clusterStarts := map[string]time.Time{}
	clusterEnds := map[string]time.Time{}

	// Subtract allocated costs from asset costs, leaving only the remaining
	// idle costs.
	as.Each(func(name string, a *Allocation) {
		cluster, err := a.Properties.GetCluster()
		if err != nil {
			// Failed to find allocation's cluster
			return
		}

		if _, ok := assetClusterResourceCosts[cluster]; !ok {
			// Failed to find assets for allocation's cluster
			return
		}

		// Set cluster (start, end) if they are either not currently set,
		// or if the detected (start, end) of the current allocation falls
		// before or after, respectively, the current values.
		if s, ok := clusterStarts[cluster]; !ok || a.Start.Before(s) {
			clusterStarts[cluster] = a.Start
		}
		if e, ok := clusterEnds[cluster]; !ok || a.End.After(e) {
			clusterEnds[cluster] = a.End
		}

		assetClusterResourceCosts[cluster]["cpu"] -= a.CPUCost
		assetClusterResourceCosts[cluster]["gpu"] -= a.GPUCost
		assetClusterResourceCosts[cluster]["ram"] -= a.RAMCost
	})

	// Turn remaining un-allocated asset costs into idle allocations
	idleAllocs := map[string]*Allocation{}
	for cluster, resources := range assetClusterResourceCosts {
		// Default start and end to the (start, end) of the given window, but
		// use the actual, detected (start, end) pair if they are available.
		start := *window.Start()
		if s, ok := clusterStarts[cluster]; ok && window.Contains(s) {
			start = s
		}
		end := *window.End()
		if e, ok := clusterEnds[cluster]; ok && window.Contains(e) {
			end = e
		}

		idleAlloc := &Allocation{
			Name:       fmt.Sprintf("%s/%s", cluster, IdleSuffix),
			Window:     window.Clone(),
			Properties: Properties{ClusterProp: cluster},
			Start:      start,
			End:        end,
			CPUCost:    resources["cpu"],
			GPUCost:    resources["gpu"],
			RAMCost:    resources["ram"],
		}

		// Do not continue if multiple idle allocations are computed for a
		// single cluster.
		if _, ok := idleAllocs[cluster]; ok {
			return nil, fmt.Errorf("duplicate idle allocations for cluster %s", cluster)
		}

		idleAllocs[cluster] = idleAlloc
	}

	return idleAllocs, nil
}

// Delete removes the allocation with the given name from the set
func (as *AllocationSet) Delete(name string) {
	if as == nil {
		return
	}

	as.Lock()
	defer as.Unlock()
	delete(as.externalKeys, name)
	delete(as.idleKeys, name)
	delete(as.allocations, name)
}

// Each invokes the given function for each Allocation in the set
func (as *AllocationSet) Each(f func(string, *Allocation)) {
	if as == nil {
		return
	}

	for k, a := range as.allocations {
		f(k, a)
	}
}

// End returns the End time of the AllocationSet window
func (as *AllocationSet) End() time.Time {
	if as == nil {
		log.Warningf("Allocation ETL: calling End on nil AllocationSet")
		return time.Unix(0, 0)
	}
	if as.Window.End() == nil {
		log.Warningf("Allocation ETL: AllocationSet with illegal window: End is nil; len(as.allocations)=%d", len(as.allocations))
		return time.Unix(0, 0)
	}
	return *as.Window.End()
}

// Get returns the Allocation at the given key in the AllocationSet
func (as *AllocationSet) Get(key string) *Allocation {
	as.RLock()
	defer as.RUnlock()

	if alloc, ok := as.allocations[key]; ok {
		return alloc
	}

	return nil
}

// ExternalAllocations returns a map of the external allocations in the set.
// Returns clones of the actual Allocations, so mutability is not a problem.
func (as *AllocationSet) ExternalAllocations() map[string]*Allocation {
	externals := map[string]*Allocation{}

	if as.IsEmpty() {
		return externals
	}

	as.RLock()
	defer as.RUnlock()

	for key := range as.externalKeys {
		if alloc, ok := as.allocations[key]; ok {
			externals[key] = alloc.Clone()
		}
	}

	return externals
}

// ExternalCost returns the total aggregated external costs of the set
func (as *AllocationSet) ExternalCost() float64 {
	if as.IsEmpty() {
		return 0.0
	}

	as.RLock()
	defer as.RUnlock()

	externalCost := 0.0
	for _, alloc := range as.allocations {
		externalCost += alloc.ExternalCost
	}

	return externalCost
}

// IdleAllocations returns a map of the idle allocations in the AllocationSet.
// Returns clones of the actual Allocations, so mutability is not a problem.
func (as *AllocationSet) IdleAllocations() map[string]*Allocation {
	idles := map[string]*Allocation{}

	if as.IsEmpty() {
		return idles
	}

	as.RLock()
	defer as.RUnlock()

	for key := range as.idleKeys {
		if alloc, ok := as.allocations[key]; ok {
			idles[key] = alloc.Clone()
		}
	}

	return idles
}

// Insert aggregates the current entry in the AllocationSet by the given Allocation,
// but only if the Allocation is valid, i.e. matches the AllocationSet's window. If
// there is no existing entry, one is created. Nil error response indicates success.
func (as *AllocationSet) Insert(that *Allocation) error {
	return as.insert(that)
}

func (as *AllocationSet) insert(that *Allocation) error {
	if as == nil {
		return fmt.Errorf("cannot insert into nil AllocationSet")
	}

	as.Lock()
	defer as.Unlock()

	if as.allocations == nil {
		as.allocations = map[string]*Allocation{}
	}

	if as.externalKeys == nil {
		as.externalKeys = map[string]bool{}
	}

	if as.idleKeys == nil {
		as.idleKeys = map[string]bool{}
	}

	// Add the given Allocation to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := as.allocations[that.Name]; !ok {
		as.allocations[that.Name] = that
	} else {
		as.allocations[that.Name].add(that)
	}

	// If the given Allocation is an external one, record that
	if that.IsExternal() {
		as.externalKeys[that.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if that.IsIdle() {
		as.idleKeys[that.Name] = true
	}

	return nil
}

// IsEmpty returns true if the AllocationSet is nil, or if it contains
// zero allocations.
func (as *AllocationSet) IsEmpty() bool {
	if as == nil || len(as.allocations) == 0 {
		return true
	}

	as.RLock()
	defer as.RUnlock()
	return as.allocations == nil || len(as.allocations) == 0
}

// Length returns the number of Allocations in the set
func (as *AllocationSet) Length() int {
	if as == nil {
		return 0
	}

	as.RLock()
	defer as.RUnlock()
	return len(as.allocations)
}

// Map clones and returns a map of the AllocationSet's Allocations
func (as *AllocationSet) Map() map[string]*Allocation {
	if as.IsEmpty() {
		return map[string]*Allocation{}
	}

	return as.Clone().allocations
}

// MarshalJSON JSON-encodes the AllocationSet
func (as *AllocationSet) MarshalJSON() ([]byte, error) {
	as.RLock()
	defer as.RUnlock()
	return json.Marshal(as.allocations)
}

// Resolution returns the AllocationSet's window duration
func (as *AllocationSet) Resolution() time.Duration {
	return as.Window.Duration()
}

// Set uses the given Allocation to overwrite the existing entry in the
// AllocationSet under the Allocation's name.
func (as *AllocationSet) Set(alloc *Allocation) error {
	if as.IsEmpty() {
		as.Lock()
		as.allocations = map[string]*Allocation{}
		as.externalKeys = map[string]bool{}
		as.idleKeys = map[string]bool{}
		as.Unlock()
	}

	as.Lock()
	defer as.Unlock()

	as.allocations[alloc.Name] = alloc

	// If the given Allocation is an external one, record that
	if alloc.IsExternal() {
		as.externalKeys[alloc.Name] = true
	}

	// If the given Allocation is an idle one, record that
	if alloc.IsIdle() {
		as.idleKeys[alloc.Name] = true
	}

	return nil
}

// Start returns the Start time of the AllocationSet window
func (as *AllocationSet) Start() time.Time {
	if as == nil {
		log.Warningf("Allocation ETL: calling Start on nil AllocationSet")
		return time.Unix(0, 0)
	}
	if as.Window.Start() == nil {
		log.Warningf("Allocation ETL: AllocationSet with illegal window: Start is nil; len(as.allocations)=%d", len(as.allocations))
		return time.Unix(0, 0)
	}
	return *as.Window.Start()
}

// String represents the given Allocation as a string
func (as *AllocationSet) String() string {
	if as == nil {
		return "<nil>"
	}
	return fmt.Sprintf("AllocationSet{length: %d; window: %s; totalCost: %.2f}",
		as.Length(), as.Window, as.TotalCost())
}

// TotalCost returns the sum of all TotalCosts of the allocations contained
func (as *AllocationSet) TotalCost() float64 {
	if as.IsEmpty() {
		return 0.0
	}

	as.RLock()
	defer as.RUnlock()

	tc := 0.0
	for _, a := range as.allocations {
		tc += a.TotalCost()
	}
	return tc
}

// UTCOffset returns the AllocationSet's configured UTCOffset.
func (as *AllocationSet) UTCOffset() time.Duration {
	_, zone := as.Start().Zone()
	return time.Duration(zone) * time.Second
}

func (as *AllocationSet) accumulate(that *AllocationSet) (*AllocationSet, error) {
	if as.IsEmpty() {
		return that, nil
	}

	if that.IsEmpty() {
		return as, nil
	}

	// Set start, end to min(start), max(end)
	start := as.Start()
	end := as.End()
	if that.Start().Before(start) {
		start = that.Start()
	}
	if that.End().After(end) {
		end = that.End()
	}

	acc := NewAllocationSet(start, end)

	as.RLock()
	defer as.RUnlock()

	that.RLock()
	defer that.RUnlock()

	for _, alloc := range as.allocations {
		err := acc.insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	for _, alloc := range that.allocations {
		err := acc.insert(alloc)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// AllocationSetRange is a thread-safe slice of AllocationSets. It is meant to
// be used such that the AllocationSets held are consecutive and coherent with
// respect to using the same aggregation properties, UTC offset, and
// resolution. However these rules are not necessarily enforced, so use wisely.
type AllocationSetRange struct {
	sync.RWMutex
	allocations []*AllocationSet
}

// NewAllocationSetRange instantiates a new range composed of the given
// AllocationSets in the order provided.
func NewAllocationSetRange(allocs ...*AllocationSet) *AllocationSetRange {
	return &AllocationSetRange{
		allocations: allocs,
	}
}

// Accumulate sums each AllocationSet in the given range, returning a single cumulative
// AllocationSet for the entire range.
func (asr *AllocationSetRange) Accumulate() (*AllocationSet, error) {
	var allocSet *AllocationSet
	var err error

	asr.RLock()
	defer asr.RUnlock()

	for _, as := range asr.allocations {
		allocSet, err = allocSet.accumulate(as)
		if err != nil {
			return nil, err
		}
	}

	return allocSet, nil
}

// TODO niko/etl accumulate into lower-resolution chunks of the given resolution
// func (asr *AllocationSetRange) AccumulateBy(resolution time.Duration) *AllocationSetRange

// AggregateBy aggregates each AllocationSet in the range by the given
// properties and options.
func (asr *AllocationSetRange) AggregateBy(properties Properties, options *AllocationAggregationOptions) error {
	aggRange := &AllocationSetRange{allocations: []*AllocationSet{}}

	asr.Lock()
	defer asr.Unlock()

	for _, as := range asr.allocations {
		err := as.AggregateBy(properties, options)
		if err != nil {
			return err
		}
		aggRange.allocations = append(aggRange.allocations, as)
	}

	asr.allocations = aggRange.allocations

	return nil
}

// Append appends the given AllocationSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (asr *AllocationSetRange) Append(that *AllocationSet) {
	asr.Lock()
	defer asr.Unlock()
	asr.allocations = append(asr.allocations, that)
}

// Each invokes the given function for each AllocationSet in the range
func (asr *AllocationSetRange) Each(f func(int, *AllocationSet)) {
	if asr == nil {
		return
	}

	for i, as := range asr.allocations {
		f(i, as)
	}
}

// Get retrieves the AllocationSet at the given index of the range.
func (asr *AllocationSetRange) Get(i int) (*AllocationSet, error) {
	if i < 0 || i >= len(asr.allocations) {
		return nil, fmt.Errorf("AllocationSetRange: index out of range: %d", i)
	}

	asr.RLock()
	defer asr.RUnlock()
	return asr.allocations[i], nil
}

// InsertRange merges the given AllocationSetRange into the receiving one by
// lining up sets with matching windows, then inserting each allocation from
// the given ASR into the respective set in the receiving ASR. If the given
// ASR contains an AllocationSet from a window that does not exist in the
// receiving ASR, then an error is returned. However, the given ASR does not
// need to cover the full range of the receiver.
func (asr *AllocationSetRange) InsertRange(that *AllocationSetRange) error {
	if asr == nil {
		return fmt.Errorf("cannot insert range into nil AllocationSetRange")
	}

	// keys maps window to index in asr
	keys := map[string]int{}
	asr.Each(func(i int, as *AllocationSet) {
		if as == nil {
			return
		}
		keys[as.Window.String()] = i
	})

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
		as, err := asr.Get(i)
		if err != nil {
			err = fmt.Errorf("AllocationSetRange index does not exist: %d", i)
			return
		}

		// Insert each Allocation from the given set
		thatAS.Each(func(k string, alloc *Allocation) {
			err = as.Insert(alloc)
			if err != nil {
				err = fmt.Errorf("error inserting allocation: %s", err)
				return
			}
		})
	})

	// err might be nil
	return err
}

// Length returns the length of the range, which is zero if nil
func (asr *AllocationSetRange) Length() int {
	if asr == nil || asr.allocations == nil {
		return 0
	}

	asr.RLock()
	defer asr.RUnlock()
	return len(asr.allocations)
}

// MarshalJSON JSON-encodes the range
func (asr *AllocationSetRange) MarshalJSON() ([]byte, error) {
	asr.RLock()
	asr.RUnlock()
	return json.Marshal(asr.allocations)
}

// Slice copies the underlying slice of AllocationSets, maintaining order,
// and returns the copied slice.
func (asr *AllocationSetRange) Slice() []*AllocationSet {
	if asr == nil || asr.allocations == nil {
		return nil
	}

	asr.RLock()
	defer asr.RUnlock()
	copy := []*AllocationSet{}
	for _, as := range asr.allocations {
		copy = append(copy, as.Clone())
	}
	return copy
}

// String represents the given AllocationSetRange as a string
func (asr *AllocationSetRange) String() string {
	if asr == nil {
		return "<nil>"
	}
	return fmt.Sprintf("AllocationSetRange{length: %d}", asr.Length())
}

// UTCOffset returns the detected UTCOffset of the AllocationSets within the
// range. Defaults to 0 if the range is nil or empty. Does not warn if there
// are sets with conflicting UTCOffsets (just returns the first).
func (asr *AllocationSetRange) UTCOffset() time.Duration {
	if asr.Length() == 0 {
		return 0
	}

	as, err := asr.Get(0)
	if err != nil {
		return 0
	}
	return as.UTCOffset()
}

// Window returns the full window that the AllocationSetRange spans, from the
// start of the first AllocationSet to the end of the last one.
func (asr *AllocationSetRange) Window() Window {
	if asr == nil || asr.Length() == 0 {
		return NewWindow(nil, nil)
	}

	start := asr.allocations[0].Start()
	end := asr.allocations[asr.Length()-1].End()

	return NewWindow(&start, &end)
}
