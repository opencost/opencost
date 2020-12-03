package kubecost

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
)

// IdleSuffix indicates an idle allocation property
const IdleSuffix = "__idle__"

// SharedSuffix indicates an shared allocation property
const SharedSuffix = "__shared__"

// UnallocatedSuffix indicates an unallocated allocation property
const UnallocatedSuffix = "__unallocated__"

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
type Allocation struct {
	Name            string     `json:"name"`
	Properties      Properties `json:"properties,omitempty"`
	Start           time.Time  `json:"start"`
	End             time.Time  `json:"end"`
	Minutes         float64    `json:"minutes"`
	ActiveStart     time.Time  `json:"-"`
	CPUCoreHours    float64    `json:"cpuCoreHours"`
	CPUCost         float64    `json:"cpuCost"`
	CPUEfficiency   float64    `json:"cpuEfficiency"`
	GPUHours        float64    `json:"gpuHours"`
	GPUCost         float64    `json:"gpuCost"`
	NetworkCost     float64    `json:"networkCost"`
	PVByteHours     float64    `json:"pvByteHours"`
	PVCost          float64    `json:"pvCost"`
	RAMByteHours    float64    `json:"ramByteHours"`
	RAMCost         float64    `json:"ramCost"`
	RAMEfficiency   float64    `json:"ramEfficiency"`
	SharedCost      float64    `json:"sharedCost"`
	TotalCost       float64    `json:"totalCost"`
	TotalEfficiency float64    `json:"totalEfficiency"`
	// Profiler        *log.Profiler `json:"-"`
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

	if !a.Start.Equal(that.Start) || !a.End.Equal(that.End) {
		return nil, fmt.Errorf("error adding Allocations: mismatched windows")
	}

	agg := a.Clone()
	// agg.Profiler = a.Profiler
	agg.add(that, false, false)

	return agg, nil
}

// Clone returns a deep copy of the given Allocation
func (a *Allocation) Clone() *Allocation {
	if a == nil {
		return nil
	}

	return &Allocation{
		Name:            a.Name,
		Properties:      a.Properties.Clone(),
		Start:           a.Start,
		End:             a.End,
		Minutes:         a.Minutes,
		ActiveStart:     a.ActiveStart,
		CPUCoreHours:    a.CPUCoreHours,
		CPUCost:         a.CPUCost,
		CPUEfficiency:   a.CPUEfficiency,
		GPUHours:        a.GPUHours,
		GPUCost:         a.GPUCost,
		NetworkCost:     a.NetworkCost,
		PVByteHours:     a.PVByteHours,
		PVCost:          a.PVCost,
		RAMByteHours:    a.RAMByteHours,
		RAMCost:         a.RAMCost,
		RAMEfficiency:   a.RAMEfficiency,
		SharedCost:      a.SharedCost,
		TotalCost:       a.TotalCost,
		TotalEfficiency: a.TotalEfficiency,
	}
}

func (a *Allocation) Equal(that *Allocation) bool {
	if a == nil || that == nil {
		return false
	}

	if a.Name != that.Name {
		return false
	}
	if !a.Start.Equal(that.Start) {
		return false
	}
	if !a.End.Equal(that.End) {
		return false
	}
	if a.Minutes != that.Minutes {
		return false
	}
	if !a.ActiveStart.Equal(that.ActiveStart) {
		return false
	}
	if a.CPUCoreHours != that.CPUCoreHours {
		return false
	}
	if a.CPUCost != that.CPUCost {
		return false
	}
	if a.CPUEfficiency != that.CPUEfficiency {
		return false
	}
	if a.GPUHours != that.GPUHours {
		return false
	}
	if a.GPUCost != that.GPUCost {
		return false
	}
	if a.NetworkCost != that.NetworkCost {
		return false
	}
	if a.PVByteHours != that.PVByteHours {
		return false
	}
	if a.PVCost != that.PVCost {
		return false
	}
	if a.RAMByteHours != that.RAMByteHours {
		return false
	}
	if a.RAMCost != that.RAMCost {
		return false
	}
	if a.RAMEfficiency != that.RAMEfficiency {
		return false
	}
	if a.SharedCost != that.SharedCost {
		return false
	}
	if a.TotalCost != that.TotalCost {
		return false
	}
	if a.TotalEfficiency != that.TotalEfficiency {
		return false
	}
	if !a.Properties.Equal(&that.Properties) {
		return false
	}

	return true
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

// IsIdle is true if the given Allocation represents idle costs.
func (a *Allocation) IsIdle() bool {
	return strings.Contains(a.Name, IdleSuffix)
}

// IsUnallocated is true if the given Allocation represents unallocated costs.
func (a *Allocation) IsUnallocated() bool {
	return strings.Contains(a.Name, UnallocatedSuffix)
}

// MatchesFilter returns true if the Allocation passes the given AllocationFilter
func (a *Allocation) MatchesFilter(f AllocationMatchFunc) bool {
	return f(a)
}

// MatchesAll takes a variadic list of Properties, returning true iff the
// Allocation matches each set of Properties.
func (a *Allocation) MatchesAll(ps ...Properties) bool {
	// nil Allocation don't match any Properties
	if a == nil {
		return false
	}

	for _, p := range ps {
		if !a.Properties.Matches(p) {
			return false
		}
	}

	return true
}

// MatchesOne takes a variadic list of Properties, returning true iff the
// Allocation matches at least one of the set of Properties.
func (a *Allocation) MatchesOne(ps ...Properties) bool {
	// nil Allocation don't match any Properties
	if a == nil {
		return false
	}

	for _, p := range ps {
		if a.Properties.Matches(p) {
			return true
		}
	}

	return false
}

// Share works like Add, but converts the entire cost of the given Allocation
// to SharedCost, rather than adding to the individual resource costs.
func (a *Allocation) Share(that *Allocation) (*Allocation, error) {
	if a == nil {
		return that.Clone(), nil
	}

	if !a.Start.Equal(that.Start) {
		return nil, fmt.Errorf("mismatched start time: expected %s, received %s", a.Start, that.Start)
	}
	if !a.End.Equal(that.End) {
		return nil, fmt.Errorf("mismatched start time: expected %s, received %s", a.End, that.End)
	}

	agg := a.Clone()
	agg.add(that, true, false)

	return agg, nil
}

// String represents the given Allocation as a string
func (a *Allocation) String() string {
	return fmt.Sprintf("%s%s=%.2f", a.Name, NewWindow(&a.Start, &a.End), a.TotalCost)
}

func (a *Allocation) add(that *Allocation, isShared, isAccumulating bool) {
	if a == nil {
		a = that

		// reset properties
		thatCluster, _ := that.Properties.GetCluster()
		thatNode, _ := that.Properties.GetNode()
		a.Properties = Properties{ClusterProp: thatCluster, NodeProp: thatNode}

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

	if that.ActiveStart.Before(a.ActiveStart) {
		a.ActiveStart = that.ActiveStart
	}

	if isAccumulating {
		if a.Start.After(that.Start) {
			a.Start = that.Start
		}

		if a.End.Before(that.End) {
			a.End = that.End
		}

		a.Minutes += that.Minutes
	} else if that.Minutes > a.Minutes {
		a.Minutes = that.Minutes
	}

	// isShared determines whether the given allocation should be spread evenly
	// across resources (e.g. sharing idle allocation) or lumped into a shared
	// cost category (e.g. sharing namespace, labels).
	if isShared {
		a.SharedCost += that.TotalCost
	} else {
		a.CPUCoreHours += that.CPUCoreHours
		a.GPUHours += that.GPUHours
		a.RAMByteHours += that.RAMByteHours
		a.PVByteHours += that.PVByteHours

		aggCPUCost := a.CPUCost + that.CPUCost
		if aggCPUCost > 0 {
			a.CPUEfficiency = (a.CPUEfficiency*a.CPUCost + that.CPUEfficiency*that.CPUCost) / aggCPUCost
		} else {
			a.CPUEfficiency = 0.0
		}

		aggRAMCost := a.RAMCost + that.RAMCost
		if aggRAMCost > 0 {
			a.RAMEfficiency = (a.RAMEfficiency*a.RAMCost + that.RAMEfficiency*that.RAMCost) / aggRAMCost
		} else {
			a.RAMEfficiency = 0.0
		}

		aggTotalCost := a.TotalCost + that.TotalCost
		if aggTotalCost > 0 {
			a.TotalEfficiency = (a.TotalEfficiency*a.TotalCost + that.TotalEfficiency*that.TotalCost) / aggTotalCost
		} else {
			aggTotalCost = 0.0
		}

		a.SharedCost += that.SharedCost
		a.CPUCost += that.CPUCost
		a.GPUCost += that.GPUCost
		a.NetworkCost += that.NetworkCost
		a.RAMCost += that.RAMCost
		a.PVCost += that.PVCost
	}

	a.TotalCost += that.TotalCost
}

// AllocationSet stores a set of Allocations, each with a unique name, that share
// a window. An AllocationSet is mutable, so treat it like a threadsafe map.
type AllocationSet struct {
	sync.RWMutex
	// Profiler    *log.Profiler
	allocations map[string]*Allocation
	idleKeys    map[string]bool
	Window      Window
}

// NewAllocationSet instantiates a new AllocationSet and, optionally, inserts
// the given list of Allocations
func NewAllocationSet(start, end time.Time, allocs ...*Allocation) *AllocationSet {
	as := &AllocationSet{
		allocations: map[string]*Allocation{},
		Window:      NewWindow(&start, &end),
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
	// 1. move shared and/or idle allocations to separate sets if options
	//    indicate that they should be shared
	// 2. idle coefficients
	// 2.a) if idle allocation is to be shared, compute idle coefficients
	//      (do not compute shared coefficients here, see step 5)
	// 2.b) if idle allocation is NOT shared, but filters are present, compute
	//      idle filtration coefficients for the purpose of only returning the
	//      portion of idle allocation that would have been shared with the
	//      unfiltered results set. (See unit tests 5.a,b,c)
	// 3. ignore allocation if it fails any of the FilterFuncs
	// 4. generate aggregation key and insert allocation into the output set
	// 5. if there are shared allocations, compute sharing coefficients on
	//    the aggregated set, then share allocation accordingly
	// 6. if the merge idle option is enabled, merge any remaining idle
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
		// Profiler: as.Profiler,
		Window: as.Window.Clone(),
	}

	// idleSet will be shared among aggSet after initial aggregation
	// is complete
	idleSet := &AllocationSet{
		// Profiler: as.Profiler,
		Window: as.Window.Clone(),
	}

	// shareSet will be shared among aggSet after initial aggregation
	// is complete
	shareSet := &AllocationSet{
		// Profiler: as.Profiler
		Window: as.Window.Clone(),
	}

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
				TotalCost:  totalSharedCost,
			})
		}
	}

	as.Lock()
	defer as.Unlock()

	// Loop and find all of the idle and shared allocations initially. Add
	// them to their respective sets, removing them from the set of
	// allocations to aggregate.
	for _, alloc := range as.allocations {
		cluster, err := alloc.Properties.GetCluster()
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: missing cluster for allocation: %s", alloc.Name)
			return err
		}

		// Idle allocation doesn't get aggregated, so it can be passed through,
		// whether or not it is shared. If it is shared, it is put in idleSet
		// because shareSet may be split by different rules (even/weighted).
		if alloc.IsIdle() {
			// Can't recursively call Delete() due to lock acquisition
			delete(as.idleKeys, alloc.Name)
			delete(as.allocations, alloc.Name)

			if options.ShareIdle == ShareEven || options.ShareIdle == ShareWeighted {
				idleSet.Insert(alloc)
			} else {
				aggSet.Insert(alloc)
			}
		}

		// If any of the share funcs succeed, share the allocation. Do this
		// prior to filtering so that shared namespaces, etc do not get
		// filtered out before we have a chance to share them.
		for _, sf := range options.ShareFuncs {
			if sf(alloc) {
				// Can't recursively call Delete() due to lock acquisition
				delete(as.idleKeys, alloc.Name)
				delete(as.allocations, alloc.Name)

				alloc.Name = fmt.Sprintf("%s/%s", cluster, SharedSuffix)
				shareSet.Insert(alloc)
				break
			}
		}
	}

	if len(as.allocations) == 0 {
		log.Warningf("ETL: AggregateBy: no allocations to aggregate")
		emptySet := &AllocationSet{
			Window: as.Window.Clone(),
		}
		as.allocations = emptySet.allocations
		return nil
	}

	// In order to correctly apply idle and shared resource coefficients appropriately,
	// we need to determine the coefficients for the full set of data. The ensures that
	// the ratios are maintained through filtering.
	// idleCoefficients are organized by [cluster][allocation][resource]=coeff
	var idleCoefficients map[string]map[string]map[string]float64
	// shareCoefficients are organized by [allocation][resource]=coeff (no cluster)
	var shareCoefficients map[string]float64
	var err error

	if idleSet.Length() > 0 && options.ShareIdle != ShareNone {
		idleCoefficients, err = computeIdleCoeffs(properties, options, as)
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: compute idle coeff: %s", err)
			return err
		}
	}

	// If we're not sharing idle and we're filtering, we need to track the
	// amount of each idle allocation to "delete" in order to maintain parity
	// with the idle-allocated results. That is, we want to return only the
	// idle cost that would have been shared with the unfiltered portion of
	// the results, not the full idle cost.
	var idleFiltrationCoefficients map[string]map[string]map[string]float64
	if len(options.FilterFuncs) > 0 && options.ShareIdle == ShareNone {
		idleFiltrationCoefficients, err = computeIdleCoeffs(properties, options, as)
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: compute idle coeff: %s", err)
			return err
		}
	}

	for _, alloc := range as.allocations {
		cluster, err := alloc.Properties.GetCluster()
		if err != nil {
			log.Warningf("AllocationSet.AggregateBy: missing cluster for allocation: %s", alloc.Name)
			return err
		}

		skip := false

		// If any of the filter funcs fail, immediately skip the allocation.
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

		// Split idle allocations and distribute among aggregated allocations
		// NOTE: if idle allocation is off (i.e. ShareIdle == ShareNone) then all
		// idle allocations will be in the aggSet at this point.
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
				alloc.TotalCost += idleCPUCost + idleGPUCost + idleRAMCost
			}
		}

		key, err := alloc.generateKey(properties)
		if err != nil {
			return err
		}

		alloc.Name = key
		if options.MergeUnallocated && alloc.IsUnallocated() {
			alloc.Name = UnallocatedSuffix
		}

		aggSet.Insert(alloc)
	}

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

	// If we have filters, and so have computed coefficients for scaling idle
	// allocation costs by cluster, then use those coefficients to scale down
	// each idle coefficient in the aggSet.
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
				idleAlloc.TotalCost = idleAlloc.CPUCost + idleAlloc.RAMCost
			}

		}
	}

	// Split shared allocations and distribute among aggregated allocations
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

				alloc.SharedCost += sharedAlloc.TotalCost * shareCoefficients[alloc.Name]
				alloc.TotalCost += sharedAlloc.TotalCost * shareCoefficients[alloc.Name]
			}
		}
	}

	// Combine all idle allocations into a single "__idle__" allocation
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
			coeffs[name] += alloc.TotalCost
			total += alloc.TotalCost
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

func (alloc *Allocation) generateKey(properties Properties) (string, error) {
	// Names will ultimately be joined into a single name, which uniquely
	// identifies allocations.
	names := []string{}

	if properties.HasCluster() {
		cluster, err := alloc.Properties.GetCluster()
		if err != nil {
			return "", err
		}
		names = append(names, cluster)
	}

	if properties.HasNode() {
		node, err := alloc.Properties.GetNode()
		if err != nil {
			return "", err
		}
		names = append(names, node)
	}

	if properties.HasNamespace() {
		namespace, err := alloc.Properties.GetNamespace()
		if err != nil {
			return "", err
		}
		names = append(names, namespace)
	}

	if properties.HasControllerKind() {
		controllerKind, err := alloc.Properties.GetControllerKind()
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
			controllerKind, err := alloc.Properties.GetControllerKind()
			if err == nil {
				names = append(names, controllerKind)
			}
		}

		controller, err := alloc.Properties.GetController()
		if err != nil {
			// Indicate that allocation has no controller
			controller = UnallocatedSuffix
		}

		names = append(names, controller)
	}

	if properties.HasPod() {
		pod, err := alloc.Properties.GetPod()
		if err != nil {
			return "", err
		}

		names = append(names, pod)
	}

	if properties.HasContainer() {
		container, err := alloc.Properties.GetContainer()
		if err != nil {
			return "", err
		}

		names = append(names, container)
	}

	if properties.HasService() {
		services, err := alloc.Properties.GetServices()
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

	if properties.HasLabel() {
		labels, err := alloc.Properties.GetLabels() // labels that the individual allocation possesses
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

	return &AllocationSet{
		allocations: allocs,
		Window:      as.Window.Clone(),
	}
}

// Delete removes the allocation with the given name from the set
func (as *AllocationSet) Delete(name string) {
	if as == nil {
		return
	}

	as.Lock()
	defer as.Unlock()
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
	return as.insert(that, false)
}

func (as *AllocationSet) insert(that *Allocation, accumulate bool) error {
	if as.IsEmpty() {
		as.Lock()
		as.allocations = map[string]*Allocation{}
		as.idleKeys = map[string]bool{}
		as.Unlock()
	}

	as.Lock()
	defer as.Unlock()

	// Add the given Allocation to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := as.allocations[that.Name]; !ok {
		as.allocations[that.Name] = that
	} else {
		as.allocations[that.Name].add(that, false, accumulate)
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

func (as *AllocationSet) Set(alloc *Allocation) error {
	if as.IsEmpty() {
		as.Lock()
		as.allocations = map[string]*Allocation{}
		as.idleKeys = map[string]bool{}
		as.Unlock()
	}

	as.Lock()
	defer as.Unlock()

	as.allocations[alloc.Name] = alloc

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
		tc += a.TotalCost
	}
	return tc
}

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

	if that.Start().Before(as.End()) {
		timefmt := "2006-01-02T15:04:05"
		err := fmt.Sprintf("that [%s, %s); that [%s, %s)\n", as.Start().Format(timefmt), as.End().Format(timefmt), that.Start().Format(timefmt), that.End().Format(timefmt))
		return nil, fmt.Errorf("error accumulating AllocationSets: overlapping windows: %s", err)
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
		// Change Start and End to match the new window. However, do not
		// change Minutes because that will be accounted for during the
		// insert step, if in fact there are two allocations to add.
		alloc.Start = start
		alloc.End = end

		err := acc.insert(alloc, true)
		if err != nil {
			return nil, err
		}
	}

	for _, alloc := range that.allocations {
		// Change Start and End to match the new window. However, do not
		// change Minutes because that will be accounted for during the
		// insert step, if in fact there are two allocations to add.
		alloc.Start = start
		alloc.End = end

		err := acc.insert(alloc, true)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

type AllocationSetRange struct {
	sync.RWMutex
	allocations []*AllocationSet
}

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

func (asr *AllocationSetRange) Get(i int) (*AllocationSet, error) {
	if i < 0 || i >= len(asr.allocations) {
		return nil, fmt.Errorf("AllocationSetRange: index out of range: %d", i)
	}

	asr.RLock()
	defer asr.RUnlock()
	return asr.allocations[i], nil
}

func (asr *AllocationSetRange) Length() int {
	if asr == nil || asr.allocations == nil {
		return 0
	}

	asr.RLock()
	defer asr.RUnlock()
	return len(asr.allocations)
}

func (asr *AllocationSetRange) MarshalJSON() ([]byte, error) {
	asr.RLock()
	asr.RUnlock()
	return json.Marshal(asr.allocations)
}

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
