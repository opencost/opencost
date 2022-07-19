package kubecost

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util"
	"github.com/opencost/opencost/pkg/util/json"
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
// TODO:CLEANUP consider dropping name in favor of just Allocation and an
// Assets-style key() function for AllocationSet.
type Allocation struct {
	Name                       string                `json:"name"`
	Properties                 *AllocationProperties `json:"properties,omitempty"`
	Window                     Window                `json:"window"`
	Start                      time.Time             `json:"start"`
	End                        time.Time             `json:"end"`
	CPUCoreHours               float64               `json:"cpuCoreHours"`
	CPUCoreRequestAverage      float64               `json:"cpuCoreRequestAverage"`
	CPUCoreUsageAverage        float64               `json:"cpuCoreUsageAverage"`
	CPUCost                    float64               `json:"cpuCost"`
	CPUCostAdjustment          float64               `json:"cpuCostAdjustment"`
	GPUHours                   float64               `json:"gpuHours"`
	GPUCost                    float64               `json:"gpuCost"`
	GPUCostAdjustment          float64               `json:"gpuCostAdjustment"`
	NetworkTransferBytes       float64               `json:"networkTransferBytes"`
	NetworkReceiveBytes        float64               `json:"networkReceiveBytes"`
	NetworkCost                float64               `json:"networkCost"`
	NetworkCostAdjustment      float64               `json:"networkCostAdjustment"`
	LoadBalancerCost           float64               `json:"loadBalancerCost"`
	LoadBalancerCostAdjustment float64               `json:"loadBalancerCostAdjustment"`
	PVs                        PVAllocations         `json:"-"`
	PVCostAdjustment           float64               `json:"pvCostAdjustment"`
	RAMByteHours               float64               `json:"ramByteHours"`
	RAMBytesRequestAverage     float64               `json:"ramByteRequestAverage"`
	RAMBytesUsageAverage       float64               `json:"ramByteUsageAverage"`
	RAMCost                    float64               `json:"ramCost"`
	RAMCostAdjustment          float64               `json:"ramCostAdjustment"`
	SharedCost                 float64               `json:"sharedCost"`
	ExternalCost               float64               `json:"externalCost"`
	// RawAllocationOnly is a pointer so if it is not present it will be
	// marshalled as null rather than as an object with Go default values.
	RawAllocationOnly *RawAllocationOnlyData `json:"rawAllocationOnly"`
}

// RawAllocationOnlyData is information that only belong in "raw" Allocations,
// those which have not undergone aggregation, accumulation, or any other form
// of combination to produce a new Allocation from other Allocations.
//
// Max usage data belongs here because computing the overall maximum from two
// or more Allocations is a non-trivial operation that cannot be defined without
// maintaining a large amount of state. Consider the following example:
// _______________________________________________
//
// A1 Using 3 CPU    ----      -----     ------
// A2 Using 2 CPU      ----      -----      ----
// A3 Using 1 CPU         ---       --
// _______________________________________________
//                   Time ---->
//
// The logical maximum CPU usage is 5, but this cannot be calculated iteratively,
// which is how we calculate aggregations and accumulations of Allocations currently.
// This becomes a problem I could call "maximum sum of overlapping intervals" and is
// essentially a variant of an interval scheduling algorithm.
//
// If we had types to differentiate between regular Allocations and AggregatedAllocations
// then this type would be unnecessary and its fields would go into the regular Allocation
// and not in the AggregatedAllocation.
type RawAllocationOnlyData struct {
	CPUCoreUsageMax  float64 `json:"cpuCoreUsageMax"`
	RAMBytesUsageMax float64 `json:"ramByteUsageMax"`
}

// PVAllocations is a map of Disk Asset Identifiers to the
// usage of them by an Allocation as recorded in a PVAllocation
type PVAllocations map[PVKey]*PVAllocation

// Clone creates a deep copy of a PVAllocations
func (pv *PVAllocations) Clone() PVAllocations {
	if pv == nil || *pv == nil {
		return nil
	}
	apv := *pv
	clonePV := make(map[PVKey]*PVAllocation, len(apv))
	for k, v := range apv {
		clonePV[k] = &PVAllocation{
			ByteHours: v.ByteHours,
			Cost:      v.Cost,
		}
	}
	return clonePV
}

// Add adds contents of that to the calling PVAllocations
func (pv *PVAllocations) Add(that PVAllocations) PVAllocations {
	apv := pv.Clone()
	if that != nil {
		if apv == nil {
			apv = PVAllocations{}
		}
		for pvKey, thatPVAlloc := range that {
			apvAlloc, ok := apv[pvKey]
			if !ok {
				apvAlloc = &PVAllocation{}
			}
			apvAlloc.Cost += thatPVAlloc.Cost
			apvAlloc.ByteHours += thatPVAlloc.ByteHours
			apv[pvKey] = apvAlloc
		}
	}
	return apv
}

// PVKey for identifying Disk type assets
type PVKey struct {
	Cluster string `json:"cluster"`
	Name    string `json:"name"`
}

// PVAllocation contains the byte hour usage
// and cost of an Allocation for a single PV
type PVAllocation struct {
	ByteHours float64 `json:"byteHours"`
	Cost      float64 `json:"cost"`
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
		Name:                       a.Name,
		Properties:                 a.Properties.Clone(),
		Window:                     a.Window.Clone(),
		Start:                      a.Start,
		End:                        a.End,
		CPUCoreHours:               a.CPUCoreHours,
		CPUCoreRequestAverage:      a.CPUCoreRequestAverage,
		CPUCoreUsageAverage:        a.CPUCoreUsageAverage,
		CPUCost:                    a.CPUCost,
		CPUCostAdjustment:          a.CPUCostAdjustment,
		GPUHours:                   a.GPUHours,
		GPUCost:                    a.GPUCost,
		GPUCostAdjustment:          a.GPUCostAdjustment,
		NetworkTransferBytes:       a.NetworkTransferBytes,
		NetworkReceiveBytes:        a.NetworkReceiveBytes,
		NetworkCost:                a.NetworkCost,
		NetworkCostAdjustment:      a.NetworkCostAdjustment,
		LoadBalancerCost:           a.LoadBalancerCost,
		LoadBalancerCostAdjustment: a.LoadBalancerCostAdjustment,
		PVs:                        a.PVs.Clone(),
		PVCostAdjustment:           a.PVCostAdjustment,
		RAMByteHours:               a.RAMByteHours,
		RAMBytesRequestAverage:     a.RAMBytesRequestAverage,
		RAMBytesUsageAverage:       a.RAMBytesUsageAverage,
		RAMCost:                    a.RAMCost,
		RAMCostAdjustment:          a.RAMCostAdjustment,
		SharedCost:                 a.SharedCost,
		ExternalCost:               a.ExternalCost,
		RawAllocationOnly:          a.RawAllocationOnly.Clone(),
	}
}

// Clone returns a deep copy of the given RawAllocationOnlyData
func (r *RawAllocationOnlyData) Clone() *RawAllocationOnlyData {
	if r == nil {
		return nil
	}

	return &RawAllocationOnlyData{
		CPUCoreUsageMax:  r.CPUCoreUsageMax,
		RAMBytesUsageMax: r.RAMBytesUsageMax,
	}
}

// Equal returns true if the values held in the given Allocation precisely
// match those of the receiving Allocation. nil does not match nil. Floating
// point values need to match according to util.IsApproximately, which accounts
// for small, reasonable floating point error margins.
func (a *Allocation) Equal(that *Allocation) bool {
	if a == nil || that == nil {
		return false
	}

	if a.Name != that.Name {
		return false
	}
	if !a.Properties.Equal(that.Properties) {
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
	if !util.IsApproximately(a.CPUCostAdjustment, that.CPUCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.GPUHours, that.GPUHours) {
		return false
	}
	if !util.IsApproximately(a.GPUCost, that.GPUCost) {
		return false
	}
	if !util.IsApproximately(a.GPUCostAdjustment, that.GPUCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.NetworkTransferBytes, that.NetworkTransferBytes) {
		return false
	}
	if !util.IsApproximately(a.NetworkReceiveBytes, that.NetworkReceiveBytes) {
		return false
	}
	if !util.IsApproximately(a.NetworkCost, that.NetworkCost) {
		return false
	}
	if !util.IsApproximately(a.NetworkCostAdjustment, that.NetworkCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.LoadBalancerCost, that.LoadBalancerCost) {
		return false
	}
	if !util.IsApproximately(a.LoadBalancerCostAdjustment, that.LoadBalancerCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.PVCostAdjustment, that.PVCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.RAMByteHours, that.RAMByteHours) {
		return false
	}
	if !util.IsApproximately(a.RAMCost, that.RAMCost) {
		return false
	}
	if !util.IsApproximately(a.RAMCostAdjustment, that.RAMCostAdjustment) {
		return false
	}
	if !util.IsApproximately(a.SharedCost, that.SharedCost) {
		return false
	}
	if !util.IsApproximately(a.ExternalCost, that.ExternalCost) {
		return false
	}

	if a.RawAllocationOnly == nil && that.RawAllocationOnly != nil {
		return false
	}
	if a.RawAllocationOnly != nil && that.RawAllocationOnly == nil {
		return false
	}

	if a.RawAllocationOnly != nil && that.RawAllocationOnly != nil {
		if !util.IsApproximately(a.RawAllocationOnly.CPUCoreUsageMax, that.RawAllocationOnly.CPUCoreUsageMax) {
			return false
		}
		if !util.IsApproximately(a.RawAllocationOnly.RAMBytesUsageMax, that.RawAllocationOnly.RAMBytesUsageMax) {
			return false
		}
	}

	aPVs := a.PVs
	thatPVs := that.PVs
	if len(aPVs) == len(thatPVs) {
		for k, pv := range aPVs {
			tv, ok := thatPVs[k]
			if !ok || *tv != *pv {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

// TotalCost is the total cost of the Allocation including adjustments
func (a *Allocation) TotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.CPUTotalCost() + a.GPUTotalCost() + a.RAMTotalCost() + a.PVTotalCost() + a.NetworkTotalCost() + a.LBTotalCost() + a.SharedTotalCost() + a.ExternalCost
}

// CPUTotalCost calculates total CPU cost of Allocation including adjustment
func (a *Allocation) CPUTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.CPUCost + a.CPUCostAdjustment
}

// GPUTotalCost calculates total GPU cost of Allocation including adjustment
func (a *Allocation) GPUTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.GPUCost + a.GPUCostAdjustment
}

// RAMTotalCost calculates total RAM cost of Allocation including adjustment
func (a *Allocation) RAMTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.RAMCost + a.RAMCostAdjustment
}

// PVTotalCost calculates total PV cost of Allocation including adjustment
func (a *Allocation) PVTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.PVCost() + a.PVCostAdjustment
}

// NetworkTotalCost calculates total Network cost of Allocation including adjustment
func (a *Allocation) NetworkTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.NetworkCost + a.NetworkCostAdjustment
}

// LBTotalCost calculates total LB cost of Allocation including adjustment
// TODO deprecate
func (a *Allocation) LBTotalCost() float64 {
	return a.LoadBalancerTotalCost()
}

// LoadBalancerTotalCost calculates total LB cost of Allocation including adjustment
func (a *Allocation) LoadBalancerTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.LoadBalancerCost + a.LoadBalancerCostAdjustment
}

// SharedTotalCost calculates total shared cost of Allocation including adjustment
func (a *Allocation) SharedTotalCost() float64 {
	if a == nil {
		return 0.0
	}

	return a.SharedCost
}

// PVCost calculate cumulative cost of all PVs that Allocation is attached to
func (a *Allocation) PVCost() float64 {
	if a == nil {
		return 0.0
	}

	cost := 0.0
	for _, pv := range a.PVs {
		cost += pv.Cost
	}
	return cost
}

// PVByteHours calculate cumulative ByteHours of all PVs that Allocation is attached to
func (a *Allocation) PVByteHours() float64 {
	if a == nil {
		return 0.0
	}

	byteHours := 0.0
	for _, pv := range a.PVs {
		byteHours += pv.ByteHours
	}
	return byteHours
}

// CPUEfficiency is the ratio of usage to request. If there is no request and
// no usage or cost, then efficiency is zero. If there is no request, but there
// is usage or cost, then efficiency is 100%.
func (a *Allocation) CPUEfficiency() float64 {
	if a == nil {
		return 0.0
	}

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
	if a == nil {
		return 0.0
	}

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
	if a == nil {
		return 0.0
	}

	if a.RAMTotalCost()+a.CPUTotalCost() > 0 {
		ramCostEff := a.RAMEfficiency() * a.RAMTotalCost()
		cpuCostEff := a.CPUEfficiency() * a.CPUTotalCost()
		return (ramCostEff + cpuCostEff) / (a.CPUTotalCost() + a.RAMTotalCost())
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

// GPUs converts the Allocation's GPUHours into average GPUs
func (a *Allocation) GPUs() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.GPUHours / (a.Minutes() / 60.0)
}

// PVBytes converts the Allocation's PVByteHours into average PVBytes
func (a *Allocation) PVBytes() float64 {
	if a.Minutes() <= 0.0 {
		return 0.0
	}
	return a.PVByteHours() / (a.Minutes() / 60.0)
}

// ResetAdjustments sets all cost adjustment fields to zero
func (a *Allocation) ResetAdjustments() {
	if a == nil {
		return
	}

	a.CPUCostAdjustment = 0.0
	a.GPUCostAdjustment = 0.0
	a.RAMCostAdjustment = 0.0
	a.PVCostAdjustment = 0.0
	a.NetworkCostAdjustment = 0.0
	a.LoadBalancerCostAdjustment = 0.0
}

// MarshalJSON implements json.Marshaler interface
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
	jsonEncodeFloat64(buffer, "cpuCostAdjustment", a.CPUCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "cpuEfficiency", a.CPUEfficiency(), ",")
	jsonEncodeFloat64(buffer, "gpuCount", a.GPUs(), ",")
	jsonEncodeFloat64(buffer, "gpuHours", a.GPUHours, ",")
	jsonEncodeFloat64(buffer, "gpuCost", a.GPUCost, ",")
	jsonEncodeFloat64(buffer, "gpuCostAdjustment", a.GPUCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "networkTransferBytes", a.NetworkTransferBytes, ",")
	jsonEncodeFloat64(buffer, "networkReceiveBytes", a.NetworkReceiveBytes, ",")
	jsonEncodeFloat64(buffer, "networkCost", a.NetworkCost, ",")
	jsonEncodeFloat64(buffer, "networkCostAdjustment", a.NetworkCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "loadBalancerCost", a.LoadBalancerCost, ",")
	jsonEncodeFloat64(buffer, "loadBalancerCostAdjustment", a.LoadBalancerCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "pvBytes", a.PVBytes(), ",")
	jsonEncodeFloat64(buffer, "pvByteHours", a.PVByteHours(), ",")
	jsonEncodeFloat64(buffer, "pvCost", a.PVCost(), ",")
	jsonEncode(buffer, "pvs", a.PVs, ",") // Todo Sean: this does not work properly
	jsonEncodeFloat64(buffer, "pvCostAdjustment", a.PVCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "ramBytes", a.RAMBytes(), ",")
	jsonEncodeFloat64(buffer, "ramByteRequestAverage", a.RAMBytesRequestAverage, ",")
	jsonEncodeFloat64(buffer, "ramByteUsageAverage", a.RAMBytesUsageAverage, ",")
	jsonEncodeFloat64(buffer, "ramByteHours", a.RAMByteHours, ",")
	jsonEncodeFloat64(buffer, "ramCost", a.RAMCost, ",")
	jsonEncodeFloat64(buffer, "ramCostAdjustment", a.RAMCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "ramEfficiency", a.RAMEfficiency(), ",")
	jsonEncodeFloat64(buffer, "sharedCost", a.SharedCost, ",")
	jsonEncodeFloat64(buffer, "externalCost", a.ExternalCost, ",")
	jsonEncodeFloat64(buffer, "totalCost", a.TotalCost(), ",")
	jsonEncodeFloat64(buffer, "totalEfficiency", a.TotalEfficiency(), ",")
	jsonEncode(buffer, "rawAllocationOnly", a.RawAllocationOnly, "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// Resolution returns the duration of time covered by the Allocation
func (a *Allocation) Resolution() time.Duration {
	return a.End.Sub(a.Start)
}

// IsAggregated is true if the given Allocation has been aggregated, which we
// define by a lack of AllocationProperties.
func (a *Allocation) IsAggregated() bool {
	return a == nil || a.Properties == nil
}

// IsExternal is true if the given Allocation represents external costs.
func (a *Allocation) IsExternal() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, ExternalSuffix)
}

// IsIdle is true if the given Allocation represents idle costs.
func (a *Allocation) IsIdle() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, IdleSuffix)
}

// IsUnallocated is true if the given Allocation represents unallocated costs.
func (a *Allocation) IsUnallocated() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, UnallocatedSuffix)
}

// IsUnmounted is true if the given Allocation represents unmounted volume costs.
func (a *Allocation) IsUnmounted() bool {
	if a == nil {
		return false
	}

	return strings.Contains(a.Name, UnmountedSuffix)
}

// Minutes returns the number of minutes the Allocation represents, as defined
// by the difference between the end and start times.
func (a *Allocation) Minutes() float64 {
	if a == nil {
		return 0.0
	}

	return a.End.Sub(a.Start).Minutes()
}

// Share adds the TotalCost of the given Allocation to the SharedCost of the
// receiving Allocation. No Start, End, Window, or AllocationProperties are considered.
// Neither Allocation is mutated; a new Allocation is always returned.
func (a *Allocation) Share(that *Allocation) (*Allocation, error) {
	if that == nil {
		return a.Clone(), nil
	}

	if a == nil {
		return nil, fmt.Errorf("cannot share with nil Allocation")
	}

	agg := a.Clone()
	agg.SharedCost += that.TotalCost()

	return agg, nil
}

// String represents the given Allocation as a string
func (a *Allocation) String() string {
	if a == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%s%s=%.2f", a.Name, NewWindow(&a.Start, &a.End), a.TotalCost())
}

func (a *Allocation) add(that *Allocation) {
	if a == nil {
		log.Warnf("Allocation.AggregateBy: trying to add a nil receiver")
		return
	}

	// Generate keys for each allocation to allow for special logic to set the controller
	// in the case of keys matching but controllers not matching.
	aggByForKey := []string{"cluster", "node", "namespace", "pod", "container"}
	leftKey := a.generateKey(aggByForKey, nil)
	rightKey := that.generateKey(aggByForKey, nil)
	leftProperties := a.Properties
	rightProperties := that.Properties

	// Preserve string properties that are matching between the two allocations
	a.Properties = a.Properties.Intersection(that.Properties)

	// Overwrite regular intersection logic for the controller name property in the
	// case that the Allocation keys are the same but the controllers are not.
	if leftKey == rightKey &&
		leftProperties != nil &&
		rightProperties != nil &&
		leftProperties.Controller != rightProperties.Controller {
		if leftProperties.Controller == "" {
			a.Properties.Controller = rightProperties.Controller
		} else if rightProperties.Controller == "" {
			a.Properties.Controller = leftProperties.Controller
		} else {
			controllers := []string{
				leftProperties.Controller,
				rightProperties.Controller,
			}
			sort.Strings(controllers)
			a.Properties.Controller = controllers[0]
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

	// Convert cumulative request and usage back into rates
	// TODO:TEST write a unit test that fails if this is done incorrectly
	if a.Minutes() > 0 {
		a.CPUCoreRequestAverage = cpuReqCoreMins / a.Minutes()
		a.CPUCoreUsageAverage = cpuUseCoreMins / a.Minutes()
		a.RAMBytesRequestAverage = ramReqByteMins / a.Minutes()
		a.RAMBytesUsageAverage = ramUseByteMins / a.Minutes()
	} else {
		a.CPUCoreRequestAverage = 0.0
		a.CPUCoreUsageAverage = 0.0
		a.RAMBytesRequestAverage = 0.0
		a.RAMBytesUsageAverage = 0.0
	}

	// Sum all cumulative resource fields
	a.CPUCoreHours += that.CPUCoreHours
	a.GPUHours += that.GPUHours
	a.RAMByteHours += that.RAMByteHours
	a.NetworkTransferBytes += that.NetworkTransferBytes
	a.NetworkReceiveBytes += that.NetworkReceiveBytes

	// Sum all cumulative cost fields
	a.CPUCost += that.CPUCost
	a.GPUCost += that.GPUCost
	a.RAMCost += that.RAMCost
	a.NetworkCost += that.NetworkCost
	a.LoadBalancerCost += that.LoadBalancerCost
	a.SharedCost += that.SharedCost
	a.ExternalCost += that.ExternalCost

	// Sum PVAllocations
	a.PVs = a.PVs.Add(that.PVs)

	// Sum all cumulative adjustment fields
	a.CPUCostAdjustment += that.CPUCostAdjustment
	a.RAMCostAdjustment += that.RAMCostAdjustment
	a.GPUCostAdjustment += that.GPUCostAdjustment
	a.PVCostAdjustment += that.PVCostAdjustment
	a.NetworkCostAdjustment += that.NetworkCostAdjustment
	a.LoadBalancerCostAdjustment += that.LoadBalancerCostAdjustment

	// Any data that is in a "raw allocation only" is not valid in any
	// sort of cumulative Allocation (like one that is added).
	a.RawAllocationOnly = nil
}

// AllocationSet stores a set of Allocations, each with a unique name, that share
// a window. An AllocationSet is mutable, so treat it like a threadsafe map.
type AllocationSet struct {
	sync.RWMutex
	allocations  map[string]*Allocation
	externalKeys map[string]bool
	idleKeys     map[string]bool
	FromSource   string // stores the name of the source used to compute the data
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
	AllocationTotalsStore AllocationTotalsStore
	Filter                AllocationFilter
	IdleByNode            bool
	LabelConfig           *LabelConfig
	MergeUnallocated      bool
	Reconcile             bool
	ReconcileNetwork      bool
	ShareFuncs            []AllocationMatchFunc
	ShareIdle             string
	ShareSplit            string
	SharedHourlyCosts     map[string]float64
	SplitIdle             bool
}

// AggregateBy aggregates the Allocations in the given AllocationSet by the given
// AllocationProperty. This will only be legal if the AllocationSet is divisible by the
// given AllocationProperty; e.g. Containers can be divided by Namespace, but not vice-a-versa.
func (as *AllocationSet) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	// The order of operations for aggregating allocations is as follows:
	//
	//  1. Partition external, idle, and shared allocations into separate sets.
	//     Also, create the aggSet into which the results will be aggregated.
	//
	//  2. Compute sharing coefficients for idle and shared resources
	//     a) if idle allocation is to be shared, compute idle coefficients
	//     b) if idle allocation is NOT shared, but filters are present, compute
	//        idle filtration coefficients for the purpose of only returning the
	//        portion of idle allocation that would have been shared with the
	//        unfiltered results. (See unit tests 5.a,b,c)
	//     c) generate shared allocation for them given shared overhead, which
	//        must happen after (2a) and (2b)
	//     d) if there are shared resources, compute share coefficients
	//
	//  3. Drop any allocation that fails any of the filters
	//
	//  4. Distribute idle allocations according to the idle coefficients
	//
	//  5. Generate aggregation key and insert allocation into the output set
	//
	//  6. If idle is shared and resources are shared, some idle might be shared
	//     with a shared resource. Distribute that to the shared resources
	//     prior to sharing them with the aggregated results.
	//
	//  7. Apply idle filtration coefficients from step (2b)
	//
	//  8. Distribute shared allocations according to the share coefficients.
	//
	//  9. If there are external allocations that can be aggregated into
	//     the output (i.e. they can be used to generate a valid key for
	//     the given properties) then aggregate; otherwise... ignore them?
	//
	// 10. Distribute any undistributed idle, in the case that idle
	//     coefficients end up being zero and some idle is not shared.
	//
	// 11. If the merge idle option is enabled, merge any remaining idle
	//     allocations into a single idle allocation. If there was any idle
	//	   whose costs were not distributed because there was no usage of a
	//     specific resource type, re-add the idle to the aggregation with
	//     only that type.

	if as.IsEmpty() {
		return nil
	}

	if options == nil {
		options = &AllocationAggregationOptions{}
	}

	if options.LabelConfig == nil {
		options.LabelConfig = NewLabelConfig()
	}

	// idleFiltrationCoefficients relies on this being explicitly set
	if options.ShareIdle != ShareWeighted {
		options.ShareIdle = ShareNone
	}

	// Pre-flatten the filter so we can just check == nil to see if there are
	// filters.
	if options.Filter != nil {
		options.Filter = options.Filter.Flattened()
	}

	var allocatedTotalsMap map[string]map[string]float64

	// If aggregateBy is nil, we don't aggregate anything. On the other hand,
	// an empty slice implies that we should aggregate everything. See
	// generateKey for why that makes sense.
	shouldAggregate := aggregateBy != nil
	shouldFilter := options.Filter != nil
	shouldShare := len(options.SharedHourlyCosts) > 0 || len(options.ShareFuncs) > 0
	if !shouldAggregate && !shouldFilter && !shouldShare && options.ShareIdle == ShareNone {
		// There is nothing for AggregateBy to do, so simply return nil
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

		// Idle allocations should be separated into idleSet if they are to be
		// shared later on. If they are not to be shared, then add them to the
		// aggSet like any other allocation.
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
		// aggregation and filtering. That is, if any of the ShareFuncs return
		// true for the allocation, then move it to shareSet.
		for _, sf := range options.ShareFuncs {
			if sf(alloc) {
				delete(as.idleKeys, alloc.Name)
				delete(as.allocations, alloc.Name)
				shareSet.Insert(alloc)
				break
			}
		}
	}

	// It's possible that no more un-shared, non-idle, non-external allocations
	// remain at this point. This always results in an emptySet, so return early.
	if len(as.allocations) == 0 {
		emptySet := &AllocationSet{
			Window: as.Window.Clone(),
		}
		as.allocations = emptySet.allocations
		return nil
	}

	// (2) In order to correctly share idle and shared costs, we first compute
	// sharing coefficients, which represent the proportion of each cost to
	// share with each allocation. Idle allocations are shared per-cluster or per-node,
	// per-allocation, and per-resource, while shared resources are shared per-
	// allocation only.
	//
	// For an idleCoefficient example, the entries:
	//   [cluster1][cluster1/namespace1/pod1/container1][cpu] = 0.166667
	//   [cluster1][cluster1/namespace1/pod1/container1][gpu] = 0.166667
	//   [cluster1][cluster1/namespace1/pod1/container1][ram] = 0.687500
	// mean that the allocation "cluster1/namespace1/pod1/container1" will
	// receive 16.67% of cluster1's idle CPU and GPU costs and 68.75% of its
	// RAM costs.
	//
	// For a shareCoefficient example, the entries:
	//   [namespace2] = 0.666667
	//   [__filtered__] = 0.333333
	// mean that the post-aggregation allocation "namespace2" will receive
	// 66.67% of the shared resource costs, while the remaining 33.33% will
	// be filtered out, as they were shared with allocations that did not pass
	// one of the given filters.
	//
	// In order to maintain stable results when multiple operations are being
	// carried out (e.g. sharing idle, sharing resources, and filtering) these
	// coefficients are computed for the full set of allocations prior to
	// adding shared overhead and prior to applying filters.

	var err error

	// (2a) If there are idle costs to be shared, compute the coefficients for
	// sharing them among the non-idle, non-aggregated allocations (including
	// the shared allocations).
	var idleCoefficients map[string]map[string]map[string]float64
	if idleSet.Length() > 0 && options.ShareIdle != ShareNone {
		idleCoefficients, allocatedTotalsMap, err = computeIdleCoeffs(options, as, shareSet)
		if err != nil {
			log.Warnf("AllocationSet.AggregateBy: compute idle coeff: %s", err)
			return fmt.Errorf("error computing idle coefficients: %s", err)
		}
	}

	// (2b) If idle costs are not to be shared, but there are filters, then we
	// need to track the amount of each idle allocation to "filter" in order to
	// maintain parity with the results when idle is shared. That is, we want
	// to return only the idle costs that would have been shared with the given
	// results, even if the filter had not been applied.
	//
	// For example, consider these results from aggregating by namespace with
	// two clusters:
	//
	//  namespace1: 25.00
	//  namespace2: 30.00
	//  namespace3: 15.00
	//  idle:       30.00
	//
	// When we then filter by cluster==cluster1, namespaces 2 and 3 are
	// reduced by the amount that existed on cluster2. Then, idle must also be
	// reduced by the relevant amount:
	//
	//  namespace1: 25.00
	//  namespace2: 15.00
	//  idle:       20.00
	//
	// Note that this can happen for any field, not just cluster, so we again
	// need to track this on a per-cluster or per-node, per-allocation, per-resource basis.
	var idleFiltrationCoefficients map[string]map[string]map[string]float64
	if shouldFilter && options.ShareIdle == ShareNone {
		idleFiltrationCoefficients, _, err = computeIdleCoeffs(options, as, shareSet)
		if err != nil {
			return fmt.Errorf("error computing idle filtration coefficients: %s", err)
		}
	}

	// (2c) Convert SharedHourlyCosts to Allocations in the shareSet. This must
	// come after idle coefficients are computed so that allocations generated
	// by shared overhead do not skew the idle coefficient computation.
	for name, cost := range options.SharedHourlyCosts {
		if cost > 0.0 {
			hours := as.Resolution().Hours()

			// If set ends in the future, adjust hours accordingly
			diff := time.Since(as.End())
			if diff < 0.0 {
				hours += diff.Hours()
			}

			totalSharedCost := cost * hours

			shareSet.Insert(&Allocation{
				Name:       fmt.Sprintf("%s/%s", name, SharedSuffix),
				Start:      as.Start(),
				End:        as.End(),
				SharedCost: totalSharedCost,
				Properties: &AllocationProperties{Cluster: SharedSuffix}, // The allocation needs to belong to a cluster,but it really doesn't matter which one, so just make it clear.
			})
		}
	}

	// (2d) Compute share coefficients for shared resources. These are computed
	// after idle coefficients, and are computed for the aggregated allocations
	// of the main allocation set. See above for details and an example.
	var shareCoefficients map[string]float64
	if shareSet.Length() > 0 {
		shareCoefficients, err = computeShareCoeffs(aggregateBy, options, as)
		if err != nil {
			return fmt.Errorf("error computing share coefficients: %s", err)
		}
	}

	// (3-5) Filter, distribute idle cost, and aggregate (in that order)
	for _, alloc := range as.allocations {
		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "AllocationSet.AggregateBy: missing idleId for allocation: %s", alloc.Name)
		}

		skip := false

		// (3) If the allocation does not match the filter, immediately skip the
		// allocation.
		if options.Filter != nil {
			skip = !options.Filter.Matches(alloc)
		}
		if skip {
			// If we are tracking idle filtration coefficients, delete the
			// entry corresponding to the filtered allocation. (Deleting the
			// entry will result in that proportional amount being removed
			// from the idle allocation at the end of the process.)
			if idleFiltrationCoefficients != nil {
				if ifcc, ok := idleFiltrationCoefficients[idleId]; ok {
					delete(ifcc, alloc.Name)
				}
			}

			continue
		}

		// (4) Distribute idle allocations according to the idle coefficients
		// NOTE: if idle allocation is off (i.e. ShareIdle == ShareNone) then
		// all idle allocations will be in the aggSet at this point, so idleSet
		// will be empty and we won't enter this block.
		if idleSet.Length() > 0 {
			// Distribute idle allocations by coefficient per-idleId, per-allocation
			for _, idleAlloc := range idleSet.allocations {
				// Only share idle if the idleId matches; i.e. the allocation
				// is from the same idleId as the idle costs
				iaidleId, err := idleAlloc.getIdleId(options)
				if err != nil {
					log.Errorf("AllocationSet.AggregateBy: Idle allocation is missing idleId %s", idleAlloc.Name)
					return err
				}

				if iaidleId != idleId {
					continue
				}

				// Make sure idle coefficients exist
				if _, ok := idleCoefficients[idleId]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient: no idleId '%s' for '%s'", idleId, alloc.Name)
					continue
				}
				if _, ok := idleCoefficients[idleId][alloc.Name]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient for '%s'", alloc.Name)
					continue
				}

				alloc.CPUCoreHours += idleAlloc.CPUCoreHours * idleCoefficients[idleId][alloc.Name]["cpu"]
				alloc.GPUHours += idleAlloc.GPUHours * idleCoefficients[idleId][alloc.Name]["gpu"]
				alloc.RAMByteHours += idleAlloc.RAMByteHours * idleCoefficients[idleId][alloc.Name]["ram"]

				idleCPUCost := idleAlloc.CPUCost * idleCoefficients[idleId][alloc.Name]["cpu"]
				idleGPUCost := idleAlloc.GPUCost * idleCoefficients[idleId][alloc.Name]["gpu"]
				idleRAMCost := idleAlloc.RAMCost * idleCoefficients[idleId][alloc.Name]["ram"]
				alloc.CPUCost += idleCPUCost
				alloc.GPUCost += idleGPUCost
				alloc.RAMCost += idleRAMCost
			}
		}

		// (5) generate key to use for aggregation-by-key and allocation name
		key := alloc.generateKey(aggregateBy, options.LabelConfig)

		alloc.Name = key
		if options.MergeUnallocated && alloc.IsUnallocated() {
			alloc.Name = UnallocatedSuffix
		}

		// Inserting the allocation with the generated key for a name will
		// perform the actual basic aggregation step.
		aggSet.Insert(alloc)
	}

	// (6) If idle is shared and resources are shared, it's possible that some
	// amount of idle cost will be shared with a shared resource. Distribute
	// that idle allocation, if it exists, to the respective shared allocations
	// before sharing with the aggregated allocations.
	if idleSet.Length() > 0 && shareSet.Length() > 0 {
		for _, alloc := range shareSet.allocations {
			idleId, err := alloc.getIdleId(options)
			if err != nil {
				log.DedupedWarningf(3, "AllocationSet.AggregateBy: missing idleId for allocation: %s", alloc.Name)
			}
			// Distribute idle allocations by coefficient per-idleId, per-allocation
			for _, idleAlloc := range idleSet.allocations {
				// Only share idle if the idleId matches; i.e. the allocation
				// is from the same idleId as the idle costs
				iaidleId, _ := idleAlloc.getIdleId(options)

				if iaidleId != idleId {
					continue
				}

				// Make sure idle coefficients exist
				if _, ok := idleCoefficients[idleId]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient: no idleId '%s' for '%s'", idleId, alloc.Name)
					continue
				}
				if _, ok := idleCoefficients[idleId][alloc.Name]; !ok {
					log.Warnf("AllocationSet.AggregateBy: error getting idle coefficient for '%s'", alloc.Name)
					continue
				}

				alloc.CPUCoreHours += idleAlloc.CPUCoreHours * idleCoefficients[idleId][alloc.Name]["cpu"]
				alloc.GPUHours += idleAlloc.GPUHours * idleCoefficients[idleId][alloc.Name]["gpu"]
				alloc.RAMByteHours += idleAlloc.RAMByteHours * idleCoefficients[idleId][alloc.Name]["ram"]

				idleCPUCost := idleAlloc.CPUCost * idleCoefficients[idleId][alloc.Name]["cpu"]
				idleGPUCost := idleAlloc.GPUCost * idleCoefficients[idleId][alloc.Name]["gpu"]
				idleRAMCost := idleAlloc.RAMCost * idleCoefficients[idleId][alloc.Name]["ram"]
				alloc.CPUCost += idleCPUCost
				alloc.GPUCost += idleGPUCost
				alloc.RAMCost += idleRAMCost
			}
		}
	}

	// groupingIdleFiltrationCoeffs is used to track per-resource idle
	// coefficients on a cluster-by-cluster or node-by-node basis depending
	// on the IdleByNode option. It is, essentailly, an aggregation of
	// idleFiltrationCoefficients after they have been
	// filtered above (in step 3)
	var groupingIdleFiltrationCoeffs map[string]map[string]float64
	if idleFiltrationCoefficients != nil {
		groupingIdleFiltrationCoeffs = map[string]map[string]float64{}

		for idleId, m := range idleFiltrationCoefficients {
			if _, ok := groupingIdleFiltrationCoeffs[idleId]; !ok {
				groupingIdleFiltrationCoeffs[idleId] = map[string]float64{
					"cpu": 0.0,
					"gpu": 0.0,
					"ram": 0.0,
				}
			}

			for _, n := range m {
				for resource, val := range n {
					groupingIdleFiltrationCoeffs[idleId][resource] += val
				}
			}
		}
	}

	// (7) If we have both un-shared idle allocations and idle filtration
	// coefficients then apply those. See step (2b) for an example.
	if len(aggSet.idleKeys) > 0 && groupingIdleFiltrationCoeffs != nil {
		for idleKey := range aggSet.idleKeys {
			idleAlloc := aggSet.Get(idleKey)
			iaidleId, err := idleAlloc.getIdleId(options)
			if err != nil {
				log.Errorf("AllocationSet.AggregateBy: Idle allocation is missing idleId %s", idleAlloc.Name)
				return err
			}

			if resourceCoeffs, ok := groupingIdleFiltrationCoeffs[iaidleId]; ok {
				idleAlloc.CPUCost *= resourceCoeffs["cpu"]
				idleAlloc.CPUCoreHours *= resourceCoeffs["cpu"]
				idleAlloc.RAMCost *= resourceCoeffs["ram"]
				idleAlloc.RAMByteHours *= resourceCoeffs["ram"]
				idleAlloc.GPUCost *= resourceCoeffs["gpu"]
				idleAlloc.GPUHours *= resourceCoeffs["gpu"]
			}
		}
	}

	// (8) Distribute shared allocations according to the share coefficients.
	if shareSet.Length() > 0 {
		for _, alloc := range aggSet.allocations {
			for _, sharedAlloc := range shareSet.allocations {
				if _, ok := shareCoefficients[alloc.Name]; !ok {
					if !alloc.IsIdle() && !alloc.IsUnmounted() {
						log.Warnf("AllocationSet.AggregateBy: error getting share coefficienct for '%s'", alloc.Name)
					}
					continue
				}

				alloc.SharedCost += sharedAlloc.TotalCost() * shareCoefficients[alloc.Name]
			}
		}
	}

	// (9) Aggregate external allocations into aggregated allocations. This may
	// not be possible for every external allocation, but attempt to find an
	// exact key match, given each external allocation's proerties, and
	// aggregate if an exact match is found.
	for _, alloc := range externalSet.allocations {
		skip := false
		if options.Filter != nil {
			skip = !options.Filter.Matches(alloc)
		}
		if !skip {
			key := alloc.generateKey(aggregateBy, options.LabelConfig)

			alloc.Name = key
			aggSet.Insert(alloc)
		}
	}

	// (10) In the edge case that some idle has not been distributed because
	// there is no usage of that resource type, add idle back to
	// aggregations with only that cost applied.

	// E.g. in the case where we have a result that looks like this on the
	// frontend:

	// Name		CPU		GPU		RAM
	// __idle__ $10     $12     $6
	// kubecost $2      $0      $1

	// Sharing idle weighted would result in no idle GPU cost being
	// distributed, because the coefficient for the kubecost GPU cost would
	// be zero. Thus, instead we re-add idle to the aggSet with distributed
	// costs zeroed out but the undistributed costs left in.

	// Name		CPU		GPU		RAM
	// __idle__ $0      $12     $0
	// kubecost $12     $0      $7
	if idleSet.Length() > 0 {
		for _, idleAlloc := range idleSet.allocations {
			// if the idle does not apply to the non-filtered values, skip it
			skip := false
			if options.Filter != nil {
				skip = !options.Filter.Matches(idleAlloc)
			}
			if skip {
				continue
			}

			idleId, err := idleAlloc.getIdleId(options)
			if err != nil {
				log.Errorf("AllocationSet.AggregateBy: idle allocation is missing idleId %s", idleAlloc.Name)
				continue
			}

			hasUndistributableCost := false

			if idleAlloc.CPUCost > 0 && allocatedTotalsMap[idleId]["cpu"] == 0 {
				hasUndistributableCost = true
			} else {
				idleAlloc.CPUCost = 0
			}

			if idleAlloc.GPUCost > 0 && allocatedTotalsMap[idleId]["gpu"] == 0 {
				hasUndistributableCost = true
			} else {
				idleAlloc.GPUCost = 0
			}

			if idleAlloc.RAMCost > 0 && allocatedTotalsMap[idleId]["ram"] == 0 {
				hasUndistributableCost = true
			} else {
				idleAlloc.RAMCost = 0
			}

			if hasUndistributableCost {
				idleAlloc.Name = fmt.Sprintf("%s/%s", idleId, IdleSuffix)
				aggSet.Insert(idleAlloc)
			}
		}
	}

	// (11) Combine all idle allocations into a single "__idle__" allocation
	if !options.SplitIdle {
		for _, idleAlloc := range aggSet.IdleAllocations() {
			aggSet.Delete(idleAlloc.Name)
			idleAlloc.Name = IdleSuffix
			aggSet.Insert(idleAlloc)
		}
	}

	// TODO revisit this (ideally we just remove sharing from this function!)
	// If filters and shared resources and shared idle are all enabled then
	// we will over-count idle by exactly the portion that gets shared with the
	// filtered allocations -- and idle filtration will miss this because it
	// only filters the non-idle filtered costs.
	//
	// Consider the following example, from unit tests:
	// - namespace1     28.000
	// - namespace2     36.000
	// - namespace3     18.000
	// - cluster1/idle  20.000
	// - cluster2/idle  10.000
	//
	// Now, we want to share namespace1, filter namespace2, and share idle:
	//
	// 1. Distribute idle
	//                 ns1     ns2     ns3
	//    non-idle  28.000  36.000  18.000
	//        idle  14.688  10.312   5.000
	//
	// 2. Share namespace1
	//
	//                        ns2     ns3
	//           non-idle  36.000  18.000
	//               idle  10.312   5.000
	//    shared non-idle  18.667   9.333
	//    shared     idle   9.792   4.896 (***)
	//
	// 3. Filter out all but namespace2
	//
	//    ns2 = 36.000 + 10.312 + 18.667 + 9.792 = 74.771
	//
	// So, if we had NOT shared idle, we would expect something like this:
	//
	//    ns2 = 36.000 + 18.667 = 54.667
	//   idle = 10.312 + 9.792  = 20.104
	//
	// But we will instead get this:
	//
	//    ns2 = 36.000 + 18.667 = 54.667
	//   idle = 10.312 + 14.688 = 25.000
	//
	// Which over-shoots idle by 4.896 (***), i.e. precisely the amount of idle
	// cost corresponding to namespace1 AND shared with namespace3. Phew.
	//
	// I originally wanted to fix this, but after 2 days, I'm punting with the
	// recommendation that we rewrite this function soon. Too difficult.
	// - Niko

	as.allocations = aggSet.allocations

	return nil
}

func computeShareCoeffs(aggregateBy []string, options *AllocationAggregationOptions, as *AllocationSet) (map[string]float64, error) {
	// Compute coeffs by totalling per-allocation, then dividing by the total.
	coeffs := map[string]float64{}

	// Compute totals for all allocations
	total := 0.0

	// ShareEven counts each aggregation with even weight, whereas ShareWeighted
	// counts each aggregation proportionally to its respective costs
	shareType := options.ShareSplit

	// Record allocation values first, then normalize by totals to get percentages
	for _, alloc := range as.allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}
		if alloc.IsUnmounted() {
			// Skip unmounted allocations in coefficient calculation
			continue
		}

		// Determine the post-aggregation key under which the allocation will
		// be shared.
		name := alloc.generateKey(aggregateBy, options.LabelConfig)

		// If the current allocation will be filtered out in step 3, contribute
		// its share of the shared coefficient to a "__filtered__" bin, which
		// will ultimately be dropped. This step ensures that the shared cost
		// of a non-filtered allocation will be conserved even when the filter
		// is removed. (Otherwise, all the shared cost will get redistributed
		// over the unfiltered results, inflating their shared costs.)
		filtered := false
		if options.Filter != nil {
			filtered = !options.Filter.Matches(alloc)
		}
		if filtered {
			name = "__filtered__"
		}

		if shareType == ShareEven {
			// Even distribution is not additive - set to 1.0 for everything
			coeffs[name] = 1.0
			// Total for even distribution is always the number of coefficients
			total = float64(len(coeffs))
		} else {
			// Both are additive for weighted distribution, where each
			// cumulative coefficient will be divided by the total.
			coeffs[name] += alloc.TotalCost() - alloc.SharedCost
			total += alloc.TotalCost() - alloc.SharedCost
		}
	}

	// Normalize coefficients by totals
	for a := range coeffs {
		if coeffs[a] > 0 && total > 0 {
			coeffs[a] /= total
		} else {
			log.Warnf("ETL: invalid values for shared coefficients: %v, %v", coeffs[a], total)
			coeffs[a] = 0.0
		}
	}

	return coeffs, nil
}

func computeIdleCoeffs(options *AllocationAggregationOptions, as *AllocationSet, shareSet *AllocationSet) (map[string]map[string]map[string]float64, map[string]map[string]float64, error) {
	types := []string{"cpu", "gpu", "ram"}

	// Compute idle coefficients, then save them in AllocationAggregationOptions
	// [idle_id][allocation name][resource] = [coeff]
	coeffs := map[string]map[string]map[string]float64{}

	// Compute totals per resource for CPU, GPU, RAM, and PV
	// [idle_id][resource] = [total]
	totals := map[string]map[string]float64{}

	// Record allocation values first, then normalize by totals to get percentages
	for _, alloc := range as.allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key for %s", alloc.Name)
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create key based tables if they don't exist
		if _, ok := coeffs[idleId]; !ok {
			coeffs[idleId] = map[string]map[string]float64{}
		}
		if _, ok := totals[idleId]; !ok {
			totals[idleId] = map[string]float64{}
		}

		if _, ok := coeffs[idleId][name]; !ok {
			coeffs[idleId][name] = map[string]float64{}
		}

		coeffs[idleId][name]["cpu"] += alloc.CPUTotalCost()
		coeffs[idleId][name]["gpu"] += alloc.GPUTotalCost()
		coeffs[idleId][name]["ram"] += alloc.RAMTotalCost()

		totals[idleId]["cpu"] += alloc.CPUTotalCost()
		totals[idleId]["gpu"] += alloc.GPUTotalCost()
		totals[idleId]["ram"] += alloc.RAMTotalCost()
	}

	// Do the same for shared allocations
	for _, alloc := range shareSet.allocations {
		if alloc.IsIdle() {
			// Skip idle allocations in coefficient calculation
			continue
		}

		// idleId will be providerId or cluster
		idleId, err := alloc.getIdleId(options)
		if err != nil {
			log.DedupedWarningf(3, "Missing Idle Key in share set for %s", alloc.Name)
		}

		// get the name key for the allocation
		name := alloc.Name

		// Create idleId based tables if they don't exist
		if _, ok := coeffs[idleId]; !ok {
			coeffs[idleId] = map[string]map[string]float64{}
		}
		if _, ok := totals[idleId]; !ok {
			totals[idleId] = map[string]float64{}
		}

		if _, ok := coeffs[idleId][name]; !ok {
			coeffs[idleId][name] = map[string]float64{}
		}

		coeffs[idleId][name]["cpu"] += alloc.CPUTotalCost()
		coeffs[idleId][name]["gpu"] += alloc.GPUTotalCost()
		coeffs[idleId][name]["ram"] += alloc.RAMTotalCost()

		totals[idleId]["cpu"] += alloc.CPUTotalCost()
		totals[idleId]["gpu"] += alloc.GPUTotalCost()
		totals[idleId]["ram"] += alloc.RAMTotalCost()
	}

	// Normalize coefficients by totals
	for id := range coeffs {
		for a := range coeffs[id] {
			for _, r := range types {
				if coeffs[id][a][r] > 0 && totals[id][r] > 0 {
					coeffs[id][a][r] /= totals[id][r]
				}
			}
		}
	}

	return coeffs, totals, nil
}

// getIdleId returns the providerId or cluster of an Allocation depending on the IdleByNode
// option in the AllocationAggregationOptions and an error if the respective field is missing
func (a *Allocation) getIdleId(options *AllocationAggregationOptions) (string, error) {
	var idleId string
	if options.IdleByNode {
		// Key allocations to ProviderId to match against node
		idleId = fmt.Sprintf("%s/%s", a.Properties.Cluster, a.Properties.Node)
		if idleId == "" {
			return idleId, fmt.Errorf("ProviderId is not set")
		}
	} else {
		// key the allocations by cluster id
		idleId = a.Properties.Cluster
		if idleId == "" {
			return idleId, fmt.Errorf("ClusterProp is not set")
		}
	}
	return idleId, nil
}

func (a *Allocation) generateKey(aggregateBy []string, labelConfig *LabelConfig) string {
	if a == nil {
		return ""
	}

	return a.Properties.GenerateKey(aggregateBy, labelConfig)
}

// Clone returns a new AllocationSet with a deep copy of the given
// AllocationSet's allocations.
func (as *AllocationSet) Clone() *AllocationSet {
	if as == nil {
		return nil
	}

	as.RLock()
	defer as.RUnlock()

	allocs := make(map[string]*Allocation, len(as.allocations))
	for k, v := range as.allocations {
		allocs[k] = v.Clone()
	}

	externalKeys := make(map[string]bool, len(as.externalKeys))
	for k, v := range as.externalKeys {
		externalKeys[k] = v
	}

	idleKeys := make(map[string]bool, len(as.idleKeys))
	for k, v := range as.idleKeys {
		idleKeys[k] = v
	}

	var errors []string
	var warnings []string

	if as.Errors != nil {
		errors = make([]string, len(as.Errors))
		copy(errors, as.Errors)
	} else {
		errors = nil
	}

	if as.Warnings != nil {
		warnings := make([]string, len(as.Warnings))
		copy(warnings, as.Warnings)
	} else {
		warnings = nil
	}

	return &AllocationSet{
		allocations:  allocs,
		externalKeys: externalKeys,
		idleKeys:     idleKeys,
		Window:       as.Window.Clone(),
		Errors:       errors,
		Warnings:     warnings,
	}
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
		log.Warnf("AllocationSet: calling End on nil AllocationSet")
		return time.Unix(0, 0)
	}
	if as.Window.End() == nil {
		log.Warnf("AllocationSet: AllocationSet with illegal window: End is nil; len(as.allocations)=%d", len(as.allocations))
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

	// Expand the window, just to be safe. It's possible that the Allocation will
	// be set into the map without expanding it to the AllocationSet's window.
	as.allocations[that.Name].Window = as.allocations[that.Name].Window.Expand(as.Window)

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
	if as == nil {
		return json.Marshal(map[string]*Allocation{})
	}
	as.RLock()
	defer as.RUnlock()
	return json.Marshal(as.allocations)
}

// ResetAdjustments sets all cost adjustment fields to zero
func (as *AllocationSet) ResetAdjustments() {
	if as == nil {
		return
	}

	as.Lock()
	defer as.Unlock()

	as.resetAdjustments()
}

func (as *AllocationSet) resetAdjustments() {
	for _, a := range as.allocations {
		a.ResetAdjustments()
	}
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
		log.Warnf("AllocationSet: calling Start on nil AllocationSet")
		return time.Unix(0, 0)
	}
	if as.Window.Start() == nil {
		log.Warnf("AllocationSet: AllocationSet with illegal window: Start is nil; len(as.allocations)=%d", len(as.allocations))
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
		return that.Clone(), nil
	}

	if that.IsEmpty() {
		return as.Clone(), nil
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
	FromStore   string // stores the name of the store used to retrieve the data
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

// AccumulateBy sums AllocationSets based on the resolution given. The resolution given is subject to the scale used for the AllocationSets.
// Resolutions not evenly divisible by the AllocationSetRange window durations accumulate sets until a sum greater than or equal to the resolution is met,
// at which point AccumulateBy will start summing from 0 until the requested resolution is met again.
// If the requested resolution is smaller than the window of an AllocationSet then the resolution will default to the duration of a set.
// Resolutions larger than the duration of the entire AllocationSetRange will default to the duration of the range.
func (asr *AllocationSetRange) AccumulateBy(resolution time.Duration) (*AllocationSetRange, error) {
	allocSetRange := NewAllocationSetRange()
	var allocSet *AllocationSet
	var err error

	asr.Lock()
	defer asr.Unlock()
	for i, as := range asr.allocations {
		allocSet, err = allocSet.accumulate(as)
		if err != nil {
			return nil, err
		}

		if allocSet != nil {

			// check if end of asr to sum the final set
			// If total asr accumulated sum <= resolution return 1 accumulated set
			if allocSet.Window.Duration() >= resolution || i == len(asr.allocations)-1 {
				allocSetRange.allocations = append(allocSetRange.allocations, allocSet)
				allocSet = NewAllocationSet(time.Time{}, time.Time{})
			}
		}
	}

	return allocSetRange, nil
}

// AggregateBy aggregates each AllocationSet in the range by the given
// properties and options.
func (asr *AllocationSetRange) AggregateBy(aggregateBy []string, options *AllocationAggregationOptions) error {
	aggRange := &AllocationSetRange{allocations: []*AllocationSet{}}

	asr.Lock()
	defer asr.Unlock()

	for _, as := range asr.allocations {
		err := as.AggregateBy(aggregateBy, options)
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
	if asr == nil {
		return json.Marshal([]*AllocationSet{})
	}

	asr.RLock()
	defer asr.RUnlock()
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

// Start returns the earliest start of all Allocations in the AllocationSetRange.
// It returns an error if there are no allocations.
func (asr *AllocationSetRange) Start() (time.Time, error) {
	start := time.Time{}
	firstStartNotSet := true
	asr.Each(func(i int, as *AllocationSet) {
		as.Each(func(s string, a *Allocation) {
			if firstStartNotSet {
				start = a.Start
				firstStartNotSet = false
			}
			if a.Start.Before(start) {
				start = a.Start
			}
		})
	})

	if firstStartNotSet {
		return start, fmt.Errorf("had no data to compute a start from")
	}

	return start, nil
}

// End returns the latest end of all Allocations in the AllocationSetRange.
// It returns an error if there are no allocations.
func (asr *AllocationSetRange) End() (time.Time, error) {
	end := time.Time{}
	firstEndNotSet := true
	asr.Each(func(i int, as *AllocationSet) {
		as.Each(func(s string, a *Allocation) {
			if firstEndNotSet {
				end = a.End
				firstEndNotSet = false
			}
			if a.End.After(end) {
				end = a.End
			}
		})
	})

	if firstEndNotSet {
		return end, fmt.Errorf("had no data to compute an end from")
	}

	return end, nil
}

// Minutes returns the duration, in minutes, between the earliest start
// and the latest end of all allocations in the AllocationSetRange.
func (asr *AllocationSetRange) Minutes() float64 {
	start, err := asr.Start()
	if err != nil {
		return 0
	}
	end, err := asr.End()
	if err != nil {
		return 0
	}
	duration := end.Sub(start)

	return duration.Minutes()
}

// TotalCost returns the sum of all TotalCosts of the allocations contained
func (asr *AllocationSetRange) TotalCost() float64 {
	if asr == nil || len(asr.allocations) == 0 {
		return 0.0
	}

	asr.RLock()
	defer asr.RUnlock()

	tc := 0.0
	for _, as := range asr.allocations {
		tc += as.TotalCost()
	}
	return tc
}
