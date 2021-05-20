package kubecost

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kubecost/cost-model/pkg/util"
)

const day = 24 * time.Hour

var disk = PVKey{}
var disk1 = PVKey{
	Cluster: "cluster2",
	Name:    "disk1",
}
var disk2 = PVKey{
	Cluster: "cluster2",
	Name:    "disk2",
}

func NewUnitAllocation(name string, start time.Time, resolution time.Duration, props *AllocationProperties) *Allocation {
	if name == "" {
		name = "cluster1/namespace1/pod1/container1"
	}

	properties := &AllocationProperties{}
	if props == nil {
		properties.Cluster = "cluster1"
		properties.Node = "node1"
		properties.Namespace = "namespace1"
		properties.ControllerKind = "deployment"
		properties.Controller = "deployment1"
		properties.Pod = "pod1"
		properties.Container = "container1"
	} else {
		properties = props
	}

	end := start.Add(resolution)

	alloc := &Allocation{
		Name:                  name,
		Properties:            properties,
		Window:                NewWindow(&start, &end).Clone(),
		Start:                 start,
		End:                   end,
		CPUCoreHours:          1,
		CPUCost:               1,
		CPUCoreRequestAverage: 1,
		CPUCoreUsageAverage:   1,
		GPUHours:              1,
		GPUCost:               1,
		NetworkCost:           1,
		LoadBalancerCost:      1,
		PVs: PVAllocations{
			disk: {
				ByteHours: 1,
				Cost:      1,
			},
		},
		RAMByteHours:           1,
		RAMCost:                1,
		RAMBytesRequestAverage: 1,
		RAMBytesUsageAverage:   1,
		RawAllocationOnly: &RawAllocationOnlyData{
			CPUCoreUsageMax:  1,
			RAMBytesUsageMax: 1,
		},
	}

	// If idle allocation, remove non-idle costs, but maintain total cost
	if alloc.IsIdle() {
		alloc.PVs = nil
		alloc.NetworkCost = 0.0
		alloc.LoadBalancerCost = 0.0
		alloc.CPUCoreHours += 1.0
		alloc.CPUCost += 1.0
		alloc.RAMByteHours += 1.0
		alloc.RAMCost += 1.0
	}

	return alloc
}

func TestAllocation_Add(t *testing.T) {
	var nilAlloc *Allocation
	zeroAlloc := &Allocation{}

	// nil + nil == nil
	nilNilSum, err := nilAlloc.Add(nilAlloc)
	if err != nil {
		t.Fatalf("Allocation.Add unexpected error: %s", err)
	}
	if nilNilSum != nil {
		t.Fatalf("Allocation.Add failed; exp: nil; act: %s", nilNilSum)
	}

	// nil + zero == zero
	nilZeroSum, err := nilAlloc.Add(zeroAlloc)
	if err != nil {
		t.Fatalf("Allocation.Add unexpected error: %s", err)
	}
	if nilZeroSum == nil || nilZeroSum.TotalCost() != 0.0 {
		t.Fatalf("Allocation.Add failed; exp: 0.0; act: %s", nilZeroSum)
	}

	cpuPrice := 0.02
	gpuPrice := 2.00
	ramPrice := 0.01
	pvPrice := 0.00005
	gib := 1024.0 * 1024.0 * 1024.0

	s1 := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	e1 := time.Date(2021, time.January, 1, 12, 0, 0, 0, time.UTC)
	hrs1 := e1.Sub(s1).Hours()
	a1 := &Allocation{
		Start:                 s1,
		End:                   e1,
		Properties:            &AllocationProperties{},
		CPUCoreHours:          2.0 * hrs1,
		CPUCoreRequestAverage: 2.0,
		CPUCoreUsageAverage:   1.0,
		CPUCost:               2.0 * hrs1 * cpuPrice,
		CPUCostAdjustment:     3.0,
		GPUHours:              1.0 * hrs1,
		GPUCost:               1.0 * hrs1 * gpuPrice,
		GPUCostAdjustment:     2.0,
		PVs: PVAllocations{
			disk: {
				ByteHours: 100.0 * gib * hrs1,
				Cost:      100.0 * hrs1 * pvPrice,
			},
		},
		PVCostAdjustment:       4.0,
		RAMByteHours:           8.0 * gib * hrs1,
		RAMBytesRequestAverage: 8.0 * gib,
		RAMBytesUsageAverage:   4.0 * gib,
		RAMCost:                8.0 * hrs1 * ramPrice,
		RAMCostAdjustment:      1.0,
		SharedCost:             2.00,
		ExternalCost:           1.00,
		RawAllocationOnly:      &RawAllocationOnlyData{},
	}
	a1b := a1.Clone()

	s2 := time.Date(2021, time.January, 1, 6, 0, 0, 0, time.UTC)
	e2 := time.Date(2021, time.January, 1, 24, 0, 0, 0, time.UTC)
	hrs2 := e1.Sub(s1).Hours()
	a2 := &Allocation{
		Start:                  s2,
		End:                    e2,
		Properties:             &AllocationProperties{},
		CPUCoreHours:           1.0 * hrs2,
		CPUCoreRequestAverage:  1.0,
		CPUCoreUsageAverage:    1.0,
		CPUCost:                1.0 * hrs2 * cpuPrice,
		GPUHours:               0.0,
		GPUCost:                0.0,
		RAMByteHours:           8.0 * gib * hrs2,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   8.0 * gib,
		RAMCost:                8.0 * hrs2 * ramPrice,
		NetworkCost:            0.01,
		LoadBalancerCost:       0.05,
		SharedCost:             0.00,
		ExternalCost:           1.00,
		RawAllocationOnly:      &RawAllocationOnlyData{},
	}
	a2b := a2.Clone()

	act, err := a1.Add(a2)
	if err != nil {
		t.Fatalf("Allocation.Add: unexpected error: %s", err)
	}

	// Neither Allocation should be mutated
	if !a1.Equal(a1b) {
		t.Fatalf("Allocation.Add: a1 illegally mutated")
	}
	if !a2.Equal(a2b) {
		t.Fatalf("Allocation.Add: a1 illegally mutated")
	}

	// Costs should be cumulative
	if !util.IsApproximately(a1.TotalCost()+a2.TotalCost(), act.TotalCost()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.TotalCost()+a2.TotalCost(), act.TotalCost())
	}
	if !util.IsApproximately(a1.CPUCost+a2.CPUCost, act.CPUCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.CPUCost+a2.CPUCost, act.CPUCost)
	}
	if !util.IsApproximately(a1.CPUCostAdjustment+a2.CPUCostAdjustment, act.CPUCostAdjustment) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.CPUCostAdjustment+a2.CPUCostAdjustment, act.CPUCostAdjustment)
	}
	if !util.IsApproximately(a1.GPUCost+a2.GPUCost, act.GPUCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.GPUCost+a2.GPUCost, act.GPUCost)
	}
	if !util.IsApproximately(a1.GPUCostAdjustment+a2.GPUCostAdjustment, act.GPUCostAdjustment) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.GPUCostAdjustment+a2.GPUCostAdjustment, act.GPUCostAdjustment)
	}
	if !util.IsApproximately(a1.RAMCost+a2.RAMCost, act.RAMCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.RAMCost+a2.RAMCost, act.RAMCost)
	}
	if !util.IsApproximately(a1.RAMCostAdjustment+a2.RAMCostAdjustment, act.RAMCostAdjustment) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.RAMCostAdjustment+a2.RAMCostAdjustment, act.RAMCostAdjustment)
	}
	if !util.IsApproximately(a1.PVCost()+a2.PVCost(), act.PVCost()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.PVCost()+a2.PVCost(), act.PVCost())
	}
	if !util.IsApproximately(a1.NetworkCost+a2.NetworkCost, act.NetworkCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.NetworkCost+a2.NetworkCost, act.NetworkCost)
	}
	if !util.IsApproximately(a1.LoadBalancerCost+a2.LoadBalancerCost, act.LoadBalancerCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.LoadBalancerCost+a2.LoadBalancerCost, act.LoadBalancerCost)
	}
	if !util.IsApproximately(a1.SharedCost+a2.SharedCost, act.SharedCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.SharedCost+a2.SharedCost, act.SharedCost)
	}
	if !util.IsApproximately(a1.ExternalCost+a2.ExternalCost, act.ExternalCost) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.ExternalCost+a2.ExternalCost, act.ExternalCost)
	}

	// ResourceHours should be cumulative
	if !util.IsApproximately(a1.CPUCoreHours+a2.CPUCoreHours, act.CPUCoreHours) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.CPUCoreHours+a2.CPUCoreHours, act.CPUCoreHours)
	}
	if !util.IsApproximately(a1.RAMByteHours+a2.RAMByteHours, act.RAMByteHours) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.RAMByteHours+a2.RAMByteHours, act.RAMByteHours)
	}
	if !util.IsApproximately(a1.PVByteHours()+a2.PVByteHours(), act.PVByteHours()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", a1.PVByteHours()+a2.PVByteHours(), act.PVByteHours())
	}

	// Minutes should be the duration between min(starts) and max(ends)
	if !act.Start.Equal(a1.Start) || !act.End.Equal(a2.End) {
		t.Fatalf("Allocation.Add: expected %s; actual %s", NewWindow(&a1.Start, &a2.End), NewWindow(&act.Start, &act.End))
	}
	if act.Minutes() != 1440.0 {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1440.0, act.Minutes())
	}

	// Requests and Usage should be averaged correctly
	// CPU requests = (2.0*12.0 + 1.0*18.0)/(24.0) = 1.75
	// CPU usage = (1.0*12.0 + 1.0*18.0)/(24.0) = 1.25
	// RAM requests = (8.0*12.0 + 0.0*18.0)/(24.0) = 4.00
	// RAM usage = (4.0*12.0 + 8.0*18.0)/(24.0) = 8.00
	if !util.IsApproximately(1.75, act.CPUCoreRequestAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1.75, act.CPUCoreRequestAverage)
	}
	if !util.IsApproximately(1.25, act.CPUCoreUsageAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1.25, act.CPUCoreUsageAverage)
	}
	if !util.IsApproximately(4.00*gib, act.RAMBytesRequestAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 4.00*gib, act.RAMBytesRequestAverage)
	}
	if !util.IsApproximately(8.00*gib, act.RAMBytesUsageAverage) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 8.00*gib, act.RAMBytesUsageAverage)
	}

	// Efficiency should be computed accurately from new request/usage
	// CPU efficiency = 1.25/1.75 = 0.7142857
	// RAM efficiency = 8.00/4.00 = 2.0000000
	// Total efficiency = (0.7142857*0.72 + 2.0*1.92)/(2.64) = 1.6493506
	if !util.IsApproximately(0.7142857, act.CPUEfficiency()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 0.7142857, act.CPUEfficiency())
	}
	if !util.IsApproximately(2.0000000, act.RAMEfficiency()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 2.0000000, act.RAMEfficiency())
	}
	if !util.IsApproximately(1.279690, act.TotalEfficiency()) {
		t.Fatalf("Allocation.Add: expected %f; actual %f", 1.279690, act.TotalEfficiency())
	}

	if act.RawAllocationOnly != nil {
		t.Errorf("Allocation.Add: Raw only data must be nil after an add")
	}
}

func TestAllocation_Share(t *testing.T) {
	cpuPrice := 0.02
	gpuPrice := 2.00
	ramPrice := 0.01
	pvPrice := 0.00005
	gib := 1024.0 * 1024.0 * 1024.0

	s1 := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	e1 := time.Date(2021, time.January, 1, 12, 0, 0, 0, time.UTC)
	hrs1 := e1.Sub(s1).Hours()
	a1 := &Allocation{
		Start:                 s1,
		End:                   e1,
		Properties:            &AllocationProperties{},
		CPUCoreHours:          2.0 * hrs1,
		CPUCoreRequestAverage: 2.0,
		CPUCoreUsageAverage:   1.0,
		CPUCost:               2.0 * hrs1 * cpuPrice,
		CPUCostAdjustment:     3.0,
		GPUHours:              1.0 * hrs1,
		GPUCost:               1.0 * hrs1 * gpuPrice,
		GPUCostAdjustment:     2.0,
		PVs: PVAllocations{
			disk: {
				ByteHours: 100.0 * gib * hrs1,
				Cost:      100.0 * hrs1 * pvPrice,
			},
		},
		PVCostAdjustment:       4.0,
		RAMByteHours:           8.0 * gib * hrs1,
		RAMBytesRequestAverage: 8.0 * gib,
		RAMBytesUsageAverage:   4.0 * gib,
		RAMCost:                8.0 * hrs1 * ramPrice,
		RAMCostAdjustment:      1.0,
		SharedCost:             2.00,
		ExternalCost:           1.00,
	}
	a1b := a1.Clone()

	s2 := time.Date(2021, time.January, 1, 6, 0, 0, 0, time.UTC)
	e2 := time.Date(2021, time.January, 1, 24, 0, 0, 0, time.UTC)
	hrs2 := e1.Sub(s1).Hours()
	a2 := &Allocation{
		Start:                  s2,
		End:                    e2,
		Properties:             &AllocationProperties{},
		CPUCoreHours:           1.0 * hrs2,
		CPUCoreRequestAverage:  1.0,
		CPUCoreUsageAverage:    1.0,
		CPUCost:                1.0 * hrs2 * cpuPrice,
		GPUHours:               0.0,
		GPUCost:                0.0,
		RAMByteHours:           8.0 * gib * hrs2,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   8.0 * gib,
		RAMCost:                8.0 * hrs2 * ramPrice,
		NetworkCost:            0.01,
		LoadBalancerCost:       0.05,
		SharedCost:             0.00,
		ExternalCost:           1.00,
	}
	a2b := a2.Clone()

	act, err := a1.Share(a2)
	if err != nil {
		t.Fatalf("Allocation.Share: unexpected error: %s", err)
	}

	// Neither Allocation should be mutated
	if !a1.Equal(a1b) {
		t.Fatalf("Allocation.Share: a1 illegally mutated")
	}
	if !a2.Equal(a2b) {
		t.Fatalf("Allocation.Share: a1 illegally mutated")
	}

	// SharedCost and TotalCost should reflect increase by a2.TotalCost
	if !util.IsApproximately(a1.TotalCost()+a2.TotalCost(), act.TotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.TotalCost()+a2.TotalCost(), act.TotalCost())
	}
	if !util.IsApproximately(a1.SharedCost+a2.TotalCost(), act.SharedCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.SharedCost+a2.TotalCost(), act.SharedCost)
	}

	// Costs should match before (expect TotalCost and SharedCost)
	if !util.IsApproximately(a1.CPUTotalCost(), act.CPUTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUTotalCost(), act.CPUTotalCost())
	}
	if !util.IsApproximately(a1.GPUTotalCost(), act.GPUTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.GPUTotalCost(), act.GPUTotalCost())
	}
	if !util.IsApproximately(a1.RAMTotalCost(), act.RAMTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMTotalCost(), act.RAMTotalCost())
	}
	if !util.IsApproximately(a1.PVTotalCost(), act.PVTotalCost()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.PVTotalCost(), act.PVTotalCost())
	}
	if !util.IsApproximately(a1.NetworkCost, act.NetworkCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.NetworkCost, act.NetworkCost)
	}
	if !util.IsApproximately(a1.LoadBalancerCost, act.LoadBalancerCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.LoadBalancerCost, act.LoadBalancerCost)
	}
	if !util.IsApproximately(a1.ExternalCost, act.ExternalCost) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.ExternalCost, act.ExternalCost)
	}

	// ResourceHours should match before
	if !util.IsApproximately(a1.CPUCoreHours, act.CPUCoreHours) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUCoreHours, act.CPUCoreHours)
	}
	if !util.IsApproximately(a1.RAMByteHours, act.RAMByteHours) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMByteHours, act.RAMByteHours)
	}
	if !util.IsApproximately(a1.PVByteHours(), act.PVByteHours()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.PVByteHours(), act.PVByteHours())
	}

	// Minutes should match before
	if !act.Start.Equal(a1.Start) || !act.End.Equal(a1.End) {
		t.Fatalf("Allocation.Share: expected %s; actual %s", NewWindow(&a1.Start, &a1.End), NewWindow(&act.Start, &act.End))
	}
	if act.Minutes() != a1.Minutes() {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.Minutes(), act.Minutes())
	}

	// Requests and Usage should match before
	if !util.IsApproximately(a1.CPUCoreRequestAverage, act.CPUCoreRequestAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUCoreRequestAverage, act.CPUCoreRequestAverage)
	}
	if !util.IsApproximately(a1.CPUCoreUsageAverage, act.CPUCoreUsageAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUCoreUsageAverage, act.CPUCoreUsageAverage)
	}
	if !util.IsApproximately(a1.RAMBytesRequestAverage, act.RAMBytesRequestAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMBytesRequestAverage, act.RAMBytesRequestAverage)
	}
	if !util.IsApproximately(a1.RAMBytesUsageAverage, act.RAMBytesUsageAverage) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMBytesUsageAverage, act.RAMBytesUsageAverage)
	}

	// Efficiency should match before
	if !util.IsApproximately(a1.CPUEfficiency(), act.CPUEfficiency()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.CPUEfficiency(), act.CPUEfficiency())
	}
	if !util.IsApproximately(a1.RAMEfficiency(), act.RAMEfficiency()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.RAMEfficiency(), act.RAMEfficiency())
	}
	if !util.IsApproximately(a1.TotalEfficiency(), act.TotalEfficiency()) {
		t.Fatalf("Allocation.Share: expected %f; actual %f", a1.TotalEfficiency(), act.TotalEfficiency())
	}
}

func TestAllocation_MarshalJSON(t *testing.T) {
	start := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2021, time.January, 2, 0, 0, 0, 0, time.UTC)
	hrs := 24.0

	gib := 1024.0 * 1024.0 * 1024.0

	cpuPrice := 0.02
	gpuPrice := 2.00
	ramPrice := 0.01
	pvPrice := 0.00005

	before := &Allocation{
		Name: "cluster1/namespace1/node1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Node:      "node1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Window:                NewWindow(&start, &end),
		Start:                 start,
		End:                   end,
		CPUCoreHours:          2.0 * hrs,
		CPUCoreRequestAverage: 2.0,
		CPUCoreUsageAverage:   1.0,
		CPUCost:               2.0 * hrs * cpuPrice,
		CPUCostAdjustment:     3.0,
		GPUHours:              1.0 * hrs,
		GPUCost:               1.0 * hrs * gpuPrice,
		GPUCostAdjustment:     2.0,
		NetworkCost:           0.05,
		LoadBalancerCost:      0.02,
		PVs: PVAllocations{
			disk: {
				ByteHours: 100.0 * gib * hrs,
				Cost:      100.0 * hrs * pvPrice,
			},
		},
		PVCostAdjustment:       4.0,
		RAMByteHours:           8.0 * gib * hrs,
		RAMBytesRequestAverage: 8.0 * gib,
		RAMBytesUsageAverage:   4.0 * gib,
		RAMCost:                8.0 * hrs * ramPrice,
		RAMCostAdjustment:      1.0,
		SharedCost:             2.00,
		ExternalCost:           1.00,
		RawAllocationOnly:      &RawAllocationOnlyData{},
	}

	data, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("Allocation.MarshalJSON: unexpected error: %s", err)
	}

	after := &Allocation{}
	err = json.Unmarshal(data, after)
	if err != nil {
		t.Fatalf("Allocation.UnmarshalJSON: unexpected error: %s", err)
	}

	// TODO:CLEANUP fix json marshaling of Window so that all of this works.
	// In the meantime, just set the Window so that we can test the rest.
	after.Window = before.Window.Clone()
	// TODO Sean: fix JSON marshaling of PVs
	after.PVs = before.PVs
	if !after.Equal(before) {
		t.Fatalf("Allocation.MarshalJSON: before and after are not equal")
	}
}

func TestAllocationSet_generateKey(t *testing.T) {
	var alloc *Allocation
	var key string

	props := []string{
		AllocationClusterProp,
	}

	key = alloc.generateKey(props)
	if key != "" {
		t.Fatalf("generateKey: expected \"\"; actual \"%s\"", key)
	}

	alloc = &Allocation{}
	alloc.Properties = &AllocationProperties{
		Cluster: "cluster1",
		Labels: map[string]string{
			"app": "app1",
			"env": "env1",
		},
	}

	key = alloc.generateKey(props)
	if key != "cluster1" {
		t.Fatalf("generateKey: expected \"cluster1\"; actual \"%s\"", key)
	}

	props = []string{
		AllocationClusterProp,
		AllocationNamespaceProp,
		"label:app",
	}

	key = alloc.generateKey(props)
	if key != "cluster1//app=app1" {
		t.Fatalf("generateKey: expected \"cluster1//app=app1\"; actual \"%s\"", key)
	}

	alloc.Properties = &AllocationProperties{
		Cluster:   "cluster1",
		Namespace: "namespace1",
		Labels: map[string]string{
			"app": "app1",
			"env": "env1",
		},
	}
	key = alloc.generateKey(props)
	if key != "cluster1/namespace1/app=app1" {
		t.Fatalf("generateKey: expected \"cluster1/namespace1/app=app1\"; actual \"%s\"", key)
	}
}

func TestNewAllocationSet(t *testing.T) {
	// TODO niko/etl
}

func generateAllocationSet(start time.Time) *AllocationSet {
	// Idle allocations
	a1i := NewUnitAllocation(fmt.Sprintf("cluster1/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Node:       "node1",
		ProviderID: "c1nodes",
	})
	a1i.CPUCost = 5.0
	a1i.RAMCost = 15.0
	a1i.GPUCost = 0.0

	a2i := NewUnitAllocation(fmt.Sprintf("cluster2/%s", IdleSuffix), start, day, &AllocationProperties{
		Cluster: "cluster2",
	})
	a2i.CPUCost = 5.0
	a2i.RAMCost = 5.0
	a2i.GPUCost = 0.0

	// Active allocations
	a1111 := NewUnitAllocation("cluster1/namespace1/pod1/container1", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace1",
		Pod:        "pod1",
		Container:  "container1",
		ProviderID: "c1nodes",
	})
	a1111.RAMCost = 11.00

	a11abc2 := NewUnitAllocation("cluster1/namespace1/pod-abc/container2", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace1",
		Pod:        "pod-abc",
		Container:  "container2",
		ProviderID: "c1nodes",
	})

	a11def3 := NewUnitAllocation("cluster1/namespace1/pod-def/container3", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace1",
		Pod:        "pod-def",
		Container:  "container3",
		ProviderID: "c1nodes",
	})

	a12ghi4 := NewUnitAllocation("cluster1/namespace2/pod-ghi/container4", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace2",
		Pod:        "pod-ghi",
		Container:  "container4",
		ProviderID: "c1nodes",
	})

	a12ghi5 := NewUnitAllocation("cluster1/namespace2/pod-ghi/container5", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace2",
		Pod:        "pod-ghi",
		Container:  "container5",
		ProviderID: "c1nodes",
	})

	a12jkl6 := NewUnitAllocation("cluster1/namespace2/pod-jkl/container6", start, day, &AllocationProperties{
		Cluster:    "cluster1",
		Namespace:  "namespace2",
		Pod:        "pod-jkl",
		Container:  "container6",
		ProviderID: "c1nodes",
	})

	a22mno4 := NewUnitAllocation("cluster2/namespace2/pod-mno/container4", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace2",
		Pod:        "pod-mno",
		Container:  "container4",
		ProviderID: "node1",
	})

	a22mno5 := NewUnitAllocation("cluster2/namespace2/pod-mno/container5", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace2",
		Pod:        "pod-mno",
		Container:  "container5",
		ProviderID: "node1",
	})

	a22pqr6 := NewUnitAllocation("cluster2/namespace2/pod-pqr/container6", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace2",
		Pod:        "pod-pqr",
		Container:  "container6",
		ProviderID: "node2",
	})

	a23stu7 := NewUnitAllocation("cluster2/namespace3/pod-stu/container7", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace3",
		Pod:        "pod-stu",
		Container:  "container7",
		ProviderID: "node2",
	})

	a23vwx8 := NewUnitAllocation("cluster2/namespace3/pod-vwx/container8", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace3",
		Pod:        "pod-vwx",
		Container:  "container8",
		ProviderID: "node3",
	})

	a23vwx9 := NewUnitAllocation("cluster2/namespace3/pod-vwx/container9", start, day, &AllocationProperties{
		Cluster:    "cluster2",
		Namespace:  "namespace3",
		Pod:        "pod-vwx",
		Container:  "container9",
		ProviderID: "node3",
	})

	// Controllers

	a11abc2.Properties.ControllerKind = "deployment"
	a11abc2.Properties.Controller = "deployment1"
	a11def3.Properties.ControllerKind = "deployment"
	a11def3.Properties.Controller = "deployment1"

	a12ghi4.Properties.ControllerKind = "deployment"
	a12ghi4.Properties.Controller = "deployment2"
	a12ghi5.Properties.ControllerKind = "deployment"
	a12ghi5.Properties.Controller = "deployment2"
	a22mno4.Properties.ControllerKind = "deployment"
	a22mno4.Properties.Controller = "deployment2"
	a22mno5.Properties.ControllerKind = "deployment"
	a22mno5.Properties.Controller = "deployment2"

	a23stu7.Properties.ControllerKind = "deployment"
	a23stu7.Properties.Controller = "deployment3"

	a12jkl6.Properties.ControllerKind = "daemonset"
	a12jkl6.Properties.Controller = "daemonset1"
	a22pqr6.Properties.ControllerKind = "daemonset"
	a22pqr6.Properties.Controller = "daemonset1"

	a23vwx8.Properties.ControllerKind = "statefulset"
	a23vwx8.Properties.Controller = "statefulset1"
	a23vwx9.Properties.ControllerKind = "statefulset"
	a23vwx9.Properties.Controller = "statefulset1"

	// Labels

	a1111.Properties.Labels = map[string]string{"app": "app1", "env": "env1"}
	a12ghi4.Properties.Labels = map[string]string{"app": "app2", "env": "env2"}
	a12ghi5.Properties.Labels = map[string]string{"app": "app2", "env": "env2"}
	a22mno4.Properties.Labels = map[string]string{"app": "app2"}
	a22mno5.Properties.Labels = map[string]string{"app": "app2"}

	//Annotations
	a23stu7.Properties.Annotations = map[string]string{"team": "team1"}
	a23vwx8.Properties.Annotations = map[string]string{"team": "team2"}
	a23vwx9.Properties.Annotations = map[string]string{"team": "team1"}

	// Services
	a12jkl6.Properties.Services = []string{"service1"}
	a22pqr6.Properties.Services = []string{"service1"}

	return NewAllocationSet(start, start.Add(day),
		// idle
		a1i, a2i,
		// cluster 1, namespace1
		a1111, a11abc2, a11def3,
		// cluster 1, namespace 2
		a12ghi4, a12ghi5, a12jkl6,
		// cluster 2, namespace 2
		a22mno4, a22mno5, a22pqr6,
		// cluster 2, namespace 3
		a23stu7, a23vwx8, a23vwx9,
	)
}

func generateAssetSets(start, end time.Time) []*AssetSet {
	var assetSets []*AssetSet

	// Create an AssetSet representing cluster costs for two clusters (cluster1
	// and cluster2). Include Nodes and Disks for both, even though only
	// Nodes will be counted. Whereas in practice, Assets should be aggregated
	// by type, here we will provide multiple Nodes for one of the clusters to
	// make sure the function still holds.

	// NOTE: we're re-using generateAllocationSet so this has to line up with
	// the allocated node costs from that function. See table above.

	// | Hierarchy                               | Cost |  CPU |  RAM |  GPU | Adjustment |
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1:
	//     nodes                                  100.00  55.00  44.00  11.00      -10.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 subtotal (adjusted)             100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 allocated                        48.00   6.00  16.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 idle                             72.00  44.00  24.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2:
	//     node1                                   35.00  20.00  15.00   0.00        0.00
	//     node2                                   35.00  20.00  15.00   0.00        0.00
	//     node3                                   30.00  10.00  10.00  10.00        0.00
	//     (disks should not matter for idle)
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 subtotal                        100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 allocated                        28.00   6.00   6.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 idle                             82.00  44.00  34.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+

	cluster1Nodes := NewNode("", "cluster1", "c1nodes", start, end, NewWindow(&start, &end))
	cluster1Nodes.CPUCost = 55.0
	cluster1Nodes.RAMCost = 44.0
	cluster1Nodes.GPUCost = 11.0
	cluster1Nodes.adjustment = -10.00
	cluster1Nodes.CPUCoreHours = 8
	cluster1Nodes.RAMByteHours = 6
	cluster1Nodes.GPUHours = 24

	cluster2Node1 := NewNode("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1.CPUCost = 20.0
	cluster2Node1.RAMCost = 15.0
	cluster2Node1.GPUCost = 0.0
	cluster2Node1.CPUCoreHours = 4
	cluster2Node1.RAMByteHours = 3
	cluster2Node1.GPUHours = 0

	cluster2Node2 := NewNode("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2.CPUCost = 20.0
	cluster2Node2.RAMCost = 15.0
	cluster2Node2.GPUCost = 0.0
	cluster2Node2.CPUCoreHours = 3
	cluster2Node2.RAMByteHours = 2
	cluster2Node2.GPUHours = 0

	cluster2Node3 := NewNode("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3.CPUCost = 10.0
	cluster2Node3.RAMCost = 10.0
	cluster2Node3.GPUCost = 10.0
	cluster2Node3.CPUCoreHours = 2
	cluster2Node3.RAMByteHours = 2
	cluster2Node3.GPUHours = 24

	// Add PVs
	cluster2Disk1 := NewDisk("disk1", "cluster2", "disk1", start, end, NewWindow(&start, &end))
	cluster2Disk1.Cost = 5.0
	cluster2Disk1.adjustment = 1.0
	cluster2Disk1.ByteHours = 5 * gb

	cluster2Disk2 := NewDisk("disk2", "cluster2", "disk2", start, end, NewWindow(&start, &end))
	cluster2Disk2.Cost = 10.0
	cluster2Disk2.adjustment = 3.0
	cluster2Disk2.ByteHours = 10 * gb

	cluster2Node1Disk := NewDisk("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1Disk.Cost = 1.0
	cluster2Node1Disk.ByteHours = 5 * gb

	// Add Attached Disks
	cluster2Node2Disk := NewDisk("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2Disk.Cost = 2.0
	cluster2Node2Disk.ByteHours = 5 * gb

	cluster2Node3Disk := NewDisk("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3Disk.Cost = 3.0
	cluster2Node3Disk.ByteHours = 5 * gb

	// Add Cluster Management
	cluster1ClusterManagement := NewClusterManagement("", "cluster1", NewWindow(&start, &end))
	cluster1ClusterManagement.Cost = 2.0

	cluster2ClusterManagement := NewClusterManagement("", "cluster2", NewWindow(&start, &end))
	cluster2ClusterManagement.Cost = 2.0

	// Add Networks
	c1Network := NewNetwork("", "cluster1", "c1nodes", start, end, NewWindow(&start, &end))
	c1Network.Cost = 3.0

	node1Network := NewNetwork("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	node1Network.Cost = 4.0

	node2Network := NewNetwork("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	node2Network.Cost = 5.0

	node3Network := NewNetwork("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	node3Network.Cost = 2.0

	// Add LoadBalancers
	cluster2LoadBalancer1 := NewLoadBalancer("namespace2/loadBalancer1", "cluster2", "lb1", start, end, NewWindow(&start, &end))
	cluster2LoadBalancer1.Cost = 10.0

	cluster2LoadBalancer2 := NewLoadBalancer("namespace2/loadBalancer2", "cluster2", "lb2", start, end, NewWindow(&start, &end))
	cluster2LoadBalancer2.Cost = 15.0

	assetSet1 := NewAssetSet(start, end, cluster1Nodes, cluster2Node1, cluster2Node2, cluster2Node3, cluster2Disk1,
		cluster2Disk2, cluster2Node1Disk, cluster2Node2Disk, cluster2Node3Disk, cluster1ClusterManagement,
		cluster2ClusterManagement, c1Network, node1Network, node2Network, node3Network, cluster2LoadBalancer1, cluster2LoadBalancer2)
	assetSets = append(assetSets, assetSet1)

	// NOTE: we're re-using generateAllocationSet so this has to line up with
	// the allocated node costs from that function. See table above.

	// | Hierarchy                               | Cost |  CPU |  RAM |  GPU | Adjustment |
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1:
	//     nodes                                  100.00   5.00   4.00   1.00       90.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 subtotal (adjusted)             100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 allocated                        48.00   6.00  16.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster1 idle                             72.00  44.00  24.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2:
	//     node1                                   35.00  20.00  15.00   0.00        0.00
	//     node2                                   35.00  20.00  15.00   0.00        0.00
	//     node3                                   30.00  10.00  10.00  10.00        0.00
	//     (disks should not matter for idle)
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 subtotal                        100.00  50.00  40.00  10.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 allocated                        28.00   6.00   6.00   6.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+
	//   cluster2 idle                             82.00  44.00  34.00   4.00        0.00
	// +-----------------------------------------+------+------+------+------+------------+

	cluster1Nodes = NewNode("", "cluster1", "c1nodes", start, end, NewWindow(&start, &end))
	cluster1Nodes.CPUCost = 5.0
	cluster1Nodes.RAMCost = 4.0
	cluster1Nodes.GPUCost = 1.0
	cluster1Nodes.adjustment = 90.00
	cluster1Nodes.CPUCoreHours = 8
	cluster1Nodes.RAMByteHours = 6
	cluster1Nodes.GPUHours = 24

	cluster2Node1 = NewNode("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1.CPUCost = 20.0
	cluster2Node1.RAMCost = 15.0
	cluster2Node1.GPUCost = 0.0
	cluster2Node1.CPUCoreHours = 4
	cluster2Node1.RAMByteHours = 3
	cluster2Node1.GPUHours = 0

	cluster2Node2 = NewNode("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2.CPUCost = 20.0
	cluster2Node2.RAMCost = 15.0
	cluster2Node2.GPUCost = 0.0
	cluster2Node2.CPUCoreHours = 3
	cluster2Node2.RAMByteHours = 2
	cluster2Node2.GPUHours = 0

	cluster2Node3 = NewNode("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3.CPUCost = 10.0
	cluster2Node3.RAMCost = 10.0
	cluster2Node3.GPUCost = 10.0
	cluster2Node3.CPUCoreHours = 2
	cluster2Node3.RAMByteHours = 2
	cluster2Node3.GPUHours = 24

	// Add PVs
	cluster2Disk1 = NewDisk("disk1", "cluster2", "disk1", start, end, NewWindow(&start, &end))
	cluster2Disk1.Cost = 5.0
	cluster2Disk1.adjustment = 1.0
	cluster2Disk1.ByteHours = 5 * gb

	cluster2Disk2 = NewDisk("disk2", "cluster2", "disk2", start, end, NewWindow(&start, &end))
	cluster2Disk2.Cost = 12.0
	cluster2Disk2.adjustment = 4.0
	cluster2Disk2.ByteHours = 20 * gb

	assetSet2 := NewAssetSet(start, end, cluster1Nodes, cluster2Node1, cluster2Node2, cluster2Node3, cluster2Disk1,
		cluster2Disk2, cluster2Node1Disk, cluster2Node2Disk, cluster2Node3Disk, cluster1ClusterManagement,
		cluster2ClusterManagement, c1Network, node1Network, node2Network, node3Network, cluster2LoadBalancer1, cluster2LoadBalancer2)
	assetSets = append(assetSets, assetSet2)
	return assetSets
}

func assertAllocationSetTotals(t *testing.T, as *AllocationSet, msg string, err error, length int, totalCost float64) {
	if err != nil {
		t.Fatalf("AllocationSet.AggregateBy[%s]: unexpected error: %s", msg, err)
	}
	if as.Length() != length {
		t.Fatalf("AllocationSet.AggregateBy[%s]: expected set of length %d, actual %d", msg, length, as.Length())
	}
	if math.Round(as.TotalCost()*100) != math.Round(totalCost*100) {
		t.Fatalf("AllocationSet.AggregateBy[%s]: expected total cost %.2f, actual %.2f", msg, totalCost, as.TotalCost())
	}
}

func assertAllocationTotals(t *testing.T, as *AllocationSet, msg string, exps map[string]float64) {
	as.Each(func(k string, a *Allocation) {
		if exp, ok := exps[a.Name]; ok {
			if math.Round(a.TotalCost()*100) != math.Round(exp*100) {
				t.Fatalf("AllocationSet.AggregateBy[%s]: expected total cost %f, actual %f", msg, exp, a.TotalCost())
			}
		} else {
			t.Fatalf("AllocationSet.AggregateBy[%s]: unexpected allocation: %s", msg, a.Name)
		}
	})
}

func assertAllocationWindow(t *testing.T, as *AllocationSet, msg string, expStart, expEnd time.Time, expMinutes float64) {
	as.Each(func(k string, a *Allocation) {
		if !a.Start.Equal(expStart) {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected start %s, actual %s", msg, expStart, a.Start)
		}
		if !a.End.Equal(expEnd) {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected end %s, actual %s", msg, expEnd, a.End)
		}
		if a.Minutes() != expMinutes {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected minutes %f, actual %f", msg, expMinutes, a.Minutes())
		}
	})
}

func printAllocationSet(msg string, as *AllocationSet) {
	fmt.Printf("--- %s ---\n", msg)
	as.Each(func(k string, a *Allocation) {
		fmt.Printf(" > %s\n", a)
	})
}

func TestAllocationSet_AggregateBy(t *testing.T) {
	// Test AggregateBy against the following workload topology, which is
	// generated by generateAllocationSet:

	// | Hierarchy                              | Cost |  CPU |  RAM |  GPU |   PV |  Net |  LB  |
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   cluster1:
	//     idle:                                  20.00   5.00  15.00   0.00   0.00   0.00   0.00
	//     namespace1:
	//       pod1:
	//         container1: [app=app1, env=env1]   16.00   1.00  11.00   1.00   1.00   1.00   1.00
	//       pod-abc: (deployment1)
	//         container2:                         6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-def: (deployment1)
	//         container3:                         6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//     namespace2:
	//       pod-ghi: (deployment2)
	//         container4: [app=app2, env=env2]    6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app=app2, env=env2]    6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-jkl: (daemonset1)
	//         container6: {service1}              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	// +-----------------------------------------+------+------+------+------+------+------+------+
	//   cluster1 subtotal                        66.00  11.00  31.00   6.00   6.00   6.00   6.00
	// +-----------------------------------------+------+------+------+------+------+------+------+
	//   cluster2:
	//     idle:                                  10.00   5.00   5.00   0.00   0.00   0.00   0.00
	//     namespace2:
	//       pod-mno: (deployment2)
	//         container4: [app=app2]              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app=app2]              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-pqr: (daemonset1)
	//         container6: {service1}              6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//     namespace3:
	//       pod-stu: (deployment3)
	//         container7: an[team=team1]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//       pod-vwx: (statefulset1)
	//         container8: an[team=team2]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	//         container9: an[team=team1]          6.00   1.00   1.00   1.00   1.00   1.00   1.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   cluster2 subtotal                        46.00  11.00  11.00   6.00   6.00   6.00   6.00
	// +----------------------------------------+------+------+------+------+------+------+------+
	//   total                                   112.00  22.00  42.00  12.00  12.00  12.00  12.00
	// +----------------------------------------+------+------+------+------+------+------+------+

	// Scenarios to test:

	// 1  Single-aggregation
	// 1a AggregationProperties=(Cluster)
	// 1b AggregationProperties=(Namespace)
	// 1c AggregationProperties=(Pod)
	// 1d AggregationProperties=(Container)
	// 1e AggregationProperties=(ControllerKind)
	// 1f AggregationProperties=(Controller)
	// 1g AggregationProperties=(Service)
	// 1h AggregationProperties=(Label:app)

	// 2  Multi-aggregation
	// 2a AggregationProperties=(Cluster, Namespace)
	// 2b AggregationProperties=(Namespace, Label:app)
	// 2c AggregationProperties=(Cluster, Namespace, Pod, Container)
	// 2d AggregationProperties=(Label:app, Label:environment)

	// 3  Share idle
	// 3a AggregationProperties=(Namespace) ShareIdle=ShareWeighted
	// 3b AggregationProperties=(Namespace) ShareIdle=ShareEven (TODO niko/etl)

	// 4  Share resources
	// 4a Share namespace ShareEven
	// 4b Share cluster ShareWeighted
	// 4c Share label ShareEven
	// 4d Share overhead ShareWeighted

	// 5  Filters
	// 5a Filter by cluster with separate idle
	// 5b Filter by cluster with shared idle
	// TODO niko/idle more filter tests

	// 6  Combinations and options
	// 6a SplitIdle
	// 6b Share idle with filters
	// 6c Share resources with filters
	// 6d Share idle and share resources

	// 7  Edge cases and errors
	// 7a Empty AggregationProperties
	// 7b Filter all
	// 7c Share all
	// 7d Share and filter the same allocations

	// Definitions and set-up:

	var as *AllocationSet
	var err error

	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)

	numClusters := 2
	numNamespaces := 3
	numPods := 9
	numContainers := 9
	numControllerKinds := 3
	numControllers := 5
	numServices := 1
	numLabelApps := 2

	// By default, idle is reported as a single, merged allocation
	numIdle := 1
	// There will only ever be one __unallocated__
	numUnallocated := 1
	// There are two clusters, so each gets an idle entry when they are split
	numSplitIdle := 2

	activeTotalCost := 82.0
	idleTotalCost := 30.0
	sharedOverheadHourlyCost := 7.0

	isNamespace3 := func(a *Allocation) bool {
		ns := a.Properties.Namespace
		return ns == "namespace3"
	}

	isApp1 := func(a *Allocation) bool {
		ls := a.Properties.Labels
		if app, ok := ls["app"]; ok && app == "app1" {
			return true
		}
		return false
	}

	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day)

	// Tests:

	// 1  Single-aggregation

	// 1a AggregationProperties=(Cluster)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationClusterProp}, nil)
	assertAllocationSetTotals(t, as, "1a", err, numClusters+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1a", map[string]float64{
		"cluster1": 46.00,
		"cluster2": 36.00,
		IdleSuffix: 30.00,
	})
	assertAllocationWindow(t, as, "1a", startYesterday, endYesterday, 1440.0)

	// 1b AggregationProperties=(Namespace)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, nil)
	assertAllocationSetTotals(t, as, "1b", err, numNamespaces+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1b", map[string]float64{
		"namespace1": 28.00,
		"namespace2": 36.00,
		"namespace3": 18.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "1b", startYesterday, endYesterday, 1440.0)

	// 1c AggregationProperties=(Pod)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationPodProp}, nil)
	assertAllocationSetTotals(t, as, "1c", err, numPods+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1c", map[string]float64{
		"pod-jkl":  6.00,
		"pod-stu":  6.00,
		"pod-abc":  6.00,
		"pod-pqr":  6.00,
		"pod-def":  6.00,
		"pod-vwx":  12.00,
		"pod1":     16.00,
		"pod-mno":  12.00,
		"pod-ghi":  12.00,
		IdleSuffix: 30.00,
	})
	assertAllocationWindow(t, as, "1c", startYesterday, endYesterday, 1440.0)

	// 1d AggregationProperties=(Container)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationContainerProp}, nil)
	assertAllocationSetTotals(t, as, "1d", err, numContainers+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1d", map[string]float64{
		"container2": 6.00,
		"container9": 6.00,
		"container6": 12.00,
		"container3": 6.00,
		"container4": 12.00,
		"container7": 6.00,
		"container8": 6.00,
		"container5": 12.00,
		"container1": 16.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "1d", startYesterday, endYesterday, 1440.0)

	// 1e AggregationProperties=(ControllerKind)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationControllerKindProp}, nil)
	assertAllocationSetTotals(t, as, "1e", err, numControllerKinds+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1e", map[string]float64{
		"daemonset":       12.00,
		"deployment":      42.00,
		"statefulset":     12.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 16.00,
	})
	assertAllocationWindow(t, as, "1e", startYesterday, endYesterday, 1440.0)

	// 1f AggregationProperties=(Controller)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationControllerProp}, nil)
	assertAllocationSetTotals(t, as, "1f", err, numControllers+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1f", map[string]float64{
		"deployment:deployment2":   24.00,
		"daemonset:daemonset1":     12.00,
		"deployment:deployment3":   6.00,
		"statefulset:statefulset1": 12.00,
		"deployment:deployment1":   12.00,
		IdleSuffix:                 30.00,
		UnallocatedSuffix:          16.00,
	})
	assertAllocationWindow(t, as, "1f", startYesterday, endYesterday, 1440.0)

	// 1g AggregationProperties=(Service)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationServiceProp}, nil)
	assertAllocationSetTotals(t, as, "1g", err, numServices+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1g", map[string]float64{
		"service1":        12.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 70.00,
	})
	assertAllocationWindow(t, as, "1g", startYesterday, endYesterday, 1440.0)

	// 1h AggregationProperties=(Label:app)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{"label:app"}, nil)
	assertAllocationSetTotals(t, as, "1h", err, numLabelApps+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1h", map[string]float64{
		"app=app1":        16.00,
		"app=app2":        24.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 42.00,
	})
	assertAllocationWindow(t, as, "1h", startYesterday, endYesterday, 1440.0)

	// 1i AggregationProperties=(deployment)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationDeploymentProp}, nil)
	assertAllocationSetTotals(t, as, "1i", err, 3+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1i", map[string]float64{
		"deployment1":     12.00,
		"deployment2":     24.00,
		"deployment3":     6.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 40.00,
	})
	assertAllocationWindow(t, as, "1i", startYesterday, endYesterday, 1440.0)

	// 1j AggregationProperties=(Annotation:team)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{"annotation:team"}, nil)
	assertAllocationSetTotals(t, as, "1j", err, 2+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1j", map[string]float64{
		"team=team1":      12.00,
		"team=team2":      6.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 64.00,
	})
	assertAllocationWindow(t, as, "1j", startYesterday, endYesterday, 1440.0)

	// 1k AggregationProperties=(daemonSet)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationDaemonSetProp}, nil)
	assertAllocationSetTotals(t, as, "1k", err, 1+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1k", map[string]float64{
		"daemonset1":      12.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 70.00,
	})
	assertAllocationWindow(t, as, "1k", startYesterday, endYesterday, 1440.0)

	// 1l AggregationProperties=(statefulSet)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationStatefulSetProp}, nil)
	assertAllocationSetTotals(t, as, "1l", err, 1+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1l", map[string]float64{
		"statefulset1":    12.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 70.00,
	})
	assertAllocationWindow(t, as, "1l", startYesterday, endYesterday, 1440.0)

	// 2  Multi-aggregation

	// 2a AggregationProperties=(Cluster, Namespace)
	// 2b AggregationProperties=(Namespace, Label:app)
	// 2c AggregationProperties=(Cluster, Namespace, Pod, Container)

	// 2d AggregationProperties=(Label:app, Label:environment)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{"label:app;env"}, nil)
	// sets should be {idle, unallocated, app1/env1, app2/env2, app2/unallocated}
	assertAllocationSetTotals(t, as, "2d", err, numIdle+numUnallocated+3, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "2d", map[string]float64{
		"app=app1/env=env1":             16.00,
		"app=app2/env=env2":             12.00,
		"app=app2/" + UnallocatedSuffix: 12.00,
		IdleSuffix:                      30.00,
		UnallocatedSuffix:               42.00,
	})

	// 2e AggregationProperties=(Cluster, Label:app, Label:environment)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationClusterProp, "label:app;env"}, nil)
	assertAllocationSetTotals(t, as, "2e", err, 6, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "2e", map[string]float64{
		"cluster1/app=app2/env=env2":             12.00,
		"__idle__":                               30.00,
		"cluster1/app=app1/env=env1":             16.00,
		"cluster1/" + UnallocatedSuffix:          18.00,
		"cluster2/app=app2/" + UnallocatedSuffix: 12.00,
		"cluster2/" + UnallocatedSuffix:          24.00,
	})

	// 2f AggregationProperties=(annotation:team, pod)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationPodProp, "annotation:team"}, nil)
	assertAllocationSetTotals(t, as, "2f", err, 11, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "2f", map[string]float64{
		"pod-jkl/" + UnallocatedSuffix: 6.00,
		"pod-stu/team=team1":           6.00,
		"pod-abc/" + UnallocatedSuffix: 6.00,
		"pod-pqr/" + UnallocatedSuffix: 6.00,
		"pod-def/" + UnallocatedSuffix: 6.00,
		"pod-vwx/team=team1":           6.00,
		"pod-vwx/team=team2":           6.00,
		"pod1/" + UnallocatedSuffix:    16.00,
		"pod-mno/" + UnallocatedSuffix: 12.00,
		"pod-ghi/" + UnallocatedSuffix: 12.00,
		IdleSuffix:                     30.00,
	})

	// // TODO niko/etl

	// // 3  Share idle

	// 3a AggregationProperties=(Namespace) ShareIdle=ShareWeighted
	// namespace1: 42.6875 = 28.00 + 5.00*(3.00/6.00) + 15.0*(13.0/16.0)
	// namespace2: 46.3125 = 36.00 + 5.0*(3.0/6.0) + 15.0*(3.0/16.0) + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
	// namespace3: 23.0000 = 18.00 + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{ShareIdle: ShareWeighted})
	assertAllocationSetTotals(t, as, "3a", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "3a", map[string]float64{
		"namespace1": 42.69,
		"namespace2": 46.31,
		"namespace3": 23.00,
	})
	assertAllocationWindow(t, as, "3a", startYesterday, endYesterday, 1440.0)

	// 3b AggregationProperties=(Namespace) ShareIdle=ShareEven
	// namespace1: 38.0000 = 28.00 + 5.00*(1.0/2.0) + 15.0*(1.0/2.0)
	// namespace2: 51.0000 = 36.00 + 5.0*(1.0/2.0) + 15.0*(1.0/2.0) + 5.0*(1.0/2.0) + 5.0*(1.0/2.0)
	// namespace3: 23.0000 = 18.00 + 5.0*(1.0/2.0) + 5.0*(1.0/2.0)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{ShareIdle: ShareEven})
	assertAllocationSetTotals(t, as, "3a", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "3a", map[string]float64{
		"namespace1": 38.00,
		"namespace2": 51.00,
		"namespace3": 23.00,
	})
	assertAllocationWindow(t, as, "3b", startYesterday, endYesterday, 1440.0)

	// 4  Share resources

	// 4a Share namespace ShareEven
	// namespace1: 37.5000 = 28.00 + 18.00*(1.0/2.0)
	// namespace2: 45.5000 = 36.00 + 18.00*(1.0/2.0)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isNamespace3},
		ShareSplit: ShareEven,
	})
	assertAllocationSetTotals(t, as, "4a", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "4a", map[string]float64{
		"namespace1": 37.00,
		"namespace2": 45.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4a", startYesterday, endYesterday, 1440.0)

	// 4b Share namespace ShareWeighted
	// namespace1: 32.5000 =
	// namespace2: 37.5000 =
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isNamespace3},
		ShareSplit: ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "4b", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "4b", map[string]float64{
		"namespace1": 35.88,
		"namespace2": 46.125,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4b", startYesterday, endYesterday, 1440.0)

	// 4c Share label ShareEven
	// namespace1: 17.3333 = 28.00 - 16.00 + 16.00*(1.0/3.0)
	// namespace2: 41.3333 = 36.00 + 16.00*(1.0/3.0)
	// namespace3: 23.3333 = 18.00 + 16.00*(1.0/3.0)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isApp1},
		ShareSplit: ShareEven,
	})
	assertAllocationSetTotals(t, as, "4c", err, numNamespaces+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "4c", map[string]float64{
		"namespace1": 17.33,
		"namespace2": 41.33,
		"namespace3": 23.33,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4c", startYesterday, endYesterday, 1440.0)

	// 4d Share overhead ShareWeighted
	// namespace1: 85.366 = 28.00 + (7.0*24.0)*(28.00/82.00)
	// namespace2: 109.756 = 36.00 + (7.0*24.0)*(36.00/82.00)
	// namespace3: 54.878 = 18.00 + (7.0*24.0)*(18.00/82.00)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
		ShareSplit:        ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "4d", err, numNamespaces+numIdle, activeTotalCost+idleTotalCost+(sharedOverheadHourlyCost*24.0))
	assertAllocationTotals(t, as, "4d", map[string]float64{
		"namespace1": 85.366,
		"namespace2": 109.756,
		"namespace3": 54.878,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4d", startYesterday, endYesterday, 1440.0)

	// 5  Filters

	isCluster := func(matchCluster string) func(*Allocation) bool {
		return func(a *Allocation) bool {
			cluster := a.Properties.Cluster
			return cluster == matchCluster
		}
	}

	isNamespace := func(matchNamespace string) func(*Allocation) bool {
		return func(a *Allocation) bool {
			namespace := a.Properties.Namespace
			return namespace == matchNamespace
		}
	}

	// 5a Filter by cluster with separate idle
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationClusterProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isCluster("cluster1")},
		ShareIdle:   ShareNone,
	})
	assertAllocationSetTotals(t, as, "5a", err, 2, 66.0)
	assertAllocationTotals(t, as, "5a", map[string]float64{
		"cluster1": 46.00,
		IdleSuffix: 20.00,
	})
	assertAllocationWindow(t, as, "5a", startYesterday, endYesterday, 1440.0)

	// 5b Filter by cluster with shared idle
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationClusterProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isCluster("cluster1")},
		ShareIdle:   ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "5b", err, 1, 66.0)
	assertAllocationTotals(t, as, "5b", map[string]float64{
		"cluster1": 66.00,
	})
	assertAllocationWindow(t, as, "5b", startYesterday, endYesterday, 1440.0)

	// 5c Filter by cluster, agg by namespace, with separate idle
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isCluster("cluster1")},
		ShareIdle:   ShareNone,
	})
	assertAllocationSetTotals(t, as, "5c", err, 3, 66.0)
	assertAllocationTotals(t, as, "5c", map[string]float64{
		"namespace1": 28.00,
		"namespace2": 18.00,
		IdleSuffix:   20.00,
	})
	assertAllocationWindow(t, as, "5c", startYesterday, endYesterday, 1440.0)

	// 5d Filter by namespace, agg by cluster, with separate idle
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationClusterProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareIdle:   ShareNone,
	})
	assertAllocationSetTotals(t, as, "5d", err, 3, 46.31)
	assertAllocationTotals(t, as, "5d", map[string]float64{
		"cluster1": 18.00,
		"cluster2": 18.00,
		IdleSuffix: 10.31,
	})
	assertAllocationWindow(t, as, "5d", startYesterday, endYesterday, 1440.0)

	// 6  Combinations and options

	// 6a SplitIdle
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{SplitIdle: true})
	assertAllocationSetTotals(t, as, "6a", err, numNamespaces+numSplitIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "6a", map[string]float64{
		"namespace1":                           28.00,
		"namespace2":                           36.00,
		"namespace3":                           18.00,
		fmt.Sprintf("cluster1/%s", IdleSuffix): 20.00,
		fmt.Sprintf("cluster2/%s", IdleSuffix): 10.00,
	})
	assertAllocationWindow(t, as, "6a", startYesterday, endYesterday, 1440.0)

	// 6b Share idle weighted with filters
	// Should match values from unfiltered aggregation (3a)
	// namespace2: 46.3125 = 36.00 + 5.0*(3.0/6.0) + 15.0*(3.0/16.0) + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareIdle:   ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "6b", err, 1, 46.31)
	assertAllocationTotals(t, as, "6b", map[string]float64{
		"namespace2": 46.31,
	})
	assertAllocationWindow(t, as, "6b", startYesterday, endYesterday, 1440.0)

	// 6c Share idle even with filters
	// Should match values from unfiltered aggregation (3b)
	// namespace2: 51.0000 = 36.00 + 5.0*(1.0/2.0) + 15.0*(1.0/2.0) + 5.0*(1.0/2.0) + 5.0*(1.0/2.0)
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareIdle:   ShareEven,
	})
	assertAllocationSetTotals(t, as, "6b", err, 1, 51.00)
	assertAllocationTotals(t, as, "6b", map[string]float64{
		"namespace2": 51.00,
	})
	assertAllocationWindow(t, as, "6b", startYesterday, endYesterday, 1440.0)

	// 6d Share overhead with filters
	// namespace1: 85.366 = 28.00 + (7.0*24.0)*(28.00/82.00)
	// namespace2: 109.756 = 36.00 + (7.0*24.0)*(36.00/82.00)
	// namespace3: 54.878 = 18.00 + (7.0*24.0)*(18.00/82.00)
	// idle:       30.0000
	// Then namespace 2 is filtered.
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs:       []AllocationMatchFunc{isNamespace("namespace2")},
		SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
		ShareSplit:        ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "6d", err, 2, 139.756)
	assertAllocationTotals(t, as, "6d", map[string]float64{
		"namespace2": 109.756,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "6d", startYesterday, endYesterday, 1440.0)

	// 6e Share resources with filters
	// --- Shared ---
	// namespace1: 28.00 (gets shared among namespace2 and namespace3)
	// --- Filtered ---
	// namespace3: 27.33 = 18.00 + (28.00)*(18.00/54.00) (filtered out)
	// --- Results ---
	// namespace2: 54.667 = 36.00 + (28.00)*(36.00/54.00)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareFuncs:  []AllocationMatchFunc{isNamespace("namespace1")},
		ShareSplit:  ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "6e", err, 2, 84.667)
	assertAllocationTotals(t, as, "6e", map[string]float64{
		"namespace2": 54.667,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "6e", startYesterday, endYesterday, 1440.0)

	// 6f Share idle weighted and share resources weighted
	//
	// First, share idle weighted produces:
	//
	// namespace1:      42.6875
	//   initial cost   28.0000
	//   cluster1.cpu    2.5000 = 5.00*(3.00/6.00)
	//   cluster1.ram   12.1875 = 15.00*(13.0/16.0)
	//
	// namespace2:      46.3125
	//   initial cost   36.0000
	//   cluster1.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster1.ram    2.8125 = 15.00*(3.0/16.0)
	//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
	//
	// namespace3:      23.0000
	//   initial cost   18.0000
	//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
	//
	// Then, sharing namespace1 means sharing 39.6875 according to coefficients
	// computed before allocating idle (so that weighting idle differently
	// doesn't adversely affect the sharing mechanism):
	//
	// namespace2:      74.7708
	//   initial cost   30.0000
	//   idle cost      10.3125
	//   shared cost    28.4583 = (42.6875)*(36.0/54.0)
	//
	// namespace3:      37.2292
	//   initial cost   18.0000
	//   idle cost       5.0000
	//   shared cost    14.2292 = (42.6875)*(18.0/54.0)
	//
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isNamespace("namespace1")},
		ShareSplit: ShareWeighted,
		ShareIdle:  ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "6f", err, 2, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "6f", map[string]float64{
		"namespace2": 74.77,
		"namespace3": 37.23,
	})
	assertAllocationWindow(t, as, "6f", startYesterday, endYesterday, 1440.0)

	// 6g Share idle, share resources, and filter
	//
	// First, share idle weighted produces:
	//
	// namespace1:      42.6875
	//   initial cost   28.0000
	//   cluster1.cpu    2.5000 = 5.00*(3.00/6.00)
	//   cluster1.ram   12.1875 = 15.00*(13.0/16.0)
	//
	// namespace2:      46.3125
	//   initial cost   36.0000
	//   cluster1.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster1.ram    2.8125 = 15.00*(3.0/16.0)
	//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
	//
	// namespace3:      23.0000
	//   initial cost   18.0000
	//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
	//
	// Then, sharing namespace1 means sharing 39.6875 according to coefficients
	// computed before allocating idle (so that weighting idle differently
	// doesn't adversely affect the sharing mechanism):
	//
	// namespace2:      74.7708
	//   initial cost   36.0000
	//   idle cost      10.3125
	//   shared cost    28.4583 = (42.6875)*(36.0/54.0)
	//
	// namespace3:      37.2292
	//   initial cost   18.0000
	//   idle cost       5.0000
	//   shared cost    14.2292 = (42.6875)*(18.0/54.0)
	//
	// Then, filter for namespace2: 74.7708
	//
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareFuncs:  []AllocationMatchFunc{isNamespace("namespace1")},
		ShareSplit:  ShareWeighted,
		ShareIdle:   ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "6g", err, 1, 74.77)
	assertAllocationTotals(t, as, "6g", map[string]float64{
		"namespace2": 74.77,
	})
	assertAllocationWindow(t, as, "6g", startYesterday, endYesterday, 1440.0)

	// 6h Share idle, share resources, share overhead
	//
	// Share idle weighted:
	//
	// namespace1:      42.6875
	//   initial cost   28.0000
	//   cluster1.cpu    2.5000 = 5.00*(3.00/6.00)
	//   cluster1.ram   12.1875 = 15.00*(13.0/16.0)
	//
	// namespace2:      46.3125
	//   initial cost   36.0000
	//   cluster1.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster1.ram    2.8125 = 15.00*(3.0/16.0)
	//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
	//
	// namespace3:      23.0000
	//   initial cost   18.0000
	//   cluster2.cpu    2.5000 = 5.00*(3.0/6.0)
	//   cluster2.ram    2.5000 = 5.00*(3.0/6.0)
	//
	// Then share overhead:
	//
	// namespace1:     100.0533 = 42.6875 + (7.0*24.0)*(28.00/82.00)
	// namespace2:     120.0686 = 46.3125 + (7.0*24.0)*(36.00/82.00)
	// namespace3:      59.8780 = 23.0000 + (7.0*24.0)*(18.00/82.00)
	//
	// Then namespace 2 is filtered.
	as = generateAllocationSet(start)
	err = as.AggregateBy([]string{AllocationNamespaceProp}, &AllocationAggregationOptions{
		FilterFuncs:       []AllocationMatchFunc{isNamespace("namespace2")},
		ShareSplit:        ShareWeighted,
		ShareIdle:         ShareWeighted,
		SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
	})
	assertAllocationSetTotals(t, as, "6h", err, 1, 120.07)
	assertAllocationTotals(t, as, "6h", map[string]float64{
		"namespace2": 120.07,
	})
	assertAllocationWindow(t, as, "6h", startYesterday, endYesterday, 1440.0)

	// 7  Edge cases and errors

	// 7a Empty AggregationProperties
	// 7b Filter all
	// 7c Share all
	// 7d Share and filter the same allocations
}

// TODO niko/etl
//func TestAllocationSet_Clone(t *testing.T) {}

func TestAllocationSet_ComputeIdleAllocations(t *testing.T) {
	var as *AllocationSet
	var err error
	var idles map[string]*Allocation

	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day)

	// Generate AllocationSet and strip out any existing idle allocations
	as = generateAllocationSet(start)
	for key := range as.idleKeys {
		as.Delete(key)
	}

	assetSets := generateAssetSets(start, end)

	cases := map[string]struct {
		allocationSet *AllocationSet
		assetSet      *AssetSet
		clusters      map[string]Allocation
	}{
		"1a": {
			allocationSet: as,
			assetSet:      assetSets[0],
			clusters: map[string]Allocation{
				"cluster1": {
					CPUCost: 44.0,
					RAMCost: 24.0,
					GPUCost: 4.0,
				},
				"cluster2": {
					CPUCost: 44.0,
					RAMCost: 34.0,
					GPUCost: 4.0,
				},
			},
		},
		"1b": {
			allocationSet: as,
			assetSet:      assetSets[1],
			clusters: map[string]Allocation{
				"cluster1": {
					CPUCost: 44.0,
					RAMCost: 24.0,
					GPUCost: 4.0,
				},
				"cluster2": {
					CPUCost: 44.0,
					RAMCost: 34.0,
					GPUCost: 4.0,
				},
			},
		},
	}

	for name, testcase := range cases {
		t.Run(name, func(t *testing.T) {
			idles, err = as.ComputeIdleAllocations(testcase.assetSet)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if len(idles) != len(testcase.clusters) {
				t.Fatalf("idles: expected length %d; got length %d", len(testcase.clusters), len(idles))
			}

			for clusterName, cluster := range testcase.clusters {
				if idle, ok := idles[clusterName]; !ok {
					t.Fatalf("expected idle cost for %s", clusterName)
				} else {
					if !util.IsApproximately(idle.TotalCost(), cluster.TotalCost()) {
						t.Fatalf("%s idle: expected total cost %f; got total cost %f", clusterName, cluster.TotalCost(), idle.TotalCost())
					}
				}
				if !util.IsApproximately(idles[clusterName].CPUCost, cluster.CPUCost) {
					t.Fatalf("expected idle CPU cost for %s to be %.2f; got %.2f", clusterName, cluster.CPUCost, idles[clusterName].CPUCost)
				}
				if !util.IsApproximately(idles[clusterName].RAMCost, cluster.RAMCost) {
					t.Fatalf("expected idle RAM cost for %s to be %.2f; got %.2f", clusterName, cluster.RAMCost, idles[clusterName].RAMCost)
				}
				if !util.IsApproximately(idles[clusterName].GPUCost, cluster.GPUCost) {
					t.Fatalf("expected idle GPU cost for %s to be %.2f; got %.2f", clusterName, cluster.GPUCost, idles[clusterName].GPUCost)
				}
			}
		})
	}
}

func TestAllocationSet_AllocateAssetCosts(t *testing.T) {
	var as *AllocationSet
	var err error

	end := time.Now().UTC().Truncate(day)
	start := end.Add(-day)

	// Generate AllocationSet and strip out any existing idle allocations
	as = generateAllocationSet(start)
	for key := range as.idleKeys {
		as.Delete(key)
	}

	for _, a := range as.allocations {
		// add reconcilable pvs to pod-mno
		if a.Properties.Pod == "pod-mno" {
			a.PVs = a.PVs.Add(PVAllocations{
				disk1: {
					Cost:      2.5,
					ByteHours: 2.5 * gb,
				},
				disk2: {
					Cost:      5,
					ByteHours: 5 * gb,
				},
			})
		}
		// add loadBalancer service to allocations
		if a.Name == "cluster2/namespace2/pod-mno/container4" {
			a.Properties.Services = append(a.Properties.Services, "loadBalancer1")
		}
		if a.Name == "cluster2/namespace2/pod-mno/container5" {
			a.Properties.Services = append(a.Properties.Services, "loadBalancer2")
		}
		if a.Name == "cluster2/namespace2/pod-pqr/container6" {
			a.Properties.Services = append(a.Properties.Services, "loadBalancer1")
			a.Properties.Services = append(a.Properties.Services, "loadBalancer2")
		}
	}

	assetSets := generateAssetSets(start, end)

	cases := map[string]struct {
		allocationSet *AllocationSet
		assetSet      *AssetSet
		reconcile     bool
		shareOverhead bool
		allocations   map[string]Allocation
	}{
		"1a": {
			allocationSet: as.Clone(),
			assetSet:      assetSets[0],
			reconcile:     true,
			shareOverhead: true,
			allocations: map[string]Allocation{
				// Allocation adjustments are found with the formula:
				// ADJUSTMENT_RATE * NODE_COST * (ALLOC_HOURS / NODE_HOURS) - ALLOC_COST
				// ADJUSTMENT_RATE: 0.90909090909
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    55	    |	  8	     |     1 	  |	  1
				// RAM	|    44	    |	  6	     |     11 	  |	  1
				// GPU	|    11	    |	 24      |     1 	  |	  1
				"cluster1/namespace1/pod1/container1": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: -4.333333,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				// ADJUSTMENT_RATE: 0.90909090909
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    55	    |	  8	     |     1 	  |	  1
				// RAM	|    44	    |	  6	     |     1 	  |	  1
				// GPU	|    11	    |	 24      |     1 	  |	  1
				"cluster1/namespace1/pod-abc/container2": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace1/pod-def/container3": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace2/pod-ghi/container4": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace2/pod-ghi/container5": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace2/pod-jkl/container6": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				// ADJUSTMENT_RATE: 1.0
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    20	    |	  4	     |     1 	  |	  1
				// RAM	|    15	    |	  3	     |     1 	  |	  1
				// GPU	|    0	    |	  0      |     1 	  |	  1
				"cluster2/namespace2/pod-mno/container4": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -1.0,
					PVCostAdjustment:  2.0,
					NetworkCostAdjustment: 1.0,
					LoadBalancerCostAdjustment: 4.0,
					SharedCost:        0.833333,
				},
				"cluster2/namespace2/pod-mno/container5": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -1.0,
					PVCostAdjustment:  2.0,
					NetworkCostAdjustment: 1.0,
					LoadBalancerCostAdjustment: 6.5,
					SharedCost:        0.833333,
				},
				// ADJUSTMENT_RATE: 1.0
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    20	    |	  3	     |     1 	  |	  1
				// RAM	|    15	    |	  2	     |     1 	  |	  1
				// GPU	|    0	    |	  0      |     1 	  |	  1
				"cluster2/namespace2/pod-pqr/container6": {
					CPUCostAdjustment: 5.666667,
					RAMCostAdjustment: 6.5,
					GPUCostAdjustment: -1.0,
					NetworkCostAdjustment: 1.5,
					LoadBalancerCostAdjustment: 11.5,
					SharedCost:        1.333333,
				},
				"cluster2/namespace3/pod-stu/container7": {
					CPUCostAdjustment: 5.666667,
					RAMCostAdjustment: 6.5,
					GPUCostAdjustment: -1.0,
					NetworkCostAdjustment: 1.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        1.333333,
				},
				// ADJUSTMENT_RATE: 1.0
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    10	    |	  2	     |     1 	  |	  1
				// RAM	|    10	    |	  2	     |     1 	  |	  1
				// GPU	|    10	    |	 24      |     1 	  |	  1
				"cluster2/namespace3/pod-vwx/container8": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -0.583333,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        1.833333,
				},
				"cluster2/namespace3/pod-vwx/container9": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -0.583333,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        1.833333,
				},
			},
		},
		"1b": {
			allocationSet: as.Clone(),
			assetSet:      assetSets[1],
			reconcile:     true,
			shareOverhead: true,
			allocations: map[string]Allocation{
				// ADJUSTMENT_RATE: 10
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|     5	    |	  8	     |     1 	  |	  1
				// RAM	|     4	    |	  6	     |    11 	  |	  1
				// GPU	|     1	    |	 24      |     1 	  |	  1
				"cluster1/namespace1/pod1/container1": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: -4.333333,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				// ADJUSTMENT_RATE: 10
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|     5	    |	  8	     |     1 	  |	  1
				// RAM	|     4	    |	  6	     |     1 	  |	  1
				// GPU	|     1	    |	 24      |     1 	  |	  1
				"cluster1/namespace1/pod-abc/container2": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.6666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace1/pod-def/container3": {
					CPUCostAdjustment:    5.25,
					RAMCostAdjustment:    5.6666667,
					GPUCostAdjustment:    -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost: 0.333333,
				},
				"cluster1/namespace2/pod-ghi/container4": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.6666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace2/pod-ghi/container5": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.6666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				"cluster1/namespace2/pod-jkl/container6": {
					CPUCostAdjustment: 5.25,
					RAMCostAdjustment: 5.6666667,
					GPUCostAdjustment: -0.583333,
					NetworkCostAdjustment: -0.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        0.333333,
				},
				// ADJUSTMENT_RATE: 1.0
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    20	    |	  4	     |     1 	  |	  1
				// RAM	|    15	    |	  3	     |     1 	  |	  1
				// GPU	|    0	    |	  0      |     1 	  |	  1
				"cluster2/namespace2/pod-mno/container4": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -1.0,
					PVCostAdjustment:  -0.5,
					NetworkCostAdjustment: 1.0,
					LoadBalancerCostAdjustment: 4.0,
					SharedCost:        0.833333,
				},
				"cluster2/namespace2/pod-mno/container5": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -1.0,
					PVCostAdjustment:  -0.5,
					NetworkCostAdjustment: 1.0,
					LoadBalancerCostAdjustment: 6.5,
					SharedCost:        0.833333,
				},
				// ADJUSTMENT_RATE: 1.0
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    20	    |	  3	     |     1 	  |	  1
				// RAM	|    15	    |	  2	     |     1 	  |	  1
				// GPU	|    0	    |	  0      |     1 	  |	  1
				"cluster2/namespace2/pod-pqr/container6": {
					CPUCostAdjustment: 5.666667,
					RAMCostAdjustment: 6.5,
					GPUCostAdjustment: -1.0,
					NetworkCostAdjustment: 1.5,
					LoadBalancerCostAdjustment: 11.5,
					SharedCost:        1.333333,
				},
				"cluster2/namespace3/pod-stu/container7": {
					CPUCostAdjustment: 5.666667,
					RAMCostAdjustment: 6.5,
					GPUCostAdjustment: -1.0,
					NetworkCostAdjustment: 1.5,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        1.333333,
				},
				// ADJUSTMENT_RATE: 1.0
				// Type | NODE_COST | NODE_HOURs | ALLOC_COST | ALLOC_HOURS
				// CPU	|    10	    |	  2	     |     1 	  |	  1
				// RAM	|    10	    |	  2	     |     1 	  |	  1
				// GPU	|    10	    |	 24      |     1 	  |	  1
				"cluster2/namespace3/pod-vwx/container8": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -0.583333,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        1.833333,
				},
				"cluster2/namespace3/pod-vwx/container9": {
					CPUCostAdjustment: 4.0,
					RAMCostAdjustment: 4.0,
					GPUCostAdjustment: -0.583333,
					LoadBalancerCostAdjustment: -1.0,
					SharedCost:        1.833333,
				},
			},
		},
	}

	for name, testcase := range cases {
		t.Run(name, func(t *testing.T) {
			err = testcase.allocationSet.AllocateAssetCosts(testcase.assetSet, testcase.reconcile, testcase.shareOverhead)
			reconAllocs := testcase.allocationSet.allocations
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			for allocationName, testAlloc := range testcase.allocations {
				if _, ok := reconAllocs[allocationName]; !ok {
					t.Fatalf("expected allocation %s", allocationName)
				}

				if !util.IsApproximately(reconAllocs[allocationName].CPUCostAdjustment, testAlloc.CPUCostAdjustment) {
					t.Fatalf("expected CPU Adjustment for %s to be %f; got %f", allocationName, testAlloc.CPUCostAdjustment, reconAllocs[allocationName].CPUCostAdjustment)
				}
				if !util.IsApproximately(reconAllocs[allocationName].RAMCostAdjustment, testAlloc.RAMCostAdjustment) {
					t.Fatalf("expected RAM Adjustment for %s to be %f; got %f", allocationName, testAlloc.RAMCostAdjustment, reconAllocs[allocationName].RAMCostAdjustment)
				}
				if !util.IsApproximately(reconAllocs[allocationName].GPUCostAdjustment, testAlloc.GPUCostAdjustment) {
					t.Fatalf("expected GPU Adjustment for %s to be %f; got %f", allocationName, testAlloc.GPUCostAdjustment, reconAllocs[allocationName].GPUCostAdjustment)
				}
				if !util.IsApproximately(reconAllocs[allocationName].PVCostAdjustment, testAlloc.PVCostAdjustment) {
					t.Fatalf("expected PV Adjustment for %s to be %f; got %f", allocationName, testAlloc.PVCostAdjustment, reconAllocs[allocationName].PVCostAdjustment)
				}
				if !util.IsApproximately(reconAllocs[allocationName].NetworkCostAdjustment, testAlloc.NetworkCostAdjustment) {
					t.Fatalf("expected Network Adjustment for %s to be %f; got %f", allocationName, testAlloc.NetworkCostAdjustment, reconAllocs[allocationName].NetworkCostAdjustment)
				}
				if !util.IsApproximately(reconAllocs[allocationName].LoadBalancerCostAdjustment, testAlloc.LoadBalancerCostAdjustment) {
					t.Fatalf("expected Load Balancer Adjustment for %s to be %f; got %f", allocationName, testAlloc.LoadBalancerCostAdjustment, reconAllocs[allocationName].LoadBalancerCostAdjustment)
				}
				if !util.IsApproximately(reconAllocs[allocationName].SharedCost, testAlloc.SharedCost) {
					t.Fatalf("expected Shared Cost for %s to be %f; got %f", allocationName, testAlloc.SharedCost, reconAllocs[allocationName].SharedCost)
				}

			}
		})
	}
}

// TODO niko/etl
//func TestAllocationSet_Delete(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_End(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_IdleAllocations(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Insert(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_IsEmpty(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Length(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Map(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_MarshalJSON(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Resolution(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Seconds(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Set(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_Start(t *testing.T) {}

// TODO niko/etl
//func TestAllocationSet_TotalCost(t *testing.T) {}

// TODO niko/etl
//func TestNewAllocationSetRange(t *testing.T) {}

func TestAllocationSetRange_Accumulate(t *testing.T) {
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	// Accumulating any combination of nil and/or empty set should result in empty set
	result, err := NewAllocationSetRange(nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	result, err = NewAllocationSetRange(nil, nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	result, err = NewAllocationSetRange(NewAllocationSet(yesterday, today)).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	result, err = NewAllocationSetRange(nil, NewAllocationSet(ago2d, yesterday), nil, NewAllocationSet(today, tomorrow), nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating nil AllocationSetRange: %s", err)
	}
	if !result.IsEmpty() {
		t.Fatalf("accumulating nil AllocationSetRange: expected empty; actual %s", result)
	}

	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewUnitAllocation("", today, day, nil))

	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewUnitAllocation("", yesterday, day, nil))

	// Accumulate non-nil with nil should result in copy of non-nil, regardless of order
	result, err = NewAllocationSetRange(nil, todayAS).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 6.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 6.0; actual %f", result.TotalCost())
	}

	result, err = NewAllocationSetRange(todayAS, nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 6.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 6.0; actual %f", result.TotalCost())
	}

	result, err = NewAllocationSetRange(nil, todayAS, nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 6.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 6.0; actual %f", result.TotalCost())
	}

	// Accumulate two non-nil should result in sum of both with appropriate start, end
	result, err = NewAllocationSetRange(yesterdayAS, todayAS).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 12.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 12.0; actual %f", result.TotalCost())
	}
	allocMap := result.Map()
	if len(allocMap) != 1 {
		t.Fatalf("accumulating AllocationSetRange: expected length 1; actual length %d", len(allocMap))
	}
	alloc := allocMap["cluster1/namespace1/pod1/container1"]
	if alloc == nil {
		t.Fatalf("accumulating AllocationSetRange: expected allocation 'cluster1/namespace1/pod1/container1'")
	}
	if alloc.CPUCoreHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", result.TotalCost())
	}
	if alloc.CPUCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.CPUCost)
	}
	if alloc.CPUEfficiency() != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.CPUEfficiency())
	}
	if alloc.GPUHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.GPUHours)
	}
	if alloc.GPUCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.GPUCost)
	}
	if alloc.NetworkCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.NetworkCost)
	}
	if alloc.LoadBalancerCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.LoadBalancerCost)
	}
	if alloc.PVByteHours() != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.PVByteHours())
	}
	if alloc.PVCost() != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.PVCost())
	}
	if alloc.RAMByteHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.RAMByteHours)
	}
	if alloc.RAMCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.RAMCost)
	}
	if alloc.RAMEfficiency() != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.RAMEfficiency())
	}
	if alloc.TotalCost() != 12.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 12.0; actual %f", alloc.TotalCost())
	}
	if alloc.TotalEfficiency() != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.TotalEfficiency())
	}
	if !alloc.Start.Equal(yesterday) {
		t.Fatalf("accumulating AllocationSetRange: expected to start %s; actual %s", yesterday, alloc.Start)
	}
	if !alloc.End.Equal(tomorrow) {
		t.Fatalf("accumulating AllocationSetRange: expected to end %s; actual %s", tomorrow, alloc.End)
	}
	if alloc.Minutes() != 2880.0 {
		t.Fatalf("accumulating AllocationSetRange: expected %f minutes; actual %f", 2880.0, alloc.Minutes())
	}
}

// TODO niko/etl
// func TestAllocationSetRange_AccumulateBy(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_AggregateBy(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Append(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Each(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Get(t *testing.T) {}

func TestAllocationSetRange_InsertRange(t *testing.T) {
	// Set up
	ago2d := time.Now().UTC().Truncate(day).Add(-2 * day)
	yesterday := time.Now().UTC().Truncate(day).Add(-day)
	today := time.Now().UTC().Truncate(day)
	tomorrow := time.Now().UTC().Truncate(day).Add(day)

	unit := NewUnitAllocation("", today, day, nil)

	ago2dAS := NewAllocationSet(ago2d, yesterday)
	ago2dAS.Set(NewUnitAllocation("a", ago2d, day, nil))
	ago2dAS.Set(NewUnitAllocation("b", ago2d, day, nil))
	ago2dAS.Set(NewUnitAllocation("c", ago2d, day, nil))

	yesterdayAS := NewAllocationSet(yesterday, today)
	yesterdayAS.Set(NewUnitAllocation("a", yesterday, day, nil))
	yesterdayAS.Set(NewUnitAllocation("b", yesterday, day, nil))
	yesterdayAS.Set(NewUnitAllocation("c", yesterday, day, nil))

	todayAS := NewAllocationSet(today, tomorrow)
	todayAS.Set(NewUnitAllocation("a", today, day, nil))
	todayAS.Set(NewUnitAllocation("b", today, day, nil))
	todayAS.Set(NewUnitAllocation("c", today, day, nil))

	var nilASR *AllocationSetRange
	thisASR := NewAllocationSetRange(yesterdayAS.Clone(), todayAS.Clone())
	thatASR := NewAllocationSetRange(yesterdayAS.Clone())
	longASR := NewAllocationSetRange(ago2dAS.Clone(), yesterdayAS.Clone(), todayAS.Clone())
	var err error

	// Expect an error calling InsertRange on nil
	err = nilASR.InsertRange(thatASR)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Expect nothing to happen calling InsertRange(nil) on non-nil ASR
	err = thisASR.InsertRange(nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	thisASR.Each(func(i int, as *AllocationSet) {
		as.Each(func(k string, a *Allocation) {
			if !util.IsApproximately(a.CPUCoreHours, unit.CPUCoreHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCoreHours, a.CPUCoreHours)
			}
			if !util.IsApproximately(a.CPUCost, unit.CPUCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCost, a.CPUCost)
			}
			if !util.IsApproximately(a.RAMByteHours, unit.RAMByteHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMByteHours, a.RAMByteHours)
			}
			if !util.IsApproximately(a.RAMCost, unit.RAMCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMCost, a.RAMCost)
			}
			if !util.IsApproximately(a.GPUHours, unit.GPUHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUHours, a.GPUHours)
			}
			if !util.IsApproximately(a.GPUCost, unit.GPUCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUCost, a.GPUCost)
			}
			if !util.IsApproximately(a.PVByteHours(), unit.PVByteHours()) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours(), a.PVByteHours())
			}
			if !util.IsApproximately(a.PVCost(), unit.PVCost()) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost(), a.PVCost())
			}
			if !util.IsApproximately(a.NetworkCost, unit.NetworkCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
			}
			if !util.IsApproximately(a.LoadBalancerCost, unit.LoadBalancerCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.LoadBalancerCost, a.LoadBalancerCost)
			}
			if !util.IsApproximately(a.TotalCost(), unit.TotalCost()) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost(), a.TotalCost())
			}
		})
	})

	// Expect an error calling InsertRange with a range exceeding the receiver
	err = thisASR.InsertRange(longASR)
	if err == nil {
		t.Fatalf("expected error calling InsertRange with a range exceeding the receiver")
	}

	// Expect each Allocation in "today" to stay the same, but "yesterday" to
	// precisely double when inserting a range that only has a duplicate of
	// "yesterday", but no entry for "today"
	err = thisASR.InsertRange(thatASR)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	yAS, err := thisASR.Get(0)
	yAS.Each(func(k string, a *Allocation) {
		if !util.IsApproximately(a.CPUCoreHours, 2*unit.CPUCoreHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCoreHours, a.CPUCoreHours)
		}
		if !util.IsApproximately(a.CPUCost, 2*unit.CPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCost, a.CPUCost)
		}
		if !util.IsApproximately(a.RAMByteHours, 2*unit.RAMByteHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMByteHours, a.RAMByteHours)
		}
		if !util.IsApproximately(a.RAMCost, 2*unit.RAMCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMCost, a.RAMCost)
		}
		if !util.IsApproximately(a.GPUHours, 2*unit.GPUHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUHours, a.GPUHours)
		}
		if !util.IsApproximately(a.GPUCost, 2*unit.GPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUCost, a.GPUCost)
		}
		if !util.IsApproximately(a.PVByteHours(), 2*unit.PVByteHours()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours(), a.PVByteHours())
		}
		if !util.IsApproximately(a.PVCost(), 2*unit.PVCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost(), a.PVCost())
		}
		if !util.IsApproximately(a.NetworkCost, 2*unit.NetworkCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
		}
		if !util.IsApproximately(a.LoadBalancerCost, 2*unit.LoadBalancerCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.LoadBalancerCost, a.LoadBalancerCost)
		}

		if !util.IsApproximately(a.TotalCost(), 2*unit.TotalCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost(), a.TotalCost())
		}
	})
	tAS, err := thisASR.Get(1)
	tAS.Each(func(k string, a *Allocation) {
		if !util.IsApproximately(a.CPUCoreHours, unit.CPUCoreHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCoreHours, a.CPUCoreHours)
		}
		if !util.IsApproximately(a.CPUCost, unit.CPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.CPUCost, a.CPUCost)
		}
		if !util.IsApproximately(a.RAMByteHours, unit.RAMByteHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMByteHours, a.RAMByteHours)
		}
		if !util.IsApproximately(a.RAMCost, unit.RAMCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.RAMCost, a.RAMCost)
		}
		if !util.IsApproximately(a.GPUHours, unit.GPUHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUHours, a.GPUHours)
		}
		if !util.IsApproximately(a.GPUCost, unit.GPUCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.GPUCost, a.GPUCost)
		}
		if !util.IsApproximately(a.PVByteHours(), unit.PVByteHours()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours(), a.PVByteHours())
		}
		if !util.IsApproximately(a.PVCost(), unit.PVCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost(), a.PVCost())
		}
		if !util.IsApproximately(a.NetworkCost, unit.NetworkCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
		}
		if !util.IsApproximately(a.LoadBalancerCost, unit.LoadBalancerCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.LoadBalancerCost, a.LoadBalancerCost)
		}
		if !util.IsApproximately(a.TotalCost(), unit.TotalCost()) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost(), a.TotalCost())
		}
	})
}

// TODO niko/etl
// func TestAllocationSetRange_Length(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_MarshalJSON(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Slice(t *testing.T) {}

// TODO niko/etl
// func TestAllocationSetRange_Window(t *testing.T) {}
