package kubecost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/util"
)

func TestSummaryAllocation_Add(t *testing.T) {
	window, _ := ParseWindowUTC("yesterday")

	var sa1, sa2, osa1, osa2, nilsa *SummaryAllocation
	var err error

	sa1Start := *window.Start()

	sa1End := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                  sa1Start,
		End:                    sa1End,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                0.2,
		GPUCost:                1.0,
		NetworkCost:            0.1,
		LoadBalancerCost:       0.6,
		PVCost:                 0.005,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.05,
		SharedCost:             1.0,
		ExternalCost:           1.0,
	}
	osa1 = sa1.Clone()

	// sa2 is just as expensive, with twice as much usage and request, and half
	// the time compared to sa1

	sa2Start := *window.Start()
	sa2Start = sa2Start.Add(6 * time.Hour)

	sa2End := *window.End()
	sa2End = sa2End.Add(-6 * time.Hour)

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod2/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod2",
			Container: "container2",
		},
		Start:                  sa2Start,
		End:                    sa2End,
		CPUCoreRequestAverage:  sa1.CPUCoreRequestAverage * 2.0,
		CPUCoreUsageAverage:    sa1.CPUCoreUsageAverage * 2.0,
		CPUCost:                sa1.CPUCost,
		GPUCost:                sa1.GPUCost,
		NetworkCost:            sa1.NetworkCost,
		LoadBalancerCost:       sa1.LoadBalancerCost,
		PVCost:                 sa1.PVCost,
		RAMBytesRequestAverage: sa1.RAMBytesRequestAverage * 2.0,
		RAMBytesUsageAverage:   sa1.RAMBytesUsageAverage * 2.0,
		RAMCost:                sa1.RAMCost,
		SharedCost:             sa1.SharedCost,
		ExternalCost:           sa1.ExternalCost,
	}
	osa2 = sa2.Clone()

	// add nil to nil, expect and error
	t.Run("nil.Add(nil)", func(t *testing.T) {
		err = nilsa.Add(nilsa)
		if err == nil {
			t.Fatalf("expected error: cannot add nil SummaryAllocations")
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add sa1 to nil, expect and error
	t.Run("nil.Add(sa1)", func(t *testing.T) {
		err = nilsa.Add(sa1)
		if err == nil {
			t.Fatalf("expected error: cannot add nil SummaryAllocations")
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add nil to sa1, expect and error
	t.Run("sa1.Add(nil)", func(t *testing.T) {
		err = sa1.Add(nilsa)
		if err == nil {
			t.Fatalf("expected error: cannot add nil SummaryAllocations")
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add sa1 to sa2 and expect same averages, but double costs
	t.Run("sa2.Add(sa1)", func(t *testing.T) {
		err = sa2.Add(sa1)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if sa2.Properties != nil {
			t.Fatalf("expected properties to be nil; actual: %s", sa1.Properties)
		}
		if !util.IsApproximately(sa2.CPUCoreRequestAverage, (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage) {
			t.Fatalf("incorrect CPUCoreRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreRequestAverage)
		}
		if !util.IsApproximately(sa2.CPUCoreUsageAverage, (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreUsageAverage) {
			t.Fatalf("incorrect CPUCoreUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreUsageAverage)
		}
		if !util.IsApproximately(sa2.RAMBytesRequestAverage, (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage) {
			t.Fatalf("incorrect RAMBytesRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesRequestAverage)
		}
		if !util.IsApproximately(sa2.RAMBytesUsageAverage, (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesUsageAverage) {
			t.Fatalf("incorrect RAMBytesUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesUsageAverage)
		}
		if !util.IsApproximately(sa2.CPUCost, osa2.CPUCost+osa1.CPUCost) {
			t.Fatalf("incorrect CPUCost: expected %.5f; actual %.5f", osa2.CPUCost+osa1.CPUCost, sa2.CPUCost)
		}
		if !util.IsApproximately(sa2.GPUCost, osa2.GPUCost+osa1.GPUCost) {
			t.Fatalf("incorrect GPUCost: expected %.5f; actual %.5f", osa2.GPUCost+osa1.GPUCost, sa2.GPUCost)
		}
		if !util.IsApproximately(sa2.NetworkCost, osa2.NetworkCost+osa1.NetworkCost) {
			t.Fatalf("incorrect NetworkCost: expected %.5f; actual %.5f", osa2.NetworkCost+osa1.NetworkCost, sa2.NetworkCost)
		}
		if !util.IsApproximately(sa2.LoadBalancerCost, osa2.LoadBalancerCost+osa1.LoadBalancerCost) {
			t.Fatalf("incorrect LoadBalancerCost: expected %.5f; actual %.5f", osa2.LoadBalancerCost+osa1.LoadBalancerCost, sa2.LoadBalancerCost)
		}
		if !util.IsApproximately(sa2.PVCost, osa2.PVCost+osa1.PVCost) {
			t.Fatalf("incorrect PVCost: expected %.5f; actual %.5f", osa2.PVCost+osa1.PVCost, sa2.PVCost)
		}
		if !util.IsApproximately(sa2.RAMCost, osa2.RAMCost+osa1.RAMCost) {
			t.Fatalf("incorrect RAMCost: expected %.5f; actual %.5f", osa2.RAMCost+osa1.RAMCost, sa2.RAMCost)
		}
		if !util.IsApproximately(sa2.SharedCost, osa2.SharedCost+osa1.SharedCost) {
			t.Fatalf("incorrect SharedCost: expected %.5f; actual %.5f", osa2.SharedCost+osa1.SharedCost, sa2.SharedCost)
		}
		if !util.IsApproximately(sa2.ExternalCost, osa2.ExternalCost+osa1.ExternalCost) {
			t.Fatalf("incorrect ExternalCost: expected %.5f; actual %.5f", osa2.ExternalCost+osa1.ExternalCost, sa2.ExternalCost)
		}
	})

	// reset
	sa1 = osa1.Clone()
	sa2 = osa2.Clone()

	// add sa2 to sa1 and expect same averages, but double costs
	t.Run("sa1.Add(sa2)", func(t *testing.T) {
		err = sa1.Add(sa2)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if sa1.Properties != nil {
			t.Fatalf("expected properties to be nil; actual: %s", sa1.Properties)
		}
		if !util.IsApproximately(sa1.CPUCoreRequestAverage, (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage) {
			t.Fatalf("incorrect CPUCoreRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreRequestAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreRequestAverage)
		}
		if !util.IsApproximately(sa1.CPUCoreUsageAverage, (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreUsageAverage) {
			t.Fatalf("incorrect CPUCoreUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.CPUCoreUsageAverage)+osa1.CPUCoreRequestAverage, sa2.CPUCoreUsageAverage)
		}
		if !util.IsApproximately(sa1.RAMBytesRequestAverage, (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage) {
			t.Fatalf("incorrect RAMBytesRequestAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesRequestAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesRequestAverage)
		}
		if !util.IsApproximately(sa1.RAMBytesUsageAverage, (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesUsageAverage) {
			t.Fatalf("incorrect RAMBytesUsageAverage: expected %.5f; actual %.5f", (0.5*osa2.RAMBytesUsageAverage)+osa1.RAMBytesRequestAverage, sa2.RAMBytesUsageAverage)
		}
		if !util.IsApproximately(sa1.CPUCost, osa2.CPUCost+osa1.CPUCost) {
			t.Fatalf("incorrect CPUCost: expected %.5f; actual %.5f", osa2.CPUCost+osa1.CPUCost, sa2.CPUCost)
		}
		if !util.IsApproximately(sa1.GPUCost, osa2.GPUCost+osa1.GPUCost) {
			t.Fatalf("incorrect GPUCost: expected %.5f; actual %.5f", osa2.GPUCost+osa1.GPUCost, sa2.GPUCost)
		}
		if !util.IsApproximately(sa1.NetworkCost, osa2.NetworkCost+osa1.NetworkCost) {
			t.Fatalf("incorrect NetworkCost: expected %.5f; actual %.5f", osa2.NetworkCost+osa1.NetworkCost, sa2.NetworkCost)
		}
		if !util.IsApproximately(sa1.LoadBalancerCost, osa2.LoadBalancerCost+osa1.LoadBalancerCost) {
			t.Fatalf("incorrect LoadBalancerCost: expected %.5f; actual %.5f", osa2.LoadBalancerCost+osa1.LoadBalancerCost, sa2.LoadBalancerCost)
		}
		if !util.IsApproximately(sa1.PVCost, osa2.PVCost+osa1.PVCost) {
			t.Fatalf("incorrect PVCost: expected %.5f; actual %.5f", osa2.PVCost+osa1.PVCost, sa2.PVCost)
		}
		if !util.IsApproximately(sa1.RAMCost, osa2.RAMCost+osa1.RAMCost) {
			t.Fatalf("incorrect RAMCost: expected %.5f; actual %.5f", osa2.RAMCost+osa1.RAMCost, sa2.RAMCost)
		}
		if !util.IsApproximately(sa1.SharedCost, osa2.SharedCost+osa1.SharedCost) {
			t.Fatalf("incorrect SharedCost: expected %.5f; actual %.5f", osa2.SharedCost+osa1.SharedCost, sa2.SharedCost)
		}
		if !util.IsApproximately(sa1.ExternalCost, osa2.ExternalCost+osa1.ExternalCost) {
			t.Fatalf("incorrect ExternalCost: expected %.5f; actual %.5f", osa2.ExternalCost+osa1.ExternalCost, sa2.ExternalCost)
		}
	})
}

func TestSummaryAllocationSet_RAMEfficiency(t *testing.T) {
	// Generating 6 sample summary allocations for testing
	var sa1, sa2, sa3, sa4, sa5, sa6 *SummaryAllocation

	// Generating accumulated summary allocation sets for testing
	var sas1, sas2, sas3, sas4, sas5 *SummaryAllocationSet

	window, _ := ParseWindowUTC("7d")

	saStart := *window.Start()

	saEnd := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.05,
	}

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container2",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   15.0 * 1024.0 * 1024.0,
		RAMCost:                0.10,
	}

	sa3 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container3",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container3",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa4 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container4",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa5 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container5",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.10,
	}

	sa6 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container6",
		},
		Start:                  saStart,
		End:                    saEnd,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.10,
	}

	testcase1Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
	}

	testcase2Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
	}

	testcase3Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase4Map := map[string]*SummaryAllocation{}

	testcase5Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	sas1 = &SummaryAllocationSet{
		SummaryAllocations: testcase1Map,
		Window:             window,
	}

	sas2 = &SummaryAllocationSet{
		SummaryAllocations: testcase2Map,
		Window:             window,
	}

	sas3 = &SummaryAllocationSet{
		SummaryAllocations: testcase3Map,
		Window:             window,
	}

	sas4 = &SummaryAllocationSet{
		SummaryAllocations: testcase4Map,
		Window:             window,
	}

	sas5 = &SummaryAllocationSet{
		SummaryAllocations: testcase5Map,
		Window:             window,
	}

	cases := []struct {
		name               string
		testsas            *SummaryAllocationSet
		expectedEfficiency float64
	}{
		{
			name:               "Check RAMEfficiency when totalRAMBytesRequest over allocation summary set is greater than 0",
			testsas:            sas1,
			expectedEfficiency: 0.25,
		},
		{
			name:               "Check RAMEfficiency when totalRAMBytesRequest is 0 and totalRAMCost or totalRAMBytesUsage equal to 0",
			testsas:            sas2,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check RAMEfficiency when totalRAMBytesRequest is 0 and totalRAMCost or totalRAMBytesUsage is not 0",
			testsas:            sas3,
			expectedEfficiency: 1.0,
		},
		{
			name:               "Check RAMEfficiency when allocation summary set is empty",
			testsas:            sas4,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check RAMEfficiency over combination of all allocation summaries",
			testsas:            sas5,
			expectedEfficiency: 0.65,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnEfficiency := c.testsas.RAMEfficiency()
			if !util.IsApproximately(c.expectedEfficiency, returnEfficiency) {
				t.Errorf("Case %s failed: Expected RAM Efficiency %.2f but got RAM Efficiency of as %.2f", c.name, c.expectedEfficiency, returnEfficiency)
				t.Fail()
			}
		})
	}
}

func TestSummaryAllocationSet_CPUEfficiency(t *testing.T) {
	// Generating 6 sample summary allocations for testing
	var sa1, sa2, sa3, sa4, sa5, sa6 *SummaryAllocation

	// Generating accumulated summary allocation sets for testing
	var sas1, sas2, sas3, sas4, sas5 *SummaryAllocationSet

	window, _ := ParseWindowUTC("7d")

	saStart := *window.Start()

	saEnd := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.5,
		CPUCoreUsageAverage:   0.1,
		CPUCost:               0.2,
	}

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container2",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.5,
		CPUCoreUsageAverage:   0.2,
		CPUCost:               0.2,
	}

	sa3 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container3",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container3",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.0,
		CPUCost:               1.0,
	}

	sa4 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container4",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.0,
		CPUCost:               2.0,
	}

	sa5 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container5",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.1,
		CPUCost:               0.2,
	}

	sa6 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container6",
		},
		Start:                 saStart,
		End:                   saEnd,
		CPUCoreRequestAverage: 0.0,
		CPUCoreUsageAverage:   0.1,
		CPUCost:               0.2,
	}

	testcase1Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
	}

	testcase2Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
	}

	testcase3Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase4Map := map[string]*SummaryAllocation{}

	testcase5Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	sas1 = &SummaryAllocationSet{
		SummaryAllocations: testcase1Map,
		Window:             window,
	}

	sas2 = &SummaryAllocationSet{
		SummaryAllocations: testcase2Map,
		Window:             window,
	}

	sas3 = &SummaryAllocationSet{
		SummaryAllocations: testcase3Map,
		Window:             window,
	}

	sas4 = &SummaryAllocationSet{
		SummaryAllocations: testcase4Map,
		Window:             window,
	}

	sas5 = &SummaryAllocationSet{
		SummaryAllocations: testcase5Map,
		Window:             window,
	}

	cases := []struct {
		name               string
		testsas            *SummaryAllocationSet
		expectedEfficiency float64
	}{
		{
			name:               "Check CPUEfficiency when totalCPUCoreRequest is greater than 0 over allocation summary set",
			testsas:            sas1,
			expectedEfficiency: 0.30,
		},
		{
			name:               "Check CPUEfficiency when totalCPUCoreRequest is 0 and totalCPUCost or totalCPUCoreUsage equal to 0",
			testsas:            sas2,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check CPUEfficiency when totalCPUCoreRequest is 0 and totalCPUCost or totalCPUCoreUsage is not 0",
			testsas:            sas3,
			expectedEfficiency: 1.0,
		},
		{
			name:               "Check CPUEfficiency when allocation summary set is empty",
			testsas:            sas4,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check CPUEfficiency over combination of all allocation summaries",
			testsas:            sas5,
			expectedEfficiency: 0.50,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnEfficiency := c.testsas.CPUEfficiency()
			if !util.IsApproximately(c.expectedEfficiency, returnEfficiency) {
				t.Errorf("Case %s failed: Expected CPU Efficiency %.2f but got CPU Efficiency of as %.2f", c.name, c.expectedEfficiency, returnEfficiency)
				t.Fail()
			}
		})
	}
}

func TestSummaryAllocationSet_TotalEfficiency(t *testing.T) {
	// Generating 6 sample summary allocations for testing
	var sa1, sa2, sa3, sa4, sa5, sa6, idlesa *SummaryAllocation

	// Generating accumulated summary allocation sets for testing
	var sas1, sas2, sas3, sas4 *SummaryAllocationSet

	window, _ := ParseWindowUTC("7d")

	saStart := *window.Start()

	saEnd := *window.End()

	sa1 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container1",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                0.0,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa2 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container2",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container2",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.2,
		CPUCost:                0.0,
		RAMBytesRequestAverage: 0.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                0.0,
	}

	sa3 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container3",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container3",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.2,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	sa4 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container4",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   20.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	sa5 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container5",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.1,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   10.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	sa6 = &SummaryAllocation{
		Name: "cluster1/namespace1/pod1/container4",
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container6",
		},
		Start:                  saStart,
		End:                    saEnd,
		CPUCoreRequestAverage:  0.5,
		CPUCoreUsageAverage:    0.2,
		CPUCost:                1.0,
		RAMBytesRequestAverage: 50.0 * 1024.0 * 1024.0,
		RAMBytesUsageAverage:   20.0 * 1024.0 * 1024.0,
		RAMCost:                1.0,
	}

	idlesa = &SummaryAllocation{
		Name: IdleSuffix,
		Properties: &AllocationProperties{
			Cluster:   "cluster1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container7",
		},
		Start:   saStart,
		End:     saEnd,
		CPUCost: 1.0,
		RAMCost: 1.0,
	}

	testcase1Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container1": sa1,
		"cluster1/namespace1/pod1/container2": sa2,
	}

	testcase2Map := map[string]*SummaryAllocation{}

	testcase3Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container3": sa3,
		"cluster1/namespace1/pod1/container4": sa4,
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
	}

	testcase4Map := map[string]*SummaryAllocation{
		"cluster1/namespace1/pod1/container5": sa5,
		"cluster1/namespace1/pod1/container6": sa6,
		"cluster1/__idle__":                   idlesa,
	}

	sas1 = &SummaryAllocationSet{
		SummaryAllocations: testcase1Map,
		Window:             window,
	}

	sas2 = &SummaryAllocationSet{
		SummaryAllocations: testcase2Map,
		Window:             window,
	}

	sas3 = &SummaryAllocationSet{
		SummaryAllocations: testcase3Map,
		Window:             window,
	}

	sas4 = &SummaryAllocationSet{
		SummaryAllocations: testcase4Map,
		Window:             window,
	}

	cases := []struct {
		name               string
		testsas            *SummaryAllocationSet
		expectedEfficiency float64
	}{
		{
			name:               "When TotalEfficiency when sum of TotalRAMCost and TotalCPUCost is 0",
			testsas:            sas1,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check TotalEfficiency when allocation summary set is empty",
			testsas:            sas2,
			expectedEfficiency: 0.0,
		},
		{
			name:               "Check TotalEfficiency over all 4 allocation summaries",
			testsas:            sas3,
			expectedEfficiency: 0.30,
		},
		{
			name:               "Check TotalEfficiency with idle cost",
			testsas:            sas4,
			expectedEfficiency: 0.20,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			returnEfficiency := c.testsas.TotalEfficiency()
			if !util.IsApproximately(c.expectedEfficiency, returnEfficiency) {
				t.Errorf("Case %s failed: Expected Total Efficiency %.2f but got Total Efficiency of as %.2f", c.name, c.expectedEfficiency, returnEfficiency)
				t.Fail()
			}
		})
	}
}
