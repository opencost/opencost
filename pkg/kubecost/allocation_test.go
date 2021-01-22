package kubecost

import (
	"fmt"
	"math"
	"testing"
	"time"

	util "github.com/kubecost/cost-model/pkg/util"
)

const day = 24 * time.Hour

func NewUnitAllocation(name string, start time.Time, resolution time.Duration, props *Properties) *Allocation {
	if name == "" {
		name = "cluster1/namespace1/pod1/container1"
	}

	properties := &Properties{}
	if props == nil {
		properties.SetCluster("cluster1")
		properties.SetNode("node1")
		properties.SetNamespace("namespace1")
		properties.SetControllerKind("deployment")
		properties.SetController("deployment1")
		properties.SetPod("pod1")
		properties.SetContainer("container1")
	} else {
		properties = props
	}

	end := start.Add(resolution)

	alloc := &Allocation{
		Name:            name,
		Properties:      *properties,
		Start:           start,
		End:             end,
		Minutes:         1440,
		CPUCoreHours:    1,
		CPUCost:         1,
		CPUEfficiency:   1,
		GPUHours:        1,
		GPUCost:         1,
		NetworkCost:     1,
		PVByteHours:     1,
		PVCost:          1,
		RAMByteHours:    1,
		RAMCost:         1,
		RAMEfficiency:   1,
		TotalCost:       5,
		TotalEfficiency: 1,
	}

	// If idle allocation, remove non-idle costs, but maintain total cost
	if alloc.IsIdle() {
		alloc.PVByteHours = 0.0
		alloc.PVCost = 0.0
		alloc.NetworkCost = 0.0

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
	if nilZeroSum == nil || nilZeroSum.TotalCost != 0.0 {
		t.Fatalf("Allocation.Add failed; exp: 0.0; act: %s", nilZeroSum)
	}

	// TODO niko/etl more
}

// TODO niko/etl
// func TestAllocation_Clone(t *testing.T) {}

// TODO niko/etl
// func TestAllocation_IsIdle(t *testing.T) {}

func TestAllocation_MatchesAll(t *testing.T) {
	var alloc *Allocation

	// nil Allocations never match
	if alloc.MatchesAll() {
		t.Fatalf("Allocation.MatchesAll: expected no match on nil allocation")
	}

	today := time.Now().UTC().Truncate(day)
	alloc = NewUnitAllocation("", today, day, nil)

	// Matches when no Properties are given
	if !alloc.MatchesAll() {
		t.Fatalf("Allocation.MatchesAll: expected match on no conditions")
	}

	// Matches when all Properties match
	if !alloc.MatchesAll(Properties{
		NamespaceProp: "namespace1",
	}, Properties{
		ClusterProp:        "cluster1",
		ControllerKindProp: "deployment",
	}, Properties{
		NodeProp: "node1",
	}) {
		t.Fatalf("Allocation.MatchesAll: expected match when all Properties are met")
	}

	// Doesn't match when one Property doesn't match
	if alloc.MatchesAll(Properties{
		NamespaceProp: "namespace1",
		ServiceProp:   []string{"missing"},
	}, Properties{
		ClusterProp:        "cluster1",
		ControllerKindProp: "deployment",
	}) {
		t.Fatalf("Allocation.MatchesAll: expected no match when one Properties is not met")
	}

	// Doesn't match when no Properties are met
	if alloc.MatchesAll(Properties{
		NamespaceProp: "namespace1",
		ServiceProp:   []string{"missing"},
	}, Properties{
		ClusterProp:        "cluster2",
		ControllerKindProp: "deployment",
	}) {
		t.Fatalf("Allocation.MatchesAll: expected no match when no Properties are met")
	}
}

func TestAllocation_MatchesOne(t *testing.T) {
	var alloc *Allocation

	// nil Allocations never match
	if alloc.MatchesOne() {
		t.Fatalf("Allocation.MatchesOne: expected no match on nil allocation")
	}

	today := time.Now().UTC().Truncate(day)
	alloc = NewUnitAllocation("", today, day, nil)

	// Doesn't match when no Properties are given
	if alloc.MatchesOne() {
		t.Fatalf("Allocation.MatchesOne: expected no match on no conditions")
	}

	// Matches when all Properties match
	if !alloc.MatchesOne(Properties{
		NamespaceProp: "namespace1",
	}, Properties{
		ClusterProp:        "cluster1",
		ControllerKindProp: "deployment",
	}) {
		t.Fatalf("Allocation.MatchesOne: expected match when all Properties are met")
	}

	// Matches when one Property doesn't match
	if !alloc.MatchesOne(Properties{
		NamespaceProp: "namespace1",
		ServiceProp:   []string{"missing"},
	}, Properties{
		ClusterProp:        "cluster1",
		ControllerKindProp: "deployment",
	}) {
		t.Fatalf("Allocation.MatchesOne: expected match when one Properties is met")
	}

	// Doesn't match when no Properties are met
	if alloc.MatchesOne(Properties{
		NamespaceProp: "namespace1",
		ServiceProp:   []string{"missing"},
	}, Properties{
		ClusterProp:        "cluster2",
		ControllerKindProp: "deployment",
	}) {
		t.Fatalf("Allocation.MatchesOne: expected no match when no Properties are met")
	}
}

func TestAllocation_String(t *testing.T) {
	// TODO niko/etl
}

func TestNewAllocationSet(t *testing.T) {
	// TODO niko/etl
}

func generateAllocationSet(start time.Time) *AllocationSet {
	// Idle allocations
	a1i := NewUnitAllocation(fmt.Sprintf("cluster1/%s", IdleSuffix), start, day, &Properties{
		ClusterProp: "cluster1",
		NodeProp:    "node1",
	})
	a1i.CPUCost = 5.0
	a1i.RAMCost = 15.0
	a1i.GPUCost = 0.0
	a1i.TotalCost = 20.0

	a2i := NewUnitAllocation(fmt.Sprintf("cluster2/%s", IdleSuffix), start, day, &Properties{
		ClusterProp: "cluster2",
	})
	a2i.CPUCost = 5.0
	a2i.RAMCost = 5.0
	a2i.GPUCost = 0.0
	a2i.TotalCost = 10.0

	// Active allocations
	a1111 := NewUnitAllocation("cluster1/namespace1/pod1/container1", start, day, &Properties{
		ClusterProp:   "cluster1",
		NamespaceProp: "namespace1",
		PodProp:       "pod1",
		ContainerProp: "container1",
	})
	a1111.RAMCost = 11.00
	a1111.TotalCost = 15.00

	a11abc2 := NewUnitAllocation("cluster1/namespace1/pod-abc/container2", start, day, &Properties{
		ClusterProp:   "cluster1",
		NamespaceProp: "namespace1",
		PodProp:       "pod-abc",
		ContainerProp: "container2",
	})

	a11def3 := NewUnitAllocation("cluster1/namespace1/pod-def/container3", start, day, &Properties{
		ClusterProp:   "cluster1",
		NamespaceProp: "namespace1",
		PodProp:       "pod-def",
		ContainerProp: "container3",
	})

	a12ghi4 := NewUnitAllocation("cluster1/namespace2/pod-ghi/container4", start, day, &Properties{
		ClusterProp:   "cluster1",
		NamespaceProp: "namespace2",
		PodProp:       "pod-ghi",
		ContainerProp: "container4",
	})

	a12ghi5 := NewUnitAllocation("cluster1/namespace2/pod-ghi/container5", start, day, &Properties{
		ClusterProp:   "cluster1",
		NamespaceProp: "namespace2",
		PodProp:       "pod-ghi",
		ContainerProp: "container5",
	})

	a12jkl6 := NewUnitAllocation("cluster1/namespace2/pod-jkl/container6", start, day, &Properties{
		ClusterProp:   "cluster1",
		NamespaceProp: "namespace2",
		PodProp:       "pod-jkl",
		ContainerProp: "container6",
	})

	a22mno4 := NewUnitAllocation("cluster2/namespace2/pod-mno/container4", start, day, &Properties{
		ClusterProp:   "cluster2",
		NamespaceProp: "namespace2",
		PodProp:       "pod-mno",
		ContainerProp: "container4",
	})

	a22mno5 := NewUnitAllocation("cluster2/namespace2/pod-mno/container5", start, day, &Properties{
		ClusterProp:   "cluster2",
		NamespaceProp: "namespace2",
		PodProp:       "pod-mno",
		ContainerProp: "container5",
	})

	a22pqr6 := NewUnitAllocation("cluster2/namespace2/pod-pqr/container6", start, day, &Properties{
		ClusterProp:   "cluster2",
		NamespaceProp: "namespace2",
		PodProp:       "pod-pqr",
		ContainerProp: "container6",
	})

	a23stu7 := NewUnitAllocation("cluster2/namespace3/pod-stu/container7", start, day, &Properties{
		ClusterProp:   "cluster2",
		NamespaceProp: "namespace3",
		PodProp:       "pod-stu",
		ContainerProp: "container7",
	})

	a23vwx8 := NewUnitAllocation("cluster2/namespace3/pod-vwx/container8", start, day, &Properties{
		ClusterProp:   "cluster2",
		NamespaceProp: "namespace3",
		PodProp:       "pod-vwx",
		ContainerProp: "container8",
	})

	a23vwx9 := NewUnitAllocation("cluster2/namespace3/pod-vwx/container9", start, day, &Properties{
		ClusterProp:   "cluster2",
		NamespaceProp: "namespace3",
		PodProp:       "pod-vwx",
		ContainerProp: "container9",
	})

	// Controllers

	a11abc2.Properties.SetControllerKind("deployment")
	a11abc2.Properties.SetController("deployment1")
	a11def3.Properties.SetControllerKind("deployment")
	a11def3.Properties.SetController("deployment1")

	a12ghi4.Properties.SetControllerKind("deployment")
	a12ghi4.Properties.SetController("deployment2")
	a12ghi5.Properties.SetControllerKind("deployment")
	a12ghi5.Properties.SetController("deployment2")
	a22mno4.Properties.SetControllerKind("deployment")
	a22mno4.Properties.SetController("deployment2")
	a22mno5.Properties.SetControllerKind("deployment")
	a22mno5.Properties.SetController("deployment2")

	a23stu7.Properties.SetControllerKind("deployment")
	a23stu7.Properties.SetController("deployment3")

	a12jkl6.Properties.SetControllerKind("daemonset")
	a12jkl6.Properties.SetController("daemonset1")
	a22pqr6.Properties.SetControllerKind("daemonset")
	a22pqr6.Properties.SetController("daemonset1")

	a23vwx8.Properties.SetControllerKind("statefulset")
	a23vwx8.Properties.SetController("statefulset1")
	a23vwx9.Properties.SetControllerKind("statefulset")
	a23vwx9.Properties.SetController("statefulset1")

	// Labels

	a1111.Properties.SetLabels(map[string]string{"app": "app1", "env": "env1"})
	a12ghi4.Properties.SetLabels(map[string]string{"app": "app2", "env": "env2"})
	a12ghi5.Properties.SetLabels(map[string]string{"app": "app2", "env": "env2"})
	a22mno4.Properties.SetLabels(map[string]string{"app": "app2"})
	a22mno5.Properties.SetLabels(map[string]string{"app": "app2"})

	// Services

	a12jkl6.Properties.SetServices([]string{"service1"})
	a22pqr6.Properties.SetServices([]string{"service1"})

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
			if math.Round(a.TotalCost*100) != math.Round(exp*100) {
				t.Fatalf("AllocationSet.AggregateBy[%s]: expected total cost %.2f, actual %.2f", msg, exp, a.TotalCost)
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
		if a.Minutes != expMinutes {
			t.Fatalf("AllocationSet.AggregateBy[%s]: expected minutes %f, actual %f", msg, expMinutes, a.Minutes)
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

	// | Hierarchy                              | Cost |  CPU |  RAM |  GPU |   PV |  Net |
	// +----------------------------------------+------+------+------+------+------+------+
	//   cluster1:
	//     idle:                                  20.00   5.00  15.00   0.00   0.00   0.00
	//     namespace1:
	//       pod1:
	//         container1: [app=app1, env=env1]   15.00   1.00  11.00   1.00   1.00   1.00
	//       pod-abc: (deployment1)
	//         container2:                         5.00   1.00   1.00   1.00   1.00   1.00
	//       pod-def: (deployment1)
	//         container3:                         5.00   1.00   1.00   1.00   1.00   1.00
	//     namespace2:
	//       pod-ghi: (deployment2)
	//         container4: [app=app2, env=env2]    5.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app=app2, env=env2]    5.00   1.00   1.00   1.00   1.00   1.00
	//       pod-jkl: (daemonset1)
	//         container6: {service1}              5.00   1.00   1.00   1.00   1.00   1.00
	// +-----------------------------------------+------+------+------+------+------+------+
	//   cluster1 subtotal                        60.00  11.00  31.00   6.00   6.00   6.00
	// +-----------------------------------------+------+------+------+------+------+------+
	//   cluster2:
	//     idle:                                  10.00   5.00   5.00   0.00   0.00   0.00
	//     namespace2:
	//       pod-mno: (deployment2)
	//         container4: [app=app2]              5.00   1.00   1.00   1.00   1.00   1.00
	//         container5: [app=app2]              5.00   1.00   1.00   1.00   1.00   1.00
	//       pod-pqr: (daemonset1)
	//         container6: {service1}              5.00   1.00   1.00   1.00   1.00   1.00
	//     namespace3:
	//       pod-stu: (deployment3)
	//         container7:                         5.00   1.00   1.00   1.00   1.00   1.00
	//       pod-vwx: (statefulset1)
	//         container8:                         5.00   1.00   1.00   1.00   1.00   1.00
	//         container9:                         5.00   1.00   1.00   1.00   1.00   1.00
	// +----------------------------------------+------+------+------+------+------+------+
	//   cluster2 subtotal                        40.00  11.00  11.00   6.00   6.00   6.00
	// +----------------------------------------+------+------+------+------+------+------+
	//   total                                   100.00  22.00  42.00  12.00  12.00  12.00
	// +----------------------------------------+------+------+------+------+------+------+

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

	activeTotalCost := 70.0
	idleTotalCost := 30.0
	sharedOverheadHourlyCost := 7.0

	isNamespace3 := func(a *Allocation) bool {
		ns, err := a.Properties.GetNamespace()
		return err == nil && ns == "namespace3"
	}

	isApp1 := func(a *Allocation) bool {
		ls, _ := a.Properties.GetLabels()
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
	err = as.AggregateBy(Properties{ClusterProp: ""}, nil)
	assertAllocationSetTotals(t, as, "1a", err, numClusters+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1a", map[string]float64{
		"cluster1": 40.00,
		"cluster2": 30.00,
		IdleSuffix: 30.00,
	})
	assertAllocationWindow(t, as, "1a", startYesterday, endYesterday, 1440.0)

	// 1b AggregationProperties=(Namespace)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, nil)
	assertAllocationSetTotals(t, as, "1b", err, numNamespaces+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1b", map[string]float64{
		"namespace1": 25.00,
		"namespace2": 30.00,
		"namespace3": 15.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "1b", startYesterday, endYesterday, 1440.0)

	// 1c AggregationProperties=(Pod)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{PodProp: true}, nil)
	assertAllocationSetTotals(t, as, "1c", err, numPods+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1c", map[string]float64{
		"pod-jkl":  5.00,
		"pod-stu":  5.00,
		"pod-abc":  5.00,
		"pod-pqr":  5.00,
		"pod-def":  5.00,
		"pod-vwx":  10.00,
		"pod1":     15.00,
		"pod-mno":  10.00,
		"pod-ghi":  10.00,
		IdleSuffix: 30.00,
	})
	assertAllocationWindow(t, as, "1c", startYesterday, endYesterday, 1440.0)

	// 1d AggregationProperties=(Container)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ContainerProp: true}, nil)
	assertAllocationSetTotals(t, as, "1d", err, numContainers+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1d", map[string]float64{
		"container2": 5.00,
		"container9": 5.00,
		"container6": 10.00,
		"container3": 5.00,
		"container4": 10.00,
		"container7": 5.00,
		"container8": 5.00,
		"container5": 10.00,
		"container1": 15.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "1d", startYesterday, endYesterday, 1440.0)

	// 1e AggregationProperties=(ControllerKind)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ControllerKindProp: true}, nil)
	assertAllocationSetTotals(t, as, "1e", err, numControllerKinds+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1e", map[string]float64{
		"daemonset":       10.00,
		"deployment":      35.00,
		"statefulset":     10.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 15.00,
	})
	assertAllocationWindow(t, as, "1e", startYesterday, endYesterday, 1440.0)

	// 1f AggregationProperties=(Controller)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ControllerProp: true}, nil)
	assertAllocationSetTotals(t, as, "1f", err, numControllers+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1f", map[string]float64{
		"deployment/deployment2":   20.00,
		"daemonset/daemonset1":     10.00,
		"deployment/deployment3":   5.00,
		"statefulset/statefulset1": 10.00,
		"deployment/deployment1":   10.00,
		IdleSuffix:                 30.00,
		UnallocatedSuffix:          15.00,
	})
	assertAllocationWindow(t, as, "1f", startYesterday, endYesterday, 1440.0)

	// 1g AggregationProperties=(Service)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ServiceProp: true}, nil)
	assertAllocationSetTotals(t, as, "1g", err, numServices+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1g", map[string]float64{
		"service1":        10.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 60.00,
	})
	assertAllocationWindow(t, as, "1g", startYesterday, endYesterday, 1440.0)

	// 1h AggregationProperties=(Label:app)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{LabelProp: map[string]string{"app": ""}}, nil)
	assertAllocationSetTotals(t, as, "1h", err, numLabelApps+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1h", map[string]float64{
		"app=app1":        15.00,
		"app=app2":        20.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 35.00,
	})
	assertAllocationWindow(t, as, "1h", startYesterday, endYesterday, 1440.0)

	// 1i AggregationProperties=(ControllerKind:deployment)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ControllerKindProp: "deployment"}, nil)
	assertAllocationSetTotals(t, as, "1i", err, 1+numIdle+numUnallocated, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "1i", map[string]float64{
		"deployment":      35.00,
		IdleSuffix:        30.00,
		UnallocatedSuffix: 35.00,
	})
	assertAllocationWindow(t, as, "1i", startYesterday, endYesterday, 1440.0)

	// 2  Multi-aggregation

	// 2a AggregationProperties=(Cluster, Namespace)
	// 2b AggregationProperties=(Namespace, Label:app)
	// 2c AggregationProperties=(Cluster, Namespace, Pod, Container)

	// 2d AggregationProperties=(Label:app, Label:environment)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{LabelProp: map[string]string{"app": "", "env": ""}}, nil)
	// sets should be {idle, unallocated, app1/env1, app2/env2, app2/unallocated}
	assertAllocationSetTotals(t, as, "2d", err, numIdle+numUnallocated+3, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "2d", map[string]float64{
		"app=app1/env=env1":             15.00,
		"app=app2/env=env2":             10.00,
		"app=app2/" + UnallocatedSuffix: 10.00,
		IdleSuffix:                      30.00,
		UnallocatedSuffix:               35.00,
	})

	// 2e AggregationProperties=(Cluster, Label:app, Label:environment)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ClusterProp: "", LabelProp: map[string]string{"app": "", "env": ""}}, nil)
	assertAllocationSetTotals(t, as, "2e", err, 6, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "2e", map[string]float64{
		"cluster1/app=app2/env=env2":             10.00,
		"__idle__":                               30.00,
		"cluster1/app=app1/env=env1":             15.00,
		"cluster1/" + UnallocatedSuffix:          15.00,
		"cluster2/app=app2/" + UnallocatedSuffix: 10.00,
		"cluster2/" + UnallocatedSuffix:          20.00,
	})

	// // TODO niko/etl

	// // 3  Share idle

	// 3a AggregationProperties=(Namespace) ShareIdle=ShareWeighted
	// namespace1: 39.6875 = 25.00 + 5.00*(3.00/6.00) + 15.0*(13.0/16.0)
	// namespace2: 40.3125 = 30.00 + 5.0*(3.0/6.0) + 15.0*(3.0/16.0) + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
	// namespace3: 20.0000 = 15.00 + 5.0*(3.0/6.0) + 5.0*(3.0/6.0)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{ShareIdle: ShareWeighted})
	assertAllocationSetTotals(t, as, "3a", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "3a", map[string]float64{
		"namespace1": 39.69,
		"namespace2": 40.31,
		"namespace3": 20.00,
	})
	assertAllocationWindow(t, as, "3a", startYesterday, endYesterday, 1440.0)

	// 3b AggregationProperties=(Namespace) ShareIdle=ShareEven
	// namespace1: 35.0000 = 25.00 + 5.00*(1.0/2.0) + 15.0*(1.0/2.0)
	// namespace2: 45.0000 = 30.00 + 5.0*(1.0/2.0) + 15.0*(1.0/2.0) + 5.0*(1.0/2.0) + 5.0*(1.0/2.0)
	// namespace3: 20.0000 = 15.00 + 5.0*(1.0/2.0) + 5.0*(1.0/2.0)
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{ShareIdle: ShareEven})
	assertAllocationSetTotals(t, as, "3a", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "3a", map[string]float64{
		"namespace1": 35.00,
		"namespace2": 45.00,
		"namespace3": 20.00,
	})
	assertAllocationWindow(t, as, "3b", startYesterday, endYesterday, 1440.0)

	// 4  Share resources

	// 4a Share namespace ShareEven
	// namespace1: 32.5000 = 25.00 + 15.00*(1.0/2.0)
	// namespace2: 37.5000 = 30.00 + 15.00*(1.0/2.0)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isNamespace3},
		ShareSplit: ShareEven,
	})
	assertAllocationSetTotals(t, as, "4a", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "4a", map[string]float64{
		"namespace1": 32.50,
		"namespace2": 37.50,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4a", startYesterday, endYesterday, 1440.0)

	// 4b Share namespace ShareWeighted
	// namespace1: 32.5000 =
	// namespace2: 37.5000 =
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isNamespace3},
		ShareSplit: ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "4b", err, numNamespaces, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "4b", map[string]float64{
		"namespace1": 31.82,
		"namespace2": 38.18,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4b", startYesterday, endYesterday, 1440.0)

	// 4c Share label ShareEven
	// namespace1: 15.0000 = 25.00 - 15.00 + 15.00*(1.0/3.0)
	// namespace2: 35.0000 = 30.00 + 15.00*(1.0/3.0)
	// namespace3: 20.0000 = 15.00 + 15.00*(1.0/3.0)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{
		ShareFuncs: []AllocationMatchFunc{isApp1},
		ShareSplit: ShareEven,
	})
	assertAllocationSetTotals(t, as, "4c", err, numNamespaces+numIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "4c", map[string]float64{
		"namespace1": 15.00,
		"namespace2": 35.00,
		"namespace3": 20.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4c", startYesterday, endYesterday, 1440.0)

	// 4d Share overhead ShareWeighted
	// namespace1: 37.5000 = 25.00 + (7.0*24.0)*(25.00/70.00)
	// namespace2: 45.0000 = 30.00 + (7.0*24.0)*(30.00/70.00)
	// namespace3: 22.5000 = 15.00 + (7.0*24.0)*(15.00/70.00)
	// idle:       30.0000
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{
		SharedHourlyCosts: map[string]float64{"total": sharedOverheadHourlyCost},
		ShareSplit:        ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "4d", err, numNamespaces+numIdle, activeTotalCost+idleTotalCost+(sharedOverheadHourlyCost*24.0))
	assertAllocationTotals(t, as, "4d", map[string]float64{
		"namespace1": 85.00,
		"namespace2": 102.00,
		"namespace3": 51.00,
		IdleSuffix:   30.00,
	})
	assertAllocationWindow(t, as, "4d", startYesterday, endYesterday, 1440.0)

	// 5  Filters

	isCluster := func(matchCluster string) func(*Allocation) bool {
		return func(a *Allocation) bool {
			cluster, err := a.Properties.GetCluster()
			return err == nil && cluster == matchCluster
		}
	}

	isNamespace := func(matchNamespace string) func(*Allocation) bool {
		return func(a *Allocation) bool {
			namespace, err := a.Properties.GetNamespace()
			return err == nil && namespace == matchNamespace
		}
	}

	// 5a Filter by cluster with separate idle
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ClusterProp: ""}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isCluster("cluster1")},
		ShareIdle:   ShareNone,
	})
	assertAllocationSetTotals(t, as, "5a", err, 2, 60.0)
	assertAllocationTotals(t, as, "5a", map[string]float64{
		"cluster1": 40.00,
		IdleSuffix: 20.00,
	})
	assertAllocationWindow(t, as, "5a", startYesterday, endYesterday, 1440.0)

	// 5b Filter by cluster with shared idle
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ClusterProp: ""}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isCluster("cluster1")},
		ShareIdle:   ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "5b", err, 1, 60.0)
	assertAllocationTotals(t, as, "5b", map[string]float64{
		"cluster1": 60.00,
	})
	assertAllocationWindow(t, as, "5b", startYesterday, endYesterday, 1440.0)

	// 5c Filter by cluster, agg by namespace, with separate idle
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: ""}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isCluster("cluster1")},
		ShareIdle:   ShareNone,
	})
	assertAllocationSetTotals(t, as, "5c", err, 3, 60.0)
	assertAllocationTotals(t, as, "5c", map[string]float64{
		"namespace1": 25.00,
		"namespace2": 15.00,
		IdleSuffix:   20.00,
	})
	assertAllocationWindow(t, as, "5c", startYesterday, endYesterday, 1440.0)

	// 5d Filter by namespace, agg by cluster, with separate idle
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{ClusterProp: ""}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareIdle:   ShareNone,
	})
	assertAllocationSetTotals(t, as, "5d", err, 3, 40.31)
	assertAllocationTotals(t, as, "5d", map[string]float64{
		"cluster1": 15.00,
		"cluster2": 15.00,
		IdleSuffix: 10.31,
	})
	assertAllocationWindow(t, as, "5d", startYesterday, endYesterday, 1440.0)

	// 6  Combinations and options

	// 6a SplitIdle
	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: ""}, &AllocationAggregationOptions{SplitIdle: true})
	assertAllocationSetTotals(t, as, "6a", err, numNamespaces+numSplitIdle, activeTotalCost+idleTotalCost)
	assertAllocationTotals(t, as, "6a", map[string]float64{
		"namespace1":                           25.00,
		"namespace2":                           30.00,
		"namespace3":                           15.00,
		fmt.Sprintf("cluster1/%s", IdleSuffix): 20.00,
		fmt.Sprintf("cluster2/%s", IdleSuffix): 10.00,
	})
	assertAllocationWindow(t, as, "6a", startYesterday, endYesterday, 1440.0)

	// 6b Share idle weighted with filters

	// Should match values from unfiltered aggregation
	// as = generateAllocationSet(start)
	// err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{ShareIdle: ShareWeighted})
	// printAllocationSet("6b unfiltered", as)

	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: ""}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareIdle:   ShareWeighted,
	})
	assertAllocationSetTotals(t, as, "6b", err, 1, 40.31)
	assertAllocationTotals(t, as, "6b", map[string]float64{
		"namespace2": 40.31,
	})
	assertAllocationWindow(t, as, "6b", startYesterday, endYesterday, 1440.0)

	// 6c Share idle even with filters

	// Should match values from unfiltered aggregation
	// as = generateAllocationSet(start)
	// err = as.AggregateBy(Properties{NamespaceProp: true}, &AllocationAggregationOptions{ShareIdle: ShareEven})
	// printAllocationSet("6c unfiltered", as)

	as = generateAllocationSet(start)
	err = as.AggregateBy(Properties{NamespaceProp: ""}, &AllocationAggregationOptions{
		FilterFuncs: []AllocationMatchFunc{isNamespace("namespace2")},
		ShareIdle:   ShareEven,
	})
	assertAllocationSetTotals(t, as, "6b", err, 1, 45.00)
	assertAllocationTotals(t, as, "6b", map[string]float64{
		"namespace2": 45.00,
	})
	assertAllocationWindow(t, as, "6b", startYesterday, endYesterday, 1440.0)

	// 6d Share resources with filters
	// 6e Share idle and share resources

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

	// Create an AssetSet representing cluster costs for two clusters (cluster1
	// and cluster2). Include Nodes and Disks for both, even though only
	// Nodes will be counted. Whereas in practice, Assets should be aggregated
	// by type, here we will provide multiple Nodes for one of the clusters to
	// make sure the function still holds.

	// NOTE: we're re-using generateAllocationSet so this has to line up with
	// the allocated node costs from that function. See table above.

	// | Hierarchy                               | Cost |  CPU |  RAM |  GPU |
	// +-----------------------------------------+------+------+------+------+
	//   cluster1:
	//     nodes                                  100.00  50.00  40.00  10.00
	// +-----------------------------------------+------+------+------+------+
	//   cluster1 subtotal                        100.00  50.00  40.00  10.00
	// +-----------------------------------------+------+------+------+------+
	//   cluster1 allocated                        48.00   6.00  16.00   6.00
	// +-----------------------------------------+------+------+------+------+
	//   cluster1 idle                             72.00  44.00  24.00   4.00
	// +-----------------------------------------+------+------+------+------+
	//   cluster2:
	//     node1                                   35.00  20.00  15.00   0.00
	//     node2                                   35.00  20.00  15.00   0.00
	//     node3                                   30.00  10.00  10.00  10.00
	//     (disks should not matter for idle)
	// +-----------------------------------------+------+------+------+------+
	//   cluster2 subtotal                        100.00  50.00  40.00  10.00
	// +-----------------------------------------+------+------+------+------+
	//   cluster2 allocated                        28.00   6.00   6.00   6.00
	// +-----------------------------------------+------+------+------+------+
	//   cluster2 idle                             82.00  44.00  34.00   4.00
	// +-----------------------------------------+------+------+------+------+

	cluster1Nodes := NewNode("", "cluster1", "", start, end, NewWindow(&start, &end))
	cluster1Nodes.CPUCost = 50.0
	cluster1Nodes.RAMCost = 40.0
	cluster1Nodes.GPUCost = 10.0

	cluster2Node1 := NewNode("node1", "cluster2", "node1", start, end, NewWindow(&start, &end))
	cluster2Node1.CPUCost = 20.0
	cluster2Node1.RAMCost = 15.0
	cluster2Node1.GPUCost = 0.0

	cluster2Node2 := NewNode("node2", "cluster2", "node2", start, end, NewWindow(&start, &end))
	cluster2Node2.CPUCost = 20.0
	cluster2Node2.RAMCost = 15.0
	cluster2Node2.GPUCost = 0.0

	cluster2Node3 := NewNode("node3", "cluster2", "node3", start, end, NewWindow(&start, &end))
	cluster2Node3.CPUCost = 10.0
	cluster2Node3.RAMCost = 10.0
	cluster2Node3.GPUCost = 10.0

	cluster2Disk1 := NewDisk("disk1", "cluster2", "disk1", start, end, NewWindow(&start, &end))
	cluster2Disk1.Cost = 5.0

	assetSet := NewAssetSet(start, end, cluster1Nodes, cluster2Node1, cluster2Node2, cluster2Node3, cluster2Disk1)

	idles, err = as.ComputeIdleAllocations(assetSet)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(idles) != 2 {
		t.Fatalf("idles: expected length %d; got length %d", 2, len(idles))
	}

	if idle, ok := idles["cluster1"]; !ok {
		t.Fatalf("expected idle cost for %s", "cluster1")
	} else {
		if !util.IsApproximately(idle.TotalCost, 72.0) {
			t.Fatalf("%s idle: expected total cost %f; got total cost %f", "cluster1", 72.0, idle.TotalCost)
		}
	}

	if idle, ok := idles["cluster2"]; !ok {
		t.Fatalf("expected idle cost for %s", "cluster2")
	} else {
		if !util.IsApproximately(idle.TotalCost, 82.0) {
			t.Fatalf("%s idle: expected total cost %f; got total cost %f", "cluster2", 82.0, idle.TotalCost)
		}
	}

	// TODO assert value of each resource cost precisely
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
	if result.TotalCost() != 5.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 5.0; actual %f", result.TotalCost())
	}

	result, err = NewAllocationSetRange(todayAS, nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 5.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 5.0; actual %f", result.TotalCost())
	}

	result, err = NewAllocationSetRange(nil, todayAS, nil).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 5.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 5.0; actual %f", result.TotalCost())
	}

	// Accumulate two non-nil should result in sum of both with appropriate start, end
	result, err = NewAllocationSetRange(yesterdayAS, todayAS).Accumulate()
	if err != nil {
		t.Fatalf("unexpected error accumulating AllocationSetRange of length 1: %s", err)
	}
	if result == nil {
		t.Fatalf("accumulating AllocationSetRange: expected AllocationSet; actual %s", result)
	}
	if result.TotalCost() != 10.0 {
		t.Fatalf("accumulating AllocationSetRange: expected total cost 10.0; actual %f", result.TotalCost())
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
	if alloc.CPUEfficiency != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.CPUEfficiency)
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
	if alloc.PVByteHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.PVByteHours)
	}
	if alloc.PVCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.PVCost)
	}
	if alloc.RAMByteHours != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.RAMByteHours)
	}
	if alloc.RAMCost != 2.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 2.0; actual %f", alloc.RAMCost)
	}
	if alloc.RAMEfficiency != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.RAMEfficiency)
	}
	if alloc.TotalCost != 10.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 10.0; actual %f", alloc.TotalCost)
	}
	if alloc.TotalEfficiency != 1.0 {
		t.Fatalf("accumulating AllocationSetRange: expected 1.0; actual %f", alloc.TotalEfficiency)
	}
	if !alloc.Start.Equal(yesterday) {
		t.Fatalf("accumulating AllocationSetRange: expected to start %s; actual %s", yesterday, alloc.Start)
	}
	if !alloc.End.Equal(tomorrow) {
		t.Fatalf("accumulating AllocationSetRange: expected to end %s; actual %s", tomorrow, alloc.End)
	}
	if alloc.Minutes != 2880.0 {
		t.Fatalf("accumulating AllocationSetRange: expected %f minutes; actual %f", 2880.0, alloc.Minutes)
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
			if !util.IsApproximately(a.PVByteHours, unit.PVByteHours) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours, a.PVByteHours)
			}
			if !util.IsApproximately(a.PVCost, unit.PVCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost, a.PVCost)
			}
			if !util.IsApproximately(a.NetworkCost, unit.NetworkCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
			}
			if !util.IsApproximately(a.TotalCost, unit.TotalCost) {
				t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost, a.TotalCost)
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
		if !util.IsApproximately(a.PVByteHours, 2*unit.PVByteHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours, a.PVByteHours)
		}
		if !util.IsApproximately(a.PVCost, 2*unit.PVCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost, a.PVCost)
		}
		if !util.IsApproximately(a.NetworkCost, 2*unit.NetworkCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
		}
		if !util.IsApproximately(a.TotalCost, 2*unit.TotalCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost, a.TotalCost)
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
		if !util.IsApproximately(a.PVByteHours, unit.PVByteHours) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVByteHours, a.PVByteHours)
		}
		if !util.IsApproximately(a.PVCost, unit.PVCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.PVCost, a.PVCost)
		}
		if !util.IsApproximately(a.NetworkCost, unit.NetworkCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.NetworkCost, a.NetworkCost)
		}
		if !util.IsApproximately(a.TotalCost, unit.TotalCost) {
			t.Fatalf("allocation %s: expected %f; got %f", k, unit.TotalCost, a.TotalCost)
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
