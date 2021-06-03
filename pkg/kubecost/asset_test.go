package kubecost

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kubecost/cost-model/pkg/util"
)

var start1 = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
var start2 = start1.Add(day)
var start3 = start2.Add(day)
var start4 = start2.Add(day)

var windows = []Window{
	NewWindow(&start1, &start2),
	NewWindow(&start2, &start3),
	NewWindow(&start3, &start4),
}


// generateAssetSet generates the following topology:
//
// | Asset                        | Cost |  Adj |
// +------------------------------+------+------+
//   cluster1:
//     node1:                        6.00   1.00
//     node2:                        4.00   1.50
//     node3:                        7.00  -0.50
//     disk1:                        2.50   0.00
//     disk2:                        1.50   0.00
//     clusterManagement1:           3.00   0.00
// +------------------------------+------+------+
//   cluster1 subtotal              24.00   2.00
// +------------------------------+------+------+
//   cluster2:
//     node4:                       12.00  -1.00
//     disk3:                        2.50   0.00
//     disk4:                        1.50   0.00
//     clusterManagement2:           0.00   0.00
// +------------------------------+------+------+
//   cluster2 subtotal              16.00  -1.00
// +------------------------------+------+------+
//   cluster3:
//     node5:                       17.00   2.00
// +------------------------------+------+------+
//   cluster3 subtotal              17.00   2.00
// +------------------------------+------+------+
//   total                          57.00   3.00
// +------------------------------+------+------+
func generateAssetSet(start time.Time) *AssetSet {
	end := start.Add(day)
	window := NewWindow(&start, &end)

	hours := window.Duration().Hours()

	node1 := NewNode("node1", "cluster1", "gcp-node1", *window.Clone().start, *window.Clone().end, window.Clone())
	node1.CPUCost = 4.0
	node1.RAMCost = 4.0
	node1.GPUCost = 2.0
	node1.Discount = 0.5
	node1.CPUCoreHours = 2.0 * hours
	node1.RAMByteHours = 4.0 * gb * hours
	node1.GPUHours = 1.0 * hours
	node1.SetAdjustment(1.0)
	node1.SetLabels(map[string]string{"test": "test"})

	node2 := NewNode("node2", "cluster1", "gcp-node2", *window.Clone().start, *window.Clone().end, window.Clone())
	node2.CPUCost = 4.0
	node2.RAMCost = 4.0
	node2.GPUCost = 0.0
	node2.Discount = 0.5
	node2.CPUCoreHours = 2.0 * hours
	node2.RAMByteHours = 4.0 * gb * hours
	node2.GPUHours = 0.0 * hours
	node2.SetAdjustment(1.5)

	node3 := NewNode("node3", "cluster1", "gcp-node3", *window.Clone().start, *window.Clone().end, window.Clone())
	node3.CPUCost = 4.0
	node3.RAMCost = 4.0
	node3.GPUCost = 3.0
	node3.Discount = 0.5
	node3.CPUCoreHours = 2.0 * hours
	node3.RAMByteHours = 4.0 * gb * hours
	node3.GPUHours = 2.0 * hours
	node3.SetAdjustment(-0.5)

	node4 := NewNode("node4", "cluster2", "gcp-node4", *window.Clone().start, *window.Clone().end, window.Clone())
	node4.CPUCost = 10.0
	node4.RAMCost = 6.0
	node4.GPUCost = 0.0
	node4.Discount = 0.25
	node4.CPUCoreHours = 4.0 * hours
	node4.RAMByteHours = 12.0 * gb * hours
	node4.GPUHours = 0.0 * hours
	node4.SetAdjustment(-1.0)

	node5 := NewNode("node5", "cluster3", "aws-node5", *window.Clone().start, *window.Clone().end, window.Clone())
	node5.CPUCost = 10.0
	node5.RAMCost = 7.0
	node5.GPUCost = 0.0
	node5.Discount = 0.0
	node5.CPUCoreHours = 8.0 * hours
	node5.RAMByteHours = 24.0 * gb * hours
	node5.GPUHours = 0.0 * hours
	node5.SetAdjustment(2.0)

	disk1 := NewDisk("disk1", "cluster1", "gcp-disk1", *window.Clone().start, *window.Clone().end, window.Clone())
	disk1.Cost = 2.5
	disk1.ByteHours = 100 * gb * hours

	disk2 := NewDisk("disk2", "cluster1", "gcp-disk2", *window.Clone().start, *window.Clone().end, window.Clone())
	disk2.Cost = 1.5
	disk2.ByteHours = 60 * gb * hours

	disk3 := NewDisk("disk3", "cluster2", "gcp-disk3", *window.Clone().start, *window.Clone().end, window.Clone())
	disk3.Cost = 2.5
	disk3.ByteHours = 100 * gb * hours

	disk4 := NewDisk("disk4", "cluster2", "gcp-disk4", *window.Clone().start, *window.Clone().end, window.Clone())
	disk4.Cost = 1.5
	disk4.ByteHours = 100 * gb * hours

	cm1 := NewClusterManagement("gcp", "cluster1", window.Clone())
	cm1.Cost = 3.0

	cm2 := NewClusterManagement("gcp", "cluster2", window.Clone())
	cm2.Cost = 0.0

	return NewAssetSet(
		start, end,
		// cluster 1
		node1, node2, node3, disk1, disk2, cm1,
		// cluster 2
		node4, disk3, disk4, cm2,
		// cluster 3
		node5,
	)
}

func assertAssetSet(t *testing.T, as *AssetSet, msg string, window Window, exps map[string]float64, err error) {
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy[%s]: unexpected error: %s", msg, err)
	}
	if as.Length() != len(exps) {
		t.Fatalf("AssetSet.AggregateBy[%s]: expected set of length %d, actual %d", msg, len(exps), as.Length())
	}
	if !as.Window.Equal(window) {
		t.Fatalf("AssetSet.AggregateBy[%s]: expected window %s, actual %s", msg, window, as.Window)
	}
	as.Each(func(key string, a Asset) {
		if exp, ok := exps[key]; ok {
			if math.Round(a.TotalCost()*100) != math.Round(exp*100) {
				t.Fatalf("AssetSet.AggregateBy[%s]: key %s expected total cost %.2f, actual %.2f", msg, key, exp, a.TotalCost())
			}
			if !a.Window().Equal(window) {
				t.Fatalf("AssetSet.AggregateBy[%s]: key %s expected window %s, actual %s", msg, key, window, a.Window())
			}
		} else {
			t.Fatalf("AssetSet.AggregateBy[%s]: unexpected asset: %s", msg, key)
		}
	})
}

func printAssetSet(msg string, as *AssetSet) {
	fmt.Printf("--- %s ---\n", msg)
	as.Each(func(key string, a Asset) {
		fmt.Printf(" > %s: %s\n", key, a)
	})
}

func TestAny_Add(t *testing.T) {
	any1 := NewAsset(*windows[0].start, *windows[0].end, windows[0])
	any1.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "any1",
	})
	any1.Cost = 9.0
	any1.SetAdjustment(1.0)

	any2 := NewAsset(*windows[0].start, *windows[0].end, windows[0])
	any2.SetProperties(&AssetProperties{
		Name:       "any2",
		Cluster:    "cluster1",
		ProviderID: "any2",
	})
	any2.Cost = 4.0
	any2.SetAdjustment(1.0)

	any3 := any1.Add(any2)

	// Check that the sums and properties are correct
	if any3.TotalCost() != 15.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 15.0, any3.TotalCost())
	}
	if any3.Adjustment() != 2.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 2.0, any3.Adjustment())
	}
	if any3.Properties().Cluster != "cluster1" {
		t.Fatalf("Any.Add: expected %s; got %s", "cluster1", any3.Properties().Cluster)
	}
	if any3.Type() != AnyAssetType {
		t.Fatalf("Any.Add: expected %s; got %s", AnyAssetType, any3.Type())
	}
	if any3.Properties().ProviderID != "" {
		t.Fatalf("Any.Add: expected %s; got %s", "", any3.Properties().ProviderID)
	}
	if any3.Properties().Name != "" {
		t.Fatalf("Any.Add: expected %s; got %s", "", any3.Properties().Name)
	}

	// Check that the original assets are unchanged
	if any1.TotalCost() != 10.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 10.0, any1.TotalCost())
	}
	if any1.Adjustment() != 1.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 1.0, any1.Adjustment())
	}
	if any2.TotalCost() != 5.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 5.0, any2.TotalCost())
	}
	if any2.Adjustment() != 1.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 1.0, any2.Adjustment())
	}
}

func TestAny_Clone(t *testing.T) {
	any1 := NewAsset(*windows[0].start, *windows[0].end, windows[0])
	any1.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "any1",
	})
	any1.Cost = 9.0
	any1.SetAdjustment(1.0)

	any2 := any1.Clone()

	any1.Cost = 18.0
	any1.SetAdjustment(2.0)

	// any2 should match any1, even after mutating any1
	if any2.TotalCost() != 10.0 {
		t.Fatalf("Any.Clone: expected %f; got %f", 10.0, any2.TotalCost())
	}
	if any2.Adjustment() != 1.0 {
		t.Fatalf("Any.Clone: expected %f; got %f", 1.0, any2.Adjustment())
	}
}

func TestAny_MarshalJSON(t *testing.T) {
	any1 := NewAsset(*windows[0].start, *windows[0].end, windows[0])
	any1.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "any1",
	})
	any1.Cost = 9.0
	any1.SetAdjustment(1.0)

	_, err := json.Marshal(any1)
	if err != nil {
		t.Fatalf("Any.MarshalJSON: unexpected error: %s", err)
	}

	any2 := NewAsset(*windows[0].start, *windows[0].end, windows[0])
	any2.SetProperties(&AssetProperties{
		Name:       "any2",
		Cluster:    "cluster1",
		ProviderID: "any2",
	})
	any2.Cost = math.NaN()
	any2.SetAdjustment(1.0)

	_, err = json.Marshal(any2)
	if err != nil {
		t.Fatalf("Any.MarshalJSON: unexpected error: %s", err)
	}
}

func TestDisk_Add(t *testing.T) {
	// 1. aggregate: add size, local
	// 2. accumulate: don't add size, local

	hours := windows[0].Duration().Hours()

	// Aggregate: two disks, one window
	disk1 := NewDisk("disk1", "cluster1", "disk1", *windows[0].start, *windows[0].end, windows[0])
	disk1.ByteHours = 100.0 * gb * hours
	disk1.Cost = 9.0
	disk1.SetAdjustment(1.0)

	if disk1.Bytes() != 100.0*gb {
		t.Fatalf("Disk.Add: expected %f; got %f", 100.0*gb, disk1.Bytes())
	}

	disk2 := NewDisk("disk2", "cluster1", "disk2", *windows[0].start, *windows[0].end, windows[0])
	disk2.ByteHours = 60.0 * gb * hours
	disk2.Cost = 4.0
	disk2.Local = 1.0
	disk2.SetAdjustment(1.0)

	if disk2.Bytes() != 60.0*gb {
		t.Fatalf("Disk.Add: expected %f; got %f", 60.0*gb, disk2.Bytes())
	}

	diskT := disk1.Add(disk2).(*Disk)

	// Check that the sums and properties are correct
	if diskT.TotalCost() != 15.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 15.0, diskT.TotalCost())
	}
	if diskT.Adjustment() != 2.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 2.0, diskT.Adjustment())
	}
	if diskT.Properties().Cluster != "cluster1" {
		t.Fatalf("Disk.Add: expected %s; got %s", "cluster1", diskT.Properties().Cluster)
	}
	if diskT.Type() != DiskAssetType {
		t.Fatalf("Disk.Add: expected %s; got %s", AnyAssetType, diskT.Type())
	}
	if diskT.Properties().ProviderID != "" {
		t.Fatalf("Disk.Add: expected %s; got %s", "", diskT.Properties().ProviderID)
	}
	if diskT.Properties().Name != "" {
		t.Fatalf("Disk.Add: expected %s; got %s", "", diskT.Properties().Name)
	}
	if diskT.Bytes() != 160.0*gb {
		t.Fatalf("Disk.Add: expected %f; got %f", 160.0*gb, diskT.Bytes())
	}
	if !util.IsApproximately(diskT.Local, 0.333333) {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.333333, diskT.Local)
	}

	// Check that the original assets are unchanged
	if disk1.TotalCost() != 10.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 10.0, disk1.TotalCost())
	}
	if disk1.Adjustment() != 1.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 1.0, disk1.Adjustment())
	}
	if disk1.Local != 0.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.0, disk1.Local)
	}
	if disk2.TotalCost() != 5.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 5.0, disk2.TotalCost())
	}
	if disk2.Adjustment() != 1.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 1.0, disk2.Adjustment())
	}
	if disk2.Local != 1.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 1.0, disk2.Local)
	}

	disk3 := NewDisk("disk3", "cluster1", "disk3", *windows[0].start, *windows[0].end, windows[0])
	disk3.ByteHours = 0.0 * hours
	disk3.Cost = 0.0
	disk3.Local = 0.0
	disk3.SetAdjustment(0.0)

	disk4 := NewDisk("disk4", "cluster1", "disk4", *windows[0].start, *windows[0].end, windows[0])
	disk4.ByteHours = 0.0 * hours
	disk4.Cost = 0.0
	disk4.Local = 1.0
	disk4.SetAdjustment(0.0)

	diskT = disk3.Add(disk4).(*Disk)

	if diskT.TotalCost() != 0.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.0, diskT.TotalCost())
	}
	if diskT.Local != 0.5 {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.5, diskT.Local)
	}

	// Accumulate: one disks, two windows
	diskA1 := NewDisk("diskA1", "cluster1", "diskA1", *windows[0].start, *windows[0].end, windows[0])
	diskA1.ByteHours = 100 * gb * hours
	diskA1.Cost = 9.0
	diskA1.SetAdjustment(1.0)

	diskA2 := NewDisk("diskA2", "cluster1", "diskA2", *windows[1].start, *windows[1].end, windows[1])
	diskA2.ByteHours = 100 * gb * hours
	diskA2.Cost = 9.0
	diskA2.SetAdjustment(1.0)

	diskAT := diskA1.Add(diskA2).(*Disk)

	// Check that the sums and properties are correct
	if diskAT.TotalCost() != 20.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 20.0, diskAT.TotalCost())
	}
	if diskAT.Adjustment() != 2.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 2.0, diskAT.Adjustment())
	}
	if diskAT.Properties().Cluster != "cluster1" {
		t.Fatalf("Disk.Add: expected %s; got %s", "cluster1", diskAT.Properties().Cluster)
	}
	if diskAT.Type() != DiskAssetType {
		t.Fatalf("Disk.Add: expected %s; got %s", AnyAssetType, diskAT.Type())
	}
	if diskAT.Properties().ProviderID != "" {
		t.Fatalf("Disk.Add: expected %s; got %s", "", diskAT.Properties().ProviderID)
	}
	if diskAT.Properties().Name != "" {
		t.Fatalf("Disk.Add: expected %s; got %s", "", diskAT.Properties().Name)
	}
	if diskAT.Bytes() != 100.0*gb {
		t.Fatalf("Disk.Add: expected %f; got %f", 100.0*gb, diskT.Bytes())
	}
	if diskAT.Local != 0.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.0, diskAT.Local)
	}

	// Check that the original assets are unchanged
	if diskA1.TotalCost() != 10.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 10.0, diskA1.TotalCost())
	}
	if diskA1.Adjustment() != 1.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 1.0, diskA1.Adjustment())
	}
	if diskA1.Local != 0.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.0, diskA1.Local)
	}
	if diskA2.TotalCost() != 10.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 10.0, diskA2.TotalCost())
	}
	if diskA2.Adjustment() != 1.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 1.0, diskA2.Adjustment())
	}
	if diskA2.Local != 0.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 0.0, diskA2.Local)
	}
}

func TestDisk_Clone(t *testing.T) {
	disk1 := NewDisk("disk1", "cluster1", "disk1", *windows[0].start, *windows[0].end, windows[0])
	disk1.Local = 0.0
	disk1.Cost = 9.0
	disk1.SetAdjustment(1.0)

	disk2 := disk1.Clone().(*Disk)

	disk2.Local = 1.0
	disk1.Cost = 18.0
	disk1.SetAdjustment(2.0)

	// disk2 should match disk1, even after mutating disk1
	if disk2.TotalCost() != 10.0 {
		t.Fatalf("Any.Clone: expected %f; got %f", 10.0, disk2.TotalCost())
	}
	if disk2.Adjustment() != 1.0 {
		t.Fatalf("Any.Clone: expected %f; got %f", 1.0, disk2.Adjustment())
	}
	if disk2.Local != 1.0 {
		t.Fatalf("Disk.Add: expected %f; got %f", 1.0, disk2.Local)
	}
}

func TestDisk_MarshalJSON(t *testing.T) {
	disk := NewDisk("disk", "cluster", "providerID", *windows[0].start, *windows[0].end, windows[0])
	disk.SetLabels(AssetLabels{
		"label": "value",
	})
	disk.Cost = 9.0
	disk.SetAdjustment(1.0)

	_, err := json.Marshal(disk)
	if err != nil {
		t.Fatalf("Disk.MarshalJSON: unexpected error: %s", err)
	}
}

func TestNode_Add(t *testing.T) {
	// 1. aggregate: add size, local
	// 2. accumulate: don't add size, local

	hours := windows[0].Duration().Hours()

	// Aggregate: two nodes, one window
	node1 := NewNode("node1", "cluster1", "node1", *windows[0].start, *windows[0].end, windows[0])
	node1.CPUCoreHours = 1.0 * hours
	node1.RAMByteHours = 2.0 * gb * hours
	node1.GPUHours = 0.0 * hours
	node1.GPUCost = 0.0
	node1.CPUCost = 8.0
	node1.RAMCost = 4.0
	node1.Discount = 0.3
	node1.CPUBreakdown = &Breakdown{
		Idle:   0.6,
		System: 0.2,
		User:   0.2,
		Other:  0.0,
	}
	node1.RAMBreakdown = &Breakdown{
		Idle:   0.6,
		System: 0.2,
		User:   0.2,
		Other:  0.0,
	}
	node1.SetAdjustment(1.6)

	node2 := NewNode("node2", "cluster1", "node2", *windows[0].start, *windows[0].end, windows[0])
	node2.CPUCoreHours = 1.0 * hours
	node2.RAMByteHours = 2.0 * gb * hours
	node2.GPUHours = 0.0 * hours
	node2.GPUCost = 0.0
	node2.CPUCost = 3.0
	node2.RAMCost = 1.0
	node2.Discount = 0.0
	node1.CPUBreakdown = &Breakdown{
		Idle:   0.9,
		System: 0.05,
		User:   0.0,
		Other:  0.05,
	}
	node1.RAMBreakdown = &Breakdown{
		Idle:   0.9,
		System: 0.05,
		User:   0.0,
		Other:  0.05,
	}
	node2.SetAdjustment(1.0)

	nodeT := node1.Add(node2).(*Node)

	// Check that the sums and properties are correct
	if !util.IsApproximately(nodeT.TotalCost(), 15.0) {
		t.Fatalf("Node.Add: expected %f; got %f", 15.0, nodeT.TotalCost())
	}
	if nodeT.Adjustment() != 2.6 {
		t.Fatalf("Node.Add: expected %f; got %f", 2.6, nodeT.Adjustment())
	}
	if nodeT.Properties().Cluster != "cluster1" {
		t.Fatalf("Node.Add: expected %s; got %s", "cluster1", nodeT.Properties().Cluster)
	}
	if nodeT.Type() != NodeAssetType {
		t.Fatalf("Node.Add: expected %s; got %s", AnyAssetType, nodeT.Type())
	}
	if nodeT.Properties().ProviderID != "" {
		t.Fatalf("Node.Add: expected %s; got %s", "", nodeT.Properties().ProviderID)
	}
	if nodeT.Properties().Name != "" {
		t.Fatalf("Node.Add: expected %s; got %s", "", nodeT.Properties().Name)
	}
	if nodeT.CPUCores() != 2.0 {
		t.Fatalf("Node.Add: expected %f; got %f", 2.0, nodeT.CPUCores())
	}
	if nodeT.RAMBytes() != 4.0*gb {
		t.Fatalf("Node.Add: expected %f; got %f", 4.0*gb, nodeT.RAMBytes())
	}

	// Check that the original assets are unchanged
	if !util.IsApproximately(node1.TotalCost(), 10.0) {
		t.Fatalf("Node.Add: expected %f; got %f", 10.0, node1.TotalCost())
	}
	if node1.Adjustment() != 1.6 {
		t.Fatalf("Node.Add: expected %f; got %f", 1.0, node1.Adjustment())
	}
	if !util.IsApproximately(node2.TotalCost(), 5.0) {
		t.Fatalf("Node.Add: expected %f; got %f", 5.0, node2.TotalCost())
	}
	if node2.Adjustment() != 1.0 {
		t.Fatalf("Node.Add: expected %f; got %f", 1.0, node2.Adjustment())
	}

	// Check that we don't divide by zero computing Local
	node3 := NewNode("node3", "cluster1", "node3", *windows[0].start, *windows[0].end, windows[0])
	node3.CPUCoreHours = 0 * hours
	node3.RAMByteHours = 0 * hours
	node3.GPUHours = 0.0 * hours
	node3.GPUCost = 0
	node3.CPUCost = 0.0
	node3.RAMCost = 0.0
	node3.Discount = 0.3
	node3.SetAdjustment(0.0)

	node4 := NewNode("node4", "cluster1", "node4", *windows[0].start, *windows[0].end, windows[0])
	node4.CPUCoreHours = 0 * hours
	node4.RAMByteHours = 0 * hours
	node4.GPUHours = 0.0 * hours
	node4.GPUCost = 0
	node4.CPUCost = 0.0
	node4.RAMCost = 0.0
	node4.Discount = 0.1
	node4.SetAdjustment(0.0)

	nodeT = node3.Add(node4).(*Node)

	// Check that the sums and properties are correct and without NaNs
	if nodeT.TotalCost() != 0.0 {
		t.Fatalf("Node.Add: expected %f; got %f", 0.0, nodeT.TotalCost())
	}
	if nodeT.Discount != 0.2 {
		t.Fatalf("Node.Add: expected %f; got %f", 0.2, nodeT.Discount)
	}

	// Accumulate: one nodes, two window
	nodeA1 := NewNode("nodeA1", "cluster1", "nodeA1", *windows[0].start, *windows[0].end, windows[0])
	nodeA1.CPUCoreHours = 1.0 * hours
	nodeA1.RAMByteHours = 2.0 * gb * hours
	nodeA1.GPUHours = 0.0 * hours
	nodeA1.GPUCost = 0.0
	nodeA1.CPUCost = 8.0
	nodeA1.RAMCost = 4.0
	nodeA1.Discount = 0.3
	nodeA1.SetAdjustment(1.6)

	nodeA2 := NewNode("nodeA2", "cluster1", "nodeA2", *windows[1].start, *windows[1].end, windows[1])
	nodeA2.CPUCoreHours = 1.0 * hours
	nodeA2.RAMByteHours = 2.0 * gb * hours
	nodeA2.GPUHours = 0.0 * hours
	nodeA2.GPUCost = 0.0
	nodeA2.CPUCost = 3.0
	nodeA2.RAMCost = 1.0
	nodeA2.Discount = 0.0
	nodeA2.SetAdjustment(1.0)

	nodeAT := nodeA1.Add(nodeA2).(*Node)

	// Check that the sums and properties are correct
	if !util.IsApproximately(nodeAT.TotalCost(), 15.0) {
		t.Fatalf("Node.Add: expected %f; got %f", 15.0, nodeAT.TotalCost())
	}
	if nodeAT.Adjustment() != 2.6 {
		t.Fatalf("Node.Add: expected %f; got %f", 2.6, nodeAT.Adjustment())
	}
	if nodeAT.Properties().Cluster != "cluster1" {
		t.Fatalf("Node.Add: expected %s; got %s", "cluster1", nodeAT.Properties().Cluster)
	}
	if nodeAT.Type() != NodeAssetType {
		t.Fatalf("Node.Add: expected %s; got %s", AnyAssetType, nodeAT.Type())
	}
	if nodeAT.Properties().ProviderID != "" {
		t.Fatalf("Node.Add: expected %s; got %s", "", nodeAT.Properties().ProviderID)
	}
	if nodeAT.Properties().Name != "" {
		t.Fatalf("Node.Add: expected %s; got %s", "", nodeAT.Properties().Name)
	}
	if nodeAT.CPUCores() != 1.0 {
		t.Fatalf("Node.Add: expected %f; got %f", 1.0, nodeAT.CPUCores())
	}
	if nodeAT.RAMBytes() != 2.0*gb {
		t.Fatalf("Node.Add: expected %f; got %f", 2.0*gb, nodeAT.RAMBytes())
	}
	if nodeAT.GPUs() != 0.0 {
		t.Fatalf("Node.Add: expected %f; got %f", 0.0, nodeAT.GPUs())
	}

	// Check that the original assets are unchanged
	if !util.IsApproximately(nodeA1.TotalCost(), 10.0) {
		t.Fatalf("Node.Add: expected %f; got %f", 10.0, nodeA1.TotalCost())
	}
	if nodeA1.Adjustment() != 1.6 {
		t.Fatalf("Node.Add: expected %f; got %f", 1.0, nodeA1.Adjustment())
	}
	if !util.IsApproximately(nodeA2.TotalCost(), 5.0) {
		t.Fatalf("Node.Add: expected %f; got %f", 5.0, nodeA2.TotalCost())
	}
	if nodeA2.Adjustment() != 1.0 {
		t.Fatalf("Node.Add: expected %f; got %f", 1.0, nodeA2.Adjustment())
	}
}

func TestNode_Clone(t *testing.T) {
	// TODO
}

func TestNode_MarshalJSON(t *testing.T) {
	node := NewNode("node", "cluster", "providerID", *windows[0].start, *windows[0].end, windows[0])
	node.SetLabels(AssetLabels{
		"label": "value",
	})
	node.CPUCost = 9.0
	node.RAMCost = 0.0
	node.RAMCost = 21.0
	node.CPUCoreHours = 123.0
	node.RAMByteHours = 13323.0
	node.GPUHours = 123.0
	node.SetAdjustment(1.0)

	_, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("Node.MarshalJSON: unexpected error: %s", err)
	}
}

func TestClusterManagement_Add(t *testing.T) {
	cm1 := NewClusterManagement("gcp", "cluster1", windows[0])
	cm1.Cost = 9.0

	cm2 := NewClusterManagement("gcp", "cluster1", windows[0])
	cm2.Cost = 4.0

	cm3 := cm1.Add(cm2)

	// Check that the sums and properties are correct
	if cm3.TotalCost() != 13.0 {
		t.Fatalf("ClusterManagement.Add: expected %f; got %f", 13.0, cm3.TotalCost())
	}
	if cm3.Properties().Cluster != "cluster1" {
		t.Fatalf("ClusterManagement.Add: expected %s; got %s", "cluster1", cm3.Properties().Cluster)
	}
	if cm3.Type() != ClusterManagementAssetType {
		t.Fatalf("ClusterManagement.Add: expected %s; got %s", ClusterManagementAssetType, cm3.Type())
	}

	// Check that the original assets are unchanged
	if cm1.TotalCost() != 9.0 {
		t.Fatalf("ClusterManagement.Add: expected %f; got %f", 9.0, cm1.TotalCost())
	}
	if cm2.TotalCost() != 4.0 {
		t.Fatalf("ClusterManagement.Add: expected %f; got %f", 4.0, cm2.TotalCost())
	}
}

func TestClusterManagement_Clone(t *testing.T) {
	// TODO
}

func TestCloudAny_Add(t *testing.T) {
	ca1 := NewCloud(ComputeCategory, "ca1", *windows[0].start, *windows[0].end, windows[0])
	ca1.Cost = 9.0
	ca1.SetAdjustment(1.0)

	ca2 := NewCloud(StorageCategory, "ca2", *windows[0].start, *windows[0].end, windows[0])
	ca2.Cost = 4.0
	ca2.SetAdjustment(1.0)

	ca3 := ca1.Add(ca2)

	// Check that the sums and properties are correct
	if ca3.TotalCost() != 15.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 15.0, ca3.TotalCost())
	}
	if ca3.Adjustment() != 2.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 2.0, ca3.Adjustment())
	}
	if ca3.Type() != CloudAssetType {
		t.Fatalf("Any.Add: expected %s; got %s", CloudAssetType, ca3.Type())
	}

	// Check that the original assets are unchanged
	if ca1.TotalCost() != 10.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 10.0, ca1.TotalCost())
	}
	if ca1.Adjustment() != 1.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 1.0, ca1.Adjustment())
	}
	if ca2.TotalCost() != 5.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 5.0, ca2.TotalCost())
	}
	if ca2.Adjustment() != 1.0 {
		t.Fatalf("Any.Add: expected %f; got %f", 1.0, ca2.Adjustment())
	}
}

func TestCloudAny_Clone(t *testing.T) {
	// TODO
}

func TestAssetSet_AggregateBy(t *testing.T) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	window := NewWindow(&startYesterday, &endYesterday)

	// Scenarios to test:

	// 1  Single-aggregation
	// 1a []AssetProperty=[Cluster]
	// 1b []AssetProperty=[Type]
	// 1c []AssetProperty=[Nil]
	// 1d []AssetProperty=nil
	// 1e aggregateBy []string=["label:test"]

	// 2  Multi-aggregation
	// 2a []AssetProperty=[Cluster,Type]

	// 3  Share resources
	// 3a Shared hourly cost > 0.0

	// Definitions and set-up:

	var as *AssetSet
	var err error

	// Tests:

	// 1  Single-aggregation

	// 1a []AssetProperty=[Cluster]
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy([]string{string(AssetClusterProp)}, nil)
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1a", window, map[string]float64{
		"cluster1": 26.0,
		"cluster2": 15.0,
		"cluster3": 19.0,
	}, nil)

	// 1b []AssetProperty=[Type]
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy([]string{string(AssetTypeProp)}, nil)
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1b", window, map[string]float64{
		"Node":              49.0,
		"Disk":              8.0,
		"ClusterManagement": 3.0,
	}, nil)

	// 1c []AssetProperty=[Nil]
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy([]string{}, nil)
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1c", window, map[string]float64{
		"": 60.0,
	}, nil)

	// 1d []AssetProperty=nil
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy(nil, nil)
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1d", window, map[string]float64{
		"__undefined__/__undefined__/__undefined__/Compute/cluster1/Node/Kubernetes/gcp-node1/node1":                   7.00,
		"__undefined__/__undefined__/__undefined__/Compute/cluster1/Node/Kubernetes/gcp-node2/node2":                   5.50,
		"__undefined__/__undefined__/__undefined__/Compute/cluster1/Node/Kubernetes/gcp-node3/node3":                   6.50,
		"__undefined__/__undefined__/__undefined__/Storage/cluster1/Disk/Kubernetes/gcp-disk1/disk1":                   2.50,
		"__undefined__/__undefined__/__undefined__/Storage/cluster1/Disk/Kubernetes/gcp-disk2/disk2":                   1.50,
		"GCP/__undefined__/__undefined__/Management/cluster1/ClusterManagement/Kubernetes/__undefined__/__undefined__": 3.00,
		"__undefined__/__undefined__/__undefined__/Compute/cluster2/Node/Kubernetes/gcp-node4/node4":                   11.00,
		"__undefined__/__undefined__/__undefined__/Storage/cluster2/Disk/Kubernetes/gcp-disk3/disk3":                   2.50,
		"__undefined__/__undefined__/__undefined__/Storage/cluster2/Disk/Kubernetes/gcp-disk4/disk4":                   1.50,
		"GCP/__undefined__/__undefined__/Management/cluster2/ClusterManagement/Kubernetes/__undefined__/__undefined__": 0.00,
		"__undefined__/__undefined__/__undefined__/Compute/cluster3/Node/Kubernetes/aws-node5/node5":                   19.00,
	}, nil)

	// 1e aggregateBy []string=["label:test"]
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy([]string{"label:test"}, nil)
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1e", window, map[string]float64{
		"__undefined__": 53.00,
		"test=test":     7.00,
	}, nil)

	// 2  Multi-aggregation

	// 2a []AssetProperty=[Cluster,Type]
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy([]string{string(AssetClusterProp), string(AssetTypeProp)}, nil)
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "2a", window, map[string]float64{
		"cluster1/Node":              19.0,
		"cluster1/Disk":              4.0,
		"cluster1/ClusterManagement": 3.0,
		"cluster2/Node":              11.0,
		"cluster2/Disk":              4.0,
		"cluster2/ClusterManagement": 0.0,
		"cluster3/Node":              19.0,
	}, nil)

	// 3  Share resources

	// 3a Shared hourly cost > 0.0
	as = generateAssetSet(startYesterday)
	err = as.AggregateBy([]string{string(AssetTypeProp)}, &AssetAggregationOptions{
		SharedHourlyCosts: map[string]float64{"shared1": 0.5},
	})
	if err != nil {
		t.Fatalf("AssetSet.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1a", window, map[string]float64{
		"Node":              49.0,
		"Disk":              8.0,
		"ClusterManagement": 3.0,
		"Shared":            12.0,
	}, nil)
}

func TestAssetSet_FindMatch(t *testing.T) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)
	s, e := startYesterday, endYesterday
	w := NewWindow(&s, &e)

	var query, match Asset
	var as *AssetSet
	var err error

	// Assert success of a simple match of Type and ProviderID
	as = generateAssetSet(startYesterday)
	query = NewNode("", "", "gcp-node3", s, e, w)
	match, err = as.FindMatch(query, []string{string(AssetTypeProp), string(AssetProviderIDProp)})
	if err != nil {
		t.Fatalf("AssetSet.FindMatch: unexpected error: %s", err)
	}

	// Assert error of a simple non-match of Type and ProviderID
	as = generateAssetSet(startYesterday)
	query = NewNode("", "", "aws-node3", s, e, w)
	match, err = as.FindMatch(query, []string{string(AssetTypeProp), string(AssetProviderIDProp)})
	if err == nil {
		t.Fatalf("AssetSet.FindMatch: expected error (no match); found %s", match)
	}

	// Assert error of matching ProviderID, but not Type
	as = generateAssetSet(startYesterday)
	query = NewCloud(ComputeCategory, "gcp-node3", s, e, w)
	match, err = as.FindMatch(query, []string{string(AssetTypeProp), string(AssetProviderIDProp)})
	if err == nil {
		t.Fatalf("AssetSet.FindMatch: expected error (no match); found %s", match)
	}
}

func TestAssetSetRange_Accumulate(t *testing.T) {
	endYesterday := time.Now().UTC().Truncate(day)
	startYesterday := endYesterday.Add(-day)

	startD2 := startYesterday
	startD1 := startD2.Add(-day)
	startD0 := startD1.Add(-day)

	window := NewWindow(&startD0, &endYesterday)

	var asr *AssetSetRange
	var as *AssetSet
	var err error

	asr = NewAssetSetRange(
		generateAssetSet(startD0),
		generateAssetSet(startD1),
		generateAssetSet(startD2),
	)
	err = asr.AggregateBy(nil, nil)
	as, err = asr.Accumulate()
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1a", window, map[string]float64{
		"__undefined__/__undefined__/__undefined__/Compute/cluster1/Node/Kubernetes/gcp-node1/node1":                   21.00,
		"__undefined__/__undefined__/__undefined__/Compute/cluster1/Node/Kubernetes/gcp-node2/node2":                   16.50,
		"__undefined__/__undefined__/__undefined__/Compute/cluster1/Node/Kubernetes/gcp-node3/node3":                   19.50,
		"__undefined__/__undefined__/__undefined__/Storage/cluster1/Disk/Kubernetes/gcp-disk1/disk1":                   7.50,
		"__undefined__/__undefined__/__undefined__/Storage/cluster1/Disk/Kubernetes/gcp-disk2/disk2":                   4.50,
		"GCP/__undefined__/__undefined__/Management/cluster1/ClusterManagement/Kubernetes/__undefined__/__undefined__": 9.00,
		"__undefined__/__undefined__/__undefined__/Compute/cluster2/Node/Kubernetes/gcp-node4/node4":                   33.00,
		"__undefined__/__undefined__/__undefined__/Storage/cluster2/Disk/Kubernetes/gcp-disk3/disk3":                   7.50,
		"__undefined__/__undefined__/__undefined__/Storage/cluster2/Disk/Kubernetes/gcp-disk4/disk4":                   4.50,
		"GCP/__undefined__/__undefined__/Management/cluster2/ClusterManagement/Kubernetes/__undefined__/__undefined__": 0.00,
		"__undefined__/__undefined__/__undefined__/Compute/cluster3/Node/Kubernetes/aws-node5/node5":                   57.00,
	}, nil)

	asr = NewAssetSetRange(
		generateAssetSet(startD0),
		generateAssetSet(startD1),
		generateAssetSet(startD2),
	)
	err = asr.AggregateBy([]string{}, nil)
	as, err = asr.Accumulate()
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1b", window, map[string]float64{
		"": 180.00,
	}, nil)

	asr = NewAssetSetRange(
		generateAssetSet(startD0),
		generateAssetSet(startD1),
		generateAssetSet(startD2),
	)
	err = asr.AggregateBy([]string{string(AssetTypeProp)}, nil)
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	as, err = asr.Accumulate()
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1c", window, map[string]float64{
		"Node":              147.0,
		"Disk":              24.0,
		"ClusterManagement": 9.0,
	}, nil)

	asr = NewAssetSetRange(
		generateAssetSet(startD0),
		generateAssetSet(startD1),
		generateAssetSet(startD2),
	)
	err = asr.AggregateBy([]string{string(AssetClusterProp)}, nil)
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	as, err = asr.Accumulate()
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1c", window, map[string]float64{
		"cluster1": 78.0,
		"cluster2": 45.0,
		"cluster3": 57.0,
	}, nil)

	// Accumulation with aggregation should work, even when the first AssetSet
	// is empty (this was previously an issue)
	asr = NewAssetSetRange(
		NewAssetSet(startD0, startD1),
		generateAssetSet(startD1),
		generateAssetSet(startD2),
	)

	err = asr.AggregateBy([]string{string(AssetTypeProp)}, nil)
	as, err = asr.Accumulate()
	if err != nil {
		t.Fatalf("AssetSetRange.AggregateBy: unexpected error: %s", err)
	}
	assertAssetSet(t, as, "1d", window, map[string]float64{
		"Node":              98.00,
		"Disk":              16.00,
		"ClusterManagement": 6.00,
	}, nil)
}

func TestAssetToExternalAllocation(t *testing.T) {
	var asset Asset
	var alloc *Allocation
	var err error

	alloc, err = AssetToExternalAllocation(asset, []string{"namespace"}, map[string]string{})
	if err == nil {
		t.Fatalf("expected error due to nil asset")
	}

	// Consider this Asset:
	//   Cloud {
	// 	   TotalCost: 10.00,
	// 	   Labels{
	//       "kubernetes_namespace":"monitoring",
	// 	     "env":"prod"
	// 	   }
	//   }
	cloud := NewCloud(ComputeCategory, "abc123", start1, start2, windows[0])
	cloud.SetLabels(map[string]string{
		"namespace": "monitoring",
		"env":       "prod",
		"product":   "cost-analyzer",
	})
	cloud.Cost = 10.00
	asset = cloud

	alloc, err = AssetToExternalAllocation(asset, []string{"namespace"}, map[string]string{})
	if err != nil {
		t.Fatalf("expected to not error")
	}
	alloc, err = AssetToExternalAllocation(asset, nil, map[string]string{})
	if err == nil {
		t.Fatalf("expected error due to nil aggregateBy")
	}

	// Given the following parameters, we expect to return:
	//
	//   1) single-prop full match
	//   aggregateBy = ["namespace"]
	//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
	//   => Allocation{Name: "monitoring", ExternalCost: 10.00, TotalCost: 10.00}, nil
	//
	//   2) multi-prop full match
	//   aggregateBy = ["namespace", "label:env"]
	//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
	//   => Allocation{Name: "monitoring/env=prod", ExternalCost: 10.00, TotalCost: 10.00}, nil
	//
	//   3) multi-prop partial match
	//   aggregateBy = ["namespace", "label:foo"]
	//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
	//   => Allocation{Name: "monitoring/__unallocated__", ExternalCost: 10.00, TotalCost: 10.00}, nil
	//
	//   4) no match
	//   aggregateBy = ["cluster"]
	//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
	//   => nil, err

	// 1) single-prop full match
	alloc, err = AssetToExternalAllocation(asset, []string{"namespace"}, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if alloc.Name != "monitoring/__external__" {
		t.Fatalf("expected external allocation with name '%s'; got '%s'", "monitoring/__external__", alloc.Name)
	}
	if ns := alloc.Properties.Namespace; ns != "monitoring" {
		t.Fatalf("expected external allocation with AllocationProperties.Namespace '%s'; got '%s' (%s)", "monitoring", ns, err)
	}
	if alloc.ExternalCost != 10.00 {
		t.Fatalf("expected external allocation with ExternalCost %f; got %f", 10.00, alloc.ExternalCost)
	}
	if alloc.TotalCost() != 10.00 {
		t.Fatalf("expected external allocation with TotalCost %f; got %f", 10.00, alloc.TotalCost())
	}

	// 2) multi-prop full match
	alloc, err = AssetToExternalAllocation(asset, []string{"namespace", "label:env"}, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if alloc.Name != "monitoring/env=prod/__external__" {
		t.Fatalf("expected external allocation with name '%s'; got '%s'", "monitoring/env=prod/__external__", alloc.Name)
	}
	if ns := alloc.Properties.Namespace; ns != "monitoring" {
		t.Fatalf("expected external allocation with AllocationProperties.Namespace '%s'; got '%s' (%s)", "monitoring", ns, err)
	}
	if ls := alloc.Properties.Labels; len(ls) == 0 || ls["env"] != "prod" {
		t.Fatalf("expected external allocation with AllocationProperties.Labels[\"env\"] '%s'; got '%s' (%s)", "prod", ls["env"], err)
	}
	if alloc.ExternalCost != 10.00 {
		t.Fatalf("expected external allocation with ExternalCost %f; got %f", 10.00, alloc.ExternalCost)
	}
	if alloc.TotalCost() != 10.00 {
		t.Fatalf("expected external allocation with TotalCost %f; got %f", 10.00, alloc.TotalCost())
	}

	// 3) multi-prop partial match
	alloc, err = AssetToExternalAllocation(asset, []string{"namespace", "label:foo"}, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if alloc.Name != "monitoring/__unallocated__/__external__" {
		t.Fatalf("expected external allocation with name '%s'; got '%s'", "monitoring/__unallocated__/__external__", alloc.Name)
	}
	if ns := alloc.Properties.Namespace; ns != "monitoring" {
		t.Fatalf("expected external allocation with AllocationProperties.Namespace '%s'; got '%s' (%s)", "monitoring", ns, err)
	}
	if alloc.ExternalCost != 10.00 {
		t.Fatalf("expected external allocation with ExternalCost %f; got %f", 10.00, alloc.ExternalCost)
	}
	if alloc.TotalCost() != 10.00 {
		t.Fatalf("expected external allocation with TotalCost %f; got %f", 10.00, alloc.TotalCost())
	}

	// 3) no match
	alloc, err = AssetToExternalAllocation(asset, []string{"cluster"}, map[string]string{})
	if err == nil {
		t.Fatalf("expected 'no match' error")
	}

	alloc, err = AssetToExternalAllocation(asset, []string{"namespace", "label:app"}, map[string]string{"app": "product"})
	if alloc.ExternalCost != 10.00 {
		t.Fatalf("expected external allocation with ExternalCost %f; got %f", 10.00, alloc.ExternalCost)
	}
	if alloc.TotalCost() != 10.00 {
		t.Fatalf("expected external allocation with TotalCost %f; got %f", 10.00, alloc.TotalCost())
	}

}

// TODO merge conflict had this:

// as.Each(func(key string, a Asset) {
// 	if exp, ok := exps[key]; ok {
// 		if math.Round(a.TotalCost()*100) != math.Round(exp*100) {
// 			t.Fatalf("AssetSet.AggregateBy[%s]: key %s expected total cost %.2f, actual %.2f", msg, key, exp, a.TotalCost())
// 		}
// 		if !a.Window().Equal(window) {
// 			t.Fatalf("AssetSet.AggregateBy[%s]: key %s expected window %s, actual %s", msg, key, window, as.Window)
// 		}
// 	} else {
// 		t.Fatalf("AssetSet.AggregateBy[%s]: unexpected asset: %s", msg, key)
// 	}
// })
// }

// // generateAssetSet generates the following topology:
// //
// // | Asset                        | Cost |  Adj |
// // +------------------------------+------+------+
// //   cluster1:
// //     node1:                        6.00   1.00
// //     node2:                        4.00   1.50
// //     node3:                        7.00  -0.50
// //     disk1:                        2.50   0.00
// //     disk2:                        1.50   0.00
// //     clusterManagement1:           3.00   0.00
// // +------------------------------+------+------+
// //   cluster1 subtotal              24.00   2.00
// // +------------------------------+------+------+
// //   cluster2:
// //     node4:                       12.00  -1.00
// //     disk3:                        2.50   0.00
// //     disk4:                        1.50   0.00
// //     clusterManagement2:           0.00   0.00
// // +------------------------------+------+------+
// //   cluster2 subtotal              16.00  -1.00
// // +------------------------------+------+------+
// //   cluster3:
// //     node5:                       17.00   2.00
// // +------------------------------+------+------+
// //   cluster3 subtotal              17.00   2.00
// // +------------------------------+------+------+
// //   total                          57.00   3.00
// // +------------------------------+------+------+
// func generateAssetSet(start time.Time) *AssetSet {
// end := start.Add(day)
// window := NewWindow(&start, &end)

// hours := window.Duration().Hours()

// node1 := NewNode("node1", "cluster1", "gcp-node1", *window.Clone().start, *window.Clone().end, window.Clone())
// node1.CPUCost = 4.0
// node1.RAMCost = 4.0
// node1.GPUCost = 2.0
// node1.Discount = 0.5
// node1.CPUCoreHours = 2.0 * hours
// node1.RAMByteHours = 4.0 * gb * hours
// node1.SetAdjustment(1.0)
// node1.SetLabels(map[string]string{"test": "test"})

// node2 := NewNode("node2", "cluster1", "gcp-node2", *window.Clone().start, *window.Clone().end, window.Clone())
// node2.CPUCost = 4.0
// node2.RAMCost = 4.0
// node2.GPUCost = 0.0
// node2.Discount = 0.5
// node2.CPUCoreHours = 2.0 * hours
// node2.RAMByteHours = 4.0 * gb * hours
// node2.SetAdjustment(1.5)

// node3 := NewNode("node3", "cluster1", "gcp-node3", *window.Clone().start, *window.Clone().end, window.Clone())
// node3.CPUCost = 4.0
// node3.RAMCost = 4.0
// node3.GPUCost = 3.0
// node3.Discount = 0.5
// node3.CPUCoreHours = 2.0 * hours
// node3.RAMByteHours = 4.0 * gb * hours
// node3.SetAdjustment(-0.5)

// node4 := NewNode("node4", "cluster2", "gcp-node4", *window.Clone().start, *window.Clone().end, window.Clone())
// node4.CPUCost = 10.0
// node4.RAMCost = 6.0
// node4.GPUCost = 0.0
// node4.Discount = 0.25
// node4.CPUCoreHours = 4.0 * hours
// node4.RAMByteHours = 12.0 * gb * hours
// node4.SetAdjustment(-1.0)

// node5 := NewNode("node5", "cluster3", "aws-node5", *window.Clone().start, *window.Clone().end, window.Clone())
// node5.CPUCost = 10.0
// node5.RAMCost = 7.0
// node5.GPUCost = 0.0
// node5.Discount = 0.0
// node5.CPUCoreHours = 8.0 * hours
// node5.RAMByteHours = 24.0 * gb * hours
// node5.SetAdjustment(2.0)

// disk1 := NewDisk("disk1", "cluster1", "gcp-disk1", *window.Clone().start, *window.Clone().end, window.Clone())
// disk1.Cost = 2.5
// disk1.ByteHours = 100 * gb * hours

// disk2 := NewDisk("disk2", "cluster1", "gcp-disk2", *window.Clone().start, *window.Clone().end, window.Clone())
// disk2.Cost = 1.5
// disk2.ByteHours = 60 * gb * hours

// disk3 := NewDisk("disk3", "cluster2", "gcp-disk3", *window.Clone().start, *window.Clone().end, window.Clone())
// disk3.Cost = 2.5
// disk3.ByteHours = 100 * gb * hours

// disk4 := NewDisk("disk4", "cluster2", "gcp-disk4", *window.Clone().start, *window.Clone().end, window.Clone())
// disk4.Cost = 1.5
// disk4.ByteHours = 100 * gb * hours

// cm1 := NewClusterManagement("gcp", "cluster1", window.Clone())
// cm1.Cost = 3.0
