package costmodel

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// TestComputeIdleAllocations_ProviderID_ByNode verifies that when computing
// idle allocations by node, each idle allocation correctly contains the
// cloud provider instance ID from AssetTotals.ProviderID, not the node name.
func TestComputeIdleAllocations_ProviderID_ByNode(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := opencost.NewClosedWindow(start, end)

	// Create AssetSet with nodes that have ProviderIDs
	assetSet := opencost.NewAssetSet(start, end)

	node1 := opencost.NewNode("ip-10-2-1-100.ec2.internal", "cluster1", "aws:///us-east-1a/i-0abc123def456789", start, end, window)
	node1.CPUCost = 10.0
	node1.RAMCost = 5.0
	node1.GPUCost = 0.0

	node2 := opencost.NewNode("gke-cluster-default-pool-node-456", "cluster1", "gce://my-project/us-central1-a/instance-456", start, end, window)
	node2.CPUCost = 8.0
	node2.RAMCost = 4.0
	node2.GPUCost = 0.0

	assetSet.Insert(node1, nil)
	assetSet.Insert(node2, nil)

	// Create AllocationSet with some utilization (not 100% to ensure idle exists)
	allocSet := opencost.NewAllocationSet(start, end)

	// Create allocations that use 50% of each node's resources
	alloc1 := &opencost.Allocation{
		Name:    "namespace1/pod1/container1",
		Start:   start,
		End:     end,
		Window:  window.Clone(),
		CPUCost: 5.0, // 50% of node1's CPU
		RAMCost: 2.5, // 50% of node1's RAM
		GPUCost: 0.0,
		Properties: &opencost.AllocationProperties{
			Cluster: "cluster1",
			Node:    "ip-10-2-1-100.ec2.internal",
		},
	}

	alloc2 := &opencost.Allocation{
		Name:    "namespace2/pod2/container2",
		Start:   start,
		End:     end,
		Window:  window.Clone(),
		CPUCost: 4.0, // 50% of node2's CPU
		RAMCost: 2.0, // 50% of node2's RAM
		GPUCost: 0.0,
		Properties: &opencost.AllocationProperties{
			Cluster: "cluster1",
			Node:    "gke-cluster-default-pool-node-456",
		},
	}

	allocSet.Insert(alloc1)
	allocSet.Insert(alloc2)

	// Compute idle allocations by node (idleByNode=true)
	idleSet, err := computeIdleAllocations(allocSet, assetSet, true)
	if err != nil {
		t.Fatalf("Error computing idle allocations: %v", err)
	}

	// Expected idle allocations with ProviderIDs
	expectedIdles := map[string]struct {
		providerID string
		nodeName   string
	}{
		"cluster1/ip-10-2-1-100.ec2.internal/__idle__": {
			providerID: "aws:///us-east-1a/i-0abc123def456789",
			nodeName:   "ip-10-2-1-100.ec2.internal",
		},
		"cluster1/gke-cluster-default-pool-node-456/__idle__": {
			providerID: "gce://my-project/us-central1-a/instance-456",
			nodeName:   "gke-cluster-default-pool-node-456",
		},
	}

	// Verify each idle allocation has the correct ProviderID
	foundCount := 0
	for _, alloc := range idleSet.Allocations {
		if !alloc.IsIdle() {
			continue
		}

		expected, ok := expectedIdles[alloc.Name]
		if !ok {
			t.Errorf("Unexpected idle allocation: %s", alloc.Name)
			continue
		}

		foundCount++

		// Verify ProviderID is the cloud instance ID, not the node name
		if alloc.Properties.ProviderID != expected.providerID {
			t.Errorf("Allocation %s: expected ProviderID %q, got %q",
				alloc.Name, expected.providerID, alloc.Properties.ProviderID)
		}

		// Verify ProviderID is NOT the node name (the bug we're fixing)
		if alloc.Properties.ProviderID == expected.nodeName {
			t.Errorf("Allocation %s: ProviderID should not be node name %q",
				alloc.Name, expected.nodeName)
		}

		// Verify Node field still contains the node name
		if alloc.Properties.Node != expected.nodeName {
			t.Errorf("Allocation %s: expected Node %q, got %q",
				alloc.Name, expected.nodeName, alloc.Properties.Node)
		}

		// Verify costs are non-zero (idle exists)
		if alloc.CPUCost <= 0 && alloc.RAMCost <= 0 {
			t.Errorf("Allocation %s: expected non-zero idle costs", alloc.Name)
		}
	}

	if foundCount != len(expectedIdles) {
		t.Errorf("Expected %d idle allocations, found %d", len(expectedIdles), foundCount)
	}
}

// TestComputeIdleAllocations_ProviderID_ByCluster verifies that when computing
// idle allocations by cluster, the idle allocation has an empty ProviderID
// because there's no single instance ID for the entire cluster.
func TestComputeIdleAllocations_ProviderID_ByCluster(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := opencost.NewClosedWindow(start, end)

	// Create AssetSet with nodes that have ProviderIDs
	assetSet := opencost.NewAssetSet(start, end)

	node1 := opencost.NewNode("node1", "cluster1", "aws:///us-east-1a/i-0abc123", start, end, window)
	node1.CPUCost = 10.0
	node1.RAMCost = 5.0
	node1.GPUCost = 0.0

	node2 := opencost.NewNode("node2", "cluster1", "aws:///us-east-1b/i-0def456", start, end, window)
	node2.CPUCost = 8.0
	node2.RAMCost = 4.0
	node2.GPUCost = 0.0

	assetSet.Insert(node1, nil)
	assetSet.Insert(node2, nil)

	// Create AllocationSet with some utilization
	allocSet := opencost.NewAllocationSet(start, end)

	alloc := &opencost.Allocation{
		Name:    "namespace1/pod1/container1",
		Start:   start,
		End:     end,
		Window:  window.Clone(),
		CPUCost: 9.0, // Uses 50% of total cluster CPU (18.0 total)
		RAMCost: 4.5, // Uses 50% of total cluster RAM (9.0 total)
		GPUCost: 0.0,
		Properties: &opencost.AllocationProperties{
			Cluster: "cluster1",
			Node:    "node1",
		},
	}

	allocSet.Insert(alloc)

	// Compute idle allocations by cluster (idleByNode=false)
	idleSet, err := computeIdleAllocations(allocSet, assetSet, false)
	if err != nil {
		t.Fatalf("Error computing idle allocations: %v", err)
	}

	// Find the cluster-level idle allocation
	var clusterIdle *opencost.Allocation
	for _, alloc := range idleSet.Allocations {
		if alloc.IsIdle() && alloc.Name == "cluster1/__idle__" {
			clusterIdle = alloc
			break
		}
	}

	if clusterIdle == nil {
		t.Fatal("Expected to find cluster-level idle allocation")
	}

	// Verify ProviderID is empty for cluster-level idle
	if clusterIdle.Properties.ProviderID != "" {
		t.Errorf("Cluster-level idle allocation should have empty ProviderID, got %q",
			clusterIdle.Properties.ProviderID)
	}

	// Verify Node is also empty for cluster-level idle
	if clusterIdle.Properties.Node != "" {
		t.Errorf("Cluster-level idle allocation should have empty Node, got %q",
			clusterIdle.Properties.Node)
	}

	// Verify costs are non-zero
	if clusterIdle.CPUCost <= 0 && clusterIdle.RAMCost <= 0 {
		t.Error("Expected non-zero idle costs for cluster-level idle")
	}
}

// TestComputeIdleAllocations_ProviderID_NoProviderID verifies that nodes
// without a ProviderID (e.g., bare-metal, local clusters) result in idle
// allocations with an empty ProviderID field.
func TestComputeIdleAllocations_ProviderID_NoProviderID(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := opencost.NewClosedWindow(start, end)

	// Create AssetSet with a node that has no ProviderID
	assetSet := opencost.NewAssetSet(start, end)

	node := opencost.NewNode("bare-metal-node-1", "cluster1", "", start, end, window)
	node.CPUCost = 10.0
	node.RAMCost = 5.0
	node.GPUCost = 0.0

	assetSet.Insert(node, nil)

	// Create AllocationSet with partial utilization
	allocSet := opencost.NewAllocationSet(start, end)

	alloc := &opencost.Allocation{
		Name:    "namespace1/pod1/container1",
		Start:   start,
		End:     end,
		Window:  window.Clone(),
		CPUCost: 5.0,
		RAMCost: 2.5,
		GPUCost: 0.0,
		Properties: &opencost.AllocationProperties{
			Cluster: "cluster1",
			Node:    "bare-metal-node-1",
		},
	}

	allocSet.Insert(alloc)

	// Compute idle allocations by node
	idleSet, err := computeIdleAllocations(allocSet, assetSet, true)
	if err != nil {
		t.Fatalf("Error computing idle allocations: %v", err)
	}

	// Find the idle allocation
	var idle *opencost.Allocation
	for _, alloc := range idleSet.Allocations {
		if alloc.IsIdle() {
			idle = alloc
			break
		}
	}

	if idle == nil {
		t.Fatal("Expected to find idle allocation")
	}

	// Verify ProviderID is empty
	if idle.Properties.ProviderID != "" {
		t.Errorf("Node without ProviderID should result in empty ProviderID in idle allocation, got %q",
			idle.Properties.ProviderID)
	}

	// Verify Node field is still populated
	if idle.Properties.Node != "bare-metal-node-1" {
		t.Errorf("Expected Node %q, got %q", "bare-metal-node-1", idle.Properties.Node)
	}
}

// TestComputeIdleAllocations_ProviderID_AzureFormat tests that Azure VM
// ProviderIDs are correctly propagated to idle allocations.
func TestComputeIdleAllocations_ProviderID_AzureFormat(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := opencost.NewClosedWindow(start, end)

	// Create AssetSet with Azure VM
	assetSet := opencost.NewAssetSet(start, end)

	azureProviderID := "azure:///subscriptions/12345678-1234-1234-1234-123456789abc/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachines/aks-nodepool1-12345678-vmss000000"
	node := opencost.NewNode("aks-nodepool1-12345678-vmss000000", "cluster1", azureProviderID, start, end, window)
	node.CPUCost = 10.0
	node.RAMCost = 5.0
	node.GPUCost = 0.0

	assetSet.Insert(node, nil)

	// Create AllocationSet
	allocSet := opencost.NewAllocationSet(start, end)

	alloc := &opencost.Allocation{
		Name:    "namespace1/pod1/container1",
		Start:   start,
		End:     end,
		Window:  window.Clone(),
		CPUCost: 5.0,
		RAMCost: 2.5,
		GPUCost: 0.0,
		Properties: &opencost.AllocationProperties{
			Cluster: "cluster1",
			Node:    "aks-nodepool1-12345678-vmss000000",
		},
	}

	allocSet.Insert(alloc)

	// Compute idle allocations
	idleSet, err := computeIdleAllocations(allocSet, assetSet, true)
	if err != nil {
		t.Fatalf("Error computing idle allocations: %v", err)
	}

	// Find the idle allocation
	var idle *opencost.Allocation
	for _, alloc := range idleSet.Allocations {
		if alloc.IsIdle() {
			idle = alloc
			break
		}
	}

	if idle == nil {
		t.Fatal("Expected to find idle allocation")
	}

	// Verify Azure ProviderID is correctly set
	if idle.Properties.ProviderID != azureProviderID {
		t.Errorf("Expected ProviderID %q, got %q", azureProviderID, idle.Properties.ProviderID)
	}
}
