package opencost

import (
	"testing"
	"time"
)

// TestComputeAssetTotals_ProviderID_ByNode verifies that when computing
// AssetTotals by node (byAsset=true), the ProviderID field is correctly
// populated from the node's ProviderID.
func TestComputeAssetTotals_ProviderID_ByNode(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := NewClosedWindow(start, end)

	// Create AssetSet with nodes that have ProviderIDs
	as := NewAssetSet(start, end)

	// Node 1: AWS EC2 instance
	node1 := NewNode("node1", "cluster1", "aws:///us-east-1a/i-0abc123def456789", start, end, window)
	node1.CPUCost = 10.0
	node1.RAMCost = 5.0

	// Node 2: GCP instance
	node2 := NewNode("node2", "cluster1", "gce://my-project/us-central1-a/instance-456", start, end, window)
	node2.CPUCost = 8.0
	node2.RAMCost = 4.0

	// Node 3: Azure VM
	node3 := NewNode("node3", "cluster1", "azure:///subscriptions/sub-id/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-789", start, end, window)
	node3.CPUCost = 12.0
	node3.RAMCost = 6.0

	as.Insert(node1, nil)
	as.Insert(node2, nil)
	as.Insert(node3, nil)

	// Compute AssetTotals by node (byAsset=true)
	totals := ComputeAssetTotals(as, true)

	// Verify that each node's totals includes the correct ProviderID
	expectedProviderIDs := map[string]string{
		"cluster1/node1": "aws:///us-east-1a/i-0abc123def456789",
		"cluster1/node2": "gce://my-project/us-central1-a/instance-456",
		"cluster1/node3": "azure:///subscriptions/sub-id/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm-789",
	}

	for key, expectedProviderID := range expectedProviderIDs {
		total, ok := totals[key]
		if !ok {
			t.Errorf("Expected to find totals for key %s", key)
			continue
		}

		if total.ProviderID != expectedProviderID {
			t.Errorf("For key %s: expected ProviderID %q, got %q", key, expectedProviderID, total.ProviderID)
		}

		// Verify Node field is also set correctly
		expectedNodeName := key[len("cluster1/"):]
		if total.Node != expectedNodeName {
			t.Errorf("For key %s: expected Node %q, got %q", key, expectedNodeName, total.Node)
		}
	}
}

// TestComputeAssetTotals_ProviderID_ByCluster verifies that when computing
// AssetTotals by cluster (byAsset=false), the ProviderID field is empty
// because there's no single instance ID that represents all nodes.
func TestComputeAssetTotals_ProviderID_ByCluster(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := NewClosedWindow(start, end)

	// Create AssetSet with nodes that have ProviderIDs
	as := NewAssetSet(start, end)

	node1 := NewNode("node1", "cluster1", "aws:///us-east-1a/i-0abc123def456789", start, end, window)
	node1.CPUCost = 10.0
	node1.RAMCost = 5.0

	node2 := NewNode("node2", "cluster1", "gce://my-project/us-central1-a/instance-456", start, end, window)
	node2.CPUCost = 8.0
	node2.RAMCost = 4.0

	as.Insert(node1, nil)
	as.Insert(node2, nil)

	// Compute AssetTotals by cluster (byAsset=false)
	totals := ComputeAssetTotals(as, false)

	// Verify that cluster-level totals have an empty ProviderID
	total, ok := totals["cluster1"]
	if !ok {
		t.Fatal("Expected to find totals for cluster1")
	}

	if total.ProviderID != "" {
		t.Errorf("Expected empty ProviderID for cluster-level totals, got %q", total.ProviderID)
	}

	// Verify Node field is also empty at cluster level
	if total.Node != "" {
		t.Errorf("Expected empty Node for cluster-level totals, got %q", total.Node)
	}

	// Verify costs are aggregated correctly
	expectedCPUCost := 18.0 // 10.0 + 8.0
	expectedRAMCost := 9.0  // 5.0 + 4.0

	if total.CPUCost != expectedCPUCost {
		t.Errorf("Expected CPUCost %f, got %f", expectedCPUCost, total.CPUCost)
	}

	if total.RAMCost != expectedRAMCost {
		t.Errorf("Expected RAMCost %f, got %f", expectedRAMCost, total.RAMCost)
	}
}

// TestComputeAssetTotals_ProviderID_EmptyProviderID verifies that nodes
// without a ProviderID still work correctly and result in an empty string
// in the AssetTotals.
func TestComputeAssetTotals_ProviderID_EmptyProviderID(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := NewClosedWindow(start, end)

	// Create AssetSet with a node that has no ProviderID
	as := NewAssetSet(start, end)

	// Node without ProviderID (e.g., local/bare-metal cluster)
	node := NewNode("node1", "cluster1", "", start, end, window)
	node.CPUCost = 10.0
	node.RAMCost = 5.0

	as.Insert(node, nil)

	// Compute AssetTotals by node (byAsset=true)
	totals := ComputeAssetTotals(as, true)

	// Verify that the node's totals has an empty ProviderID
	total, ok := totals["cluster1/node1"]
	if !ok {
		t.Fatal("Expected to find totals for cluster1/node1")
	}

	if total.ProviderID != "" {
		t.Errorf("Expected empty ProviderID for node without ProviderID, got %q", total.ProviderID)
	}

	// Verify other fields are still populated correctly
	if total.Node != "node1" {
		t.Errorf("Expected Node %q, got %q", "node1", total.Node)
	}

	if total.CPUCost != 10.0 {
		t.Errorf("Expected CPUCost %f, got %f", 10.0, total.CPUCost)
	}
}

// TestComputeAssetTotals_ProviderID_MultipleNodesAggregation tests that
// when multiple nodes with different ProviderIDs are aggregated at the
// cluster level, the ProviderID remains empty.
func TestComputeAssetTotals_ProviderID_MultipleNodesAggregation(t *testing.T) {
	// Create test window
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	window := NewClosedWindow(start, end)

	// Create AssetSet with multiple nodes across different clusters
	as := NewAssetSet(start, end)

	// Cluster 1 nodes
	node1 := NewNode("node1", "cluster1", "aws:///us-east-1a/i-0abc123", start, end, window)
	node1.CPUCost = 10.0
	node1.RAMCost = 5.0

	node2 := NewNode("node2", "cluster1", "aws:///us-east-1b/i-0def456", start, end, window)
	node2.CPUCost = 8.0
	node2.RAMCost = 4.0

	// Cluster 2 node
	node3 := NewNode("node3", "cluster2", "gce://project/zone/instance", start, end, window)
	node3.CPUCost = 12.0
	node3.RAMCost = 6.0

	as.Insert(node1, nil)
	as.Insert(node2, nil)
	as.Insert(node3, nil)

	// Compute AssetTotals by node (byAsset=true)
	nodeTotal := ComputeAssetTotals(as, true)

	// Verify each node has its own ProviderID
	if nodeTotal["cluster1/node1"].ProviderID != "aws:///us-east-1a/i-0abc123" {
		t.Errorf("Node1 ProviderID mismatch")
	}
	if nodeTotal["cluster1/node2"].ProviderID != "aws:///us-east-1b/i-0def456" {
		t.Errorf("Node2 ProviderID mismatch")
	}
	if nodeTotal["cluster2/node3"].ProviderID != "gce://project/zone/instance" {
		t.Errorf("Node3 ProviderID mismatch")
	}

	// Compute AssetTotals by cluster (byAsset=false)
	clusterTotals := ComputeAssetTotals(as, false)

	// Verify cluster-level totals have empty ProviderID
	for clusterKey, total := range clusterTotals {
		if total.ProviderID != "" {
			t.Errorf("Cluster %s should have empty ProviderID, got %q", clusterKey, total.ProviderID)
		}
	}

	// Verify cluster1 aggregates both nodes correctly
	cluster1Total := clusterTotals["cluster1"]
	expectedCPU := 18.0 // 10.0 + 8.0
	expectedRAM := 9.0  // 5.0 + 4.0

	if cluster1Total.CPUCost != expectedCPU {
		t.Errorf("Cluster1 CPUCost: expected %f, got %f", expectedCPU, cluster1Total.CPUCost)
	}
	if cluster1Total.RAMCost != expectedRAM {
		t.Errorf("Cluster1 RAMCost: expected %f, got %f", expectedRAM, cluster1Total.RAMCost)
	}
}
