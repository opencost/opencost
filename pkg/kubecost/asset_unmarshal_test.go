package kubecost

import (
	"encoding/json"
	"testing"
	"time"
)

var s = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
var e = start1.Add(day)
var unmarshalWindow = NewWindow(&s, &e)

func TestNode_Unmarshal(t *testing.T) {

	hours := unmarshalWindow.Duration().Hours()

	node1 := NewNode("node1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
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

	bytes, _ := json.Marshal(node1)

	var thisnode Node
	node2 := &thisnode

	err := json.Unmarshal(bytes, node2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Node Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Node equal those in Node from unmarshal
	if !node1.properties.Equal(node2.properties) {
		t.Fatalf("Node Unmarshal: properties mutated in unmarshal")
	}
	if !node1.labels.Equal(node2.labels) {
		t.Fatalf("Node Unmarshal: labels mutated in unmarshal")
	}
	if !node1.window.Equal(node2.window) {
		t.Fatalf("Node Unmarshal: window mutated in unmarshal")
	}
	if !node1.CPUBreakdown.Equal(node2.CPUBreakdown) {
		t.Fatalf("Node Unmarshal: CPUBreakdown mutated in unmarshal")
	}
	if !node1.RAMBreakdown.Equal(node2.RAMBreakdown) {
		t.Fatalf("Node Unmarshal: RAMBreakdown mutated in unmarshal")
	}
	if !node1.start.Equal(node2.start) {
		t.Fatalf("Node Unmarshal: start mutated in unmarshal")
	}
	if !node1.end.Equal(node2.end) {
		t.Fatalf("Node Unmarshal: end mutated in unmarshal")
	}
	if node1.adjustment != node2.adjustment {
		t.Fatalf("Node Unmarshal: adjustment mutated in unmarshal")
	}
	if node1.NodeType != node2.NodeType {
		t.Fatalf("Node Unmarshal: NodeType mutated in unmarshal")
	}
	if node1.CPUCoreHours != node2.CPUCoreHours {
		t.Fatalf("Node Unmarshal: CPUCoreHours mutated in unmarshal")
	}
	if node1.RAMByteHours != node2.RAMByteHours {
		t.Fatalf("Node Unmarshal: RAMByteHours mutated in unmarshal")
	}
	if node1.GPUHours != node2.GPUHours {
		t.Fatalf("Node Unmarshal: GPUHours mutated in unmarshal")
	}
	if node1.CPUCost != node2.CPUCost {
		t.Fatalf("Node Unmarshal: CPUCost mutated in unmarshal")
	}
	if node1.GPUCost != node2.GPUCost {
		t.Fatalf("Node Unmarshal: GPUCost mutated in unmarshal")
	}
	if node1.GPUCount != node2.GPUCount {
		t.Fatalf("Node Unmarshal: GPUCount mutated in unmarshal")
	}
	if node1.RAMCost != node2.RAMCost {
		t.Fatalf("Node Unmarshal: RAMCost mutated in unmarshal")
	}
	if node1.Discount != node2.Discount {
		t.Fatalf("Node Unmarshal: Discount mutated in unmarshal")
	}
	if node1.Preemptible != node2.Preemptible {
		t.Fatalf("Node Unmarshal: Preemptible mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !node1.Equal(node2) {
		t.Fatalf("Node Unmarshal: Node mutated in unmarshal")
	}

}

func TestAssetset_Unmarshal(t *testing.T) {

	var s time.Time
	var e time.Time
	unmarshalWindow := NewWindow(&s, &e)

	node1 := NewNode("node1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	node2 := NewNode("node2", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	node3 := NewNode("node3", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)

	assetList := []Asset{node1, node2, node3}

	assetset := NewAssetSet(s, e, assetList...)
	bytes, _ := json.Marshal(assetset)

	var assetSetResponse AssetSetResponse
	assetUnmarshalResponse := &assetSetResponse

	err := json.Unmarshal(bytes, assetUnmarshalResponse)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("AssetSet Unmarshal: unexpected error: %s", err)
	}

	// For each asset in unmarshaled AssetSetResponse, check if it is equal to the corresponding AssetSet asset
	for key, asset := range assetset.assets {

		if unmarshaledAsset, exists := assetUnmarshalResponse.assets[key]; exists {

			if !asset.Equal(unmarshaledAsset) {
				t.Fatalf("AssetSet Unmarshal: asset at key '%s' from unmarshaled AssetSetResponse does not match corresponding asset from AssetSet", key)
			}

		} else {
			t.Fatalf("AssetSet Unmarshal: key '%s' from marshaled AssetSet does not exist in AssetSetResponse", key)
		}

	}

}
