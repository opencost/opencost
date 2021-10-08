package kubecost

import (
	"encoding/json"
	"testing"
	"time"
)

var s = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
var e = start1.Add(day)
var unmarshalWindow = NewWindow(&s, &e)

func TestAny_Unmarshal(t *testing.T) {

	any1 := NewAsset(*unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	any1.SetProperties(&AssetProperties{
		Name:       "any1",
		Cluster:    "cluster1",
		ProviderID: "any1",
	})
	any1.Cost = 9.0
	any1.SetAdjustment(1.0)

	bytes, _ := json.Marshal(any1)

	var testany Any
	any2 := &testany

	err := json.Unmarshal(bytes, any2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Any Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Any equal those in Any from unmarshal
	if !any1.properties.Equal(any2.properties) {
		t.Fatalf("Any Unmarshal: properties mutated in unmarshal")
	}
	if !any1.labels.Equal(any2.labels) {
		t.Fatalf("Any Unmarshal: labels mutated in unmarshal")
	}
	if !any1.window.Equal(any2.window) {
		t.Fatalf("Any Unmarshal: window mutated in unmarshal")
	}
	if !any1.start.Equal(any2.start) {
		t.Fatalf("Any Unmarshal: start mutated in unmarshal")
	}
	if !any1.end.Equal(any2.end) {
		t.Fatalf("Any Unmarshal: end mutated in unmarshal")
	}
	if any1.adjustment != any2.adjustment {
		t.Fatalf("Any Unmarshal: adjustment mutated in unmarshal")
	}
	if any1.Cost != any2.Cost {
		t.Fatalf("Any Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !any1.Equal(any2) {
		t.Fatalf("Any Unmarshal: Any mutated in unmarshal")
	}

}

func TestCloud_Unmarshal(t *testing.T) {

	cloud1 := NewCloud("Compute", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	cloud1.SetLabels(map[string]string{
		"namespace": "namespace1",
		"env":       "env1",
		"product":   "product1",
	})
	cloud1.Cost = 10.00
	cloud1.Credit = -1.0

	bytes, _ := json.Marshal(cloud1)

	var testcloud Cloud
	cloud2 := &testcloud

	err := json.Unmarshal(bytes, cloud2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Cloud Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Cloud equal those in Cloud from unmarshal
	if !cloud1.properties.Equal(cloud2.properties) {
		t.Fatalf("Cloud Unmarshal: properties mutated in unmarshal")
	}
	if !cloud1.labels.Equal(cloud2.labels) {
		t.Fatalf("Cloud Unmarshal: labels mutated in unmarshal")
	}
	if !cloud1.window.Equal(cloud2.window) {
		t.Fatalf("Cloud Unmarshal: window mutated in unmarshal")
	}
	if !cloud1.start.Equal(cloud2.start) {
		t.Fatalf("Cloud Unmarshal: start mutated in unmarshal")
	}
	if !cloud1.end.Equal(cloud2.end) {
		t.Fatalf("Cloud Unmarshal: end mutated in unmarshal")
	}
	if cloud1.adjustment != cloud2.adjustment {
		t.Fatalf("Cloud Unmarshal: adjustment mutated in unmarshal")
	}
	if cloud1.Cost != cloud2.Cost {
		t.Fatalf("Cloud Unmarshal: cost mutated in unmarshal")
	}
	if cloud1.Credit != cloud2.Credit {
		t.Fatalf("Cloud Unmarshal: credit mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !cloud1.Equal(cloud2) {
		t.Fatalf("Cloud Unmarshal: Cloud mutated in unmarshal")
	}

}

func TestClusterManagement_Unmarshal(t *testing.T) {

	cm1 := NewClusterManagement("gcp", "cluster1", unmarshalWindow)
	cm1.Cost = 9.0

	bytes, _ := json.Marshal(cm1)

	var testcm ClusterManagement
	cm2 := &testcm

	err := json.Unmarshal(bytes, cm2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("ClusterManagement Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial ClusterManagement equal those in ClusterManagement from unmarshal
	if !cm1.properties.Equal(cm2.properties) {
		t.Fatalf("ClusterManagement Unmarshal: properties mutated in unmarshal")
	}
	if !cm1.labels.Equal(cm2.labels) {
		t.Fatalf("ClusterManagement Unmarshal: labels mutated in unmarshal")
	}
	if !cm1.window.Equal(cm2.window) {
		t.Fatalf("ClusterManagement Unmarshal: window mutated in unmarshal")
	}
	if cm1.Cost != cm2.Cost {
		t.Fatalf("ClusterManagement Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !cm1.Equal(cm2) {
		t.Fatalf("ClusterManagement Unmarshal: ClusterManagement mutated in unmarshal")
	}

}

func TestDisk_Unmarshal(t *testing.T) {

	hours := unmarshalWindow.Duration().Hours()

	disk1 := NewDisk("disk1", "cluster1", "disk1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	disk1.ByteHours = 60.0 * gb * hours
	disk1.Cost = 4.0
	disk1.Local = 1.0
	disk1.SetAdjustment(1.0)
	disk1.Breakdown = &Breakdown{
		Idle:   0.1,
		System: 0.2,
		User:   0.3,
		Other:  0.4,
	}

	bytes, _ := json.Marshal(disk1)

	var testdisk Disk
	disk2 := &testdisk

	err := json.Unmarshal(bytes, disk2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Disk Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Disk equal those in Disk from unmarshal
	if !disk1.properties.Equal(disk2.properties) {
		t.Fatalf("Disk Unmarshal: properties mutated in unmarshal")
	}
	if !disk1.labels.Equal(disk2.labels) {
		t.Fatalf("Disk Unmarshal: labels mutated in unmarshal")
	}
	if !disk1.window.Equal(disk2.window) {
		t.Fatalf("Disk Unmarshal: window mutated in unmarshal")
	}
	if !disk1.Breakdown.Equal(disk2.Breakdown) {
		t.Fatalf("Disk Unmarshal: Breakdown mutated in unmarshal")
	}
	if !disk1.start.Equal(disk2.start) {
		t.Fatalf("Disk Unmarshal: start mutated in unmarshal")
	}
	if !disk1.end.Equal(disk2.end) {
		t.Fatalf("Disk Unmarshal: end mutated in unmarshal")
	}
	if disk1.adjustment != disk2.adjustment {
		t.Fatalf("Disk Unmarshal: adjustment mutated in unmarshal")
	}
	if disk1.ByteHours != disk2.ByteHours {
		t.Fatalf("Disk Unmarshal: ByteHours mutated in unmarshal")
	}
	if disk1.Cost != disk2.Cost {
		t.Fatalf("Disk Unmarshal: cost mutated in unmarshal")
	}

	// Local from Disk is not marhsaled, and cannot be calculated from marshaled values.
	// Currently, it is just ignored and not set in the resulting unmarshal to Disk. Thus,
	// it is also ignored in this test; be aware that this means a resulting Disk from an
	// unmarshal is therefore NOT equal to the originally marshaled Disk.

}

func TestNetwork_Unmarshal(t *testing.T) {

	network1 := NewNetwork("network1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	network1.Cost = 4.0
	network1.SetAdjustment(1.0)

	bytes, _ := json.Marshal(network1)

	var testnw Network
	network2 := &testnw

	err := json.Unmarshal(bytes, network2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("Network Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial Network equal those in Network from unmarshal
	if !network1.properties.Equal(network2.properties) {
		t.Fatalf("Network Unmarshal: properties mutated in unmarshal")
	}
	if !network1.labels.Equal(network2.labels) {
		t.Fatalf("Network Unmarshal: labels mutated in unmarshal")
	}
	if !network1.window.Equal(network2.window) {
		t.Fatalf("Network Unmarshal: window mutated in unmarshal")
	}
	if !network1.start.Equal(network2.start) {
		t.Fatalf("Network Unmarshal: start mutated in unmarshal")
	}
	if !network1.end.Equal(network2.end) {
		t.Fatalf("Network Unmarshal: end mutated in unmarshal")
	}
	if network1.adjustment != network2.adjustment {
		t.Fatalf("Network Unmarshal: adjustment mutated in unmarshal")
	}
	if network1.Cost != network2.Cost {
		t.Fatalf("Network Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !network1.Equal(network2) {
		t.Fatalf("Network Unmarshal: Network mutated in unmarshal")
	}

}

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

	var testnode Node
	node2 := &testnode

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

func TestLoadBalancer_Unmarshal(t *testing.T) {

	lb1 := NewLoadBalancer("loadbalancer1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	lb1.Cost = 12.0
	lb1.SetAdjustment(4.0)

	bytes, _ := json.Marshal(lb1)

	var testlb LoadBalancer
	lb2 := &testlb

	err := json.Unmarshal(bytes, lb2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("LoadBalancer Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial LoadBalancer equal those in LoadBalancer from unmarshal
	if !lb1.properties.Equal(lb2.properties) {
		t.Fatalf("LoadBalancer Unmarshal: properties mutated in unmarshal")
	}
	if !lb1.labels.Equal(lb2.labels) {
		t.Fatalf("LoadBalancer Unmarshal: labels mutated in unmarshal")
	}
	if !lb1.window.Equal(lb2.window) {
		t.Fatalf("LoadBalancer Unmarshal: window mutated in unmarshal")
	}
	if !lb1.start.Equal(lb2.start) {
		t.Fatalf("LoadBalancer Unmarshal: start mutated in unmarshal")
	}
	if !lb1.end.Equal(lb2.end) {
		t.Fatalf("LoadBalancer Unmarshal: end mutated in unmarshal")
	}
	if lb1.adjustment != lb2.adjustment {
		t.Fatalf("LoadBalancer Unmarshal: adjustment mutated in unmarshal")
	}
	if lb1.Cost != lb2.Cost {
		t.Fatalf("LoadBalancer Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !lb1.Equal(lb2) {
		t.Fatalf("LoadBalancer Unmarshal: LoadBalancer mutated in unmarshal")
	}

}

func TestSharedAsset_Unmarshal(t *testing.T) {

	sa1 := NewSharedAsset("sharedasset1", unmarshalWindow)
	sa1.Cost = 7.0

	bytes, _ := json.Marshal(sa1)

	var testsa SharedAsset
	sa2 := &testsa

	err := json.Unmarshal(bytes, sa2)

	// Check if unmarshal was successful
	if err != nil {
		t.Fatalf("SharedAsset Unmarshal: unexpected error: %s", err)
	}

	// Check if all fields in initial SharedAsset equal those in SharedAsset from unmarshal
	if !sa1.properties.Equal(sa2.properties) {
		t.Fatalf("SharedAsset Unmarshal: properties mutated in unmarshal")
	}
	if !sa1.labels.Equal(sa2.labels) {
		t.Fatalf("SharedAsset Unmarshal: labels mutated in unmarshal")
	}
	if !sa1.window.Equal(sa2.window) {
		t.Fatalf("SharedAsset Unmarshal: window mutated in unmarshal")
	}
	if sa1.Cost != sa2.Cost {
		t.Fatalf("SharedAsset Unmarshal: cost mutated in unmarshal")
	}

	// As a final check, make sure the above checks out
	if !sa1.Equal(sa2) {
		t.Fatalf("SharedAsset Unmarshal: SharedAsset mutated in unmarshal")
	}

}

func TestAssetset_Unmarshal(t *testing.T) {

	var s time.Time
	var e time.Time
	unmarshalWindow := NewWindow(&s, &e)

	any := NewAsset(*unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	cloud := NewCloud("Compute", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	cm := NewClusterManagement("gcp", "cluster1", unmarshalWindow)
	disk := NewDisk("disk1", "cluster1", "disk1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	network := NewNetwork("network1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	node := NewNode("node1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	lb := NewLoadBalancer("loadbalancer1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)
	sa := NewSharedAsset("sharedasset1", unmarshalWindow)

	assetList := []Asset{any, cloud, cm, disk, network, node, lb, sa}

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

		if unmarshaledAsset, exists := assetUnmarshalResponse.Assets[key]; exists {

			// As Disk is not marshaled with all fields, the resultant Disk will be unequal. Test all fields we have instead.
			if unmarshaledAsset.Type().String() == "Disk" {

				udiskEq := func(d1 Asset, d2 Asset) bool {

					asset, _ := asset.(*Disk)
					unmarshaledAsset, _ := unmarshaledAsset.(*Disk)

					if !asset.Labels().Equal(unmarshaledAsset.Labels()) {
						return false
					}
					if !asset.Properties().Equal(unmarshaledAsset.Properties()) {
						return false
					}

					if !asset.Start().Equal(unmarshaledAsset.Start()) {
						return false
					}
					if !asset.End().Equal(unmarshaledAsset.End()) {
						return false
					}
					if !asset.window.Equal(unmarshaledAsset.window) {
						return false
					}
					if asset.adjustment != unmarshaledAsset.adjustment {
						return false
					}
					if asset.Cost != unmarshaledAsset.Cost {
						return false
					}
					if asset.ByteHours != unmarshaledAsset.ByteHours {
						return false
					}
					if !asset.Breakdown.Equal(unmarshaledAsset.Breakdown) {
						return false
					}

					return true
				}

				if res := udiskEq(asset, unmarshaledAsset); !res {
					t.Fatalf("AssetSet Unmarshal: asset at key '%s' from unmarshaled AssetSetResponse does not match corresponding asset from AssetSet", key)
				}

			} else {

				if !asset.Equal(unmarshaledAsset) {
					t.Fatalf("AssetSet Unmarshal: asset at key '%s' from unmarshaled AssetSetResponse does not match corresponding asset from AssetSet", key)
				}

			}

		} else {
			t.Fatalf("AssetSet Unmarshal: key '%s' from marshaled AssetSet does not exist in AssetSetResponse", key)
		}

	}

}
