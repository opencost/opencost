package test

import (
	"testing"
	"github.com/kubecost/cost-model/pkg/cloud"
	v1 "k8s.io/api/core/v1"
)

const(
	providerIDMap = "spec.providerID"
	nameMap = "metadata.name"
	labelMapFoo = "metadata.labels.foo"
)
func TestTransformedValueFromMapField(t *testing.T) {
	providerIDWant := "i-05445591e0d182d42"
	n := &v1.Node{}
	n.Spec.ProviderID = "aws:///us-east-1a/i-05445591e0d182d42"
	got := cloud.NodeValueFromMapField(providerIDMap, n)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}

	providerIDWant2 := "/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0"
	n2 := &v1.Node{}
	n2.Spec.ProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/MC_test_test_eastus/providers/Microsoft.Compute/virtualMachines/aks-agentpool-20139558-0"
	got2 := cloud.NodeValueFromMapField(providerIDMap, n2)
	if got2 != providerIDWant2 {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant2, got2)
	}

	providerIDWant3 := "/subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_testspot_testspot_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-19213364-vmss/virtualMachines/0"
	n3 := &v1.Node{}
	n3.Spec.ProviderID = "azure:///subscriptions/0bd50fdf-c923-4e1e-850c-196dd3dcc5d3/resourceGroups/mc_testspot_testspot_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-19213364-vmss/virtualMachines/0"
	got3 := cloud.NodeValueFromMapField(providerIDMap, n3)
	if got3 != providerIDWant3 {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant3, got3)
	}
}


func TestNodeValueFromMapField(t *testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"

	
	n := &v1.Node{}
	n.Spec.ProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant

	got := cloud.NodeValueFromMapField(providerIDMap, n)
	if got != providerIDWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", providerIDMap, providerIDWant, got)
	}
	
	got = cloud.NodeValueFromMapField(nameMap, n)
	if got != nameWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", nameMap, nameWant, got)
	}

	got = cloud.NodeValueFromMapField(labelMapFoo, n)
	if got != labelFooWant {
		t.Errorf("Assert on '%s' want '%s' got '%s'", labelMapFoo, labelFooWant, got)
	}

}

func TestNodePriceFromCSV(t * testing.T) {
	providerIDWant := "providerid"
	nameWant := "gke-standard-cluster-1-pool-1-91dc432d-cg69"
	labelFooWant := "labelfoo"

	n := &v1.Node{}
	n.Spec.ProviderID = providerIDWant
	n.Name = nameWant
	n.Labels = make(map[string]string)
	n.Labels["foo"] = labelFooWant

	wantPrice := "0.1337"

	c := &cloud.CSVProvider{
		CSVLocation: "../configs/pricing_schema.csv",
		CustomProvider: &cloud.CustomProvider{
			Config:    cloud.NewProviderConfig("../configs/default.json"),
		},
	}
	c.DownloadPricingData()
	k := c.GetKey(n.Labels, n)
	resN, err := c.NodePricing(k)
	if err != nil {
		t.Errorf("Error in NodePricing: %s", err.Error())
	} else {
		gotPrice := resN.Cost
		if gotPrice != wantPrice {
			t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
		}
	}

	unknownN := &v1.Node{}
	unknownN.Spec.ProviderID = providerIDWant
	unknownN.Name = "unknownname"
	unknownN.Labels = make(map[string]string)
	unknownN.Labels["foo"] = labelFooWant
	k2 := c.GetKey(n.Labels, unknownN)
	resN2, _ := c.NodePricing(k2)
	if resN2 != nil {
		t.Errorf("CSV provider should return nil on missing node")
	}
	
	c2 := &cloud.CSVProvider{
		CSVLocation: "../configs/fake.csv",
		CustomProvider: &cloud.CustomProvider{
			Config:    cloud.NewProviderConfig("../configs/default.json"),
		},
	}
	k3 := c.GetKey(n.Labels, n)
	resN3, _ := c2.NodePricing(k3)
	if resN3 != nil {
		t.Errorf("CSV provider should return nil on missing csv")
	}



}
