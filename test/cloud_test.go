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
	resN, _ := c.NodePricing(k)
	gotPrice := resN.Cost

	if gotPrice != wantPrice {
		t.Errorf("Wanted price '%s' got price '%s'", wantPrice, gotPrice)
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
