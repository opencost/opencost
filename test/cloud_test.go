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
	nameWant := "name"
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


