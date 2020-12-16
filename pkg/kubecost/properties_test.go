package kubecost

import (
	"testing"
)

// TODO niko/etl
// func TestParseProperty(t *testing.T) {}

// TODO niko/etl
// func TestProperty_String(t *testing.T) {}

// TODO niko/etl
// func TestProperties_Clone(t *testing.T) {}

// TODO niko/etl
// func TestProperties_Intersection(t *testing.T) {}

func TestProperties_Matches(t *testing.T) {
	// nil Properties should match empty Properties
	var p *Properties
	propsEmpty := Properties{}

	if !p.Matches(propsEmpty) {
		t.Fatalf("Properties.Matches: expect nil to match empty")
	}

	// Empty Properties should match empty Properties
	p = &Properties{}
	if !p.Matches(propsEmpty) {
		t.Fatalf("Properties.Matches: expect nil to match empty")
	}

	p.SetCluster("cluster-one")
	p.SetNamespace("kubecost")
	p.SetController("kubecost-deployment")
	p.SetControllerKind("deployment")
	p.SetPod("kubecost-deployment-abc123")
	p.SetContainer("kubecost-cost-model")
	p.SetServices([]string{"kubecost-frontend"})
	p.SetLabels(map[string]string{
		"app":  "kubecost",
		"tier": "frontend",
	})

	// Non-empty Properties should match empty Properties, but not vice-a-versa
	if !p.Matches(propsEmpty) {
		t.Fatalf("Properties.Matches: expect nil to match empty")
	}
	if propsEmpty.Matches(*p) {
		t.Fatalf("Properties.Matches: expect empty to not match non-empty")
	}

	// Non-empty Properties should match itself
	if !p.Matches(*p) {
		t.Fatalf("Properties.Matches: expect non-empty to match itself")
	}

	// Match on all
	if !p.Matches(Properties{
		ClusterProp:        "cluster-one",
		NamespaceProp:      "kubecost",
		ControllerProp:     "kubecost-deployment",
		ControllerKindProp: "deployment",
		PodProp:            "kubecost-deployment-abc123",
		ContainerProp:      "kubecost-cost-model",
		ServiceProp:        []string{"kubecost-frontend"},
		LabelProp: map[string]string{
			"app":  "kubecost",
			"tier": "frontend",
		},
	}) {
		t.Fatalf("Properties.Matches: expect match on all")
	}

	// Match on cluster
	if !p.Matches(Properties{
		ClusterProp: "cluster-one",
	}) {
		t.Fatalf("Properties.Matches: expect match on cluster")
	}

	// No match on cluster
	if p.Matches(Properties{
		ClusterProp: "miss",
	}) {
		t.Fatalf("Properties.Matches: expect no match on cluster")
	}

	// Match on namespace
	if !p.Matches(Properties{
		NamespaceProp: "kubecost",
	}) {
		t.Fatalf("Properties.Matches: expect match on namespace")
	}

	// No match on namespace
	if p.Matches(Properties{
		NamespaceProp: "miss",
	}) {
		t.Fatalf("Properties.Matches: expect no match on namespace")
	}

	// Match on controller
	if !p.Matches(Properties{
		ControllerProp: "kubecost-deployment",
	}) {
		t.Fatalf("Properties.Matches: expect match on controller")
	}

	// No match on controller
	if p.Matches(Properties{
		ControllerProp: "miss",
	}) {
		t.Fatalf("Properties.Matches: expect no match on controller")
	}

	// Match on controller kind
	if !p.Matches(Properties{
		ControllerKindProp: "deployment",
	}) {
		t.Fatalf("Properties.Matches: expect match on controller kind")
	}

	// No match on controller kind
	if p.Matches(Properties{
		ControllerKindProp: "miss",
	}) {
		t.Fatalf("Properties.Matches: expect no match on controller kind")
	}

	// Match on pod
	if !p.Matches(Properties{
		PodProp: "kubecost-deployment-abc123",
	}) {
		t.Fatalf("Properties.Matches: expect match on pod")
	}

	// No match on pod
	if p.Matches(Properties{
		PodProp: "miss",
	}) {
		t.Fatalf("Properties.Matches: expect no match on pod")
	}

	// Match on container
	if !p.Matches(Properties{
		ContainerProp: "kubecost-cost-model",
	}) {
		t.Fatalf("Properties.Matches: expect match on container")
	}

	// No match on container
	if p.Matches(Properties{
		ContainerProp: "miss",
	}) {
		t.Fatalf("Properties.Matches: expect no match on container")
	}

	// Match on single service
	if !p.Matches(Properties{
		ServiceProp: []string{"kubecost-frontend"},
	}) {
		t.Fatalf("Properties.Matches: expect match on service")
	}

	// No match on one missing service
	if p.Matches(Properties{
		ServiceProp: []string{"missing-service", "kubecost-frontend"},
	}) {
		t.Fatalf("Properties.Matches: expect no match on 1 of 2 services")
	}

	// Match on single label
	if !p.Matches(Properties{
		LabelProp: map[string]string{
			"app": "kubecost",
		},
	}) {
		t.Fatalf("Properties.Matches: expect match on label")
	}

	// No match on one missing label
	if !p.Matches(Properties{
		LabelProp: map[string]string{
			"app":   "kubecost",
			"tier":  "frontend",
			"label": "missing",
		},
	}) {
		t.Fatalf("Properties.Matches: expect no match on 2 of 3 labels")
	}
}

// TODO niko/etl
// func TestProperties_GetCluster(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetCluster(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetContainer(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetContainer(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetController(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetController(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetControllerKind(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetControllerKind(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetLabels(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetLabels(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetNamespace(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetNamespace(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetPod(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetPod(t *testing.T) {}

// TODO niko/etl
// func TestProperties_GetServices(t *testing.T) {}

// TODO niko/etl
// func TestProperties_SetServices(t *testing.T) {}
