package kubecost

import "testing"

// TODO niko/etl
// func TestParseProperty(t *testing.T) {}

// TODO niko/etl
// func TestProperty_String(t *testing.T) {}

func TestProperties_AggregationString(t *testing.T) {
	var props *Properties
	var aggStrs []string

	// nil Properties should produce and empty slice
	aggStrs = props.AggregationStrings()
	if aggStrs == nil || len(aggStrs) > 0 {
		t.Fatalf("expected empty slice; got %v", aggStrs)
	}

	// empty Properties should product an empty slice
	props = &Properties{}
	aggStrs = props.AggregationStrings()
	if aggStrs == nil || len(aggStrs) > 0 {
		t.Fatalf("expected empty slice; got %v", aggStrs)
	}

	// Properties with single, simple property set
	props = &Properties{}
	props.SetNamespace("")
	aggStrs = props.AggregationStrings()
	if len(aggStrs) != 1 || aggStrs[0] != "namespace" {
		t.Fatalf("expected [\"namespace\"]; got %v", aggStrs)
	}

	// Properties with mutiple properties, including labels
	// Note: order matters!
	props = &Properties{}
	props.SetNamespace("")
	props.SetLabels(map[string]string{
		"env": "",
		"app": "",
	})
	props.SetCluster("")
	aggStrs = props.AggregationStrings()
	if len(aggStrs) != 4 {
		t.Fatalf("expected length %d; got lenfth %d", 4, len(aggStrs))
	}
	if aggStrs[0] != "cluster" {
		t.Fatalf("expected aggStrs[0] == \"%s\"; got \"%s\"", "cluster", aggStrs[0])
	}
	if aggStrs[1] != "namespace" {
		t.Fatalf("expected aggStrs[1] == \"%s\"; got \"%s\"", "namespace", aggStrs[1])
	}
	if aggStrs[2] != "label:app" {
		t.Fatalf("expected aggStrs[2] == \"%s\"; got \"%s\"", "label:app", aggStrs[2])
	}
	if aggStrs[3] != "label:env" {
		t.Fatalf("expected aggStrs[3] == \"%s\"; got \"%s\"", "label:env", aggStrs[3])
	}
}

// TODO niko/etl
// func TestProperties_Clone(t *testing.T) {}

// TODO niko/etl
// func TestProperties_Intersection(t *testing.T) {}

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
