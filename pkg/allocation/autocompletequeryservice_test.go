package allocation

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestQueryAllocationAutocompleteFromSetRange(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	as := opencost.NewAllocationSet(start, start.Add(24*time.Hour))
	as.Set(opencost.NewMockUnitAllocation("a1", start, 24*time.Hour, &opencost.AllocationProperties{
		Cluster:         "cluster-a",
		Namespace:       "ns-a",
		Pod:             "pod-a",
		Container:       "container-a",
		ControllerKind:  "deployment",
		Controller:      "deploy-a",
		Node:            "node-a",
		Labels:          map[string]string{"Team": "platform", "app": "api"},
		NamespaceLabels: map[string]string{"owner": "sre"},
	}))
	as.Set(opencost.NewMockUnitAllocation("a2", start, 24*time.Hour, &opencost.AllocationProperties{
		Cluster:         "cluster-b",
		Namespace:       "ns-b",
		Pod:             "pod-b",
		Container:       "container-b",
		ControllerKind:  "statefulset",
		Controller:      "db-a",
		Node:            "node-b",
		Labels:          map[string]string{"Team": "data", "app": "db"},
		NamespaceLabels: map[string]string{"owner": "db"},
	}))

	asr := opencost.NewAllocationSetRange(as)

	resp, err := QueryAllocationAutocompleteFromSetRange(asr, AllocationAutocompleteRequest{
		Field: "label",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0] != "Team" || resp.Data[1] != "app" {
		t.Fatalf("unexpected label autocomplete response: %+v", resp.Data)
	}

	valueResp, err := QueryAllocationAutocompleteFromSetRange(asr, AllocationAutocompleteRequest{
		Field:  "label:team",
		Search: "plat",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valueResp.Data) != 1 || valueResp.Data[0] != "platform" {
		t.Fatalf("unexpected label value autocomplete response: %+v", valueResp.Data)
	}

	mixedCaseResp, err := QueryAllocationAutocompleteFromSetRange(asr, AllocationAutocompleteRequest{
		Field: "label:Team",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mixedCaseResp.Data) != 2 || mixedCaseResp.Data[0] != "data" || mixedCaseResp.Data[1] != "platform" {
		t.Fatalf("expected label:team to match Team label values, got %+v", mixedCaseResp.Data)
	}

	_, err = QueryAllocationAutocompleteFromSetRange(asr, AllocationAutocompleteRequest{
		Field: "account",
	})
	if err == nil {
		t.Fatal("expected error for unsupported account field")
	}
	if !IsAutocompleteBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}

	_, err = QueryAllocationAutocompleteFromSetRange(asr, AllocationAutocompleteRequest{
		Field: "namespace",
		Limit: MaxAutocompleteResultLimit + 1,
	})
	if err == nil {
		t.Fatal("expected error for excessive limit")
	}
	if !IsAutocompleteBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}
}
