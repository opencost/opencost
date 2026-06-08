package allocation

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/autocomplete"
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
	window := opencost.NewClosedWindow(start, start.Add(24*time.Hour))

	resp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "label",
		Limit:  10,
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0] != "Team" || resp.Data[1] != "app" {
		t.Fatalf("unexpected label autocomplete response: %+v", resp.Data)
	}

	valueResp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "label:team",
		Search: "plat",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valueResp.Data) != 1 || valueResp.Data[0] != "platform" {
		t.Fatalf("unexpected label value autocomplete response: %+v", valueResp.Data)
	}

	mixedCaseResp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "label:Team",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mixedCaseResp.Data) != 2 || mixedCaseResp.Data[0] != "data" || mixedCaseResp.Data[1] != "platform" {
		t.Fatalf("expected label:team to match Team label values, got %+v", mixedCaseResp.Data)
	}

	accountResp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "account",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error for account field: %v", err)
	}
	if len(accountResp.Data) != 0 {
		t.Fatalf("expected empty account autocomplete response, got %+v", accountResp.Data)
	}

	_, err = QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "namespace",
		Limit:  autocomplete.MaxResultLimit + 1,
		Window: window,
	})
	if err == nil {
		t.Fatal("expected error for excessive limit")
	}
	if !autocomplete.IsBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}
}
