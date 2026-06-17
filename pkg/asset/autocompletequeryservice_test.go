package asset

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestQueryAssetAutocompleteFromSet(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := opencost.NewClosedWindow(start, end)

	nodeA := opencost.NewNode("node-a", "cluster-a", "provider-a", start, end, window)
	nodeA.SetLabels(map[string]string{"team": "platform", "app": "api"})
	nodeA.GetProperties().Account = "acct-a"
	nodeA.GetProperties().Category = opencost.ComputeCategory

	nodeB := opencost.NewNode("node-b", "cluster-b", "provider-b", start, end, window)
	nodeB.SetLabels(map[string]string{"team": "data", "app": "db"})
	nodeB.GetProperties().Account = "acct-b"
	nodeB.GetProperties().Category = opencost.ComputeCategory

	assetSet := opencost.NewAssetSet(start, end, nodeA, nodeB)

	resp, err := QueryAssetAutocompleteFromSet(assetSet, autocomplete.Request{
		TenantID: "opencost",
		Field:    "cluster",
		Window:   window,
		Filter:   &ast.VoidOp{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0] != "cluster-a" || resp.Data[1] != "cluster-b" {
		t.Fatalf("unexpected cluster autocomplete response: %+v", resp.Data)
	}

	labelResp, err := QueryAssetAutocompleteFromSet(assetSet, autocomplete.Request{
		TenantID: "opencost",
		Field:    "label:team",
		Search:   "plat",
		Window:   window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(labelResp.Data) != 1 || labelResp.Data[0] != "platform" {
		t.Fatalf("unexpected label autocomplete response: %+v", labelResp.Data)
	}

	_, err = QueryAssetAutocompleteFromSet(assetSet, autocomplete.Request{
		Field: "cluster",
	})
	if err == nil {
		t.Fatal("expected error when tenant ID is missing")
	}
	if !autocomplete.IsBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}

	_, err = QueryAssetAutocompleteFromSet(assetSet, autocomplete.Request{
		TenantID: "opencost",
		Field:    "labels",
		Window:   window,
	})
	if err == nil {
		t.Fatal("expected error for invalid field prefix")
	}
	if !autocomplete.IsBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}

	openWindow := opencost.NewWindow(&start, nil)
	_, err = QueryAssetAutocompleteFromSet(assetSet, autocomplete.Request{
		TenantID: "opencost",
		Field:    "name",
		Window:   openWindow,
	})
	if err == nil {
		t.Fatal("expected error for open window")
	}
	if !autocomplete.IsBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}
}
