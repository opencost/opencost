package asset

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestQueryAssetAutocompleteFromSet(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := opencost.NewWindow(&start, &end)

	nodeA := opencost.NewNode("node-a", "cluster-a", "provider-a", start, end, window)
	nodeA.SetLabels(map[string]string{"team": "platform", "app": "api"})
	nodeA.GetProperties().Account = "acct-a"
	nodeA.GetProperties().Category = opencost.ComputeCategory

	nodeB := opencost.NewNode("node-b", "cluster-b", "provider-b", start, end, window)
	nodeB.SetLabels(map[string]string{"team": "data", "app": "db"})
	nodeB.GetProperties().Account = "acct-b"
	nodeB.GetProperties().Category = opencost.ComputeCategory

	assetSet := opencost.NewAssetSet(start, end, nodeA, nodeB)

	resp, err := QueryAssetAutocompleteFromSet(assetSet, AssetAutocompleteRequest{
		TenantID: "opencost",
		Field:    "cluster",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0] != "cluster-a" || resp.Data[1] != "cluster-b" {
		t.Fatalf("unexpected cluster autocomplete response: %+v", resp.Data)
	}

	labelResp, err := QueryAssetAutocompleteFromSet(assetSet, AssetAutocompleteRequest{
		TenantID: "opencost",
		Field:    "label:team",
		Search:   "plat",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(labelResp.Data) != 1 || labelResp.Data[0] != "platform" {
		t.Fatalf("unexpected label autocomplete response: %+v", labelResp.Data)
	}
}
