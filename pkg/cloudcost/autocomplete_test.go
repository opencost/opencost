package cloudcost

import (
	"context"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestRepositoryQuerier_QueryCloudCostAutocomplete(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	repo := NewMemoryRepository()
	ccs := DefaultMockCloudCostSet(start, end, "aws", "integration-1")
	if err := repo.Put(ccs); err != nil {
		t.Fatalf("failed to seed repository: %v", err)
	}
	rq := NewRepositoryQuerier(repo)

	resp, err := rq.QueryCloudCostAutocomplete(context.Background(), CloudCostAutocompleteRequest{
		Field:  opencost.CloudCostServiceProp,
		Window: opencost.NewClosedWindow(start, end),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 service values, got %d: %+v", len(resp.Data), resp.Data)
	}

	labelResp, err := rq.QueryCloudCostAutocomplete(context.Background(), CloudCostAutocompleteRequest{
		Field:  "label:label1",
		Search: "value1",
		Window: opencost.NewClosedWindow(start, end),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(labelResp.Data) != 1 || labelResp.Data[0] != "value1" {
		t.Fatalf("unexpected label autocomplete response: %+v", labelResp.Data)
	}
}
