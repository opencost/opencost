package asset

import (
	"errors"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestNormalizeAssetAutocompleteRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	req := &AssetAutocompleteRequest{
		TenantID: "t1",
		Field:    "assettype",
		Search:   " x ",
		Limit:    0,
		Window:   opencost.NewClosedWindow(start, end),
	}
	field, err := NormalizeAssetAutocompleteRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "type" || req.Search != "x" || req.Limit != DefaultAutocompleteResultLimit {
		t.Fatalf("unexpected normalized request: field=%s search=%q limit=%d", field, req.Search, req.Limit)
	}

	_, err = NormalizeAssetAutocompleteRequest(&AssetAutocompleteRequest{Limit: MaxAutocompleteResultLimit + 1})
	if err == nil || !IsAutocompleteBadRequest(err) {
		t.Fatalf("expected bad request, got %v", err)
	}
	if !errors.Is(err, ErrAutocompleteBadRequest) {
		t.Fatalf("expected ErrAutocompleteBadRequest")
	}
}
