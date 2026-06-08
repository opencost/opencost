package asset

import (
	"errors"
	"testing"
	"time"

	coreasset "github.com/opencost/opencost/core/pkg/autocomplete/asset"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestNormalizeAssetAutocompleteRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	req := &autocomplete.Request{
		TenantID: "t1",
		Field:    "assettype",
		Search:   " x ",
		Limit:    0,
		Window:   opencost.NewClosedWindow(start, end),
	}
	opts := autocomplete.NormalizeOptions{RequireTenantID: true, WindowValidator: coreasset.ValidateWindow}
	field, err := autocomplete.NormalizeRequest(req, coreasset.ValidateField, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "type" || req.Search != "x" || req.Limit != autocomplete.DefaultResultLimit {
		t.Fatalf("unexpected normalized request: field=%s search=%q limit=%d", field, req.Search, req.Limit)
	}

	_, err = autocomplete.NormalizeRequest(&autocomplete.Request{
		TenantID: "t1",
		Field:    "cluster",
		Limit:    autocomplete.MaxResultLimit + 1,
		Window:   opencost.NewClosedWindow(start, end),
	}, coreasset.ValidateField, opts)
	if err == nil || !autocomplete.IsBadRequest(err) {
		t.Fatalf("expected bad request, got %v", err)
	}
	if !errors.Is(err, autocomplete.ErrBadRequest) {
		t.Fatalf("expected ErrBadRequest")
	}
}
