package asset

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestValidateField_assettype(t *testing.T) {
	got, err := ValidateField("assettype")
	if err != nil || got != "type" {
		t.Fatalf("ValidateField(assettype) = %q, %v", got, err)
	}
}

func TestNormalizeRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	req := &AutocompleteRequest{
		TenantID: "t1",
		Field:    "assettype",
		Search:   " x ",
		Limit:    0,
		Window:   opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
	}
	field, err := NormalizeRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "type" || req.Search != "x" {
		t.Fatalf("unexpected normalized request: %+v", req)
	}
}

func TestParseRequest(t *testing.T) {
	qp := httputil.NewQueryParams(map[string][]string{"field": {"cluster"}})
	_, err := ParseRequest(qp, ParseOptions{}, nil)
	if err == nil {
		t.Fatal("expected missing window error")
	}
	got, err := ParseRequest(qp, ParseOptions{DefaultWindow: "30d", DefaultTenantID: "t1"}, nil)
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "cluster" {
		t.Fatalf("field = %q", got.Field)
	}
}

func TestFilterStaticValues(t *testing.T) {
	got := FilterStaticValues(StaticTypes(), "node")
	if len(got) != 1 || got[0] != "node" {
		t.Fatalf("FilterStaticValues() = %v", got)
	}
}
