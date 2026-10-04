package asset

import (
	"errors"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestValidateField(t *testing.T) {
	tests := []struct {
		in   string
		want string
		err  bool
	}{
		{"assettype", "type", false},
		{"cluster", "cluster", false},
		{"account", "account", false},
		{"category", "category", false},
		{"label", "label", false},
		{"label:App", "label:App", false},
		{"bad", "", true},
	}
	for _, tt := range tests {
		got, err := ValidateField(tt.in)
		if tt.err {
			if err == nil {
				t.Fatalf("ValidateField(%q) expected error", tt.in)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Fatalf("ValidateField(%q) = %q, %v", tt.in, got, err)
		}
	}
}

func TestValidateWindow(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := ValidateWindow(opencost.NewClosedWindow(start, start.Add(time.Hour))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ValidateWindow(opencost.NewWindow(&start, nil)); err == nil {
		t.Fatal("expected open window error")
	}
	end := start.Add(time.Hour)
	if err := ValidateWindow(opencost.NewWindow(nil, &end)); err == nil {
		t.Fatal("expected open window error for nil start")
	}
	if err := ValidateWindow(opencost.NewWindow(nil, nil)); err == nil {
		t.Fatal("expected missing start/end error")
	}
}

func TestNormalizeRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	req := &autocomplete.Request{
		TenantID: "t1",
		Field:    "assettype",
		Search:   " x ",
		Limit:    0,
		Window:   opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
	}
	opts := autocomplete.NormalizeOptions{RequireTenantID: true, WindowValidator: ValidateWindow}
	field, err := autocomplete.NormalizeRequest(req, ValidateField, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "type" || req.Search != "x" {
		t.Fatalf("unexpected normalized request: %+v", req)
	}

	_, err = autocomplete.NormalizeRequest(&autocomplete.Request{Field: "cluster", Window: req.Window}, ValidateField, opts)
	if err == nil || !errors.Is(err, autocomplete.ErrBadRequest) {
		t.Fatalf("expected tenant ID error, got %v", err)
	}
}

func TestParseRequest(t *testing.T) {
	qp := httputil.NewQueryParams(map[string][]string{"field": {"cluster"}})
	_, err := ParseRequest(qp, autocomplete.ParseOptions{})
	if err == nil {
		t.Fatal("expected missing window error")
	}
	got, err := ParseRequest(qp, autocomplete.ParseOptions{DefaultWindow: "30d", DefaultTenantID: "t1"})
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "cluster" || got.TenantID != "t1" {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestRouteField(t *testing.T) {
	tests := []struct {
		field string
		route Route
		key   string
	}{
		{"type", RouteStaticType, ""},
		{"category", RouteStaticCategory, ""},
		{"label", RouteLabelKeys, ""},
		{"label:App", RouteLabelValue, "App"},
		{"cluster", RouteDefault, ""},
	}
	for _, tt := range tests {
		route, key, err := RouteField(tt.field)
		if err != nil || route != tt.route || key != tt.key {
			t.Fatalf("RouteField(%q) = %v, %q, %v; want %v, %q", tt.field, route, key, err, tt.route, tt.key)
		}
	}
}

func TestStaticValues(t *testing.T) {
	types := StaticTypes()
	if len(types) == 0 {
		t.Fatal("expected static types")
	}
	categories := StaticCategories()
	if len(categories) == 0 {
		t.Fatal("expected static categories")
	}
	got := FilterStaticValues(StaticTypes(), "node")
	if len(got) != 1 || got[0] != "node" {
		t.Fatalf("FilterStaticValues() = %v", got)
	}
}
