package allocation

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
		{"account", "account", false},
		{"cluster", "cluster", false},
		{"label", "label", false},
		{"label:App", "label:App", false},
		{"namespacelabel:Team", "namespacelabel:Team", false},
		{"", "", true},
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

func TestNormalizeRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	req := &autocomplete.Request{
		Field:  "cluster",
		Search: " x ",
		Limit:  0,
		Window: opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
	}
	field, err := autocomplete.NormalizeRequest(req, ValidateField, autocomplete.NormalizeOptions{EnsureLabelConfig: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "cluster" || req.Search != "x" || req.LabelConfig == nil {
		t.Fatalf("unexpected normalized request: %+v", req)
	}

	openReq := &autocomplete.Request{Field: "cluster", Window: opencost.NewWindow(&start, nil)}
	_, err = autocomplete.NormalizeRequest(openReq, ValidateField, autocomplete.NormalizeOptions{EnsureLabelConfig: true})
	if err == nil || !errors.Is(err, autocomplete.ErrBadRequest) {
		t.Fatalf("expected open window error, got %v", err)
	}
}

func TestParseRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	qp := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"account"},
		"search": {" ns "},
	})
	got, err := ParseRequest(qp, autocomplete.ParseOptions{})
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "account" || got.Search != "ns" {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestRouteField(t *testing.T) {
	tests := []struct {
		field string
		route Route
		key   string
	}{
		{"namespacelabel:Team", RouteNamespaceLabelValue, "Team"},
		{"label", RouteLabelKeys, ""},
		{"label:App", RouteLabelValue, "App"},
		{"namespacelabel", RouteNamespaceLabelKeys, ""},
		{"cluster", RouteDefault, ""},
	}
	for _, tt := range tests {
		route, key, err := RouteField(tt.field)
		if err != nil || route != tt.route || key != tt.key {
			t.Fatalf("RouteField(%q) = %v, %q, %v; want %v, %q", tt.field, route, key, err, tt.route, tt.key)
		}
	}
}
