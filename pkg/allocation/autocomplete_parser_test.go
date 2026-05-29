package allocation

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestParseAllocationAutocompleteRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	qp := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"account"},
		"search": {" ns "},
	})
	got, err := ParseAllocationAutocompleteRequest(qp, ParseAllocationAutocompleteOptions{
		DefaultWindow: "30d",
	}, nil)
	if err != nil {
		t.Fatalf("ParseAllocationAutocompleteRequest() error = %v", err)
	}
	if got.Field != "account" || got.Search != "ns" {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestValidateAutocompleteField_account(t *testing.T) {
	got, err := ValidateAutocompleteField("account")
	if err != nil || got != "account" {
		t.Fatalf("ValidateAutocompleteField(account) = %q, %v", got, err)
	}
}

func TestRouteAllocationAutocompleteField(t *testing.T) {
	route, key, err := RouteAllocationAutocompleteField("namespacelabel:Team")
	if err != nil || route != AllocationAutocompleteRouteNamespaceLabelValue || key != "Team" {
		t.Fatalf("RouteAllocationAutocompleteField() = %v, %q, %v", route, key, err)
	}
}
