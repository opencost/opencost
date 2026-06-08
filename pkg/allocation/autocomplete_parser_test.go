package allocation

import (
	"testing"

	coreallocation "github.com/opencost/opencost/core/pkg/autocomplete/allocation"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestParseAllocationAutocompleteRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	qp := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"account"},
		"search": {" ns "},
	})
	got, err := coreallocation.ParseRequest(qp, autocomplete.ParseOptions{
		DefaultWindow: "30d",
	})
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "account" || got.Search != "ns" {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestValidateAutocompleteField_account(t *testing.T) {
	got, err := coreallocation.ValidateField("account")
	if err != nil || got != "account" {
		t.Fatalf("ValidateField(account) = %q, %v", got, err)
	}
}

func TestRouteAllocationAutocompleteField(t *testing.T) {
	route, key, err := coreallocation.RouteField("namespacelabel:Team")
	if err != nil || route != coreallocation.RouteNamespaceLabelValue || key != "Team" {
		t.Fatalf("RouteField() = %v, %q, %v", route, key, err)
	}
}
