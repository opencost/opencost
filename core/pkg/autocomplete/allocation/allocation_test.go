package allocation

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestValidateField_account(t *testing.T) {
	got, err := ValidateField("account")
	if err != nil || got != "account" {
		t.Fatalf("ValidateField(account) = %q, %v", got, err)
	}
}

func TestParseRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	qp := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"account"},
		"search": {" ns "},
	})
	got, err := ParseRequest(qp, ParseOptions{}, nil)
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "account" || got.Search != "ns" {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestRouteField(t *testing.T) {
	route, key, err := RouteField("namespacelabel:Team")
	if err != nil || route != RouteNamespaceLabelValue || key != "Team" {
		t.Fatalf("RouteField() = %v, %q, %v", route, key, err)
	}
}
