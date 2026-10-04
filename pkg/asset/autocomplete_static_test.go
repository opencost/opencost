package asset

import (
	"testing"

	coreasset "github.com/opencost/opencost/core/pkg/autocomplete/asset"
)

func TestFilterStaticAutocompleteValues(t *testing.T) {
	got := coreasset.FilterStaticValues(coreasset.StaticTypes(), "node")
	if len(got) != 1 || got[0] != "node" {
		t.Fatalf("FilterStaticValues() = %v", got)
	}
}
