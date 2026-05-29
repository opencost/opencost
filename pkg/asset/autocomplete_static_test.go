package asset

import "testing"

func TestFilterStaticAutocompleteValues(t *testing.T) {
	got := FilterStaticAutocompleteValues(StaticAssetTypes(), "node")
	if len(got) != 1 || got[0] != "node" {
		t.Fatalf("FilterStaticAutocompleteValues() = %v", got)
	}
}
