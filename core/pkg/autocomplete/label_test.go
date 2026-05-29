package autocomplete

import "testing"

func TestParseLabelField(t *testing.T) {
	kind, key, err := ParseLabelField("label:App", LabelPrefix)
	if err != nil || kind != LabelFieldValue || key != "App" {
		t.Fatalf("ParseLabelField() = %v, %q, %v", kind, key, err)
	}
}
