package autocomplete

import "testing"

func TestParseLabelField(t *testing.T) {
	kind, key, err := ParseLabelField("label:App", LabelPrefix)
	if err != nil || kind != LabelFieldValue || key != "App" {
		t.Fatalf("ParseLabelField(label:App) = %v, %q, %v", kind, key, err)
	}

	kind, key, err = ParseLabelField("label", LabelPrefix)
	if err != nil || kind != LabelFieldKeys || key != "" {
		t.Fatalf("ParseLabelField(label) = %v, %q, %v", kind, key, err)
	}

	kind, _, err = ParseLabelField("cluster", LabelPrefix)
	if err != nil || kind != LabelFieldNone {
		t.Fatalf("ParseLabelField(cluster) = %v, %v", kind, err)
	}

	_, _, err = ParseLabelField("", LabelPrefix)
	if err == nil {
		t.Fatal("expected error for empty field")
	}

	kind, key, err = ParseLabelField("namespacelabel:Team", NamespaceLabelPrefix)
	if err != nil || kind != LabelFieldValue || key != "Team" {
		t.Fatalf("ParseLabelField(namespacelabel:Team) = %v, %q, %v", kind, key, err)
	}
}

func TestFormatLabelValueField(t *testing.T) {
	got := FormatLabelValueField(LabelPrefix, "App")
	if got != "label:App" {
		t.Fatalf("FormatLabelValueField() = %q", got)
	}
}
