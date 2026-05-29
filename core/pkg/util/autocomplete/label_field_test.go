package autocomplete

import "testing"

func TestParseLabelField(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		prefix    LabelFieldPrefix
		wantKind  LabelFieldKind
		wantKey   string
		wantErr   bool
	}{
		{name: "keys", field: "label", prefix: LabelPrefix, wantKind: LabelFieldKeys},
		{name: "value", field: "label:App", prefix: LabelPrefix, wantKind: LabelFieldValue, wantKey: "App"},
		{name: "namespace value", field: "namespacelabel:team", prefix: NamespaceLabelPrefix, wantKind: LabelFieldValue, wantKey: "team"},
		{name: "unrelated", field: "cluster", prefix: LabelPrefix, wantKind: LabelFieldNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, key, err := ParseLabelField(tt.field, tt.prefix)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseLabelField() error = %v, wantErr %v", err, tt.wantErr)
			}
			if kind != tt.wantKind || key != tt.wantKey {
				t.Fatalf("ParseLabelField() = (%v, %q), want (%v, %q)", kind, key, tt.wantKind, tt.wantKey)
			}
		})
	}
}

func TestFormatLabelValueField(t *testing.T) {
	if got := FormatLabelValueField(LabelPrefix, "App"); got != "label:App" {
		t.Fatalf("FormatLabelValueField() = %q", got)
	}
}
