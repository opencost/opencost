package autocomplete

import (
	"fmt"
	"strings"
)

// LabelFieldPrefix identifies a label key namespace for autocomplete fields.
type LabelFieldPrefix string

const (
	LabelPrefix          LabelFieldPrefix = "label"
	NamespaceLabelPrefix LabelFieldPrefix = "namespacelabel"
)

// LabelFieldKind describes how an autocomplete field relates to labels.
type LabelFieldKind int

const (
	LabelFieldNone LabelFieldKind = iota
	LabelFieldKeys
	LabelFieldValue
)

// ParseLabelField classifies field relative to prefix (e.g. label, label:app).
func ParseLabelField(field string, prefix LabelFieldPrefix) (LabelFieldKind, string, error) {
	if field == "" {
		return LabelFieldNone, "", fmt.Errorf("field is required")
	}

	p := string(prefix)
	f := strings.ToLower(field)
	if f == p {
		return LabelFieldKeys, "", nil
	}

	valuePrefix := p + ":"
	if strings.HasPrefix(f, valuePrefix) {
		_, labelKey, _ := strings.Cut(field, ":")
		return LabelFieldValue, labelKey, nil
	}

	return LabelFieldNone, "", nil
}

// FormatLabelValueField returns a normalized label:value field preserving key casing.
func FormatLabelValueField(prefix LabelFieldPrefix, labelKey string) string {
	return string(prefix) + ":" + labelKey
}
