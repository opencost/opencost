package allocation

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// ValidateField normalizes and validates an allocation autocomplete field name.
func ValidateField(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("field is required")
	}

	f := strings.ToLower(field)
	switch f {
	case "account", "cluster", "namespace", "node", "controllerkind", "controllername", "pod", "container", "label", "namespacelabel":
		return f, nil
	}

	// Alias fields (department, environment, owner, product, team) resolve to
	// label keys via the LabelConfig on the request. Implementations route
	// them with RouteAlias.
	if prop := opencost.AllocationProperty(f); prop.IsAliasedLabel() {
		return f, nil
	}

	if strings.HasPrefix(f, "label:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return autocomplete.FormatLabelValueField(autocomplete.LabelPrefix, labelKey), nil
	}
	if strings.HasPrefix(f, "namespacelabel:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return autocomplete.FormatLabelValueField(autocomplete.NamespaceLabelPrefix, labelKey), nil
	}

	return "", fmt.Errorf("unrecognized field: %s", field)
}
