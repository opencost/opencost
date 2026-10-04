package cloudcost

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// ValidateField normalizes and validates a cloud cost autocomplete field name.
func ValidateField(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("field is required")
	}

	f := strings.ToLower(field)
	if f == "label" {
		return f, nil
	}
	if strings.HasPrefix(f, "label:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return "label:" + labelKey, nil
	}

	property, err := opencost.ParseCloudCostProperty(field)
	if err != nil {
		return "", err
	}
	return string(property), nil
}
