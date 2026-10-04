package asset

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// ValidateField normalizes and validates an asset autocomplete field name.
func ValidateField(field string) (string, error) {
	f := strings.ToLower(field)
	switch f {
	case "account", "cluster", "name", "provider", "providerid", "type", "assettype", "category":
		if f == "assettype" {
			return "type", nil
		}
		return f, nil
	}
	if f == "label" {
		return f, nil
	}
	if strings.HasPrefix(f, "label:") {
		_, labelKey, _ := strings.Cut(field, ":")
		return autocomplete.FormatLabelValueField(autocomplete.LabelPrefix, labelKey), nil
	}
	return "", fmt.Errorf("unrecognized field: %s", field)
}

func ValidateWindow(window opencost.Window) error {
	if window.IsOpen() {
		return fmt.Errorf("%w: invalid window: %s", autocomplete.ErrBadRequest, window.String())
	}
	if window.Start() == nil || window.End() == nil {
		return fmt.Errorf("%w: invalid window: missing start or end", autocomplete.ErrBadRequest)
	}
	return nil
}
