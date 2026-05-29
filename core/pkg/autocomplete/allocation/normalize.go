package allocation

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// NormalizeRequest validates and normalizes an allocation autocomplete request in place.
func NormalizeRequest(req *AutocompleteRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", autocomplete.ErrBadRequest)
	}
	if req.Window.IsOpen() {
		return "", fmt.Errorf("%w: invalid window: %s", autocomplete.ErrBadRequest, req.Window.String())
	}

	field, err := ValidateField(req.Field)
	if err != nil {
		return "", fmt.Errorf("%w: invalid field: %w", autocomplete.ErrBadRequest, err)
	}

	limit, err := autocomplete.NormalizeLimit(req.Limit)
	if err != nil {
		return "", err
	}

	req.Field = field
	req.Search = autocomplete.SanitizeSearch(req.Search)
	req.Limit = limit
	if req.LabelConfig == nil {
		req.LabelConfig = opencost.NewLabelConfig()
	}
	return field, nil
}
