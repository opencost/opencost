package allocation

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// NormalizeAllocationAutocompleteRequest validates and normalizes an allocation autocomplete request in place.
func NormalizeAllocationAutocompleteRequest(req *AllocationAutocompleteRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", ErrAutocompleteBadRequest)
	}
	if req.Window.IsOpen() {
		return "", fmt.Errorf("%w: invalid window: %s", ErrAutocompleteBadRequest, req.Window.String())
	}

	field, err := ValidateAutocompleteField(req.Field)
	if err != nil {
		return "", fmt.Errorf("%w: invalid field: %w", ErrAutocompleteBadRequest, err)
	}

	limit, err := autocomplete.NormalizeLimit(req.Limit, DefaultAutocompleteResultLimit, MaxAutocompleteResultLimit)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrAutocompleteBadRequest, err)
	}

	req.Field = field
	req.Search = autocomplete.SanitizeSearch(req.Search)
	req.Limit = limit
	if req.LabelConfig == nil {
		req.LabelConfig = opencost.NewLabelConfig()
	}
	return field, nil
}
