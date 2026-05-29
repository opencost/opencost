package cloudcost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// NormalizeCloudCostAutocompleteRequest validates and normalizes a cloud cost autocomplete request in place.
func NormalizeCloudCostAutocompleteRequest(req *CloudCostAutocompleteRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", ErrAutocompleteBadRequest)
	}
	if req.Window.IsOpen() {
		return "", fmt.Errorf("%w: invalid window for autocomplete query: %s", ErrAutocompleteBadRequest, req.Window.String())
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
	return field, nil
}
