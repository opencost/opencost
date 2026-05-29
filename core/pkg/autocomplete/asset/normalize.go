package asset

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/autocomplete"
)

// NormalizeRequest validates and normalizes an asset autocomplete request in place.
func NormalizeRequest(req *AutocompleteRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", autocomplete.ErrBadRequest)
	}
	if req.TenantID == "" {
		return "", fmt.Errorf("%w: tenant ID is required", autocomplete.ErrBadRequest)
	}
	if err := validateWindow(req.Window); err != nil {
		return "", err
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
	return field, nil
}
