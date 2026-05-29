package cloudcost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/autocomplete"
)

// NormalizeRequest validates and normalizes a cloud cost autocomplete request in place.
func NormalizeRequest(req *AutocompleteRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", autocomplete.ErrBadRequest)
	}
	if req.Window.IsOpen() {
		return "", fmt.Errorf("%w: invalid window for autocomplete query: %s", autocomplete.ErrBadRequest, req.Window.String())
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
