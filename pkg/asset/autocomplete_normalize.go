package asset

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util/autocomplete"
)

// NormalizeAssetAutocompleteRequest validates and normalizes an asset autocomplete request in place.
func NormalizeAssetAutocompleteRequest(req *AssetAutocompleteRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", ErrAutocompleteBadRequest)
	}
	if req.TenantID == "" {
		return "", fmt.Errorf("%w: tenant ID is required", ErrAutocompleteBadRequest)
	}
	if err := validateAssetAutocompleteWindow(req.Window); err != nil {
		return "", err
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
