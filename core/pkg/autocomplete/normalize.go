package autocomplete

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// FieldValidator normalizes and validates an autocomplete field name.
type FieldValidator func(field string) (string, error)

// WindowValidator validates a parsed autocomplete window.
type WindowValidator func(window opencost.Window) error

// DefaultWindowValidator rejects open-ended windows.
func DefaultWindowValidator(window opencost.Window) error {
	if window.IsOpen() {
		return fmt.Errorf("%w: invalid window: %s", ErrBadRequest, window.String())
	}
	return nil
}

// NormalizeOptions configures shared request normalization.
type NormalizeOptions struct {
	RequireTenantID   bool
	EnsureLabelConfig bool
	WindowValidator   WindowValidator
}

// NormalizeRequest validates and normalizes an autocomplete request in place.
func NormalizeRequest(req *Request, validateField FieldValidator, opts NormalizeOptions) (string, error) {
	if req == nil {
		return "", fmt.Errorf("%w: request is nil", ErrBadRequest)
	}
	if opts.RequireTenantID && req.TenantID == "" {
		return "", fmt.Errorf("%w: tenant ID is required", ErrBadRequest)
	}

	windowValidator := opts.WindowValidator
	if windowValidator == nil {
		windowValidator = DefaultWindowValidator
	}
	if err := windowValidator(req.Window); err != nil {
		return "", err
	}

	field, err := validateField(req.Field)
	if err != nil {
		return "", fmt.Errorf("%w: invalid field: %w", ErrBadRequest, err)
	}

	limit, err := NormalizeLimit(req.Limit)
	if err != nil {
		return "", err
	}

	req.Field = field
	req.Search = SanitizeSearch(req.Search)
	req.Limit = limit
	if req.Filter == nil {
		req.Filter = &ast.VoidOp{}
	}
	if opts.EnsureLabelConfig && req.LabelConfig == nil {
		req.LabelConfig = opencost.NewLabelConfig()
	}
	return field, nil
}
