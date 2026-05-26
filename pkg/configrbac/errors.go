package configrbac

import "errors"

var (
	// ErrScopedViewsDisabled is returned when the API is disabled in config.json.
	ErrScopedViewsDisabled = errors.New("scoped views API is disabled")
	// ErrDuplicateID is returned when creating an entity with an existing id.
	ErrDuplicateID = errors.New("duplicate id")
)
