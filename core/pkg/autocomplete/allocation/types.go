package allocation

import (
	"context"

	"github.com/opencost/opencost/core/pkg/autocomplete"
)

type AutocompleteQueryService interface {
	QueryAllocationAutocomplete(autocomplete.Request, context.Context) (*autocomplete.Response, error)
}
