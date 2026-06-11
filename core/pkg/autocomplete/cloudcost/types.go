package cloudcost

import (
	"context"

	"github.com/opencost/opencost/core/pkg/autocomplete"
)

type AutocompleteQueryService interface {
	QueryCloudCostAutocomplete(context.Context, autocomplete.Request) (*autocomplete.Response, error)
}
