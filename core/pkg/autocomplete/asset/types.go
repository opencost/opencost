package asset

import (
	"context"

	"github.com/opencost/opencost/core/pkg/autocomplete"
)

type AutocompleteQueryService interface {
	QueryAssetAutocomplete(autocomplete.Request, context.Context) (*autocomplete.Response, error)
}
