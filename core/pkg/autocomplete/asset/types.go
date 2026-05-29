package asset

import (
	"context"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/opencost"
)

const (
	DefaultAutocompleteResultLimit = 100
	MaxAutocompleteResultLimit     = 1000
)

type AutocompleteRequest struct {
	TenantID string
	Search   string
	Field    string
	Limit    int
	Window   opencost.Window
	Filter   filter.Filter
}

type AutocompleteResponse struct {
	Data []string `json:"data"`
}

type AutocompleteQueryService interface {
	QueryAssetAutocomplete(AutocompleteRequest, context.Context) (*AutocompleteResponse, error)
}
