package allocation

import (
	"github.com/opencost/opencost/core/pkg/autocomplete"
	coreallocation "github.com/opencost/opencost/core/pkg/autocomplete/allocation"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

var ErrAutocompleteBadRequest = autocomplete.ErrBadRequest

func IsAutocompleteBadRequest(err error) bool {
	return autocomplete.IsBadRequest(err)
}

type (
	AllocationAutocompleteRequest  = coreallocation.AutocompleteRequest
	AllocationAutocompleteResponse = coreallocation.AutocompleteResponse
)

type AutocompleteQueryService = coreallocation.AutocompleteQueryService

const (
	DefaultAutocompleteResultLimit = coreallocation.DefaultAutocompleteResultLimit
	MaxAutocompleteResultLimit     = coreallocation.MaxAutocompleteResultLimit
)

type AutocompleteFilterParser = coreallocation.FilterParser

type ParseAllocationAutocompleteOptions = coreallocation.ParseOptions

func ValidateAutocompleteField(field string) (string, error) {
	return coreallocation.ValidateField(field)
}

func NormalizeAllocationAutocompleteRequest(req *AllocationAutocompleteRequest) (string, error) {
	return coreallocation.NormalizeRequest(req)
}

func RouteAllocationAutocompleteField(field string) (AllocationAutocompleteRoute, string, error) {
	return coreallocation.RouteField(field)
}

type AllocationAutocompleteRoute = coreallocation.Route

const (
	AllocationAutocompleteRouteField                = coreallocation.RouteDefault
	AllocationAutocompleteRouteLabelKeys            = coreallocation.RouteLabelKeys
	AllocationAutocompleteRouteLabelValue           = coreallocation.RouteLabelValue
	AllocationAutocompleteRouteNamespaceLabelKeys   = coreallocation.RouteNamespaceLabelKeys
	AllocationAutocompleteRouteNamespaceLabelValue  = coreallocation.RouteNamespaceLabelValue
)

func ParseAllocationAutocompleteRequest(qp httputil.QueryParams, opts ParseAllocationAutocompleteOptions, parseFilter AutocompleteFilterParser) (*AllocationAutocompleteRequest, error) {
	return coreallocation.ParseRequest(qp, opts, parseFilter)
}
