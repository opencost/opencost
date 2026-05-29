package cloudcost

import (
	"github.com/opencost/opencost/core/pkg/autocomplete"
	corecloudcost "github.com/opencost/opencost/core/pkg/autocomplete/cloudcost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

var ErrAutocompleteBadRequest = autocomplete.ErrBadRequest

func IsAutocompleteBadRequest(err error) bool {
	return autocomplete.IsBadRequest(err)
}

type (
	CloudCostAutocompleteRequest  = corecloudcost.AutocompleteRequest
	CloudCostAutocompleteResponse = corecloudcost.AutocompleteResponse
)

type AutocompleteQueryService = corecloudcost.AutocompleteQueryService

const (
	DefaultAutocompleteResultLimit = corecloudcost.DefaultAutocompleteResultLimit
	MaxAutocompleteResultLimit     = corecloudcost.MaxAutocompleteResultLimit
)

type AutocompleteFilterParser = corecloudcost.FilterParser

type ParseCloudCostAutocompleteOptions = corecloudcost.ParseOptions

func ValidateAutocompleteField(field string) (string, error) {
	return corecloudcost.ValidateField(field)
}

func NormalizeCloudCostAutocompleteRequest(req *CloudCostAutocompleteRequest) (string, error) {
	return corecloudcost.NormalizeRequest(req)
}

func ParseCloudCostAutocompleteRequest(qp httputil.QueryParams, opts ParseCloudCostAutocompleteOptions, parseFilter AutocompleteFilterParser) (*CloudCostAutocompleteRequest, error) {
	return corecloudcost.ParseRequest(qp, opts, parseFilter)
}

func ParseCloudCostAutocompleteRequestFromQueryParams(qp httputil.QueryParams) (*CloudCostAutocompleteRequest, error) {
	return corecloudcost.ParseRequestFromQueryParams(qp)
}
