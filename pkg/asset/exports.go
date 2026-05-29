package asset

import (
	"github.com/opencost/opencost/core/pkg/autocomplete"
	coreasset "github.com/opencost/opencost/core/pkg/autocomplete/asset"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

var ErrAutocompleteBadRequest = autocomplete.ErrBadRequest

func IsAutocompleteBadRequest(err error) bool {
	return autocomplete.IsBadRequest(err)
}

type (
	AssetAutocompleteRequest  = coreasset.AutocompleteRequest
	AssetAutocompleteResponse = coreasset.AutocompleteResponse
)

type AutocompleteQueryService = coreasset.AutocompleteQueryService

const (
	DefaultAutocompleteResultLimit = coreasset.DefaultAutocompleteResultLimit
	MaxAutocompleteResultLimit     = coreasset.MaxAutocompleteResultLimit
)

type AutocompleteFilterParser = coreasset.FilterParser

type ParseAssetAutocompleteOptions = coreasset.ParseOptions

func ValidateAutocompleteField(field string) (string, error) {
	return coreasset.ValidateField(field)
}

func NormalizeAssetAutocompleteRequest(req *AssetAutocompleteRequest) (string, error) {
	return coreasset.NormalizeRequest(req)
}

func RouteAssetAutocompleteField(field string) (AssetAutocompleteRoute, string, error) {
	return coreasset.RouteField(field)
}

type AssetAutocompleteRoute = coreasset.Route

const (
	AssetAutocompleteRouteField          = coreasset.RouteDefault
	AssetAutocompleteRouteLabelKeys      = coreasset.RouteLabelKeys
	AssetAutocompleteRouteLabelValue     = coreasset.RouteLabelValue
	AssetAutocompleteRouteStaticType     = coreasset.RouteStaticType
	AssetAutocompleteRouteStaticCategory = coreasset.RouteStaticCategory
)

func StaticAssetTypes() []string       { return coreasset.StaticTypes() }
func StaticAssetCategories() []string { return coreasset.StaticCategories() }
func FilterStaticAutocompleteValues(values []string, search string) []string {
	return coreasset.FilterStaticValues(values, search)
}

func ParseAssetAutocompleteRequest(qp httputil.QueryParams, opts ParseAssetAutocompleteOptions, parseFilter AutocompleteFilterParser) (*AssetAutocompleteRequest, error) {
	return coreasset.ParseRequest(qp, opts, parseFilter)
}
