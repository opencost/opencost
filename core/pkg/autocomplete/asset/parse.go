package asset

import (
	assetfilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// ParseRequest builds an autocomplete.Request from query parameters.
func ParseRequest(qp httputil.QueryParams, opts autocomplete.ParseOptions) (*autocomplete.Request, error) {
	if opts.WindowValidator == nil {
		opts.WindowValidator = ValidateWindow
	}
	parser := assetfilter.NewAssetFilterParser()
	return autocomplete.ParseRequest(qp, opts, ValidateField, parser.Parse)
}
