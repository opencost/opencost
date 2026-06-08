package cloudcost

import (
	cloudcostfilter "github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// ParseRequest builds an autocomplete.Request from query parameters.
func ParseRequest(qp httputil.QueryParams, opts autocomplete.ParseOptions) (*autocomplete.Request, error) {
	parser := cloudcostfilter.NewCloudCostFilterParser()
	return autocomplete.ParseRequest(qp, opts, ValidateField, parser.Parse)
}
