package allocation

import (
	allocationfilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

// ParseRequest builds an autocomplete.Request from query parameters.
func ParseRequest(qp httputil.QueryParams, opts autocomplete.ParseOptions) (*autocomplete.Request, error) {
	parser := allocationfilter.NewAllocationFilterParser()
	return autocomplete.ParseRequest(qp, opts, ValidateField, parser.Parse)
}
