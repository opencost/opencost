package costmodel

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/util/httputil"
)

// ComputeAllocationHandler returns the assets from the CostModel.
func (a *Accesses) ComputeAssetsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := kubecost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	assetSet, err := a.Model.ComputeAssets(*window.Start(), *window.End())
	if err != nil {
		http.Error(w, fmt.Sprintf("Error computing asset set: %s", err), http.StatusInternalServerError)
		return
	}

	w.Write(WrapData(assetSet, nil))
}
