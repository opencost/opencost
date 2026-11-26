package costmodel

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	assetfilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/carbon"
	"github.com/opencost/opencost/pkg/env"
)

// ComputeAllocationHandler returns the assets from the CostModel.
func (a *Accesses) ComputeAssetsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	filter, err := parseAssetFilter(qp.Get("filter", ""))
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'filter' parameter: %s", err), http.StatusBadRequest)
		return
	}

	assetSet, err := a.ComputeAssetsFromCostmodel(window, filter)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting assets: %s", err), http.StatusInternalServerError)
		return
	}

	WriteData(w, assetSet, nil)
}

// ComputeAllocationHandler returns the assets from the CostModel.
func (a *Accesses) ComputeAssetsCarbonHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	qp := httputil.NewQueryParams(r.URL.Query())

	// Window is a required field describing the window of time over which to
	// compute allocation data.
	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	filter, err := parseAssetFilter(qp.Get("filter", ""))
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'filter' parameter: %s", err), http.StatusBadRequest)
		return
	}

	assetSet, err := a.ComputeAssetsFromCostmodel(window, filter)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting assets: %s", err), http.StatusInternalServerError)
		return
	}

	carbonEstimates, err := carbon.RelateCarbonAssets(assetSet)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error relating carbon assets: %s", err), http.StatusInternalServerError)
		return
	}

	WriteData(w, carbonEstimates, nil)
}

// parseAssetFilter parses and compiles a v2 asset filter. An empty string
// matches every asset.
func parseAssetFilter(filterString string) (opencost.AssetMatcher, error) {
	if filterString == "" {
		return &matcher.AllPass[opencost.Asset]{}, nil
	}

	tree, err := assetfilter.NewAssetFilterParser().Parse(filterString)
	if err != nil {
		return nil, fmt.Errorf("err parsing filter '%s': %v", filterString, err)
	}
	filter, err := opencost.NewAssetMatchCompiler().Compile(tree)
	if err != nil {
		return nil, fmt.Errorf("err compiling filter '%s': %v", filterString, err)
	}
	if filter == nil {
		return nil, fmt.Errorf("unexpected nil filter")
	}

	return filter, nil
}

func (a *Accesses) ComputeAssetsFromCostmodel(window opencost.Window, filter opencost.AssetMatcher) (*opencost.AssetSet, error) {

	assetSet, err := a.Model.ComputeAssets(*window.Start(), *window.End())
	if err != nil {
		return nil, fmt.Errorf("error computing asset set: %s", err)
	}

	for key, asset := range assetSet.Assets {
		if !filter.Matches(asset) {
			delete(assetSet.Assets, key)
		}
	}

	return assetSet, nil
}

// KubeModelHandler returns KubeModelSets from pipeline storage for a given window and resolution.
//
// Query params:
//
//	window     (required) — same format as allocation/asset endpoints, e.g. "today", "7d", "2024-04-01T00:00:00Z,2024-04-02T00:00:00Z"
func (a *Accesses) KubeModelHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	if a.KubeModelQuerier == nil {
		http.Error(w, "KubeModel pipeline not initialized", http.StatusServiceUnavailable)
		return
	}

	qp := httputil.NewQueryParams(r.URL.Query())

	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	sets, err := a.KubeModelQuerier.Query(window)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying KubeModel: %s", err), http.StatusInternalServerError)
		return
	}

	WriteData(w, sets, nil)
}
