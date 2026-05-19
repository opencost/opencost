package costmodel

import (
	"context"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/filter"
	allocationfilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	assetfilter "github.com/opencost/opencost/core/pkg/filter/asset"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/allocation"
	"github.com/opencost/opencost/pkg/asset"
	"github.com/opencost/opencost/pkg/env"
)

func (a *Accesses) ComputeAllocationAutocompleteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	qp := httputil.NewQueryParams(r.URL.Query())

	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	var parsedFilter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := allocationfilter.NewAllocationFilterParser()
		parsedFilter, err = parser.Parse(filterString)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid 'filter' parameter: %s", err), http.StatusBadRequest)
			return
		}
	}

	resp, err := a.QueryAllocationAutocomplete(allocation.AllocationAutocompleteRequest{
		Search:      qp.Get("search", ""),
		Field:       qp.Get("field", ""),
		Limit:       qp.GetInt("limit", 0),
		Window:      window,
		Filter:      parsedFilter,
		LabelConfig: opencost.NewLabelConfig(),
	}, filterString, r.Context())
	if err != nil {
		status := http.StatusInternalServerError
		if allocation.IsAutocompleteBadRequest(err) {
			status = http.StatusBadRequest
		}
		http.Error(w, fmt.Sprintf("Error getting allocation autocomplete: %s", err), status)
		return
	}

	WriteData(w, resp, nil)
}

func (a *Accesses) QueryAllocationAutocomplete(req allocation.AllocationAutocompleteRequest, filterString string, ctx context.Context) (*allocation.AllocationAutocompleteResponse, error) {
	asr, err := a.Model.QueryAllocation(req.Window, req.Window.Duration(), nil, false, false, false, false, false, opencost.AccumulateOptionNone, false, filterString)
	if err != nil {
		return nil, fmt.Errorf("error querying allocations: %w", err)
	}
	return allocation.QueryAllocationAutocompleteFromSetRange(asr, req)
}

func (a *Accesses) ComputeAssetsAutocompleteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	qp := httputil.NewQueryParams(r.URL.Query())

	window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
		return
	}

	var parsedFilter filter.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := assetfilter.NewAssetFilterParser()
		parsedFilter, err = parser.Parse(filterString)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid 'filter' parameter: %s", err), http.StatusBadRequest)
			return
		}
	}

	resp, err := a.QueryAssetAutocomplete(asset.AssetAutocompleteRequest{
		TenantID: qp.Get("tenantId", "opencost"),
		Search:   qp.Get("search", ""),
		Field:    qp.Get("field", ""),
		Limit:    qp.GetInt("limit", 0),
		Window:   window,
		Filter:   parsedFilter,
	}, r.Context())
	if err != nil {
		status := http.StatusInternalServerError
		if asset.IsAutocompleteBadRequest(err) {
			status = http.StatusBadRequest
		}
		http.Error(w, fmt.Sprintf("Error getting asset autocomplete: %s", err), status)
		return
	}

	WriteData(w, resp, nil)
}

func (a *Accesses) QueryAssetAutocomplete(req asset.AssetAutocompleteRequest, ctx context.Context) (*asset.AssetAutocompleteResponse, error) {
	assetSet, err := a.Model.ComputeAssets(*req.Window.Start(), *req.Window.End())
	if err != nil {
		return nil, fmt.Errorf("error computing assets: %w", err)
	}
	return asset.QueryAssetAutocompleteFromSet(assetSet, req)
}
