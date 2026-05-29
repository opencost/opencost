package costmodel

import (
	"context"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/allocation"
	"github.com/opencost/opencost/pkg/asset"
	"github.com/opencost/opencost/pkg/env"
)

func (a *Accesses) ComputeAllocationAutocompleteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	qp := httputil.NewQueryParams(r.URL.Query())

	offset := env.GetParsedUTCOffset()
	req, err := allocation.ParseAllocationAutocompleteRequest(qp, allocation.ParseAllocationAutocompleteOptions{
		LabelConfig: opencost.NewLabelConfig(),
		UTCOffset:   &offset,
	}, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid allocation autocomplete request: %s", err), http.StatusBadRequest)
		return
	}

	filterString := qp.Get("filter", "")
	resp, err := a.QueryAllocationAutocomplete(*req, filterString, r.Context())
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

	offset := env.GetParsedUTCOffset()
	req, err := asset.ParseAssetAutocompleteRequest(qp, asset.ParseAssetAutocompleteOptions{
		DefaultTenantID: qp.Get("tenantId", "opencost"),
		UTCOffset:       &offset,
	}, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid asset autocomplete request: %s", err), http.StatusBadRequest)
		return
	}

	resp, err := a.QueryAssetAutocomplete(*req, r.Context())
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
	if req.Window.IsOpen() || req.Window.Start() == nil || req.Window.End() == nil {
		return nil, fmt.Errorf("%w: invalid window: %s", asset.ErrAutocompleteBadRequest, req.Window.String())
	}
	assetSet, err := a.Model.ComputeAssets(*req.Window.Start(), *req.Window.End())
	if err != nil {
		return nil, fmt.Errorf("error computing assets: %w", err)
	}
	return asset.QueryAssetAutocompleteFromSet(assetSet, req)
}
