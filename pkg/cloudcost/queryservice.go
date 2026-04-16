package cloudcost

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/currency"
	"go.opentelemetry.io/otel"
)

const tracerName = "github.com/opencost/ooencost/pkg/cloudcost"

const (
	csvFormat = "csv"
)

// QueryService surfaces endpoints for accessing CloudCost data in raw form or for display in views
type QueryService struct {
	Querier          Querier
	ViewQuerier      ViewQuerier
	CurrencyConverter currency.Converter
}

func NewQueryService(querier Querier, viewQuerier ViewQuerier) *QueryService {
	return &QueryService{
		Querier:     querier,
		ViewQuerier: viewQuerier,
	}
}

func (s *QueryService) GetCloudCostHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.Querier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.Querier.Query(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		// Extract currency parameter and convert if needed
		currencyParam := strings.ToUpper(strings.TrimSpace(qp.Get("currency", "USD")))
		if currencyParam != "USD" && s.CurrencyConverter != nil && resp != nil {
			err = convertCloudCostSetRange(resp, s.CurrencyConverter, currencyParam)
			if err != nil {
				log.Warnf("Currency conversion failed for currency %s: %v", currencyParam, err)
				// Continue with USD values if conversion fails
			}
		}

		_, spanResp := tracer.Start(ctx, "write response")
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
		spanResp.End()
	}
}

func (s *QueryService) GetCloudCostViewGraphHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostViewGraphHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.ViewQuerier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostViewRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.ViewQuerier.QueryViewGraph(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		_, spanResp := tracer.Start(ctx, "write response")
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
		spanResp.End()
	}
}

func (s *QueryService) GetCloudCostViewTotalsHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostViewTotalsHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.ViewQuerier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostViewRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.ViewQuerier.QueryViewTotals(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		_, spanResp := tracer.Start(ctx, "write response")
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
		spanResp.End()
	}
}

func (s *QueryService) GetCloudCostViewTableHandler(tokenHook func(ViewTableRows) string) func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCloudCostViewTableHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if s == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if s.ViewQuerier == nil {
			http.Error(w, "CloudCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCloudCostViewRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		format := qp.Get("format", "json")
		if strings.HasPrefix(format, csvFormat) {
			w.Header().Set("Content-Type", "text/csv")
			w.Header().Set("Transfer-Encoding", "chunked")
		} else {
			// By default, send JSON
			w.Header().Set("Content-Type", "application/json")
		}

		rows, err := s.ViewQuerier.QueryViewTable(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		resp := protocol.NewResponse().WithData(rows)

		if tokenHook != nil {
			resp = resp.WithMeta(map[string]any{
				"token": tokenHook(rows),
			})
		}

		_, spanResp := tracer.Start(ctx, "write response")
		defer spanResp.End()
		if format == csvFormat {
			window := opencost.NewClosedWindow(request.Start, request.End)
			writeCloudCostViewTableRowsAsCSV(w, rows, window.String())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteResponse(w, resp)
	}
}

// convertCloudCostSetRange converts all cloud costs in a range from USD
// to target currency in place. Best-effort: per-field converter failures
// are logged and the field is left in USD rather than rolling the whole
// response back to USD. Only CostMetric.Cost is mutated; KubernetesPercent
// and other non-cost fields are untouched.
func convertCloudCostSetRange(ccsr *opencost.CloudCostSetRange, converter currency.Converter, targetCurrency string) error {
	if ccsr == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))

	tryConvert := func(val float64, logCtx string) float64 {
		if val == 0 {
			return val
		}
		converted, err := converter.Convert(val, "USD", targetCurrency)
		if err != nil {
			log.Warnf("currency: leaving %s in USD (convert to %s failed): %v", logCtx, targetCurrency, err)
			return val
		}
		return converted
	}

	for _, set := range ccsr.CloudCostSets {
		if set == nil {
			continue
		}
		for _, cc := range set.CloudCosts {
			if cc == nil {
				continue
			}
			cc.ListCost.Cost = tryConvert(cc.ListCost.Cost, "CloudCost.ListCost")
			cc.NetCost.Cost = tryConvert(cc.NetCost.Cost, "CloudCost.NetCost")
			cc.AmortizedNetCost.Cost = tryConvert(cc.AmortizedNetCost.Cost, "CloudCost.AmortizedNetCost")
			cc.InvoicedCost.Cost = tryConvert(cc.InvoicedCost.Cost, "CloudCost.InvoicedCost")
			cc.AmortizedCost.Cost = tryConvert(cc.AmortizedCost.Cost, "CloudCost.AmortizedCost")
		}
	}

	return nil
}
