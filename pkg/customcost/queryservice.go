package customcost

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/currency"
	"go.opentelemetry.io/otel"
)

const tracerName = "github.com/opencost/opencost/pkg/customcost"

type QueryService struct {
	Querier           Querier
	CurrencyConverter currency.Converter
}

func NewQueryService(querier Querier) *QueryService {
	return &QueryService{
		Querier: querier,
	}
}

func (qs *QueryService) GetCustomCostTotalHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCustomCostTotalHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if qs == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if qs.Querier == nil {
			http.Error(w, "CustomCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCustomCostTotalRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := qs.Querier.QueryTotal(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		// Extract currency parameter and convert if needed
		currencyParam := strings.ToUpper(strings.TrimSpace(qp.Get("currency", "USD")))
		if currencyParam != "USD" && qs.CurrencyConverter != nil && resp != nil {
			err = convertCustomCostResponse(resp, qs.CurrencyConverter, currencyParam)
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

func (qs *QueryService) GetCustomCostTimeseriesHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(r.Context(), "Service.GetCustomCostTimeseriesHandler")
		defer span.End()

		// If Query Service is nil, always return 501
		if qs == nil {
			http.Error(w, "Query Service is nil", http.StatusNotImplemented)
			return
		}

		if qs.Querier == nil {
			http.Error(w, "CustomCost Query Service is nil", http.StatusNotImplemented)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		request, err := ParseCustomCostTimeseriesRequest(qp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := qs.Querier.QueryTimeseries(ctx, *request)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		// Extract currency parameter and convert if needed
		currencyParam := strings.ToUpper(strings.TrimSpace(qp.Get("currency", "USD")))
		if currencyParam != "USD" && qs.CurrencyConverter != nil && resp != nil {
			err = convertCustomCostTimeseriesResponse(resp, qs.CurrencyConverter, currencyParam)
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

// convertCustomCostResponse converts all cost values in a CostResponse
// from USD to target currency in place. Returns an error if the target
// rate cannot be looked up upfront (no mutation occurs). Per-field
// converter failures after the probe succeeded are handled best-effort:
// the field is left in USD and a warning is logged.
//
// CustomCost uses float32; conversion is done in float64 to avoid
// compounding precision loss, then cast back.
func convertCustomCostResponse(resp *CostResponse, converter currency.Converter, targetCurrency string) error {
	if resp == nil || converter == nil {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))
	if targetCurrency == "" || targetCurrency == "USD" {
		return nil
	}
	if _, err := converter.GetRate("USD", targetCurrency); err != nil {
		return fmt.Errorf("currency rate lookup USD->%s failed: %w", targetCurrency, err)
	}

	convertResponseFields(resp, converter, targetCurrency)
	return nil
}

// convertResponseFields applies the per-field conversion without
// re-probing the rate. The caller must have already validated the
// target currency.
func convertResponseFields(resp *CostResponse, converter currency.Converter, targetCurrency string) {
	tryConvert32 := func(val float32, logCtx string) float32 {
		if val == 0 {
			return val
		}
		converted, err := converter.Convert(float64(val), "USD", targetCurrency)
		if err != nil {
			log.Warnf("currency: leaving %s in USD (convert to %s failed): %v", logCtx, targetCurrency, err)
			return val
		}
		return float32(converted)
	}

	resp.TotalCost = tryConvert32(resp.TotalCost, "CostResponse.TotalCost")

	for _, cc := range resp.CustomCosts {
		if cc == nil {
			continue
		}
		cc.Cost = tryConvert32(cc.Cost, "CustomCost.Cost")
		cc.ListUnitPrice = tryConvert32(cc.ListUnitPrice, "CustomCost.ListUnitPrice")
	}
}

// convertCustomCostTimeseriesResponse converts all cost values in a
// CostTimeseriesResponse. Returns an error if the target rate cannot be
// looked up upfront (no mutation occurs). Per-field conversion is
// best-effort thereafter.
func convertCustomCostTimeseriesResponse(resp *CostTimeseriesResponse, converter currency.Converter, targetCurrency string) error {
	if resp == nil || converter == nil {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))
	if targetCurrency == "" || targetCurrency == "USD" {
		return nil
	}
	if _, err := converter.GetRate("USD", targetCurrency); err != nil {
		return fmt.Errorf("currency rate lookup USD->%s failed: %w", targetCurrency, err)
	}

	for _, costResp := range resp.Timeseries {
		if costResp == nil {
			continue
		}
		convertResponseFields(costResp, converter, targetCurrency)
	}

	return nil
}
