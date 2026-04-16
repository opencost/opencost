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
	Querier          Querier
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

// convertCustomCostResponse converts all cost values in a CostResponse from USD to target currency
func convertCustomCostResponse(resp *CostResponse, converter currency.Converter, targetCurrency string) error {
	if resp == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))

	// Convert TotalCost
	if resp.TotalCost != 0 {
		converted, err := converter.Convert(float64(resp.TotalCost), "USD", targetCurrency)
		if err != nil {
			return fmt.Errorf("failed to convert TotalCost: %w", err)
		}
		resp.TotalCost = float32(converted)
	}

	// Convert all CustomCost items
	for _, cc := range resp.CustomCosts {
		if cc == nil {
			continue
		}
		// Convert Cost
		if cc.Cost != 0 {
			converted, err := converter.Convert(float64(cc.Cost), "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert CustomCost.Cost: %w", err)
			}
			cc.Cost = float32(converted)
		}
		// Convert ListUnitPrice
		if cc.ListUnitPrice != 0 {
			converted, err := converter.Convert(float64(cc.ListUnitPrice), "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert CustomCost.ListUnitPrice: %w", err)
			}
			cc.ListUnitPrice = float32(converted)
		}
	}

	return nil
}

// convertCustomCostTimeseriesResponse converts all cost values in a CostTimeseriesResponse
func convertCustomCostTimeseriesResponse(resp *CostTimeseriesResponse, converter currency.Converter, targetCurrency string) error {
	if resp == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	// Convert each CostResponse in the timeseries
	for _, costResp := range resp.Timeseries {
		if err := convertCustomCostResponse(costResp, converter, targetCurrency); err != nil {
			return err
		}
	}

	return nil
}
