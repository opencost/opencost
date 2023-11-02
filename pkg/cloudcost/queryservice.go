package cloudcost

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	filter21 "github.com/opencost/opencost/pkg/filter21"
	"github.com/opencost/opencost/pkg/filter21/cloudcost"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/util/httputil"
	"go.opentelemetry.io/otel"
)

const tracerName = "github.com/opencost/ooencost/pkg/cloudcost"

const (
	csvFormat = "csv"
)

// QueryService surfaces endpoints for accessing CloudCost data in raw form or for display in views
type QueryService struct {
	Querier     Querier
	ViewQuerier ViewQuerier
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

		request, err := parseCloudCostRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.Querier.Query(*request, ctx)
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

		request, err := parseCloudCostViewRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := s.ViewQuerier.QueryViewGraph(*request, ctx)
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

type CloudCostViewTotalsResponse struct {
	NumResults int           `json:"numResults"`
	Combined   *ViewTableRow `json:"combined"`
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

		request, err := parseCloudCostViewRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		totals, count, err := s.ViewQuerier.QueryViewTotals(*request, ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		resp := CloudCostViewTotalsResponse{
			NumResults: count,
			Combined:   totals,
		}

		_, spanResp := tracer.Start(ctx, "write response")
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
		spanResp.End()
	}
}

func (s *QueryService) GetCloudCostViewTableHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
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

		request, err := parseCloudCostViewRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		qp := httputil.NewQueryParams(r.URL.Query())
		format := qp.Get("format", "json")
		if strings.HasPrefix(format, csvFormat) {
			w.Header().Set("Content-Type", "text/csv")
			w.Header().Set("Transfer-Encoding", "chunked")
		} else {
			// By default, send JSON
			w.Header().Set("Content-Type", "application/json")
		}

		resp, err := s.ViewQuerier.QueryViewTable(*request, ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("Internal server error: %s", err), http.StatusInternalServerError)
			return
		}

		_, spanResp := tracer.Start(ctx, "write response")
		defer spanResp.End()
		if format == csvFormat {
			window := kubecost.NewClosedWindow(request.Start, request.End)
			writeCloudCostViewTableRowsAsCSV(w, resp, window.String())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		protocol.WriteData(w, resp)
	}
}

func parseCloudCostRequest(r *http.Request) (*QueryRequest, error) {
	qp := httputil.NewQueryParams(r.URL.Query())

	windowStr := qp.Get("window", "")
	if windowStr == "" {
		return nil, fmt.Errorf("missing require window param")
	}

	window, err := kubecost.ParseWindowUTC(windowStr)
	if err != nil {
		return nil, fmt.Errorf("invalid window parameter: %w", err)
	}
	if window.IsOpen() {
		return nil, fmt.Errorf("invalid window parameter: %s", window.String())
	}

	aggregateByRaw := qp.GetList("aggregate", ",")
	aggregateBy := []string{}
	for _, aggBy := range aggregateByRaw {
		prop, err := ParseCloudCostProperty(aggBy)
		if err != nil {
			return nil, fmt.Errorf("error parsing aggregate by %v", err)
		}
		aggregateBy = append(aggregateBy, prop)
	}
	if len(aggregateBy) == 0 {
		aggregateBy = []string{
			kubecost.CloudCostInvoiceEntityIDProp,
			kubecost.CloudCostAccountIDProp,
			kubecost.CloudCostProviderProp,
			kubecost.CloudCostProviderIDProp,
			kubecost.CloudCostCategoryProp,
			kubecost.CloudCostServiceProp,
		}
	}

	accumulate := kubecost.ParseAccumulate(qp.Get("accumulate", ""))

	var filter filter21.Filter
	filterString := qp.Get("filter", "")
	if filterString != "" {
		parser := cloudcost.NewCloudCostFilterParser()
		filter, err = parser.Parse(filterString)
		if err != nil {
			return nil, fmt.Errorf("Parsing 'filter' parameter: %s", err)
		}
	}

	opts := &QueryRequest{
		Start:       *window.Start(),
		End:         *window.End(),
		AggregateBy: aggregateBy,
		Accumulate:  accumulate,
		Filter:      filter,
	}

	return opts, nil
}

func ParseCloudCostProperty(text string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case strings.ToLower(kubecost.CloudCostInvoiceEntityIDProp):
		return kubecost.CloudCostInvoiceEntityIDProp, nil
	case strings.ToLower(kubecost.CloudCostAccountIDProp):
		return kubecost.CloudCostAccountIDProp, nil
	case strings.ToLower(kubecost.CloudCostProviderProp):
		return kubecost.CloudCostProviderProp, nil
	case strings.ToLower(kubecost.CloudCostProviderIDProp):
		return kubecost.CloudCostProviderIDProp, nil
	case strings.ToLower(kubecost.CloudCostCategoryProp):
		return kubecost.CloudCostCategoryProp, nil
	case strings.ToLower(kubecost.CloudCostServiceProp):
		return kubecost.CloudCostServiceProp, nil
	}

	if strings.HasPrefix(text, "label:") {
		label := prom.SanitizeLabelName(strings.TrimSpace(strings.TrimPrefix(text, "label:")))
		return fmt.Sprintf("label:%s", label), nil
	}

	return "", fmt.Errorf("invalid cloud cost property: %s", text)
}

func parseCloudCostViewRequest(r *http.Request) (*ViewQueryRequest, error) {
	qr, err := parseCloudCostRequest(r)
	if err != nil {
		return nil, err
	}
	qp := httputil.NewQueryParams(r.URL.Query())

	// parse cost metric
	costMetricName, err := kubecost.ParseCostMetricName(qp.Get("costMetric", string(kubecost.CostMetricAmortizedNetCost)))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'costMetric': %w", err)
	}

	limit := qp.GetInt("limit", 0)
	offset := qp.GetInt("offset", 0)

	// parse order
	order, err := ParseSortDirection(qp.Get("sortByOrder", "desc"))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'sortByOrder: %w", err)
	}

	sortColumn, err := ParseSortField(qp.Get("sortBy", "cost"))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'sortBy': %w", err)
	}

	return &ViewQueryRequest{
		QueryRequest:     *qr,
		CostMetricName:   costMetricName,
		ChartItemsLength: DefaultChartItemsLength,
		Limit:            limit,
		Offset:           offset,
		SortDirection:    order,
		SortColumn:       sortColumn,
	}, nil
}

// CloudCostViewTableRowsToCSV takes the csv writer and writes the ViewTableRows into the writer.
func CloudCostViewTableRowsToCSV(writer *csv.Writer, ctr ViewTableRows, window string) error {
	defer writer.Flush()
	// Write the column headers
	headers := []string{
		"Name",
		"K8s Utilization",
		"Total",
		"Window",
	}
	err := writer.Write(headers)
	if err != nil {
		return fmt.Errorf("CloudCostViewTableRowsToCSV: failed to convert ViewTableRows to csv with error: %w", err)
	}

	// Write one row per entry in the ViewTableRows
	for _, row := range ctr {
		err = writer.Write([]string{
			row.Name,
			fmt.Sprintf("%.3f", row.KubernetesPercent),
			fmt.Sprintf("%.3f", row.Cost),
			window,
		})
		if err != nil {
			return fmt.Errorf("CloudCostViewTableRowsToCSV: failed to convert ViewTableRows to csv with error: %w", err)
		}
	}

	return nil
}

func writeCloudCostViewTableRowsAsCSV(w http.ResponseWriter, ctr ViewTableRows, window string) {
	writer := csv.NewWriter(w)

	err := CloudCostViewTableRowsToCSV(writer, ctr, window)
	if err != nil {
		protocol.WriteError(w, protocol.InternalServerError(err.Error()))
		return
	}
}
