package cloudcost

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strings"

	filter21 "github.com/opencost/opencost/pkg/filter21"
	"github.com/opencost/opencost/pkg/filter21/cloudcost"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/util/httputil"
)

func ParseCloudCostRequest(qp httputil.QueryParams) (*QueryRequest, error) {

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
	var aggregateBy []string
	for _, aggBy := range aggregateByRaw {
		prop, err := ParseCloudCostProperty(aggBy)
		if err != nil {
			return nil, fmt.Errorf("error parsing aggregate by %v", err)
		}
		aggregateBy = append(aggregateBy, prop)
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

func parseCloudCostViewRequest(qp httputil.QueryParams) (*ViewQueryRequest, error) {
	qr, err := ParseCloudCostRequest(qp)
	if err != nil {
		return nil, err
	}

	// parse cost metric
	costMetricName, err := kubecost.ParseCostMetricName(qp.Get("costMetric", string(kubecost.CostMetricAmortizedNetCost)))
	if err != nil {
		return nil, fmt.Errorf("error parsing 'costMetric': %w", err)
	}

	limit := qp.GetInt("limit", 0)
	if limit < 0 {
		return nil, fmt.Errorf("invalid value for limit %d", limit)
	}
	offset := qp.GetInt("offset", 0)
	if offset < 0 {
		return nil, fmt.Errorf("invalid value for offset %d", offset)
	}

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
