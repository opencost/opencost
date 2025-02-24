package prom

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

var (
	// Static Warnings for data point parsing
	InfWarning warning = newWarning("Found Inf value parsing vector data point for metric")
	NaNWarning warning = newWarning("Found NaN value parsing vector data point for metric")
)

func DataFieldFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'data' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func DataPointFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: improperly formatted datapoint. Query: '%s'. Response: '%+v'", query, promResponse)
}

func MetricFieldDoesNotExistErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'metric' field does not exist in data result vector. Query: '%s'. Response: '%+v'", query, promResponse)
}

func MetricFieldFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'metric' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func NoDataErr(query string) error {
	return source.NewNoDataError(query)
}

func PromUnexpectedResponseErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: unexpected response. Query: '%s'. Response: '%+v'", query, promResponse)
}

func QueryResultNilErr(query string) error {
	return source.NewCommError(query)
}

func ResultFieldDoesNotExistErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'result' field does not exist. Query: '%s'. Response: '%+v'", query, promResponse)
}

func ResultFieldFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'result' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func ResultFormatErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'result' field improperly formatted. Query: '%s'. Response: '%+v'", query, promResponse)
}

func ValueFieldDoesNotExistErr(query string, promResponse interface{}) error {
	return fmt.Errorf("Error parsing Prometheus response: 'value' field does not exist in data result vector. Query: '%s'. Response: '%+v'", query, promResponse)
}

// NewQueryResultError returns a QueryResults object with an error set and does not parse a result.
func NewQueryResultError(query string, err error) *source.QueryResults {
	qrs := source.NewQueryResults(query)
	qrs.Error = err
	return qrs
}

// NewQueryResults accepts the raw prometheus query result and returns an array of
// QueryResult objects
func NewQueryResults(query string, queryResult interface{}, resultKeys *source.ResultKeys) *source.QueryResults {
	qrs := source.NewQueryResults(query)

	if queryResult == nil {
		qrs.Error = QueryResultNilErr(query)
		return qrs
	}

	data, ok := queryResult.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(query, queryResult)
		if err != nil {
			qrs.Error = err
			return qrs
		}
		qrs.Error = fmt.Errorf("%s", e)
		return qrs
	}

	// Deep Check for proper formatting
	d, ok := data.(map[string]interface{})
	if !ok {
		qrs.Error = DataFieldFormatErr(query, data)
		return qrs
	}
	resultData, ok := d["result"]
	if !ok {
		qrs.Error = ResultFieldDoesNotExistErr(query, d)
		return qrs
	}
	resultsData, ok := resultData.([]interface{})
	if !ok {
		qrs.Error = ResultFieldFormatErr(query, resultData)
		return qrs
	}

	// Result vectors from the query
	var results []*source.QueryResult

	// Parse raw results and into QueryResults
	for _, val := range resultsData {
		resultInterface, ok := val.(map[string]interface{})
		if !ok {
			qrs.Error = ResultFormatErr(query, val)
			return qrs
		}

		metricInterface, ok := resultInterface["metric"]
		if !ok {
			qrs.Error = MetricFieldDoesNotExistErr(query, resultInterface)
			return qrs
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			qrs.Error = MetricFieldFormatErr(query, metricInterface)
			return qrs
		}

		// Define label string for values to ensure that we only run labelsForMetric once
		// if we receive multiple warnings.
		var labelString string = ""

		// Determine if the result is a ranged data set or single value
		_, isRange := resultInterface["values"]

		var vectors []*util.Vector
		if !isRange {
			dataPoint, ok := resultInterface["value"]
			if !ok {
				qrs.Error = ValueFieldDoesNotExistErr(query, resultInterface)
				return qrs
			}

			// Append new data point, log warnings
			v, warn, err := parseDataPoint(query, dataPoint)
			if err != nil {
				qrs.Error = err
				return qrs
			}
			if warn != nil {
				log.DedupedWarningf(5, "%s\nQuery: %s\nLabels: %s", warn.Message(), query, labelsForMetric(metricMap))
			}

			vectors = append(vectors, v)
		} else {
			values, ok := resultInterface["values"].([]interface{})
			if !ok {
				qrs.Error = fmt.Errorf("Values field is improperly formatted")
				return qrs
			}

			// Append new data points, log warnings
			for _, value := range values {
				v, warn, err := parseDataPoint(query, value)
				if err != nil {
					qrs.Error = err
					return qrs
				}
				if warn != nil {
					if labelString == "" {
						labelString = labelsForMetric(metricMap)
					}
					log.DedupedWarningf(5, "%s\nQuery: %s\nLabels: %s", warn.Message(), query, labelString)
				}

				vectors = append(vectors, v)
			}
		}

		results = append(results, source.NewQueryResult(metricMap, vectors, resultKeys))
	}

	qrs.Results = results
	return qrs
}

// parseDataPoint parses a data point from raw prometheus query results and returns
// a new Vector instance containing the parsed data along with any warnings or errors.
func parseDataPoint(query string, dataPoint interface{}) (*util.Vector, warning, error) {
	var w warning = nil

	value, ok := dataPoint.([]interface{})
	if !ok || len(value) != 2 {
		return nil, w, DataPointFormatErr(query, dataPoint)
	}

	strVal := value[1].(string)
	v, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		return nil, w, err
	}

	// Test for +Inf and -Inf (sign: 0), Test for NaN
	if math.IsInf(v, 0) {
		w = InfWarning
		v = 0.0
	} else if math.IsNaN(v) {
		w = NaNWarning
		v = 0.0
	}

	return &util.Vector{
		Timestamp: math.Round(value[0].(float64)/10) * 10,
		Value:     v,
	}, w, nil
}

func labelsForMetric(metricMap map[string]interface{}) string {
	var pairs []string
	for k, v := range metricMap {
		pairs = append(pairs, fmt.Sprintf("%s: %+v", k, v))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}

func wrapPrometheusError(query string, qr interface{}) (string, error) {
	e, ok := qr.(map[string]interface{})["error"]
	if !ok {
		return "", PromUnexpectedResponseErr(query, qr)
	}
	eStr, ok := e.(string)
	return fmt.Sprintf("'%s' parsing query '%s'", eStr, query), nil
}
