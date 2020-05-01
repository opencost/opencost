package prom

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/kubecost/cost-model/pkg/util"
	"k8s.io/klog"
)

// QueryResultsChan is a channel of query results
type QueryResultsChan chan []*QueryResult

// Read returns query results, blocking until they are made available, and
// deferring the closure of the underlying channel
func (qrc QueryResultsChan) Read() []*QueryResult {
	defer close(qrc)
	return <-qrc
}

// QueryResult contains a single result from a prometheus query. It's common
// to refer to query results as a slice of QueryResult
type QueryResult struct {
	Metric map[string]interface{}
	Values []*util.Vector
}

// NewQueryResults accepts the raw prometheus query result and returns an array of
// QueryResult objects
func NewQueryResults(queryResult interface{}) ([]*QueryResult, error) {
	var result []*QueryResult
	if queryResult == nil {
		return nil, fmt.Errorf("[Error] nil result from prometheus, has it gone down?")
	}
	data, ok := queryResult.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(queryResult)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}

	// Deep Check for proper formatting
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Data field improperly formatted in prometheus repsonse")
	}
	resultData, ok := d["result"]
	if !ok {
		return nil, fmt.Errorf("Result field not present in prometheus response")
	}
	resultsData, ok := resultData.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result field improperly formatted in prometheus response")
	}

	// Scan Results
	for _, val := range resultsData {
		resultInterface, ok := val.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Result is improperly formatted")
		}

		metricInterface, ok := resultInterface["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}

		// Wrap execution of this lazily in case the data is not used
		labels := func() string { return labelsForMetric(metricMap) }

		// Determine if the result is a ranged data set or single value
		_, isRange := resultInterface["values"]

		var vectors []*util.Vector
		if !isRange {
			dataPoint, ok := resultInterface["value"]
			if !ok {
				return nil, fmt.Errorf("Value field does not exist in data result vector")
			}

			v, err := parseDataPoint(dataPoint, labels)
			if err != nil {
				return nil, err
			}
			vectors = append(vectors, v)
		} else {
			values, ok := resultInterface["values"].([]interface{})
			if !ok {
				return nil, fmt.Errorf("Values field is improperly formatted")
			}

			for _, value := range values {
				v, err := parseDataPoint(value, labels)
				if err != nil {
					return nil, err
				}

				vectors = append(vectors, v)
			}
		}

		result = append(result, &QueryResult{
			Metric: metricMap,
			Values: vectors,
		})
	}

	return result, nil
}

// GetString returns the requested field, or an error if it does not exist
func (qr *QueryResult) GetString(field string) (string, error) {
	f, ok := qr.Metric[field]
	if !ok {
		return "", fmt.Errorf("%s field does not exist in data result vector", field)
	}

	strField, ok := f.(string)
	if !ok {
		return "", fmt.Errorf("%s field is improperly formatted", field)
	}

	return strField, nil
}

// GetLabels returns all labels and their values from the query result
func (qr *QueryResult) GetLabels() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix label_, remove prefix, add to labels
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "label_") {
			continue
		}

		label := k[6:]
		value, ok := v.(string)
		if !ok {
			klog.V(3).Infof("Failed to parse label value for label: %s", label)
			continue
		}

		result[label] = value
	}

	return result
}

func parseDataPoint(dataPoint interface{}, labels func() string) (*util.Vector, error) {
	value, ok := dataPoint.([]interface{})
	if !ok || len(value) != 2 {
		return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
	}

	strVal := value[1].(string)
	v, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		return nil, err
	}

	// Test for +Inf and -Inf (sign: 0), Test for NaN
	if math.IsInf(v, 0) {
		klog.V(1).Infof("[Warning] Found Inf value parsing vector data point for metric: %s", labels())
		v = 0.0
	} else if math.IsNaN(v) {
		klog.V(1).Infof("[Warning] Found NaN value parsing vector data point for metric: %s", labels())
		v = 0.0
	}

	return &util.Vector{
		Timestamp: math.Round(value[0].(float64)/10) * 10,
		Value:     v,
	}, nil
}

func labelsForMetric(metricMap map[string]interface{}) string {
	var pairs []string
	for k, v := range metricMap {
		pairs = append(pairs, fmt.Sprintf("%s: %+v", k, v))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}

func wrapPrometheusError(qr interface{}) (string, error) {
	e, ok := qr.(map[string]interface{})["error"]
	if !ok {
		return "", fmt.Errorf("Unexpected response from Prometheus")
	}
	eStr, ok := e.(string)
	return eStr, nil
}
