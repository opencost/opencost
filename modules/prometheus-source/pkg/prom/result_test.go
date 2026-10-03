package prom

import (
	"fmt"
	"strings"
	"testing"
)

func TestErrorFunctions(t *testing.T) {
	testCases := []struct {
		name     string
		fn       func(string, any) error
		query    string
		response any
	}{
		{
			name:     "DataFieldFormatErr",
			fn:       DataFieldFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]string{"foo": "bar"},
		},
		{
			name:     "DataPointFormatErr",
			fn:       DataPointFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: []string{"invalid"},
		},
		{
			name:     "MetricFieldDoesNotExistErr",
			fn:       MetricFieldDoesNotExistErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]any{"values": []any{}},
		},
		{
			name:     "MetricFieldFormatErr",
			fn:       MetricFieldFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: "invalid",
		},
		{
			name:     "PromUnexpectedResponseErr",
			fn:       PromUnexpectedResponseErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: nil,
		},
		{
			name:     "ResultFieldDoesNotExistErr",
			fn:       ResultFieldDoesNotExistErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]any{"resultType": "matrix"},
		},
		{
			name:     "ResultFieldFormatErr",
			fn:       ResultFieldFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: "invalid",
		},
		{
			name:     "ResultFormatErr",
			fn:       ResultFormatErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: 123,
		},
		{
			name:     "ValueFieldDoesNotExistErr",
			fn:       ValueFieldDoesNotExistErr,
			query:    "avg(node_total_hourly_cost{}) by (node, cluster, provider_id)[24h:5m]",
			response: map[string]any{"metric": map[string]any{}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn(tc.query, tc.response)
			if err == nil {
				t.Errorf("Expected error, got nil")
				return
			}

			// Verify error contains key components without being overly strict about exact wording
			if !strings.Contains(err.Error(), tc.query) {
				t.Errorf("Error message missing query string '%s': %s", tc.query, err.Error())
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("%+v", tc.response)) {
				t.Errorf("Error message missing response value '%+v': %s", tc.response, err.Error())
			}
		})
	}
}

func TestNewQueryResultsResultField(t *testing.T) {
	query := "avg(kube_pod_container_status_running{} != 0) by (pod, namespace, uid, cluster_id)[1d:5m]"

	testCases := []struct {
		name      string
		result    any
		expectErr bool
	}{
		{
			// Google Managed Prometheus returns "result": null instead of an empty
			// array for empty matrix results.
			name:      "null result is treated as empty",
			result:    nil,
			expectErr: false,
		},
		{
			name:      "empty result array",
			result:    []any{},
			expectErr: false,
		},
		{
			name:      "invalid result type",
			result:    "invalid",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryResult := map[string]any{
				"data": map[string]any{
					"resultType": "matrix",
					"result":     tc.result,
				},
			}

			qrs := NewQueryResults(query, queryResult, nil)
			if tc.expectErr {
				if qrs.Error == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if qrs.Error != nil {
				t.Errorf("Expected no error, got: %s", qrs.Error)
			}
			if len(qrs.Results) != 0 {
				t.Errorf("Expected empty results, got %d", len(qrs.Results))
			}
		})
	}
}
