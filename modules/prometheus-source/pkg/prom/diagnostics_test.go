package prom

import (
	"strings"
	"testing"
)

func TestNewDiagnostic_PlaceholderCounting(t *testing.T) {
	testCases := []struct {
		name           string
		definition     *diagnosticDefinition
		filter         string
		offset         string
		expectedQuery  string
	}{
		{
			name: "single filter and offset pair",
			definition: &diagnosticDefinition{
				ID:       "test_single",
				QueryFmt: `absent_over_time(container_cpu_usage_seconds_total{%s}[5m] %s)`,
				Label:    "Test Single",
			},
			filter:        `container="test"`,
			offset:        `offset 1h`,
			expectedQuery: `absent_over_time(container_cpu_usage_seconds_total{container="test"}[5m] offset 1h)`,
		},
		{
			name: "two filter and offset pairs (like CPUThrottling)",
			definition: &diagnosticDefinition{
				ID: CPUThrottlingDiagnosticMetricID,
				QueryFmt: `avg(increase(container_cpu_cfs_throttled_periods_total{container="cost-model", %s}[10m] %s)) by (container_name, pod_name, namespace)
/ avg(increase(container_cpu_cfs_periods_total{container="cost-model",%s}[10m] %s)) by (container_name, pod_name, namespace) > 0.2`,
				Label: "CPU Throttling",
			},
			filter: `cluster="test-cluster"`,
			offset: `offset 2h`,
			expectedQuery: `avg(increase(container_cpu_cfs_throttled_periods_total{container="cost-model", cluster="test-cluster"}[10m] offset 2h)) by (container_name, pod_name, namespace)
/ avg(increase(container_cpu_cfs_periods_total{container="cost-model",cluster="test-cluster"}[10m] offset 2h)) by (container_name, pod_name, namespace) > 0.2`,
		},
		{
			name: "empty filter and offset",
			definition: &diagnosticDefinition{
				ID:       "test_empty",
				QueryFmt: `test_metric{%s}[5m] %s`,
				Label:    "Test Empty",
			},
			filter:        "",
			offset:        "",
			expectedQuery: `test_metric{}[5m] `,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			diagnostic := tc.definition.NewDiagnostic(tc.filter, tc.offset)

			if diagnostic.Query != tc.expectedQuery {
				t.Errorf("Query mismatch.\nExpected:\n%s\n\nGot:\n%s", tc.expectedQuery, diagnostic.Query)
			}

			if diagnostic.ID != tc.definition.ID {
				t.Errorf("ID mismatch. Expected %s, got %s", tc.definition.ID, diagnostic.ID)
			}

			if diagnostic.Label != tc.definition.Label {
				t.Errorf("Label mismatch. Expected %s, got %s", tc.definition.Label, diagnostic.Label)
			}
		})
	}
}

func TestNewDiagnostic_AllDefinitions(t *testing.T) {
	// Test that all predefined diagnostic definitions work correctly
	filter := `cluster="test"`
	offset := `offset 1h`

	for id, definition := range diagnosticDefinitions {
		t.Run(id, func(t *testing.T) {
			diagnostic := definition.NewDiagnostic(filter, offset)

			// Verify the query was formatted without panicking
			if diagnostic.Query == "" {
				t.Errorf("Query should not be empty for definition %s", id)
			}

			// Verify no %s placeholders remain
			if strings.Contains(diagnostic.Query, "%s") {
				t.Errorf("Query still contains unformatted %%s placeholders: %s", diagnostic.Query)
			}

			// Verify filter was substituted
			if !strings.Contains(diagnostic.Query, filter) && strings.Count(definition.QueryFmt, "%s") > 0 {
				t.Errorf("Query should contain filter '%s': %s", filter, diagnostic.Query)
			}

			// Verify offset was substituted (for queries with 2+ placeholders)
			if strings.Count(definition.QueryFmt, "%s") >= 2 && !strings.Contains(diagnostic.Query, offset) {
				t.Errorf("Query should contain offset '%s': %s", offset, diagnostic.Query)
			}
		})
	}
}
