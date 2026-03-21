// test-queries is a CLI tool for testing prometheus-otel-source MetricsQuerier methods
// against a local Mimir/Prometheus instance. It validates that the actual querier
// implementation works correctly with OTel metrics.
//
// Usage:
//
//	PROMETHEUS_SERVER_ENDPOINT="http://localhost:8080/prometheus" \
//	PROMETHEUS_HEADER_X_SCOPE_ORGID="main" \
//	go run ./cmd/test-queries/main.go
package main

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/prometheus-otel-source/pkg/prom"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// testResult holds the result of executing a querier method
type testResult struct {
	Name        string
	ResultCount int
	SampleData  string
	Error       error
	Duration    time.Duration
}

func main() {
	// Setup zerolog for console output
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Logger()

	// Get configuration from environment
	endpoint := os.Getenv("PROMETHEUS_SERVER_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8080/prometheus"
		log.Warn().Msgf("PROMETHEUS_SERVER_ENDPOINT not set, using default: %s", endpoint)
	}

	tenantID := os.Getenv("PROMETHEUS_HEADER_X_SCOPE_ORGID")
	if tenantID == "" {
		log.Info().Msg("PROMETHEUS_HEADER_X_SCOPE_ORGID not set, no tenant header will be sent")
	}

	clusterLabel := os.Getenv("PROM_CLUSTER_ID_LABEL")
	if clusterLabel == "" {
		clusterLabel = "k8s_cluster_name" // OTel default
		log.Warn().Msgf("PROM_CLUSTER_ID_LABEL not set, using default: %s", clusterLabel)
	}

	clusterID := os.Getenv("CLUSTER_ID")
	clusterFilter := ""
	if clusterID != "" {
		clusterFilter = fmt.Sprintf(`%s="%s"`, clusterLabel, clusterID)
		log.Info().Msgf("Using cluster filter: %s", clusterFilter)
	} else {
		log.Warn().Msg("CLUSTER_ID not set, querying all clusters")
	}

	log.Info().
		Str("endpoint", endpoint).
		Str("tenant_id", tenantID).
		Msg("Testing prometheus-otel-source MetricsQuerier")

	// Create prometheus config manually (similar to NewOpenCostPrometheusConfigFromEnv but with our settings)
	promConfig := &promsource.OpenCostPrometheusConfig{
		ServerEndpoint:        endpoint,
		Version:               "0.0.0",
		IsOffsetResolution:    false,
		ScrapeInterval:        time.Minute,
		JobName:               "kubecost",
		Offset:                "",
		QueryOffset:           0,
		MaxQueryDuration:      24 * time.Hour,
		ClusterLabel:          clusterLabel,
		ClusterID:             clusterID,
		ClusterFilter:         clusterFilter,
		UseOTelLabels:         true, // Use OTel label names for decoding results
		DataResolution:        5 * time.Minute,
		DataResolutionMinutes: 5,
		ClientConfig: &promsource.PrometheusClientConfig{
			Timeout:               30 * time.Second,
			KeepAlive:             30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			TLSInsecureSkipVerify: false,
			QueryConcurrency:      5,
			HeaderXScopeOrgId:     tenantID,
		},
	}

	// Create prometheus client
	promClient, err := promsource.NewPrometheusClient(endpoint, promConfig.ClientConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create prometheus client")
	}

	// Create context factory and metrics querier
	promContexts := promsource.NewContextFactory(promClient, promConfig)
	querier := prom.NewPrometheusMetricsQuerierForTesting(promConfig, promClient, promContexts)

	// Debug: Test raw query
	log.Info().Msgf("Tenant ID from config: '%s'", promConfig.ClientConfig.HeaderXScopeOrgId)
	ctx := promContexts.NewNamedContext("test")
	testQuery := fmt.Sprintf(`avg(avg_over_time(k8s_node_allocatable_cpu{%s}[1h])) by (%s, k8s_node_name)`, clusterFilter, clusterLabel)
	log.Info().Msgf("Debug raw query: %s", testQuery)
	rawResult, err := ctx.RawQuery(testQuery, time.Now())
	if err != nil {
		log.Error().Err(err).Msg("Raw query failed")
	} else {
		log.Info().Msgf("Raw result (first 500 chars): %s", string(rawResult)[:min(500, len(rawResult))])
	}

	// Calculate time window (last 1 hour)
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	log.Info().
		Time("start", start).
		Time("end", end).
		Msg("Query time window")

	fmt.Println()
	fmt.Println(strings.Repeat("=", 100))
	fmt.Printf("Testing PrometheusMetricsQuerier methods against %s (tenant: %s)\n", endpoint, tenantID)
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println()

	// Define test cases - each calls a real MetricsQuerier method
	type querierTest struct {
		name string
		exec func() (int, string, error)
	}

	tests := []querierTest{
		// Node capacity/allocatable
		{
			name: "QueryNodeCPUCoresCapacity",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeCPUCoresCapacity(start, end))
			},
		},
		{
			name: "QueryNodeCPUCoresAllocatable",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeCPUCoresAllocatable(start, end))
			},
		},
		{
			name: "QueryNodeRAMBytesCapacity",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeRAMBytesCapacity(start, end))
			},
		},
		{
			name: "QueryNodeRAMBytesAllocatable",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeRAMBytesAllocatable(start, end))
			},
		},

		// Container CPU
		{
			name: "QueryCPUUsageAvg",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryCPUUsageAvg(start, end))
			},
		},
		{
			name: "QueryCPURequests",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryCPURequests(start, end))
			},
		},
		{
			name: "QueryCPULimits",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryCPULimits(start, end))
			},
		},

		// Container RAM
		{
			name: "QueryRAMUsageAvg",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryRAMUsageAvg(start, end))
			},
		},
		{
			name: "QueryRAMBytesAllocated",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryRAMBytesAllocated(start, end))
			},
		},
		{
			name: "QueryRAMRequests",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryRAMRequests(start, end))
			},
		},
		{
			name: "QueryRAMLimits",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryRAMLimits(start, end))
			},
		},

		// Pods
		{
			name: "QueryPods",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryPods(start, end))
			},
		},

		// PV/PVC
		{
			name: "QueryPVActiveMinutes",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryPVActiveMinutes(start, end))
			},
		},
		{
			name: "QueryPVCInfo",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryPVCInfo(start, end))
			},
		},
		{
			name: "QueryPVUsedAverage",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryPVUsedAverage(start, end))
			},
		},

		// Local storage
		{
			name: "QueryLocalStorageBytes",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryLocalStorageBytes(start, end))
			},
		},
		{
			name: "QueryLocalStorageUsedAvg",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryLocalStorageUsedAvg(start, end))
			},
		},

		// Node metrics
		{
			name: "QueryNodeCPUModeTotal",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeCPUModeTotal(start, end))
			},
		},
		{
			name: "QueryNodeRAMSystemPercent",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeRAMSystemPercent(start, end))
			},
		},
		{
			name: "QueryNodeRAMUserPercent",
			exec: func() (int, string, error) {
				return awaitAndFormat(querier.QueryNodeRAMUserPercent(start, end))
			},
		},
	}

	// Execute tests and collect results
	var results []testResult
	successCount := 0
	failCount := 0
	emptyCount := 0

	for _, t := range tests {
		startTime := time.Now()
		count, sample, err := t.exec()
		duration := time.Since(startTime)

		result := testResult{
			Name:        t.name,
			ResultCount: count,
			SampleData:  sample,
			Error:       err,
			Duration:    duration,
		}
		results = append(results, result)

		// Print result
		printResult(result)

		if err != nil {
			failCount++
		} else if count == 0 {
			emptyCount++
		} else {
			successCount++
		}
	}

	// Print summary
	fmt.Println()
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println("SUMMARY")
	fmt.Println(strings.Repeat("=", 100))
	fmt.Printf("Total queries:     %d\n", len(tests))
	fmt.Printf("Successful:        %d (returned data)\n", successCount)
	fmt.Printf("Empty results:     %d (no data in time window)\n", emptyCount)
	fmt.Printf("Failed:            %d (errors)\n", failCount)
	fmt.Println()

	if failCount > 0 {
		fmt.Println("FAILED QUERIES:")
		for _, r := range results {
			if r.Error != nil {
				fmt.Printf("  - %s: %v\n", r.Name, r.Error)
			}
		}
		os.Exit(1)
	}
}

// awaitAndFormat is a generic helper that awaits a Future and formats the results
func awaitAndFormat[T any](future *source.Future[T]) (int, string, error) {
	results, err := future.Await()
	if err != nil {
		return 0, "", err
	}

	count := len(results)
	sample := ""

	if count > 0 {
		// Format first few results
		samples := make([]string, 0, 3)
		for i, r := range results {
			if i >= 3 {
				break
			}
			samples = append(samples, formatResult(r))
		}
		sample = strings.Join(samples, " | ")
	}

	return count, sample, nil
}

// formatResult formats a single result using reflection to extract key fields
func formatResult(r any) string {
	if r == nil {
		return "<nil>"
	}

	v := reflect.ValueOf(r)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return "<nil>"
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return fmt.Sprintf("%v", r)
	}

	// Extract key fields - these are the common fields in source package result types
	var parts []string
	keyFields := []string{
		"Node", "Container", "Pod", "Namespace",
		"PersistentVolume", "PersistentVolumeClaim",
		"StorageClass", "VolumeName",
		"Value", "Cores", "Bytes", "Mode",
	}

	for _, fieldName := range keyFields {
		field := v.FieldByName(fieldName)
		if field.IsValid() && !field.IsZero() {
			val := field.Interface()
			// Format numeric values nicely
			switch fv := val.(type) {
			case float64:
				if fv > 1000000000 {
					parts = append(parts, fmt.Sprintf("%s=%.2fG", fieldName, fv/1000000000))
				} else if fv > 1000000 {
					parts = append(parts, fmt.Sprintf("%s=%.2fM", fieldName, fv/1000000))
				} else if fv > 1000 {
					parts = append(parts, fmt.Sprintf("%s=%.2fK", fieldName, fv/1000))
				} else {
					parts = append(parts, fmt.Sprintf("%s=%.4f", fieldName, fv))
				}
			case string:
				if fv != "" {
					parts = append(parts, fmt.Sprintf("%s=%s", fieldName, fv))
				}
			default:
				parts = append(parts, fmt.Sprintf("%s=%v", fieldName, val))
			}
		}
	}

	if len(parts) == 0 {
		// If no key fields found, show the struct summary
		t := v.Type()
		for i := 0; i < t.NumField() && i < 4; i++ {
			f := t.Field(i)
			fv := v.Field(i)
			if fv.IsValid() && !fv.IsZero() {
				parts = append(parts, fmt.Sprintf("%s=%v", f.Name, fv.Interface()))
			}
		}
	}

	if len(parts) == 0 {
		return fmt.Sprintf("%+v", r)
	}

	return strings.Join(parts, ", ")
}

// printResult prints a formatted test result
func printResult(r testResult) {
	statusIcon := "✓"
	statusColor := "\033[32m" // green

	if r.Error != nil {
		statusIcon := "✗"
		statusColor = "\033[31m" // red
		_ = statusIcon
	} else if r.ResultCount == 0 {
		statusIcon = "○"
		statusColor = "\033[33m" // yellow
	}

	reset := "\033[0m"

	fmt.Printf("%s%s%s %-35s | %4d results | %6.2fs\n",
		statusColor, statusIcon, reset,
		r.Name,
		r.ResultCount,
		r.Duration.Seconds())

	if r.Error != nil {
		fmt.Printf("    Error: %v\n", r.Error)
	} else if r.ResultCount > 0 && r.SampleData != "" {
		// Truncate sample data if too long
		sample := r.SampleData
		if len(sample) > 100 {
			sample = sample[:97] + "..."
		}
		fmt.Printf("    Sample: %s\n", sample)
	}
	fmt.Println()
}
