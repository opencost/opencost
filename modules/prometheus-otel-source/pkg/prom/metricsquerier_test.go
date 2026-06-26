package prom

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// spyPromClient is a Prometheus client that captures the query strings sent to it
// and synchronizes the goroutine-based query execution with the test goroutine.
type spyPromClient struct {
	mu      sync.Mutex
	queries []string
	done    chan struct{}
}

func newSpyPromClient() *spyPromClient {
	return &spyPromClient{
		done: make(chan struct{}, 100),
	}
}

func (s *spyPromClient) URL(ep string, args map[string]string) *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   "localhost:9090",
		Path:   ep,
	}
}

func (s *spyPromClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	q := req.URL.Query().Get("query")
	s.mu.Lock()
	if q != "" {
		s.queries = append(s.queries, q)
	}
	s.mu.Unlock()

	s.done <- struct{}{}

	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, body, nil
}

func (s *spyPromClient) waitForQuery(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for query to be captured")
	}
}

func (s *spyPromClient) lastQuery() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queries) == 0 {
		return ""
	}
	return s.queries[len(s.queries)-1]
}

func (s *spyPromClient) reset() {
	s.mu.Lock()
	s.queries = nil
	s.mu.Unlock()
	// drain the done channel
	for {
		select {
		case <-s.done:
		default:
			return
		}
	}
}

func newTestQuerier(spy *spyPromClient) *PrometheusMetricsQuerier {
	config := &promsource.OpenCostPrometheusConfig{
		ServerEndpoint:        "http://localhost:9090",
		ClusterLabel:          "cluster_id",
		ClusterFilter:         `cluster_id="test-cluster"`,
		DataResolution:        5 * time.Minute,
		DataResolutionMinutes: 5,
		IsOffsetResolution:    false,
		UseOTelLabels:         true,
	}
	contextFactory := promsource.NewContextFactory(spy, config)
	return newPrometheusMetricsQuerier(config, spy, contextFactory)
}

var (
	testEnd   = time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	testStart = testEnd.Add(-1 * time.Hour)
)

// captureQuery calls a query function, waits for the spy to capture the query,
// and returns the captured query string.
func captureQuery(t *testing.T, spy *spyPromClient, fn func()) string {
	t.Helper()
	spy.reset()
	fn()
	spy.waitForQuery(t)
	q := spy.lastQuery()
	if q == "" {
		t.Fatal("expected a query to be captured, got empty")
	}
	return q
}

// --- Individual OTel Label Tests ---

func TestQueryRAMBytesAllocated_UsesOTelLabels(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)
	q := captureQuery(t, spy, func() { querier.QueryRAMBytesAllocated(testStart, testEnd) })

	for _, label := range []string{"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name"} {
		if !strings.Contains(q, label) {
			t.Errorf("expected OTel label %q in query, got:\n%s", label, q)
		}
	}
	if !strings.Contains(q, "container_memory_usage") {
		t.Errorf("expected metric 'container_memory_usage' in query, got:\n%s", q)
	}
}

func TestQueryCPUUsageAvg_UsesOTelLabels(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)
	q := captureQuery(t, spy, func() { querier.QueryCPUUsageAvg(testStart, testEnd) })

	if !strings.Contains(q, "container_cpu_time") {
		t.Errorf("expected metric 'container_cpu_time' in query, got:\n%s", q)
	}
	for _, label := range []string{"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name"} {
		if !strings.Contains(q, label) {
			t.Errorf("expected OTel label %q in query, got:\n%s", label, q)
		}
	}
	if !strings.Contains(q, "rate(") {
		t.Errorf("expected rate() function in query, got:\n%s", q)
	}
}

func TestQueryNodeCPUCoresAllocatable_UsesOTelLabels(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)
	q := captureQuery(t, spy, func() { querier.QueryNodeCPUCoresAllocatable(testStart, testEnd) })

	if !strings.Contains(q, "k8s_node_allocatable_cpu") {
		t.Errorf("expected metric 'k8s_node_allocatable_cpu' in query, got:\n%s", q)
	}
	if !strings.Contains(q, "k8s_node_name") {
		t.Errorf("expected 'k8s_node_name' label in query, got:\n%s", q)
	}
}

func TestQueryNodeCPUModeTotal_UsesOTelLabels(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)
	q := captureQuery(t, spy, func() { querier.QueryNodeCPUModeTotal(testStart, testEnd) })

	if !strings.Contains(q, "system_cpu_time") {
		t.Errorf("expected metric 'system_cpu_time' in query, got:\n%s", q)
	}
	if !strings.Contains(q, "k8s_node_name") {
		t.Errorf("expected 'k8s_node_name' in query, got:\n%s", q)
	}
	if !strings.Contains(q, "state") {
		t.Errorf("expected 'state' label in query, got:\n%s", q)
	}
	if strings.Contains(q, "node_cpu_seconds_total") {
		t.Errorf("unexpected classic metric 'node_cpu_seconds_total' in query, got:\n%s", q)
	}
}

func TestQueryNetTransferBytes_UsesOTelLabels(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)
	q := captureQuery(t, spy, func() { querier.QueryNetTransferBytes(testStart, testEnd) })

	if !strings.Contains(q, "k8s_pod_network_io") {
		t.Errorf("expected metric 'k8s_pod_network_io' in query, got:\n%s", q)
	}
	if !strings.Contains(q, "k8s_pod_name") {
		t.Errorf("expected 'k8s_pod_name' in query, got:\n%s", q)
	}
	if !strings.Contains(q, "k8s_namespace_name") {
		t.Errorf("expected 'k8s_namespace_name' in query, got:\n%s", q)
	}
	if !strings.Contains(q, `direction="transmit"`) {
		t.Errorf("expected direction=\"transmit\" filter in query, got:\n%s", q)
	}
	if strings.Contains(q, "container_network_transmit_bytes_total") {
		t.Errorf("unexpected classic metric in query, got:\n%s", q)
	}
}

func TestQueryNetReceiveBytes_UsesOTelLabels(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)
	q := captureQuery(t, spy, func() { querier.QueryNetReceiveBytes(testStart, testEnd) })

	if !strings.Contains(q, "k8s_pod_network_io") {
		t.Errorf("expected metric 'k8s_pod_network_io' in query, got:\n%s", q)
	}
	if !strings.Contains(q, `direction="receive"`) {
		t.Errorf("expected direction=\"receive\" filter in query, got:\n%s", q)
	}
}

// --- Table-driven OTel Label Presence Tests ---

func TestOTelLabelUsageInQueries(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)

	tests := []struct {
		name           string
		queryFunc      func()
		expectContains []string
		expectAbsent   []string
	}{
		{
			name:      "QueryRAMRequests",
			queryFunc: func() { querier.QueryRAMRequests(testStart, testEnd) },
			expectContains: []string{
				"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name",
				"kube_pod_container_resource_requests",
			},
		},
		{
			name:      "QueryRAMLimits",
			queryFunc: func() { querier.QueryRAMLimits(testStart, testEnd) },
			expectContains: []string{
				"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name",
				"k8s_container_memory_limit",
			},
		},
		{
			name:      "QueryCPUCoresAllocated",
			queryFunc: func() { querier.QueryCPUCoresAllocated(testStart, testEnd) },
			expectContains: []string{
				"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name",
				"container_cpu_allocation",
			},
		},
		{
			name:      "QueryCPURequests",
			queryFunc: func() { querier.QueryCPURequests(testStart, testEnd) },
			expectContains: []string{
				"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name",
				"kube_pod_container_resource_requests",
			},
		},
		{
			name:      "QueryCPULimits",
			queryFunc: func() { querier.QueryCPULimits(testStart, testEnd) },
			expectContains: []string{
				"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name",
				"k8s_container_cpu_limit",
			},
		},
		{
			name:      "QueryNodeRAMBytesAllocatable",
			queryFunc: func() { querier.QueryNodeRAMBytesAllocatable(testStart, testEnd) },
			expectContains: []string{
				"k8s_node_name",
				"k8s_node_allocatable_memory",
			},
		},
		{
			name:      "QueryNodeRAMBytesCapacity",
			queryFunc: func() { querier.QueryNodeRAMBytesCapacity(testStart, testEnd) },
			expectContains: []string{
				"k8s_node_name",
				"kube_node_status_capacity",
			},
		},
		{
			name:      "QueryRAMUsageAvg",
			queryFunc: func() { querier.QueryRAMUsageAvg(testStart, testEnd) },
			expectContains: []string{
				"k8s_container_name", "k8s_pod_name", "k8s_namespace_name", "k8s_node_name",
				"container_memory_working_set",
			},
		},
		{
			name:      "QueryPods",
			queryFunc: func() { querier.QueryPods(testStart, testEnd) },
			expectContains: []string{
				"k8s_pod_name", "k8s_namespace_name",
			},
		},
		{
			name:      "QueryNodeRAMSystemPercent",
			queryFunc: func() { querier.QueryNodeRAMSystemPercent(testStart, testEnd) },
			expectContains: []string{
				"k8s_node_name",
				"container_memory_working_set",
				"k8s_node_allocatable_memory",
				"k8s_namespace_name",
			},
			expectAbsent: []string{
				"node_memory_MemTotal_bytes",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := captureQuery(t, spy, tc.queryFunc)

			for _, expected := range tc.expectContains {
				if !strings.Contains(q, expected) {
					t.Errorf("expected %q in query, got:\n%s", expected, q)
				}
			}
			for _, absent := range tc.expectAbsent {
				if strings.Contains(q, absent) {
					t.Errorf("unexpected %q in query, got:\n%s", absent, q)
				}
			}
		})
	}
}

// --- Classic Label Absence Tests ---

func TestNoClassicLabelsInContainerQueries(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)

	containerQueries := []struct {
		name      string
		queryFunc func()
	}{
		{"QueryRAMBytesAllocated", func() { querier.QueryRAMBytesAllocated(testStart, testEnd) }},
		{"QueryCPUUsageAvg", func() { querier.QueryCPUUsageAvg(testStart, testEnd) }},
		{"QueryCPUCoresAllocated", func() { querier.QueryCPUCoresAllocated(testStart, testEnd) }},
		{"QueryRAMUsageAvg", func() { querier.QueryRAMUsageAvg(testStart, testEnd) }},
		{"QueryRAMUsageMax", func() { querier.QueryRAMUsageMax(testStart, testEnd) }},
	}

	// Classic labels that should NOT appear as standalone selectors or group-by labels.
	classicPatterns := []string{
		`container="`,   // classic container label selector
		`pod="`,         // classic pod label selector
		`namespace="`,   // classic namespace label selector
		`, container,`,  // classic container in by-clause
		`, pod,`,        // classic pod in by-clause
		`, namespace,`,  // classic namespace in by-clause
		`by (container`, // classic container starting by-clause
		`by (pod`,       // classic pod starting by-clause
		`by (namespace`, // classic namespace starting by-clause
	}

	for _, tc := range containerQueries {
		t.Run(tc.name, func(t *testing.T) {
			q := captureQuery(t, spy, tc.queryFunc)

			for _, pattern := range classicPatterns {
				if strings.Contains(q, pattern) {
					t.Errorf("found classic label pattern %q in query:\n%s", pattern, q)
				}
			}
		})
	}
}

// --- durationStringFor Tests ---

func TestDurationStringFor(t *testing.T) {
	tests := []struct {
		name               string
		start              time.Time
		end                time.Time
		minsPerResolution  int
		extrapolated       bool
		isOffsetResolution bool
		expected           string
	}{
		{
			name:               "1h duration, no offset",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       false,
			isOffsetResolution: false,
			expected:           "1h",
		},
		{
			name:               "1h duration, with offset resolution (60+5=65m)",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       false,
			isOffsetResolution: true,
			expected:           "65m",
		},
		{
			name:               "1h duration, offset resolution but extrapolated (no offset added)",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       true,
			isOffsetResolution: true,
			expected:           "1h",
		},
		{
			name:               "24h duration, no offset",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       false,
			isOffsetResolution: false,
			expected:           "1d",
		},
		{
			name:               "24h duration, with offset (1440+5=1445m)",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       false,
			isOffsetResolution: true,
			expected:           "1445m",
		},
		{
			name:               "2h duration, 10m resolution, with offset (120+10=130m)",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
			minsPerResolution:  10,
			extrapolated:       false,
			isOffsetResolution: true,
			expected:           "130m",
		},
		{
			name:               "30m duration, no offset",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       false,
			isOffsetResolution: false,
			expected:           "30m",
		},
		{
			name:               "2h duration, no offset",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
			minsPerResolution:  5,
			extrapolated:       false,
			isOffsetResolution: false,
			expected:           "2h",
		},
		{
			name:               "2h with 60m resolution and offset (120+60=180m=3h)",
			start:              time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
			minsPerResolution:  60,
			extrapolated:       false,
			isOffsetResolution: true,
			expected:           "3h",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spy := newSpyPromClient()
			config := &promsource.OpenCostPrometheusConfig{
				ServerEndpoint:        "http://localhost:9090",
				ClusterLabel:          "cluster_id",
				ClusterFilter:         `cluster_id="test-cluster"`,
				DataResolution:        time.Duration(tc.minsPerResolution) * time.Minute,
				DataResolutionMinutes: tc.minsPerResolution,
				IsOffsetResolution:    tc.isOffsetResolution,
				UseOTelLabels:         true,
			}
			contextFactory := promsource.NewContextFactory(spy, config)
			querier := newPrometheusMetricsQuerier(config, spy, contextFactory)

			result := querier.durationStringFor(tc.start, tc.end, tc.minsPerResolution, tc.extrapolated)
			if result != tc.expected {
				t.Errorf("durationStringFor(%v, %v, %d, %v) = %q, want %q",
					tc.start, tc.end, tc.minsPerResolution, tc.extrapolated, result, tc.expected)
			}
		})
	}
}

// --- Cluster Filter Tests ---

func TestClusterFilterInQueries(t *testing.T) {
	spy := newSpyPromClient()
	querier := newTestQuerier(spy)

	queries := []struct {
		name      string
		queryFunc func()
	}{
		{"QueryRAMBytesAllocated", func() { querier.QueryRAMBytesAllocated(testStart, testEnd) }},
		{"QueryNodeCPUCoresAllocatable", func() { querier.QueryNodeCPUCoresAllocatable(testStart, testEnd) }},
		{"QueryNetTransferBytes", func() { querier.QueryNetTransferBytes(testStart, testEnd) }},
		{"QueryNodeCPUModeTotal", func() { querier.QueryNodeCPUModeTotal(testStart, testEnd) }},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			q := captureQuery(t, spy, tc.queryFunc)

			if !strings.Contains(q, `cluster_id="test-cluster"`) {
				t.Errorf("expected cluster filter in query, got:\n%s", q)
			}
		})
	}
}
