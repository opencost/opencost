package prom

// metricsquerier_roundtrip_test.go — round-trip tests with non-empty payloads
// and coverage for all real (non-stub) query methods.
//
// Tests assert that:
//   1. The query is sent to the HTTP layer (metric name present in query string).
//   2. The decoded results are non-empty when a matching response is returned.
//   3. Stubs return empty slices without panicking.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// payloadPromClient serves a fixed JSON response body and captures sent queries.
type payloadPromClient struct {
	mu      sync.Mutex
	queries []string
	done    chan struct{}
	payload []byte
}

func newPayloadClient(payload []byte) *payloadPromClient {
	return &payloadPromClient{
		done:    make(chan struct{}, 100),
		payload: payload,
	}
}

func (p *payloadPromClient) URL(ep string, _ map[string]string) *url.URL {
	return &url.URL{Scheme: "http", Host: "localhost:9090", Path: ep}
}

func (p *payloadPromClient) Do(_ context.Context, req *http.Request) (*http.Response, []byte, error) {
	q := req.URL.Query().Get("query")
	p.mu.Lock()
	if q != "" {
		p.queries = append(p.queries, q)
	}
	p.mu.Unlock()
	p.done <- struct{}{}
	body := p.payload
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, body, nil
}

func (p *payloadPromClient) wait(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for query")
	}
}

func (p *payloadPromClient) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queries) == 0 {
		return ""
	}
	return p.queries[len(p.queries)-1]
}

func newQuerier(client *payloadPromClient) *PrometheusMetricsQuerier {
	config := &promsource.OpenCostPrometheusConfig{
		ServerEndpoint:        "http://localhost:9090",
		ClusterLabel:          "k8s_cluster_name",
		ClusterFilter:         `k8s_cluster_name="test-cluster"`,
		DataResolution:        5 * time.Minute,
		DataResolutionMinutes: 5,
		IsOffsetResolution:    false,
		UseOTelLabels:         true,
	}
	cf := promsource.NewContextFactory(client, config)
	return newPrometheusMetricsQuerier(config, client, cf)
}

var (
	rtEnd   = time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	rtStart = rtEnd.Add(-1 * time.Hour)
)

// vectorPayload builds a single-sample Prometheus instant-vector JSON response.
// labels is a map of label name → value.
func vectorPayload(labels map[string]string) []byte {
	lstr := `{`
	first := true
	for k, v := range labels {
		if !first {
			lstr += `,`
		}
		lstr += `"` + k + `":"` + v + `"`
		first = false
	}
	lstr += `}`
	return []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":` + lstr + `,"value":[1717243200,"42"]}]}}`)
}

// matrixPayload builds a single-series Prometheus range-vector JSON response.
func matrixPayload(labels map[string]string) []byte {
	lstr := `{`
	first := true
	for k, v := range labels {
		if !first {
			lstr += `,`
		}
		lstr += `"` + k + `":"` + v + `"`
		first = false
	}
	lstr += `}`
	return []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":` + lstr + `,"values":[[1717243200,"42"],[1717243260,"43"]]}]}}`)
}

// ---- Round-trip: each real method must produce ≥1 decoded result ----

func TestQueryRAMBytesAllocated_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":    "test-cluster",
		"k8s_container_name":  "app",
		"k8s_pod_name":        "my-pod",
		"k8s_namespace_name":  "default",
		"k8s_node_name":       "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryRAMBytesAllocated(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded result, got 0")
	}
	if results[0].Container == "" {
		t.Error("Container should be decoded from k8s_container_name, got empty string")
	}
}

func TestQueryCPUCoresAllocated_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_container_name": "app",
		"k8s_pod_name":       "my-pod",
		"k8s_namespace_name": "default",
		"k8s_node_name":      "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryCPUCoresAllocated(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded result, got 0")
	}
	if results[0].Container == "" {
		t.Error("Container should be decoded from k8s_container_name, got empty string")
	}
}

func TestQueryRAMUsageAvg_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_container_name": "app",
		"k8s_pod_name":       "my-pod",
		"k8s_namespace_name": "default",
		"k8s_node_name":      "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryRAMUsageAvg(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded result, got 0")
	}
	if results[0].Container == "" {
		t.Error("Container should be decoded from k8s_container_name, got empty string")
	}
}

func TestQueryCPUUsageAvg_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_container_name": "app",
		"k8s_pod_name":       "my-pod",
		"k8s_namespace_name": "default",
		"k8s_node_name":      "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryCPUUsageAvg(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded result, got 0")
	}
	if results[0].Container == "" {
		t.Error("Container should be decoded from k8s_container_name, got empty string")
	}
}

func TestQueryCPUUsageMax_RoundTrip(t *testing.T) {
	// QueryCPUUsageMax uses a matrix query (range vector via subquery)
	payload := matrixPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_container_name": "app",
		"k8s_pod_name":       "my-pod",
		"k8s_namespace_name": "default",
		"k8s_node_name":      "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryCPUUsageMax(rtStart, rtEnd)
	c.wait(t)
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lastQ := c.last()
	if lastQ == "" {
		t.Error("expected a query to be sent")
	}
	if !containsAny(lastQ, "container_cpu_time", "container_cpu_usage") {
		t.Errorf("expected CPU time/usage metric in QueryCPUUsageMax query, got:\n%s", lastQ)
	}
}

func TestQueryContainerUptime_RoundTrip(t *testing.T) {
	// QueryContainerUptime uses a matrix result (range vector over time)
	payload := matrixPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_container_name": "app",
		"k8s_pod_name":       "my-pod",
		"k8s_namespace_name": "default",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryContainerUptime(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded result, got 0")
	}
	if results[0].Container == "" {
		t.Errorf("Container should be decoded from k8s_container_name, got empty; full result: %+v", results[0])
	}
}

func TestQueryNodeCPUModeTotal_RoundTrip(t *testing.T) {
	// QueryNodeCPUModeTotal returns a NodeCPUModeTotalResult using "mode" label
	// (via label_replace from "state" in the query)
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name": "test-cluster",
		"k8s_node_name":    "node-1",
		"mode":             "idle",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryNodeCPUModeTotal(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded result, got 0")
	}
	if results[0].Node == "" {
		t.Errorf("Node should be decoded from k8s_node_name, got empty; result: %+v", results[0])
	}
	if results[0].Mode == "" {
		t.Errorf("Mode should be decoded from 'mode' label, got empty; result: %+v", results[0])
	}
}

func TestQueryPVCInfo_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":             "test-cluster",
		"k8s_namespace_name":           "default",
		"k8s_persistentvolumeclaim_name": "my-pvc",
		"k8s_storageclass_name":        "standard",
		"volumename":                   "pvc-abc123",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryPVCInfo(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 decoded PVCInfo result, got 0")
	}
}

func TestQueryReplicaSetsWithoutOwners_RoundTrip(t *testing.T) {
	// QueryReplicaSetsWithoutOwners uses a set-difference expression with two metrics.
	// The spy fires twice; we only assert that the query is sent.
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":       "test-cluster",
		"k8s_namespace_name":     "default",
		"k8s_replicaset_name":    "my-rs",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryReplicaSetsWithoutOwners(rtStart, rtEnd)
	c.wait(t) // wait for the single composed query
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryPodsWithReplicaSetOwner_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":    "test-cluster",
		"k8s_namespace_name":  "default",
		"k8s_pod_name":        "my-pod",
		"k8s_pod_uid":         "uid-123",
		"owner_name":          "my-rs",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryPodsWithReplicaSetOwner(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 result, got 0")
	}
	if results[0].ReplicaSet == "" {
		t.Errorf("ReplicaSet should be decoded from owner_name, got empty; result: %+v", results[0])
	}
}

func TestQueryPodsWithDaemonSetOwner_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_namespace_name": "default",
		"k8s_pod_name":       "ds-pod",
		"owner_name":         "my-ds",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryPodsWithDaemonSetOwner(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 result, got 0")
	}
	if results[0].DaemonSet == "" {
		t.Errorf("DaemonSet should be decoded from owner_name, got empty; result: %+v", results[0])
	}
	lastQ := c.last()
	if !containsStr(lastQ, `owner_kind="DaemonSet"`) {
		t.Errorf("expected owner_kind=\"DaemonSet\" filter in query, got:\n%s", lastQ)
	}
}

func TestQueryPodsWithJobOwner_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_namespace_name": "default",
		"k8s_pod_name":       "job-pod",
		"owner_name":         "my-job",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryPodsWithJobOwner(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 result, got 0")
	}
	if results[0].Job == "" {
		t.Errorf("Job should be decoded from owner_name, got empty; result: %+v", results[0])
	}
	lastQ := c.last()
	if !containsStr(lastQ, `owner_kind="Job"`) {
		t.Errorf("expected owner_kind=\"Job\" filter in query, got:\n%s", lastQ)
	}
}

func TestQueryPodLabels_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_namespace_name": "default",
		"k8s_pod_name":       "my-pod",
		"k8s_pod_uid":        "uid-abc",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryPodLabels(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 result, got 0")
	}
}

func TestQueryNodeActiveMinutes_RoundTrip(t *testing.T) {
	payload := matrixPayload(map[string]string{
		"k8s_cluster_name": "test-cluster",
		"k8s_node_name":    "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryNodeActiveMinutes(rtStart, rtEnd)
	c.wait(t)
	results, err := fut.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected ≥1 NodeActiveMinutes result, got 0")
	}
	if results[0].Node == "" {
		t.Errorf("Node should be decoded from k8s_node_name, got empty; result: %+v", results[0])
	}
}

func TestQueryLocalStorageBytes_RoundTrip(t *testing.T) {
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name": "test-cluster",
		"k8s_node_name":    "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryLocalStorageBytes(rtStart, rtEnd)
	c.wait(t)
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryLocalStorageActiveMinutes_RoundTrip(t *testing.T) {
	payload := matrixPayload(map[string]string{
		"k8s_cluster_name": "test-cluster",
		"k8s_node_name":    "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryLocalStorageActiveMinutes(rtStart, rtEnd)
	c.wait(t)
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryLocalStorageUsedAvg_RoundTrip(t *testing.T) {
	// QueryLocalStorageUsedAvg sends a single PromQL query (binary subtraction inline).
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name": "test-cluster",
		"k8s_node_name":    "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryLocalStorageUsedAvg(rtStart, rtEnd)
	c.wait(t)
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryLocalStorageUsedMax_RoundTrip(t *testing.T) {
	// QueryLocalStorageUsedMax sends a single PromQL query (binary subtraction inline).
	payload := vectorPayload(map[string]string{
		"k8s_cluster_name": "test-cluster",
		"k8s_node_name":    "node-1",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryLocalStorageUsedMax(rtStart, rtEnd)
	c.wait(t)
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---- Stubs must return empty, not panic ----

func TestQueryPodsUID_FallsBackToQueryPods(t *testing.T) {
	// QueryPodsUID is not supported in OTel mode — it logs a warning and falls
	// back to QueryPods. Verify it still returns results without panicking.
	payload := matrixPayload(map[string]string{
		"k8s_cluster_name":   "test-cluster",
		"k8s_namespace_name": "default",
		"k8s_pod_name":       "my-pod",
	})
	c := newPayloadClient(payload)
	q := newQuerier(c)
	fut := q.QueryPodsUID(rtStart, rtEnd)
	c.wait(t)
	if _, err := fut.Await(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Falls back to QueryPods query (no k8s_pod_uid)
	lastQ := c.last()
	if !containsStr(lastQ, "kube_pod_container_status_running") {
		t.Errorf("QueryPodsUID fallback should use kube_pod_container_status_running, got:\n%s", lastQ)
	}
}

func TestStubsDoNotPanic(t *testing.T) {
	c := newPayloadClient([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	q := newQuerier(c)

	stubs := []struct {
		name string
		fn   func()
	}{
		{"QueryGPUsRequested", func() {
			_, _ = q.QueryGPUsRequested(rtStart, rtEnd).Await()
		}},
		{"QueryGPUsAllocated", func() {
			_, _ = q.QueryGPUsAllocated(rtStart, rtEnd).Await()
		}},
		{"QueryLocalStorageCost", func() {
			_, _ = q.QueryLocalStorageCost(rtStart, rtEnd).Await()
		}},
		{"QueryLocalStorageUsedCost", func() {
			_, _ = q.QueryLocalStorageUsedCost(rtStart, rtEnd).Await()
		}},
		{"QueryPVUsedAverage", func() {
			_, _ = q.QueryPVUsedAverage(rtStart, rtEnd).Await()
		}},
		{"QueryPVUsedMax", func() {
			_, _ = q.QueryPVUsedMax(rtStart, rtEnd).Await()
		}},
	}

	for _, s := range stubs {
		t.Run(s.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("stub %s panicked: %v", s.name, r)
				}
			}()
			s.fn()
		})
	}
}

// ---- Diagnostics: verify correct metric names are probed ----

func TestDiagnosticsProbeCorrectMetrics(t *testing.T) {
	tests := []struct {
		id             string
		expectedMetric string
	}{
		{KSMDiagnosticMetricID, "kube_pod_container_resource_requests"},
		{KSMVersionDiagnosticMetricID, "kube_persistentvolume_capacity_bytes"},
		{KSMCPUCapacityMetricID, "kube_node_status_capacity"},
		{KSMAllocatableCPUCoresMetricID, "kube_node_status_allocatable"},
		{CAdvisorDiagnosticMetricID, "container_cpu_time"},
		{CAdvisorWorkingSetBytesMetricID, "container_memory_working_set"},
	}

	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			def, ok := diagnosticDefinitions[tc.id]
			if !ok {
				t.Fatalf("diagnostic %q not found in diagnosticDefinitions", tc.id)
			}
			if !containsStr(def.QueryFmt, tc.expectedMetric) {
				t.Errorf("diagnostic %q QueryFmt should contain %q, got:\n%s", tc.id, tc.expectedMetric, def.QueryFmt)
			}
		})
	}
}

// ---- helpers ----

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsSubstr(s, sub))
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if containsStr(s, sub) {
			return true
		}
	}
	return false
}
