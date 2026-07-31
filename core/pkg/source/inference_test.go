package source

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/util"
)

func TestDecodeInferenceServerMetricResult(t *testing.T) {
	tests := []struct {
		name   string
		result *QueryResult
		want   *InferenceServerMetricResult
	}{
		{
			name: "full labels and value",
			result: NewQueryResult(
				map[string]any{
					InferenceModelNameLabel: "Qwen3-32B",
					NamespaceLabel:          "llm-d",
					PodLabel:                "vllm-0",
				},
				[]*util.Vector{{Value: 0.42}},
				nil,
			),
			want: &InferenceServerMetricResult{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.42},
		},
		{
			name: "last vector value wins",
			result: NewQueryResult(
				map[string]any{
					InferenceModelNameLabel: "Qwen3-32B",
					NamespaceLabel:          "llm-d",
					PodLabel:                "vllm-0",
				},
				[]*util.Vector{{Value: 0.1}, {Value: 0.9}},
				nil,
			),
			want: &InferenceServerMetricResult{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.9},
		},
		{
			name:   "missing labels and values decode to zero values",
			result: NewQueryResult(map[string]any{}, nil, nil),
			want:   &InferenceServerMetricResult{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, DecodeInferenceServerMetricResult(tt.result))
		})
	}
}

// inferenceSaturationQueries enumerates every saturation query on the
// MetricsQuerier interface so the noop/mock/record plumbing is exercised
// uniformly.
func inferenceSaturationQueries(q MetricsQuerier) map[string]func(start, end time.Time) *Future[InferenceServerMetricResult] {
	return map[string]func(start, end time.Time) *Future[InferenceServerMetricResult]{
		QueryInferenceKVCacheUsageAvg:    q.QueryInferenceKVCacheUsageAvg,
		QueryInferenceKVCacheUsageMax:    q.QueryInferenceKVCacheUsageMax,
		QueryInferenceKVCacheUsageP95:    q.QueryInferenceKVCacheUsageP95,
		QueryInferenceQueueDepthAvg:      q.QueryInferenceQueueDepthAvg,
		QueryInferenceQueueDepthMax:      q.QueryInferenceQueueDepthMax,
		QueryInferenceQueueDepthP95:      q.QueryInferenceQueueDepthP95,
		QueryInferenceRunningRequestsAvg: q.QueryInferenceRunningRequestsAvg,
		QueryInferenceRunningRequestsMax: q.QueryInferenceRunningRequestsMax,
		QueryInferenceRunningRequestsP95: q.QueryInferenceRunningRequestsP95,
		QueryInferencePreemptions:        q.QueryInferencePreemptions,
	}
}

func TestNoOpInferenceSaturationQueries(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	for name, query := range inferenceSaturationQueries(&NoOpMetricsQuerier{}) {
		t.Run(name, func(t *testing.T) {
			results, err := query(start, end).Await()
			require.NoError(t, err)
			require.Empty(t, results)
		})
	}
}

func TestMockInferenceSaturationQueries(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	t.Run("without override falls back to empty noop results", func(t *testing.T) {
		ds := NewMockOpenCostDataSource()
		for name, query := range inferenceSaturationQueries(ds.Querier) {
			results, err := query(start, end).Await()
			require.NoError(t, err, name)
			require.Empty(t, results, name)
		}
	})

	t.Run("override returns the typed results", func(t *testing.T) {
		ds := NewMockOpenCostDataSource()
		expected := []*InferenceServerMetricResult{
			{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.42},
		}
		for name := range inferenceSaturationQueries(ds.Querier) {
			ds.Querier.SetOverride(name, expected)
		}
		for name, query := range inferenceSaturationQueries(ds.Querier) {
			results, err := query(start, end).Await()
			require.NoError(t, err, name)
			require.Equal(t, expected, results, name)
		}
	})
}

func TestRecordInferenceSaturationQueries(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	rec := NewRecordMetricsQuerier(&NoOpMetricsQuerier{})
	for name, query := range inferenceSaturationQueries(rec) {
		results, err := query(start, end).Await()
		require.NoError(t, err, name)
		require.Empty(t, results, name)
		require.Equal(t, 1, rec.Calls[name], name)
	}
}
