package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeInferenceServers(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.InferenceServer
	}{
		{
			name:      "no data returns empty inference server map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.InferenceServer{},
		},
		{
			name: "all gauges and counters populate a single replica",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.42},
				},
				source.QueryInferenceKVCacheUsageMax: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.97},
				},
				source.QueryInferenceQueueDepthAvg: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.5},
				},
				source.QueryInferenceQueueDepthMax: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 12},
				},
				source.QueryInferenceRunningRequestsAvg: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 33},
				},
				source.QueryInferencePreemptions: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 7},
				},
				source.QueryInferenceKVCacheUsageP95: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.91},
				},
				source.QueryInferenceQueueDepthP95: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 8},
				},
				source.QueryInferenceRunningRequestsMax: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 48},
				},
				source.QueryInferenceRunningRequestsP95: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 46},
				},
			},
			want: map[string]*kubemodel.InferenceServer{
				"Qwen3-32B:llm-d": {
					ModelName: "Qwen3-32B",
					Namespace: "llm-d",
					Engine:    kubemodel.EngineVLLM,
					Start:     start,
					End:       end,
					Replicas: map[string]kubemodel.InferenceServerReplica{
						"vllm-0": {
							KVCacheUsageAvg:    0.42,
							KVCacheUsageMax:    0.97,
							QueueDepthAvg:      0.5,
							QueueDepthMax:      12,
							RunningRequestsAvg: 33,
							Preemptions:        7,
							KVCacheUsageP95:    0.91,
							QueueDepthP95:      8,
							RunningRequestsMax: 48,
							RunningRequestsP95: 46,
						},
					},
				},
			},
		},
		{
			name: "replicas of the same model are grouped under one server",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-0", Value: 0.9},
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "vllm-1", Value: 0.017},
				},
			},
			want: map[string]*kubemodel.InferenceServer{
				"Qwen3-32B:llm-d": {
					ModelName: "Qwen3-32B",
					Namespace: "llm-d",
					Engine:    kubemodel.EngineVLLM,
					Start:     start,
					End:       end,
					Replicas: map[string]kubemodel.InferenceServerReplica{
						"vllm-0": {KVCacheUsageAvg: 0.9},
						"vllm-1": {KVCacheUsageAvg: 0.017},
					},
				},
			},
		},
		{
			name: "same model in different namespaces yields separate servers",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceServerMetricResult{
					{ModelName: "Qwen3-32B", Namespace: "team-a", Pod: "vllm-0", Value: 0.5},
					{ModelName: "Qwen3-32B", Namespace: "team-b", Pod: "vllm-0", Value: 0.6},
				},
			},
			want: map[string]*kubemodel.InferenceServer{
				"Qwen3-32B:team-a": {
					ModelName: "Qwen3-32B",
					Namespace: "team-a",
					Engine:    kubemodel.EngineVLLM,
					Start:     start,
					End:       end,
					Replicas: map[string]kubemodel.InferenceServerReplica{
						"vllm-0": {KVCacheUsageAvg: 0.5},
					},
				},
				"Qwen3-32B:team-b": {
					ModelName: "Qwen3-32B",
					Namespace: "team-b",
					Engine:    kubemodel.EngineVLLM,
					Start:     start,
					End:       end,
					Replicas: map[string]kubemodel.InferenceServerReplica{
						"vllm-0": {KVCacheUsageAvg: 0.6},
					},
				},
			},
		},
		{
			name: "results with missing identity labels are skipped",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceServerMetricResult{
					{ModelName: "", Namespace: "llm-d", Pod: "vllm-0", Value: 0.5},
					{ModelName: "Qwen3-32B", Namespace: "", Pod: "vllm-0", Value: 0.5},
					{ModelName: "Qwen3-32B", Namespace: "llm-d", Pod: "", Value: 0.5},
				},
			},
			want: map[string]*kubemodel.InferenceServer{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			seedCluster(ds, start, end)
			for method, result := range tt.overrides {
				ds.Querier.SetOverride(method, result)
			}

			km, err := NewKubeModel(testClusterUID, false, ds)
			require.NoError(t, err)

			kms := kubemodel.NewKubeModelSet(start, end)

			err = km.computeInferenceServers(kms, start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.InferenceServers)
		})
	}
}
