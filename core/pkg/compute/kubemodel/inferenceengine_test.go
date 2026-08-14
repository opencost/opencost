package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeInferenceEngines(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.InferenceEngine
	}{
		{
			name:      "no data returns empty inference server map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.InferenceEngine{},
		},
		{
			name: "all gauges and counters populate a single pod entry",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 0.42},
				},
				source.QueryInferenceKVCacheUsageMax: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 0.97},
				},
				source.QueryInferenceQueueDepthAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 0.5},
				},
				source.QueryInferenceQueueDepthMax: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 12},
				},
				source.QueryInferenceRunningRequestsAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 33},
				},
				source.QueryInferencePreemptions: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 7},
				},
				source.QueryInferenceKVCacheUsageP95: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 0.91},
				},
				source.QueryInferenceQueueDepthP95: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 8},
				},
				source.QueryInferenceRunningRequestsMax: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 48},
				},
				source.QueryInferenceRunningRequestsP95: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 46},
				},
			},
			want: map[string]*kubemodel.InferenceEngine{
				"pod-uid-0": {
					PodUID:             "pod-uid-0",
					NamespaceUID:       "ns-uid",
					ModelName:          "Qwen3-32B",
					Engine:             kubemodel.EngineVLLM,
					KVCacheUsageAvg:    0.42,
					KVCacheUsageP95:    0.91,
					KVCacheUsageMax:    0.97,
					QueueDepthAvg:      0.5,
					QueueDepthP95:      8,
					QueueDepthMax:      12,
					RunningRequestsAvg: 33,
					RunningRequestsP95: 46,
					RunningRequestsMax: 48,
					Preemptions:        7,
				},
			},
		},
		{
			name: "replicas of the same model are separate pod entries",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 0.9},
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "pod-uid-1", Value: 0.017},
				},
			},
			want: map[string]*kubemodel.InferenceEngine{
				"pod-uid-0": {
					PodUID: "pod-uid-0", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.9,
				},
				"pod-uid-1": {
					PodUID: "pod-uid-1", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.017,
				},
			},
		},
		{
			name: "same model in different namespaces stays separate by pod uid",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid-a", PodUID: "pod-uid-a", Value: 0.5},
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid-b", PodUID: "pod-uid-b", Value: 0.6},
				},
			},
			want: map[string]*kubemodel.InferenceEngine{
				"pod-uid-a": {
					PodUID: "pod-uid-a", NamespaceUID: "ns-uid-a", ModelName: "Qwen3-32B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.5,
				},
				"pod-uid-b": {
					PodUID: "pod-uid-b", NamespaceUID: "ns-uid-b", ModelName: "Qwen3-32B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.6,
				},
			},
		},
		{
			name: "pods reusing a name in different namespaces no longer collide",
			overrides: map[string]any{
				// Both replicas are named "vllm-0"; only the UID separates them.
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid-a", PodUID: "pod-uid-a", Value: 0.5},
					{ModelName: "Llama-3-70B", NamespaceUID: "ns-uid-b", PodUID: "pod-uid-b", Value: 0.6},
				},
			},
			want: map[string]*kubemodel.InferenceEngine{
				"pod-uid-a": {
					PodUID: "pod-uid-a", NamespaceUID: "ns-uid-a", ModelName: "Qwen3-32B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.5,
				},
				"pod-uid-b": {
					PodUID: "pod-uid-b", NamespaceUID: "ns-uid-b", ModelName: "Llama-3-70B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.6,
				},
			},
		},
		{
			name: "results with missing identity labels are skipped",
			overrides: map[string]any{
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "", NamespaceUID: "ns-uid", PodUID: "pod-uid-0", Value: 0.5},
					{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid", PodUID: "", Value: 0.5},
				},
			},
			want: map[string]*kubemodel.InferenceEngine{},
		},
		{
			name: "a missing namespace uid still yields an entry",
			overrides: map[string]any{
				// namespace_uid is a convenience: it is derivable from
				// kms.Pods[PodUID], so its absence must not drop the sample.
				source.QueryInferenceKVCacheUsageAvg: []*source.InferenceEngineMetricResult{
					{ModelName: "Qwen3-32B", NamespaceUID: "", PodUID: "pod-uid-0", Value: 0.5},
				},
			},
			want: map[string]*kubemodel.InferenceEngine{
				"pod-uid-0": {
					PodUID: "pod-uid-0", ModelName: "Qwen3-32B",
					Engine: kubemodel.EngineVLLM, KVCacheUsageAvg: 0.5,
				},
			},
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

			err = km.computeInferenceEngines(kms, start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.InferenceEngines)
		})
	}
}
