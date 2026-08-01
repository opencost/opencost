package scrape

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func inferencePod(name, namespace, ip string, labels, annotations map[string]string, phase v1.PodPhase) *clustercache.Pod {
	return &clustercache.Pod{
		UID:         types.UID(name + "-uid"),
		Name:        name,
		Namespace:   namespace,
		Labels:      labels,
		Annotations: annotations,
		Status: clustercache.PodStatus{
			PodIP: ip,
			Phase: phase,
		},
	}
}

func inferenceNamespaces() []*clustercache.Namespace {
	return []*clustercache.Namespace{
		{UID: types.UID("llm-d-uid"), Name: "llm-d"},
		{UID: types.UID("default-uid"), Name: "default"},
	}
}

func TestInferenceScraperGetTargets(t *testing.T) {
	modelLabels := map[string]string{"llm-d.ai/model": "Qwen3-32B"}

	cache := &clustercache.MockClusterCache{
		Namespaces: inferenceNamespaces(),
		Pods: []*clustercache.Pod{
			// selected, default port
			inferencePod("vllm-0", "llm-d", "10.0.0.1", modelLabels, nil, v1.PodRunning),
			// selected, port from prometheus.io/port annotation
			inferencePod("vllm-1", "llm-d", "10.0.0.2", modelLabels, map[string]string{"prometheus.io/port": "9090"}, v1.PodRunning),
			// selected, unparseable annotation falls back to default port
			inferencePod("vllm-2", "llm-d", "10.0.0.3", modelLabels, map[string]string{"prometheus.io/port": "not-a-port"}, v1.PodRunning),
			// skipped: no model label
			inferencePod("web-0", "default", "10.0.0.4", map[string]string{"app": "web"}, nil, v1.PodRunning),
			// skipped: not running
			inferencePod("vllm-3", "llm-d", "10.0.0.5", modelLabels, nil, v1.PodPending),
			// skipped: no pod IP
			inferencePod("vllm-4", "llm-d", "", modelLabels, nil, v1.PodRunning),
		},
	}

	s := newInferenceScraper(cache)
	targets := s.getTargets()

	require.Len(t, targets, 3)
	byPod := map[string]inferenceTarget{}
	for _, tgt := range targets {
		byPod[tgt.pod] = tgt
	}
	require.Equal(t, "llm-d", byPod["vllm-0"].namespace)
	require.Contains(t, byPod, "vllm-1")
	require.Contains(t, byPod, "vllm-2")

	// UIDs are what the KubeModel joins on, so every target must carry them.
	for pod, tgt := range byPod {
		require.Equal(t, pod+"-uid", tgt.podUID, pod)
		require.Equal(t, "llm-d-uid", tgt.namespaceUID, pod)
	}
}

func TestInferenceScraperGetTargetsUnknownNamespace(t *testing.T) {
	// A pod whose namespace is missing from the cache still yields a target:
	// the pod UID alone is enough to identify it, and namespace_uid is a
	// convenience label.
	modelLabels := map[string]string{"llm-d.ai/model": "Qwen3-32B"}
	cache := &clustercache.MockClusterCache{
		Pods: []*clustercache.Pod{
			inferencePod("vllm-0", "llm-d", "10.0.0.1", modelLabels, nil, v1.PodRunning),
		},
	}

	targets := newInferenceScraper(cache).getTargets()

	require.Len(t, targets, 1)
	require.Equal(t, "vllm-0-uid", targets[0].podUID)
	require.Empty(t, targets[0].namespaceUID)
}

func TestInferenceScraperScrape(t *testing.T) {
	exposition := `# HELP vllm:kv_cache_usage_perc KV cache usage
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{model_name="Qwen3-32B"} 0.42
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="Qwen3-32B"} 3
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="Qwen3-32B"} 17
# TYPE vllm:num_preemptions_total counter
vllm:num_preemptions_total{model_name="Qwen3-32B"} 5
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{model_name="Qwen3-32B"} 123456
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{model_name="Qwen3-32B"} 654321
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{model_name="Qwen3-32B",enable_prefix_caching="true"} 1
# TYPE vllm:not_collected gauge
vllm:not_collected{model_name="Qwen3-32B"} 99
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/metrics", r.URL.Path)
		fmt.Fprint(w, exposition)
	}))
	defer server.Close()

	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	host, portStr, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	_, err = strconv.Atoi(portStr)
	require.NoError(t, err)

	modelLabels := map[string]string{"llm-d.ai/model": "Qwen3-32B"}
	cache := &clustercache.MockClusterCache{
		Namespaces: inferenceNamespaces(),
		Pods: []*clustercache.Pod{
			inferencePod("vllm-0", "llm-d", host, modelLabels, map[string]string{"prometheus.io/port": portStr}, v1.PodRunning),
			// unreachable target: errors are collected, scrape continues
			inferencePod("vllm-broken", "llm-d", "127.0.0.1", modelLabels, map[string]string{"prometheus.io/port": "1"}, v1.PodRunning),
		},
	}

	s := newInferenceScraper(cache)
	updates := s.Scrape()

	// The saturation gauges plus the inference cost counters are kept;
	// anything outside the whitelist (vllm:not_collected) is dropped.
	require.Len(t, updates, 7)

	seen := map[string]float64{}
	for _, update := range updates {
		seen[update.Name] = update.Value
		require.Equal(t, "llm-d", update.Labels[source.NamespaceLabel], update.Name)
		require.Equal(t, "llm-d-uid", update.Labels[source.NamespaceUIDLabel], update.Name)
		require.Equal(t, "vllm-0", update.Labels[source.PodLabel], update.Name)
		require.Equal(t, "vllm-0-uid", update.Labels[source.PodUIDLabel], update.Name)
		require.Equal(t, "Qwen3-32B", update.Labels[source.InferenceModelNameLabel], update.Name)
	}
	require.Equal(t, 0.42, seen[metric.VLLMKVCacheUsagePerc])
	require.Equal(t, float64(3), seen[metric.VLLMNumRequestsWaiting])
	require.Equal(t, float64(17), seen[metric.VLLMNumRequestsRunning])
	require.Equal(t, float64(5), seen[metric.VLLMNumPreemptionsTotal])
	require.Equal(t, float64(123456), seen[metric.VLLMGenerationTokensTotal])
	require.Equal(t, float64(654321), seen[metric.VLLMPromptTokensTotal])
	require.NotContains(t, seen, "vllm:not_collected")

	// cache_config_info is an info metric: the Info aggregator reads its
	// payload from AdditionalInfo, so the labels must be carried there.
	var cacheConfig *metric.Update
	for i := range updates {
		if updates[i].Name == metric.VLLMCacheConfigInfo {
			cacheConfig = &updates[i]
		}
	}
	require.NotNil(t, cacheConfig)
	require.Equal(t, "true", cacheConfig.AdditionalInfo[source.EnablePrefixCachingLabel])
}
