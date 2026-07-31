package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInferenceServerKey(t *testing.T) {
	is := &InferenceServer{ModelName: "Qwen3-32B", Namespace: "llm-d"}
	require.Equal(t, "Qwen3-32B:llm-d", is.Key())
}

func TestValidateInferenceServer(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	window := Window{Start: start, End: end}

	tests := []struct {
		name    string
		server  *InferenceServer
		wantErr string
	}{
		{
			name:    "empty model name",
			server:  &InferenceServer{Namespace: "llm-d", Start: start, End: end},
			wantErr: "ModelName is missing for InferenceServer in namespace 'llm-d'",
		},
		{
			name:    "empty namespace",
			server:  &InferenceServer{ModelName: "Qwen3-32B", Start: start, End: end},
			wantErr: "Namespace is missing for InferenceServer with model 'Qwen3-32B'",
		},
		{
			name:    "outside window",
			server:  &InferenceServer{ModelName: "Qwen3-32B", Namespace: "llm-d", Start: start.Add(-time.Hour), End: end},
			wantErr: checkWindow(window, start.Add(-time.Hour), end).Error(),
		},
		{
			name:   "valid",
			server: &InferenceServer{ModelName: "Qwen3-32B", Namespace: "llm-d", Start: start, End: end},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.server.ValidateInferenceServer(window)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterInferenceServer(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	t.Run("valid server is registered and counted", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)
		before := kms.Metadata.ObjectCount

		err := kms.RegisterInferenceServer(&InferenceServer{
			ModelName: "Qwen3-32B",
			Namespace: "llm-d",
			Engine:    EngineVLLM,
			Start:     start,
			End:       end,
		})
		require.NoError(t, err)
		require.Len(t, kms.InferenceServers, 1)
		require.Contains(t, kms.InferenceServers, "Qwen3-32B:llm-d")
		require.Equal(t, before+1, kms.Metadata.ObjectCount)
	})

	t.Run("invalid server is rejected", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		err := kms.RegisterInferenceServer(&InferenceServer{
			Namespace: "llm-d",
			Start:     start,
			End:       end,
		})
		require.Error(t, err)
		require.Empty(t, kms.InferenceServers)
	})

	t.Run("duplicate key keeps the first registration", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		first := &InferenceServer{ModelName: "Qwen3-32B", Namespace: "llm-d", Engine: EngineVLLM, Start: start, End: end}
		require.NoError(t, kms.RegisterInferenceServer(first))

		second := &InferenceServer{ModelName: "Qwen3-32B", Namespace: "llm-d", Engine: "other", Start: start, End: end}
		require.NoError(t, kms.RegisterInferenceServer(second))

		require.Len(t, kms.InferenceServers, 1)
		require.Same(t, first, kms.InferenceServers["Qwen3-32B:llm-d"])
	})
}

func TestInferenceServerCodecRoundTrip(t *testing.T) {
	is := &InferenceServer{
		ModelName: "Qwen3-32B",
		Namespace: "llm-d",
		Engine:    EngineVLLM,
		Start:     time.Now().UTC().Truncate(time.Hour),
		End:       time.Now().UTC().Truncate(time.Hour).Add(time.Hour),
		Replicas: map[string]InferenceServerReplica{
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
			"vllm-1": {},
		},
	}

	b, err := is.MarshalBinary()
	require.NoError(t, err)

	act := new(InferenceServer)
	require.NoError(t, act.UnmarshalBinary(b))
	require.Equal(t, is, act)
}
