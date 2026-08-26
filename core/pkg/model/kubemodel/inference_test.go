package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateInferenceEngine(t *testing.T) {
	tests := []struct {
		name    string
		server  *InferenceEngine
		wantErr string
	}{
		{
			name:    "empty pod uid",
			server:  &InferenceEngine{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid"},
			wantErr: "PodUID is missing for InferenceEngine with model 'Qwen3-32B'",
		},
		{
			name:    "empty model name",
			server:  &InferenceEngine{PodUID: "pod-uid", NamespaceUID: "ns-uid"},
			wantErr: "ModelName is missing for InferenceEngine on pod 'pod-uid'",
		},
		{
			name:   "valid",
			server: &InferenceEngine{PodUID: "pod-uid", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B"},
		},
		{
			// The namespace UID is a convenience for consumers, not identity:
			// it is derivable from kms.Pods[PodUID].NamespaceUID.
			name:   "valid without namespace uid",
			server: &InferenceEngine{PodUID: "pod-uid", ModelName: "Qwen3-32B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.server.ValidateInferenceEngine()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterInferenceEngine(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	t.Run("valid server is registered under its pod uid and counted", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)
		before := kms.Metadata.ObjectCount

		err := kms.RegisterInferenceEngine(&InferenceEngine{
			PodUID:       "pod-uid",
			NamespaceUID: "ns-uid",
			ModelName:    "Qwen3-32B",
			Engine:       EngineVLLM,
		})
		require.NoError(t, err)
		require.Len(t, kms.InferenceEngines, 1)
		require.Contains(t, kms.InferenceEngines, "pod-uid")
		require.Equal(t, before+1, kms.Metadata.ObjectCount)
	})

	t.Run("invalid server is rejected", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		err := kms.RegisterInferenceEngine(&InferenceEngine{
			NamespaceUID: "ns-uid",
			ModelName:    "Qwen3-32B",
		})
		require.Error(t, err)
		require.Empty(t, kms.InferenceEngines)
	})

	t.Run("duplicate pod uid keeps the first registration", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		first := &InferenceEngine{PodUID: "pod-uid", ModelName: "Qwen3-32B", Engine: EngineVLLM}
		require.NoError(t, kms.RegisterInferenceEngine(first))

		second := &InferenceEngine{PodUID: "pod-uid", ModelName: "Qwen3-32B", Engine: "other"}
		require.NoError(t, kms.RegisterInferenceEngine(second))

		require.Len(t, kms.InferenceEngines, 1)
		require.Same(t, first, kms.InferenceEngines["pod-uid"])
	})

	t.Run("replicas of one model are separate entries", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		require.NoError(t, kms.RegisterInferenceEngine(&InferenceEngine{
			PodUID: "pod-uid-a", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))
		require.NoError(t, kms.RegisterInferenceEngine(&InferenceEngine{
			PodUID: "pod-uid-b", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))

		require.Len(t, kms.InferenceEngines, 2)
		require.Contains(t, kms.InferenceEngines, "pod-uid-a")
		require.Contains(t, kms.InferenceEngines, "pod-uid-b")
	})

	t.Run("same model name in two namespaces stays separate", func(t *testing.T) {
		// The old "model_name:namespace" key made this case depend on the
		// namespace name being carried; pod UID separates them structurally.
		kms := NewKubeModelSet(start, end)

		require.NoError(t, kms.RegisterInferenceEngine(&InferenceEngine{
			PodUID: "prod-pod", NamespaceUID: "ns-prod", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))
		require.NoError(t, kms.RegisterInferenceEngine(&InferenceEngine{
			PodUID: "staging-pod", NamespaceUID: "ns-staging", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))

		require.Len(t, kms.InferenceEngines, 2)
		require.Equal(t, "ns-prod", kms.InferenceEngines["prod-pod"].NamespaceUID)
		require.Equal(t, "ns-staging", kms.InferenceEngines["staging-pod"].NamespaceUID)
	})
}

func TestInferenceEngineCodecRoundTrip(t *testing.T) {
	is := &InferenceEngine{
		PodUID:             "pod-uid",
		NamespaceUID:       "ns-uid",
		ModelName:          "Qwen3-32B",
		Engine:             EngineVLLM,
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
	}

	b, err := is.MarshalBinary()
	require.NoError(t, err)

	act := new(InferenceEngine)
	require.NoError(t, act.UnmarshalBinary(b))
	require.Equal(t, is, act)
}

func TestInferenceEngineCodecRoundTripZeroValues(t *testing.T) {
	is := &InferenceEngine{PodUID: "pod-uid", ModelName: "Qwen3-32B"}

	b, err := is.MarshalBinary()
	require.NoError(t, err)

	act := new(InferenceEngine)
	require.NoError(t, act.UnmarshalBinary(b))
	require.Equal(t, is, act)
}
