package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateInferenceServer(t *testing.T) {
	tests := []struct {
		name    string
		server  *InferenceServer
		wantErr string
	}{
		{
			name:    "empty pod uid",
			server:  &InferenceServer{ModelName: "Qwen3-32B", NamespaceUID: "ns-uid"},
			wantErr: "PodUID is missing for InferenceServer with model 'Qwen3-32B'",
		},
		{
			name:    "empty model name",
			server:  &InferenceServer{PodUID: "pod-uid", NamespaceUID: "ns-uid"},
			wantErr: "ModelName is missing for InferenceServer on pod 'pod-uid'",
		},
		{
			name:   "valid",
			server: &InferenceServer{PodUID: "pod-uid", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B"},
		},
		{
			// The namespace UID is a convenience for consumers, not identity:
			// it is derivable from kms.Pods[PodUID].NamespaceUID.
			name:   "valid without namespace uid",
			server: &InferenceServer{PodUID: "pod-uid", ModelName: "Qwen3-32B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.server.ValidateInferenceServer()
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

	t.Run("valid server is registered under its pod uid and counted", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)
		before := kms.Metadata.ObjectCount

		err := kms.RegisterInferenceServer(&InferenceServer{
			PodUID:       "pod-uid",
			NamespaceUID: "ns-uid",
			ModelName:    "Qwen3-32B",
			Engine:       EngineVLLM,
		})
		require.NoError(t, err)
		require.Len(t, kms.InferenceServers, 1)
		require.Contains(t, kms.InferenceServers, "pod-uid")
		require.Equal(t, before+1, kms.Metadata.ObjectCount)
	})

	t.Run("invalid server is rejected", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		err := kms.RegisterInferenceServer(&InferenceServer{
			NamespaceUID: "ns-uid",
			ModelName:    "Qwen3-32B",
		})
		require.Error(t, err)
		require.Empty(t, kms.InferenceServers)
	})

	t.Run("duplicate pod uid keeps the first registration", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		first := &InferenceServer{PodUID: "pod-uid", ModelName: "Qwen3-32B", Engine: EngineVLLM}
		require.NoError(t, kms.RegisterInferenceServer(first))

		second := &InferenceServer{PodUID: "pod-uid", ModelName: "Qwen3-32B", Engine: "other"}
		require.NoError(t, kms.RegisterInferenceServer(second))

		require.Len(t, kms.InferenceServers, 1)
		require.Same(t, first, kms.InferenceServers["pod-uid"])
	})

	t.Run("replicas of one model are separate entries", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		require.NoError(t, kms.RegisterInferenceServer(&InferenceServer{
			PodUID: "pod-uid-a", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))
		require.NoError(t, kms.RegisterInferenceServer(&InferenceServer{
			PodUID: "pod-uid-b", NamespaceUID: "ns-uid", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))

		require.Len(t, kms.InferenceServers, 2)
		require.Contains(t, kms.InferenceServers, "pod-uid-a")
		require.Contains(t, kms.InferenceServers, "pod-uid-b")
	})

	t.Run("same model name in two namespaces stays separate", func(t *testing.T) {
		// The old "model_name:namespace" key made this case depend on the
		// namespace name being carried; pod UID separates them structurally.
		kms := NewKubeModelSet(start, end)

		require.NoError(t, kms.RegisterInferenceServer(&InferenceServer{
			PodUID: "prod-pod", NamespaceUID: "ns-prod", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))
		require.NoError(t, kms.RegisterInferenceServer(&InferenceServer{
			PodUID: "staging-pod", NamespaceUID: "ns-staging", ModelName: "Qwen3-32B", Engine: EngineVLLM,
		}))

		require.Len(t, kms.InferenceServers, 2)
		require.Equal(t, "ns-prod", kms.InferenceServers["prod-pod"].NamespaceUID)
		require.Equal(t, "ns-staging", kms.InferenceServers["staging-pod"].NamespaceUID)
	})
}

func TestInferenceServerCodecRoundTrip(t *testing.T) {
	is := &InferenceServer{
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

	act := new(InferenceServer)
	require.NoError(t, act.UnmarshalBinary(b))
	require.Equal(t, is, act)
}

func TestInferenceServerCodecRoundTripZeroValues(t *testing.T) {
	is := &InferenceServer{PodUID: "pod-uid", ModelName: "Qwen3-32B"}

	b, err := is.MarshalBinary()
	require.NoError(t, err)

	act := new(InferenceServer)
	require.NoError(t, act.UnmarshalBinary(b))
	require.Equal(t, is, act)
}
