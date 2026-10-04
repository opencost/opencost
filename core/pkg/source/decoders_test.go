package source

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util"
	"github.com/stretchr/testify/require"
)

// vecs is a helper to build a slice of vectors from raw values. The timestamps
// are monotonically increasing so that "last value" semantics are exercised.
func vecs(values ...float64) []*util.Vector {
	out := make([]*util.Vector, 0, len(values))
	for i, v := range values {
		out = append(out, &util.Vector{Timestamp: float64(i), Value: v})
	}
	return out
}

func TestDecodeInferenceTokensResult(t *testing.T) {
	cases := []struct {
		name        string
		metric      map[string]any
		values      []*util.Vector
		expectedKey string
		expectedVal float64
	}{
		{
			name:        "uses last value with model and namespace",
			metric:      map[string]any{"model_name": "llama3", "namespace": "default"},
			values:      vecs(10, 20, 42),
			expectedKey: "llama3:default",
			expectedVal: 42,
		},
		{
			name:        "empty values defaults to zero",
			metric:      map[string]any{"model_name": "llama3", "namespace": "default"},
			values:      vecs(),
			expectedKey: "llama3:default",
			expectedVal: 0,
		},
		{
			name:        "single value",
			metric:      map[string]any{"model_name": "mistral", "namespace": "team-a"},
			values:      vecs(7),
			expectedKey: "mistral:team-a",
			expectedVal: 7,
		},
		{
			name:        "missing labels yields separator-only key",
			metric:      map[string]any{},
			values:      vecs(5),
			expectedKey: ":",
			expectedVal: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewQueryResult(tc.metric, tc.values, nil)

			decoded := DecodeInferenceTokensResult(result)

			require.NotNil(t, decoded)
			require.Len(t, decoded.Values, 1)
			val, ok := decoded.Values[tc.expectedKey]
			require.True(t, ok, "expected key %q to be present", tc.expectedKey)
			require.Equal(t, tc.expectedVal, val)
		})
	}
}

func TestDecodeInferenceProcessingTimeResult(t *testing.T) {
	cases := []struct {
		name        string
		metric      map[string]any
		values      []*util.Vector
		expectedKey string
		expectedVal float64
	}{
		{
			name:        "uses last value with model and namespace",
			metric:      map[string]any{"model_name": "llama3", "namespace": "default"},
			values:      vecs(1.5, 2.5, 3.75),
			expectedKey: "llama3:default",
			expectedVal: 3.75,
		},
		{
			name:        "empty values defaults to zero",
			metric:      map[string]any{"model_name": "llama3", "namespace": "default"},
			values:      vecs(),
			expectedKey: "llama3:default",
			expectedVal: 0,
		},
		{
			name:        "missing labels yields separator-only key",
			metric:      map[string]any{},
			values:      vecs(9.9),
			expectedKey: ":",
			expectedVal: 9.9,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewQueryResult(tc.metric, tc.values, nil)

			decoded := DecodeInferenceProcessingTimeResult(result)

			require.NotNil(t, decoded)
			require.Len(t, decoded.Values, 1)
			val, ok := decoded.Values[tc.expectedKey]
			require.True(t, ok, "expected key %q to be present", tc.expectedKey)
			require.Equal(t, tc.expectedVal, val)
		})
	}
}

func TestDecodeInferenceCacheConfigResult(t *testing.T) {
	cases := []struct {
		name            string
		metric          map[string]any
		values          []*util.Vector
		expectedKey     string
		expectedEnabled bool
	}{
		{
			name:            "positive value means prefix caching enabled",
			metric:          map[string]any{"model_name": "llama3", "namespace": "default"},
			values:          vecs(1),
			expectedKey:     "llama3:default",
			expectedEnabled: true,
		},
		{
			name:            "zero value means prefix caching disabled",
			metric:          map[string]any{"model_name": "llama3", "namespace": "default"},
			values:          vecs(0),
			expectedKey:     "llama3:default",
			expectedEnabled: false,
		},
		{
			name:            "negative value means prefix caching disabled",
			metric:          map[string]any{"model_name": "llama3", "namespace": "default"},
			values:          vecs(-1),
			expectedKey:     "llama3:default",
			expectedEnabled: false,
		},
		{
			name:            "uses last value to determine enabled",
			metric:          map[string]any{"model_name": "mistral", "namespace": "team-a"},
			values:          vecs(1, 0, 1),
			expectedKey:     "mistral:team-a",
			expectedEnabled: true,
		},
		{
			name:            "empty values defaults to disabled",
			metric:          map[string]any{"model_name": "llama3", "namespace": "default"},
			values:          vecs(),
			expectedKey:     "llama3:default",
			expectedEnabled: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewQueryResult(tc.metric, tc.values, nil)

			decoded := DecodeInferenceCacheConfigResult(result)

			require.NotNil(t, decoded)
			require.Len(t, decoded.Configs, 1)
			config, ok := decoded.Configs[tc.expectedKey]
			require.True(t, ok, "expected key %q to be present", tc.expectedKey)
			require.NotNil(t, config)
			require.Equal(t, tc.expectedEnabled, config.PrefixCachingEnabled)
		})
	}
}
