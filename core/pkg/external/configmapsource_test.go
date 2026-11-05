package external

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSource returns a ConfigMapSource for the given config.
func newSource(cfg *Config) *ConfigMapSource {
	return &ConfigMapSource{cfg: cfg}
}

func TestConfigMapSource_NilConfig(t *testing.T) {
	s := newSource(nil)

	res, err := s.ExtractNodeLabels(map[string]string{})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.EqualError(t, err, "nil config")
}

// --- traditional ConfigMap (Key and Route both empty) ---

func TestConfigMapSource_Traditional_PassesDataDirectly(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "", "")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	res, err := s.ExtractNodeLabels(map[string]string{"env": "prod", "region": "us-east-1"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "region": "us-east-1"}, res)
}

func TestConfigMapSource_Traditional_EmptyData(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "", "")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	res, err := s.ExtractNodeLabels(map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, res)
}

// --- block-scalar ConfigMap ---

func TestConfigMapSource_BlockScalar_EmptyLabelsMap(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yamlData := `
labels: {}
`

	res, err := s.ExtractNodeLabels(map[string]string{
		"config.yaml": yamlData,
	})

	require.NoError(t, err)
	assert.Empty(t, res)
	assert.NotNil(t, res)
}

const prometheusConfig = `
prometheusK8s:
  externalLabels:
    cluster: prod-cluster
    region: eu-west-1
`

func TestConfigMapSource_BlockScalar_SingleLevel(t *testing.T) {
	nlCfg := NewNodeLabelConfig("prometheus-cm", "", "config.yaml", "prometheusK8s.externalLabels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	res, err := s.ExtractNodeLabels(map[string]string{"config.yaml": prometheusConfig})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cluster": "prod-cluster",
		"region":  "eu-west-1",
	}, res)
}

func TestConfigMapSource_BlockScalar_TopLevelRoute(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "data", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yaml := `
labels:
  team: platform
  env: staging
`
	res, err := s.ExtractNodeLabels(map[string]string{"data": yaml})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform", "env": "staging"}, res)
}

// --- error cases ---

func TestConfigMapSource_BlockScalar_EmptyMapDataInConfigYaml(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	res, err := s.ExtractNodeLabels(map[string]string{
		"config.yaml": "",
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "failed to locate route")
}

func TestConfigMapSource_BlockScalar_MissingKey(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "externalLabels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	_, err := s.ExtractNodeLabels(map[string]string{"other-key": "value"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `key "config.yaml" not found`)
}

func TestConfigMapSource_BlockScalar_InvalidYAML(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	_, err := s.ExtractNodeLabels(map[string]string{"config.yaml": ":\tinvalid: yaml: {"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "found character that cannot start any token")
}

func TestConfigMapSource_BlockScalar_RouteSegmentNotFound(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "does.not.exist")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	_, err := s.ExtractNodeLabels(map[string]string{"config.yaml": "foo: bar\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `failed to locate route`)
}

func TestConfigMapSource_BlockScalar_RouteSegmentNotANodeSequenceType(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels.nested")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	// labels is a string, not a map — traversing into it should fail
	_, err := s.ExtractNodeLabels(map[string]string{"config.yaml": "labels: just-a-string\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a map")
}

func TestConfigMapSource_BlockScalar_RoutePointsToNodeScalarType(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	// labels is a string, not a map — final conversion should fail
	_, err := s.ExtractNodeLabels(map[string]string{"config.yaml": "labels: just-a-string\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a map")
}

func TestConfigMapSource_BlockScalar_BoolValuesConvertedToString(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yaml := `
labels:
  active: true
  deprecated: false
`
	res, err := s.ExtractNodeLabels(map[string]string{"config.yaml": yaml})
	require.NoError(t, err)
	assert.Equal(t, "true", res["active"])
	assert.Equal(t, "false", res["deprecated"])
}

func TestConfigMapSource_BlockScalar_IntValuesConvertedToString(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yaml := `
labels:
  priority: 42
  replicas: 3
`
	res, err := s.ExtractNodeLabels(map[string]string{"config.yaml": yaml})
	require.NoError(t, err)
	assert.Equal(t, "42", res["priority"])
	assert.Equal(t, "3", res["replicas"])
}

func TestConfigMapSource_BlockScalar_SequenceValueRejected(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yamlData := `
labels:
  environments:
    - dev
    - staging
    - prod
`

	res, err := s.ExtractNodeLabels(map[string]string{
		"config.yaml": yamlData,
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), `cannot unmarshal !!seq into string`)
}

func TestConfigMapSource_BlockScalar_NestedMapValueRejected(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yamlData := `
labels:
  inner-map:
    my-key: value
    other-key: value
`

	res, err := s.ExtractNodeLabels(map[string]string{
		"config.yaml": yamlData,
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), `unmarshal !!map into string`)
}

func TestConfigMapSource_BlockScalar_NullValues(t *testing.T) {
	tests := []struct {
		name      string
		nullValue string
	}{
		{
			name:      "explicit null",
			nullValue: "null",
		},
		{
			name:      "tilde",
			nullValue: "~",
		},
		{
			name:      "empty value",
			nullValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
			cfg := NewConfig(nlCfg)
			s := newSource(cfg)
			yamlData := fmt.Sprintf(`
labels:
  priority: %s
`, tt.nullValue)

			res, err := s.ExtractNodeLabels(map[string]string{
				"config.yaml": yamlData,
			})

			require.NoError(t, err)
			assert.Equal(t, "", res["priority"])
		})
	}
}

func TestConfigMapSource_BlockScalar_AliasValue(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yamlData := `
defaultEnvironment: &defaultEnvironment production

labels:
  environment: *defaultEnvironment
`

	res, err := s.ExtractNodeLabels(map[string]string{
		"config.yaml": yamlData,
	})

	require.NoError(t, err)
	assert.Equal(t, "production", res["environment"])
}

func TestConfigMapSource_BlockScalar_ComplexYamlWithTwoUnsupportedNodeType(t *testing.T) {
	nlCfg := NewNodeLabelConfig("my-cm", "", "config.yaml", "labels")
	cfg := NewConfig(nlCfg)
	s := newSource(cfg)

	yaml := `
labels:
  name: complex
  priority: 42
  replicas: 3
  other:
    - "hello"
    - "this"
    - "is"
    - "a"
    - "test"
  inner-map:
    my-key: "value"
    other-key: "value"
`

	res, err := s.ExtractNodeLabels(map[string]string{"config.yaml": yaml})

	require.Error(t, err)
	assert.Nil(t, res)
}

// --- filterValidLabels ---

func TestFilterValidLabels_AllValid_ReturnedUnchanged(t *testing.T) {
	in := map[string]string{
		"env":                    "prod",
		"region":                 "us-east-1",
		"app.kubernetes.io/name": "opencost",
		"my-key":                 "my-value",
		"a":                      "b",
	}
	out := filterValidLabels(in)
	assert.Equal(t, in, out)
}

func TestFilterValidLabels_EmptyInput_ReturnsEmptyMap(t *testing.T) {
	out := filterValidLabels(map[string]string{})
	assert.NotNil(t, out)
	assert.Empty(t, out)
}

func TestFilterValidLabels_InvalidKey_EntryDropped(t *testing.T) {
	in := map[string]string{
		"valid-key":  "value",
		"":           "empty-key-dropped",
		"-bad-start": "dropped",
	}
	out := filterValidLabels(in)
	assert.Equal(t, map[string]string{"valid-key": "value"}, out)
}

func TestFilterValidLabels_InvalidValue_EntryDropped(t *testing.T) {
	in := map[string]string{
		"good-key":  "good-value",
		"bad-value": "-starts-with-dash",
	}
	out := filterValidLabels(in)
	assert.Equal(t, map[string]string{"good-key": "good-value"}, out)
}

func TestFilterValidLabels_ValueTooLong_EntryDropped(t *testing.T) {
	longValue := string(make([]byte, 64))
	for i := range longValue {
		longValue = longValue[:i] + "a" + longValue[i+1:]
	}
	in := map[string]string{
		"ok":      "fine",
		"toolong": longValue,
	}
	out := filterValidLabels(in)
	assert.Equal(t, map[string]string{"ok": "fine"}, out)
}

func TestFilterValidLabels_KeyTooLong_EntryDropped(t *testing.T) {
	longKey := string(make([]byte, 64))
	for i := range longKey {
		longKey = longKey[:i] + "a" + longKey[i+1:]
	}
	in := map[string]string{
		"ok":    "fine",
		longKey: "value",
	}
	out := filterValidLabels(in)
	assert.Equal(t, map[string]string{"ok": "fine"}, out)
}

func TestFilterValidLabels_PrefixedKey_Valid(t *testing.T) {
	in := map[string]string{
		"app.kubernetes.io/name": "opencost",
		"example.com/env":        "staging",
	}
	out := filterValidLabels(in)
	assert.Equal(t, in, out)
}

func TestFilterValidLabels_PrefixedKey_InvalidPrefix_EntryDropped(t *testing.T) {
	in := map[string]string{
		"valid":         "kept",
		"-bad.prefix/k": "dropped",
	}
	out := filterValidLabels(in)
	assert.Equal(t, map[string]string{"valid": "kept"}, out)
}

func TestFilterValidLabels_MixedValidAndInvalid_OnlyValidReturned(t *testing.T) {
	in := map[string]string{
		"cluster":   "prod",
		"":          "no-key",
		"bad-value": "-oops",
		"region":    "eu-west-1",
	}
	out := filterValidLabels(in)
	assert.Equal(t, map[string]string{"cluster": "prod", "region": "eu-west-1"}, out)
}
