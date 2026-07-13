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

	res, err := s.Extract(map[string]string{})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.EqualError(t, err, "nil config")
}

// --- traditional ConfigMap (Key and Route both empty) ---

func TestConfigMapSource_Traditional_PassesDataDirectly(t *testing.T) {
	s := newSource(&Config{})

	res, err := s.Extract(map[string]string{"env": "prod", "region": "us-east-1"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "region": "us-east-1"}, res)
}

func TestConfigMapSource_Traditional_EmptyData(t *testing.T) {
	s := newSource(&Config{})

	res, err := s.Extract(map[string]string{})
	require.NoError(t, err)
	assert.Empty(t, res)
}

// --- block-scalar ConfigMap ---

func TestConfigMapSource_BlockScalar_EmptyYAMLDocument(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	res, err := s.Extract(map[string]string{
		"config.yaml": "",
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "empty YAML document")
}

func TestConfigMapSource_BlockScalar_EmptyLabelsMap(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	yamlData := `
labels: {}
`

	res, err := s.Extract(map[string]string{
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
	s := newSource(&Config{
		ConfigMapName: "prometheus-cm",
		Key:           "config.yaml",
		Route:         "prometheusK8s.externalLabels",
	})

	res, err := s.Extract(map[string]string{"config.yaml": prometheusConfig})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cluster": "prod-cluster",
		"region":  "eu-west-1",
	}, res)
}

func TestConfigMapSource_BlockScalar_TopLevelRoute(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "data",
		Route:         "labels",
	})

	yaml := `
labels:
  team: platform
  env: staging
`
	res, err := s.Extract(map[string]string{"data": yaml})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform", "env": "staging"}, res)
}

// --- error cases ---

func TestConfigMapSource_BlockScalar_MissingKey(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "externalLabels",
	})

	_, err := s.Extract(map[string]string{"other-key": "value"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `key "config.yaml" not found`)
}

func TestConfigMapSource_BlockScalar_InvalidYAML(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	_, err := s.Extract(map[string]string{"config.yaml": ":\tinvalid: yaml: {"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse YAML")
}

func TestConfigMapSource_BlockScalar_RouteSegmentNotFound(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "does.not.exist",
	})

	_, err := s.Extract(map[string]string{"config.yaml": "foo: bar\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `route segment "does" not found`)
}

func TestConfigMapSource_BlockScalar_RouteSegmentNotANodeSequenceType(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels.nested",
	})

	// labels is a string, not a map — traversing into it should fail
	_, err := s.Extract(map[string]string{"config.yaml": "labels: just-a-string\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent is not a map")
}

func TestConfigMapSource_BlockScalar_RoutePointsToNodeScalarType(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	// labels is a string, not a map — final conversion should fail
	_, err := s.Extract(map[string]string{"config.yaml": "labels: just-a-string\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not point to a map")
}

func TestConfigMapSource_BlockScalar_BoolValuesConvertedToString(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	yaml := `
labels:
  active: true
  deprecated: false
`
	res, err := s.Extract(map[string]string{"config.yaml": yaml})
	require.NoError(t, err)
	assert.Equal(t, "true", res["active"])
	assert.Equal(t, "false", res["deprecated"])
}

func TestConfigMapSource_BlockScalar_IntValuesConvertedToString(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	yaml := `
labels:
  priority: 42
  replicas: 3
`
	res, err := s.Extract(map[string]string{"config.yaml": yaml})
	require.NoError(t, err)
	assert.Equal(t, "42", res["priority"])
	assert.Equal(t, "3", res["replicas"])
}

func TestConfigMapSource_BlockScalar_SequenceValueRejected(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	yamlData := `
labels:
  environments:
    - dev
    - staging
    - prod
`

	res, err := s.Extract(map[string]string{
		"config.yaml": yamlData,
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), `invalid label "environments"`)
	assert.Contains(t, err.Error(), "lists are not supported as label values")
}

func TestConfigMapSource_BlockScalar_NestedMapValueRejected(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	yamlData := `
labels:
  inner-map:
    my-key: value
    other-key: value
`

	res, err := s.Extract(map[string]string{
		"config.yaml": yamlData,
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), `invalid label "inner-map"`)
	assert.Contains(t, err.Error(), "nested maps are not supported as label values")
}

func TestConfigMapSource_BlockScalar_NullValuesRejected(t *testing.T) {
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
			s := newSource(&Config{
				ConfigMapName: "my-cm",
				Key:           "config.yaml",
				Route:         "labels",
			})

			yamlData := fmt.Sprintf(`
labels:
  priority: %s
`, tt.nullValue)

			res, err := s.Extract(map[string]string{
				"config.yaml": yamlData,
			})

			require.Error(t, err)
			assert.Nil(t, res)
			assert.Contains(t, err.Error(), `invalid label "priority"`)
			assert.Contains(t, err.Error(), "null values are not supported")
		})
	}
}

func TestConfigMapSource_BlockScalar_AliasValueRejected(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

	yamlData := `
defaultEnvironment: &defaultEnvironment production

labels:
  environment: *defaultEnvironment
`

	res, err := s.Extract(map[string]string{
		"config.yaml": yamlData,
	})

	require.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), `invalid label "environment"`)
	assert.Contains(t, err.Error(), "aliases are not supported as label values")
}

func TestConfigMapSource_BlockScalar_ComplexYamlWithTwoUnsupportedNodeType(t *testing.T) {
	s := newSource(&Config{
		ConfigMapName: "my-cm",
		Key:           "config.yaml",
		Route:         "labels",
	})

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

	res, err := s.Extract(map[string]string{"config.yaml": yaml})

	require.Error(t, err)
	assert.Nil(t, res)
}
