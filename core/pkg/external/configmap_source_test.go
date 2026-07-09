package external

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSource returns a ConfigMapSource for the given config.
func newSource(cfg *Config) *ConfigMapSource {
	return &ConfigMapSource{cfg: cfg}
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

func TestConfigMapSource_BlockScalar_RouteSegmentNotAMap(t *testing.T) {
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

func TestConfigMapSource_BlockScalar_RoutePointsToScalar(t *testing.T) {
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
