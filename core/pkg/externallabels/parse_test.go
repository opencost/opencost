package externallabels

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newProvider is a test helper that returns a fresh ConfigMapProvider.
func newProvider(t *testing.T) *ConfigMapProvider {
	t.Helper()
	return NewConfigMapProvider()
}

// labelsOf is a test helper that returns the current labels from the provider.
func labelsOf(t *testing.T, p *ConfigMapProvider) map[string]string {
	t.Helper()
	labels, err := p.Labels(context.Background())
	require.NoError(t, err)
	return labels
}

// --- traditional ConfigMap (Key and Route both empty) ---

func TestParseFunc_Traditional_PassesDataDirectly(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{}, p)

	err := fn("my-cm", map[string]string{"env": "prod", "region": "us-east-1"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "region": "us-east-1"}, labelsOf(t, p))
}

func TestParseFunc_Traditional_EmptyData(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{}, p)

	require.NoError(t, fn("my-cm", map[string]string{}))
	assert.Empty(t, labelsOf(t, p))
}

// --- block-scalar ConfigMap ---

const prometheusConfig = `
prometheusK8s:
  externalLabels:
    cluster: prod-cluster
    region: eu-west-1
`

func TestParseFunc_BlockScalar_SingleLevel(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "prometheusK8s.externalLabels",
	}, p)

	err := fn("prometheus-cm", map[string]string{"config.yaml": prometheusConfig})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"cluster": "prod-cluster",
		"region":  "eu-west-1",
	}, labelsOf(t, p))
}

func TestParseFunc_BlockScalar_TopLevelRoute(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "data",
		Route: "labels",
	}, p)

	yaml := `
labels:
  team: platform
  env: staging
`
	require.NoError(t, fn("my-cm", map[string]string{"data": yaml}))
	assert.Equal(t, map[string]string{"team": "platform", "env": "staging"}, labelsOf(t, p))
}

// --- error cases ---

func TestParseFunc_BlockScalar_MissingKey(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "externalLabels",
	}, p)

	err := fn("my-cm", map[string]string{"other-key": "value"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `key "config.yaml" not found`)
}

func TestParseFunc_BlockScalar_InvalidYAML(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "labels",
	}, p)

	err := fn("my-cm", map[string]string{"config.yaml": ":\tinvalid: yaml: {"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse YAML")
}

func TestParseFunc_BlockScalar_RouteSegmentNotFound(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "does.not.exist",
	}, p)

	err := fn("my-cm", map[string]string{"config.yaml": "foo: bar\n"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `route segment "does" not found`)
}

func TestParseFunc_BlockScalar_RouteSegmentNotAMap(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "labels.nested",
	}, p)

	// labels is a string, not a map — traversing into it should fail
	yaml := "labels: just-a-string\n"
	err := fn("my-cm", map[string]string{"config.yaml": yaml})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent is not a map")
}

func TestParseFunc_BlockScalar_RoutePointsToScalar(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "labels",
	}, p)

	// labels is a string, not a map — final conversion should fail
	yaml := "labels: just-a-string\n"
	err := fn("my-cm", map[string]string{"config.yaml": yaml})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not point to a map")
}

func TestParseFunc_BlockScalar_BoolValuesConvertedToString(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "labels",
	}, p)

	yaml := `
labels:
  active: true
  deprecated: false
`
	require.NoError(t, fn("my-cm", map[string]string{"config.yaml": yaml}))
	labels := labelsOf(t, p)
	assert.Equal(t, "true", labels["active"])
	assert.Equal(t, "false", labels["deprecated"])
}

func TestParseFunc_BlockScalar_IntValuesConvertedToString(t *testing.T) {
	p := newProvider(t)
	fn := ParseFunc(&Config{
		Key:   "config.yaml",
		Route: "labels",
	}, p)

	yaml := `
labels:
  priority: 42
  replicas: 3
`
	require.NoError(t, fn("my-cm", map[string]string{"config.yaml": yaml}))
	labels := labelsOf(t, p)
	assert.Equal(t, "42", labels["priority"])
	assert.Equal(t, "3", labels["replicas"])
}
