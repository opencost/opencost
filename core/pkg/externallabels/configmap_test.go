package externallabels

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigMapProvider_Labels(t *testing.T) {
	p := NewConfigMapProvider()

	require.NoError(t, p.Update("external-labels", map[string]string{
		"cluster": "1de3e77b-266d-48c1-91cb-ec5e22902af7",
		"env":     "dev",
		"region":  "nam",
	}))

	labels, err := p.Labels(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "1de3e77b-266d-48c1-91cb-ec5e22902af7", labels["cluster"])
	assert.Equal(t, "dev", labels["env"])
	assert.Equal(t, "nam", labels["region"])
}

func TestConfigMapProvider_EmptyOnNoUpdates(t *testing.T) {
	p := NewConfigMapProvider()

	labels, err := p.Labels(context.Background())
	require.NoError(t, err)
	assert.Empty(t, labels)
}

func TestConfigMapProvider_UpdateDropsRemovedKeys(t *testing.T) {
	p := NewConfigMapProvider()

	require.NoError(t, p.Update("cm", map[string]string{"a": "1", "b": "2"}))
	// second update removes "b" — the whole map is replaced
	require.NoError(t, p.Update("cm", map[string]string{"a": "1"}))

	labels, err := p.Labels(context.Background())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, labels)
}
