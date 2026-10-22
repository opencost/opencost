package external

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- test doubles ---

// stubLabelSource is a LabelSource whose behaviour is controlled by the test.
type stubLabelSource struct {
	labels map[string]string
	err    error
}

func (s *stubLabelSource) ExtractNodeLabels(_ map[string]string) (map[string]string, error) {
	return s.labels, s.err
}

// stubLabelProvider records the last Update call.
type stubLabelProvider struct {
	name   string
	labels map[string]string
	err    error
}

func (p *stubLabelProvider) Update(name string, data map[string]string) error {
	p.name = name
	p.labels = data
	return p.err
}

func (p *stubLabelProvider) Labels() (map[string]string, error) {
	return p.labels, nil
}

// --- WatchFunc tests ---

func TestWatchFunc_NilSource_ReturnsError(t *testing.T) {
	provider := &stubLabelProvider{}
	fn := WatchFunc(nil, provider)

	err := fn("cm", map[string]string{"k": "v"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil LabelSource")
}

func TestWatchFunc_NilProvider_ReturnsError(t *testing.T) {
	src := &stubLabelSource{labels: map[string]string{"k": "v"}}
	fn := WatchFunc(src, nil)

	err := fn("cm", map[string]string{"k": "v"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil LabelProvider")
}

func TestWatchFunc_HappyPath_LabelsForwardedToProvider(t *testing.T) {
	src := &stubLabelSource{labels: map[string]string{"cluster": "prod", "region": "us-east-1"}}
	provider := &stubLabelProvider{}

	fn := WatchFunc(src, provider)
	err := fn("my-cm", map[string]string{"raw": "data"})

	require.NoError(t, err)
	assert.Equal(t, "my-cm", provider.name)
	assert.Equal(t, map[string]string{"cluster": "prod", "region": "us-east-1"}, provider.labels)
}

func TestWatchFunc_SourceExtractError_PropagatesError(t *testing.T) {
	src := &stubLabelSource{err: fmt.Errorf("extract failed")}
	provider := &stubLabelProvider{}

	fn := WatchFunc(src, provider)
	err := fn("cm", map[string]string{})

	require.Error(t, err)
	assert.EqualError(t, err, "extract failed")
	// provider.Update must not have been called
	assert.Nil(t, provider.labels)
}

func TestWatchFunc_ProviderUpdateError_PropagatesError(t *testing.T) {
	src := &stubLabelSource{labels: map[string]string{"env": "dev"}}
	provider := &stubLabelProvider{err: fmt.Errorf("update failed")}

	fn := WatchFunc(src, provider)
	err := fn("cm", map[string]string{})

	require.Error(t, err)
	assert.EqualError(t, err, "update failed")
}

func TestWatchFunc_EmptyLabels_ForwardedToProvider(t *testing.T) {
	src := &stubLabelSource{labels: map[string]string{}}
	provider := &stubLabelProvider{}

	fn := WatchFunc(src, provider)
	err := fn("cm", map[string]string{})

	require.NoError(t, err)
	assert.Empty(t, provider.labels)
}

func TestWatchFunc_ReturnedFuncCalledMultipleTimes_ProviderUpdatedEachTime(t *testing.T) {
	src := &stubLabelSource{}
	provider := &stubLabelProvider{}

	fn := WatchFunc(src, provider)

	src.labels = map[string]string{"a": "1"}
	require.NoError(t, fn("cm", map[string]string{}))
	assert.Equal(t, map[string]string{"a": "1"}, provider.labels)

	src.labels = map[string]string{"b": "2"}
	require.NoError(t, fn("cm", map[string]string{}))
	assert.Equal(t, map[string]string{"b": "2"}, provider.labels)
}
