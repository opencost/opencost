package external

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerge_ExternalAddedToBase(t *testing.T) {
	base := map[string]string{"node": "worker-1"}
	external := map[string]string{"region": "us-east-1"}

	got := Merge(base, external)

	assert.Equal(t, map[string]string{"node": "worker-1", "region": "us-east-1"}, got)
}

func TestMerge_BaseWinsOnConflict(t *testing.T) {
	base := map[string]string{"region": "from-node"}
	external := map[string]string{"region": "from-configmap"}

	got := Merge(base, external)

	assert.Equal(t, "from-node", got["region"])
}

func TestMerge_EmptyExternal(t *testing.T) {
	base := map[string]string{"node": "worker-1"}

	got := Merge(base, map[string]string{})

	assert.Equal(t, base, got)
}

func TestMerge_NilExternal(t *testing.T) {
	base := map[string]string{"node": "worker-1"}

	got := Merge(base, nil)

	assert.Equal(t, base, got)
}

func TestMerge_EmptyBase(t *testing.T) {
	external := map[string]string{"region": "us-east-1"}

	got := Merge(map[string]string{}, external)

	assert.Equal(t, map[string]string{"region": "us-east-1"}, got)
}

func TestMerge_BothEmpty(t *testing.T) {
	got := Merge(map[string]string{}, map[string]string{})

	assert.Empty(t, got)
}

// TestMerge_DoesNotMutateBase proves the original base map is not modified.
// A naive implementation using `out := base` copies the map header only,
// so writes to out also mutate the caller's map.
func TestMerge_DoesNotMutateBase(t *testing.T) {
	base := map[string]string{"node": "worker-1"}
	external := map[string]string{"region": "us-east-1"}

	Merge(base, external)

	assert.Equal(t, map[string]string{"node": "worker-1"}, base, "Merge must not mutate the base map")
}
