package scaleway

import (
	"testing"

	"github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/stretchr/testify/assert"
)

// mockKey implements models.Key for testing
type mockKey struct {
	features string
	gpuType  string
}

func (m *mockKey) Features() string { return m.features }
func (m *mockKey) GPUType() string  { return m.gpuType }
func (m *mockKey) ID() string       { return "" }

func TestNodePricing_Success(t *testing.T) {
	gpuCount := uint64(0)
	pricing := map[string]*ScalewayPricing{
		"fr-par-1": {
			NodesInfos: map[string]*instance.ServerType{
				"DEV1-S": {
					HourlyPrice: 0.01,
					Ncpus:       2,
					RAM:         2147483648,
					Gpu:         &gpuCount,
					PerVolumeConstraint: &instance.ServerTypeVolumeConstraintsByType{
						LSSD: &instance.ServerTypeVolumeConstraintSizes{
							MinSize: 20000000000,
						},
					},
				},
			},
		},
	}

	c := &Scaleway{Pricing: pricing}
	key := &mockKey{features: "fr-par-1,DEV1-S"}

	node, meta, err := c.NodePricing(key)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, meta)
	assert.Equal(t, "DEV1-S", node.InstanceType)
	assert.Equal(t, "fr-par-1", node.Region)
}

func TestNodePricing_UnknownInstanceType(t *testing.T) {
	gpuCount := uint64(0)
	pricing := map[string]*ScalewayPricing{
		"fr-par-1": {
			NodesInfos: map[string]*instance.ServerType{
				"DEV1-S": {
					HourlyPrice: 0.01,
					Ncpus:       2,
					RAM:         2147483648,
					Gpu:         &gpuCount,
					PerVolumeConstraint: &instance.ServerTypeVolumeConstraintsByType{
						LSSD: &instance.ServerTypeVolumeConstraintSizes{
							MinSize: 20000000000,
						},
					},
				},
			},
		},
	}

	c := &Scaleway{Pricing: pricing}
	key := &mockKey{features: "fr-par-1,UNKNOWN-TYPE"}

	node, _, err := c.NodePricing(key)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "Unable to find node pricing")
}

func TestNodePricing_UnknownZone(t *testing.T) {
	pricing := map[string]*ScalewayPricing{
		"fr-par-1": {
			NodesInfos: map[string]*instance.ServerType{},
		},
	}

	c := &Scaleway{Pricing: pricing}
	key := &mockKey{features: "unknown-zone,DEV1-S"}

	node, _, err := c.NodePricing(key)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "Unable to find node pricing")
}

func TestNodePricing_EmptyPricing(t *testing.T) {
	c := &Scaleway{Pricing: map[string]*ScalewayPricing{}}
	key := &mockKey{features: "fr-par-1,DEV1-S"}

	node, _, err := c.NodePricing(key)

	assert.Error(t, err)
	assert.Nil(t, node)
}

func TestNodePricing_SingleElementFeatures(t *testing.T) {
	pricing := map[string]*ScalewayPricing{
		"fr-par-1": {
			NodesInfos: map[string]*instance.ServerType{},
		},
	}

	c := &Scaleway{Pricing: pricing}
	key := &mockKey{features: "fr-par-1"}

	node, _, err := c.NodePricing(key)

	// Should fail because instance type is empty
	assert.Error(t, err)
	assert.Nil(t, node)
}
