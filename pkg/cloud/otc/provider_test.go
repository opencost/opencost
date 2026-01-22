package otc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockKey implements models.Key for testing
type mockKey struct {
	features string
}

func (m *mockKey) Features() string { return m.features }
func (m *mockKey) GPUType() string  { return "" }
func (m *mockKey) GPUCount() int    { return 0 }
func (m *mockKey) ID() string       { return "" }

func TestNodePricing_Success(t *testing.T) {
	pricing := map[string]*OTCPricing{
		"eu-de,s2.large.4,Open Linux": {
			NodeAttributes: &OTCNodeAttributes{
				Type:  "s2.large.4",
				OS:    "Open Linux",
				Price: "0.05",
				RAM:   "8",
				VCPU:  "2",
			},
		},
	}

	otc := &OTC{
		Pricing:          pricing,
		ValidPricingKeys: map[string]bool{"eu-de,s2.large.4,Open Linux": true},
		BaseCPUPrice:     "0.01",
		BaseRAMPrice:     "0.001",
		BaseGPUPrice:     "0.1",
	}
	key := &mockKey{features: "eu-de,s2.large.4,Open Linux"}

	node, meta, err := otc.NodePricing(key)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.NotNil(t, meta)
	assert.Equal(t, "0.05", node.Cost)
	assert.Equal(t, "2", node.VCPU)
	assert.Equal(t, "8", node.RAM)
}

func TestNodePricing_InvalidKey(t *testing.T) {
	otc := &OTC{
		Pricing:          map[string]*OTCPricing{},
		ValidPricingKeys: map[string]bool{},
		BaseCPUPrice:     "0.01",
		BaseRAMPrice:     "0.001",
		BaseGPUPrice:     "0.1",
	}
	key := &mockKey{features: "unknown-region,unknown-type,unknown-os"}

	node, _, err := otc.NodePricing(key)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "invalid Pricing Key")
}

func TestNodePricing_ValidKeyButNotLoaded(t *testing.T) {
	// This tests the case where the key is valid but pricing data is not loaded
	// In a real scenario, DownloadPricingData would be called, but we can't easily mock that
	// So we test the error path where download fails

	otc := &OTC{
		Pricing:          map[string]*OTCPricing{},
		ValidPricingKeys: map[string]bool{"eu-de,s2.large.4,Open Linux": true},
		BaseCPUPrice:     "0.01",
		BaseRAMPrice:     "0.001",
		BaseGPUPrice:     "0.1",
	}
	key := &mockKey{features: "eu-de,s2.large.4,Open Linux"}

	// This will trigger the download path which will fail since we're not in a real environment
	// but it exercises the logging code path
	node, _, err := otc.NodePricing(key)

	// The function returns a node with base pricing on error
	assert.Error(t, err)
	if node != nil {
		// If we got a node back, it should have base pricing
		assert.Equal(t, "0.01", node.BaseCPUPrice)
	}
}

func TestNodePricing_EmptyPricing(t *testing.T) {
	otc := &OTC{
		Pricing:          map[string]*OTCPricing{},
		ValidPricingKeys: map[string]bool{},
		BaseCPUPrice:     "0.01",
		BaseRAMPrice:     "0.001",
		BaseGPUPrice:     "0.1",
	}
	key := &mockKey{features: "eu-de,s2.large.4,Open Linux"}

	node, _, err := otc.NodePricing(key)

	assert.Error(t, err)
	assert.Nil(t, node)
}
