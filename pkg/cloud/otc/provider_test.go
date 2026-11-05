package otc

import (
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
)

// otcMockConfig implements models.ProviderConfig for testing RefreshCustomPricing.
type otcMockConfig struct {
	customPricing *models.CustomPricing
}

func (m *otcMockConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	return m.customPricing, nil
}

func (m *otcMockConfig) Update(_ func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *otcMockConfig) UpdateFromMap(_ map[string]string) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *otcMockConfig) ConfigFileManager() *config.ConfigFileManager {
	return nil
}

func TestOTCRefreshCustomPricing(t *testing.T) {
	cp := &models.CustomPricing{
		CPU:       "0.025",
		RAM:       "0.003",
		GPU:       "1.0",
		ProjectID: "my-project",
	}
	o := &OTC{Config: &otcMockConfig{customPricing: cp}}
	if err := o.RefreshCustomPricing(); err != nil {
		t.Fatalf("OTC.RefreshCustomPricing() unexpected error: %v", err)
	}
	if o.BaseCPUPrice != cp.CPU {
		t.Errorf("BaseCPUPrice = %q, want %q", o.BaseCPUPrice, cp.CPU)
	}
	if o.BaseRAMPrice != cp.RAM {
		t.Errorf("BaseRAMPrice = %q, want %q", o.BaseRAMPrice, cp.RAM)
	}
	if o.BaseGPUPrice != cp.GPU {
		t.Errorf("BaseGPUPrice = %q, want %q", o.BaseGPUPrice, cp.GPU)
	}
	if o.projectID != cp.ProjectID {
		t.Errorf("projectID = %q, want %q", o.projectID, cp.ProjectID)
	}
}
