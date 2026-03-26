package aws

import (
	"testing"
)

func TestPublicAPIPricingSource_GetPricing(t *testing.T) {
	p := PublicAPIPricingSource{}
	_, err := p.GetPricing()
	if err != nil {
		t.Errorf("GetPricing() error = %v", err)
		return
	}
}
