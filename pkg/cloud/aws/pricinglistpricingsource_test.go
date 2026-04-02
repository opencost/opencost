package aws

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/env"
)

func TestPublicAPIPricingSource_GetPricing(t *testing.T) {
	env.Set(env.ConfigPathEnvVar, "./")
	p := PublicAPIPricingSource{}
	_, err := p.GetPricing()
	if err != nil {
		t.Errorf("GetPricing() error = %v", err)
		return
	}
}
