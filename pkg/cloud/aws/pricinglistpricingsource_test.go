package aws

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
)

func TestPublicAPIPricingSource_GetPricing(t *testing.T) {
	env.Set(env.ConfigPathEnvVar, "./")
	p := PublicAPIPricingSource{}
	_, err := p.GetPricing(time.Now(), time.Now())
	if err != nil {
		t.Errorf("GetPricing() error = %v", err)
		return
	}
}
