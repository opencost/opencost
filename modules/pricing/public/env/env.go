package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	AWSPricingURLEnvVar = "AWS_PRICING_URL"
)

// GetAWSPricingURL returns an optional alternative URL to fetch AWS pricing data from; for use in airgapped environments
func GetAWSPricingURL() string {
	return env.Get(AWSPricingURLEnvVar, "")
}
