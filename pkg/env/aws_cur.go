package env

import (
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
)

const (
	// CURNodePricingEnabledEnvVar enables CUR-based effective node pricing reconciliation.
	CURNodePricingEnabledEnvVar = "CUR_NODE_PRICING_ENABLED"

	// CURNodePricingRefreshHoursEnvVar controls how often (in hours) the CUR node
	// pricing cache is refreshed from Athena.
	CURNodePricingRefreshHoursEnvVar = "CUR_NODE_PRICING_REFRESH_HOURS"
)

// IsCURNodePricingEnabled returns true when CUR_NODE_PRICING_ENABLED is set to "true".
// Defaults to false so existing deployments are unaffected.
func IsCURNodePricingEnabled() bool {
	return coreenv.GetBool(CURNodePricingEnabledEnvVar, false)
}

// GetCURNodePricingRefreshHours returns how many hours between CUR node pricing
// cache refreshes. Defaults to 6.
func GetCURNodePricingRefreshHours() time.Duration {
	hours := coreenv.GetInt(CURNodePricingRefreshHoursEnvVar, 6)
	if hours < 1 {
		hours = 1
	}
	return time.Duration(hours) * time.Hour
}
