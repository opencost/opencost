package env

import (
	"strings"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
)

const (
	// CURNodePricingEnabledEnvVar enables CUR-based effective node pricing reconciliation.
	CURNodePricingEnabledEnvVar = "CUR_NODE_PRICING_ENABLED"

	// CURNodePricingRefreshHoursEnvVar controls how often (in hours) the CUR node
	// pricing cache is refreshed from Athena.
	CURNodePricingRefreshHoursEnvVar = "CUR_NODE_PRICING_REFRESH_HOURS"

	// CURNodePricingGranularityEnvVar declares the CUR export granularity:
	// "auto" (default), "hourly" or "daily". CUR exports can be configured at
	// either granularity; misreading daily rows as hourly inflates rates 24x.
	// "auto" derives covered hours from usage_start/usage_end per row, which is
	// exact for hourly, daily and mixed exports.
	CURNodePricingGranularityEnvVar = "CUR_NODE_PRICING_GRANULARITY"
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

// GetCURNodePricingGranularity returns the configured CUR export granularity:
// "auto", "hourly" or "daily". Invalid values fall back to "auto".
func GetCURNodePricingGranularity() string {
	g := strings.ToLower(strings.TrimSpace(coreenv.Get(CURNodePricingGranularityEnvVar, "auto")))
	switch g {
	case "auto", "hourly", "daily":
		return g
	default:
		return "auto"
	}
}
