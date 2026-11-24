package inferencecost

import (
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/pkg/env"
)

func isInferenceCostEnabled() bool         { return env.IsInferenceCostEnabled() }
func getPrometheusURL() string             { return coreenv.Get("PROMETHEUS_SERVER_ENDPOINT", "") }
func getModelLabel() string                { return env.GetInferenceModelLabel() }
func getSharedInfraLabel() string          { return env.GetInferenceSharedInfraLabel() }
func getSharedInfraLabelValue() string     { return env.GetInferenceSharedInfraLabelValue() }
func getCollectionInterval() time.Duration { return env.GetInferenceCollectionInterval() }

func isInferenceAttributionEnabled() bool                { return env.IsInferenceAttributionEnabled() }
func getAttributionConfigPath() string                   { return env.GetInferenceAttributionConfigPath() }
func getAttributionInputTokensSeries() string            { return env.GetInferenceAttributionInputTokensSeries() }
func getAttributionOutputTokensSeries() string           { return env.GetInferenceAttributionOutputTokensSeries() }
func getAttributionInputTokensEstimatedSeries() string   { return env.GetInferenceAttributionInputTokensEstimatedSeries() }
func getAttributionOutputTokensEstimatedSeries() string  { return env.GetInferenceAttributionOutputTokensEstimatedSeries() }
func getAttributionTenantLabel() string                  { return env.GetInferenceAttributionTenantLabel() }
func getAttributionWorkloadLabel() string                { return env.GetInferenceAttributionWorkloadLabel() }
func getAttributionUserLabel() string                    { return env.GetInferenceAttributionUserLabel() }
func getAttributionModelLabel() string                   { return env.GetInferenceAttributionModelLabel() }
func getAttributionNamespaceLabel() string               { return env.GetInferenceAttributionNamespaceLabel() }
func getAttributionNamespaceMap() string                 { return env.GetInferenceAttributionNamespaceMap() }
func getAttributionUserSentinel() string                 { return env.GetInferenceAttributionUserSentinel() }
func getAttributionIdentitySentinel() string             { return env.GetInferenceAttributionIdentitySentinel() }
func getAttributionCoverageThreshold() float64           { return env.GetInferenceAttributionCoverageThreshold() }
func getAttributionOverattributionTolerance() float64    { return env.GetInferenceAttributionOverattributionTolerance() }
func getAttributionReferenceEngine() string              { return env.GetInferenceAttributionReferenceEngine() }
func isInferenceAttributionUserAggregationEnabled() bool { return env.IsInferenceAttributionUserAggregationEnabled() }
