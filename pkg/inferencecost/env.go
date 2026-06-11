package inferencecost

import (
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/pkg/env"
)

func isInferenceCostEnabled() bool         { return env.IsInferenceCostEnabled() }
func getPrometheusURL() string              { return coreenv.Get("PROMETHEUS_SERVER_ENDPOINT", "") }
func getModelLabel() string                 { return env.GetInferenceModelLabel() }
func getSharedInfraLabel() string           { return env.GetInferenceSharedInfraLabel() }
func getSharedInfraLabelValue() string      { return env.GetInferenceSharedInfraLabelValue() }
func getKVCacheBlockSize() float64          { return env.GetInferenceKVCacheBlockSize() }
func getCollectionInterval() time.Duration  { return env.GetInferenceCollectionInterval() }
