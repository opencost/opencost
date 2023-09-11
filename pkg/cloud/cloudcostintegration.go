package cloud

import (
	"time"

	"github.com/opencost/opencost/pkg/kubecost"
)

// CloudCostIntegration is an interface for retrieving daily granularity CloudCost data for a given range
type CloudCostIntegration interface {
	GetCloudCost(time.Time, time.Time) (*kubecost.CloudCostSetRange, error)
}
