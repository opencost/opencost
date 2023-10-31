package cloudcost

import (
	"time"

	"github.com/opencost/opencost/pkg/kubecost"
)

// Repository is an interface for storing and retrieving CloudCost data
type Repository interface {
	Has(time.Time, string) (bool, error)
	Get(time.Time, string) (*kubecost.CloudCostSet, error)
	Keys() ([]string, error)
	Put(*kubecost.CloudCostSet) error
	Expire(time.Time) error
}
