package cloudcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// Repository is an interface for storing and retrieving CloudCost data
type Repository interface {
	Has(time.Time, string) (bool, error)
	Get(time.Time, string) (*opencost.CloudCostSet, error)
	Keys() ([]string, error)
	// Put takes ownership of the set and may compact it in place; callers must not use it afterwards.
	Put(*opencost.CloudCostSet) error
	Expire(time.Time) error
}
