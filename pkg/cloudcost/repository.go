package cloudcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// Repository is an interface for storing and retrieving CloudCost data.
// Sets returned by Get may be shared with the repository's internal storage
// and must be treated as read-only by callers. Sets passed to Put become owned
// by the repository and must not be mutated afterward.
type Repository interface {
	Has(time.Time, string) (bool, error)
	Get(time.Time, string) (*opencost.CloudCostSet, error)
	Keys() ([]string, error)
	Put(*opencost.CloudCostSet) error
	Expire(time.Time) error
}
