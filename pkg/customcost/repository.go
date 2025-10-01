package customcost

import (
	"time"

	custompb "github.com/opencost/opencost/core/pkg/customcost/pb"
)

// Repository is an interface for storing and retrieving CustomCost data
type Repository interface {
	Has(time.Time, string) (bool, error)
	Get(time.Time, string) (*custompb.CustomCostResponse, error)
	Keys() ([]string, error)
	Put(*custompb.CustomCostResponse) error
	Expire(time.Time) error
}
