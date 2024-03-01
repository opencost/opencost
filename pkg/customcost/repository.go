package customcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/model"
)

// Repository is an interface for storing and retrieving CloudCost data
type Repository interface {
	Has(time.Time, string) (bool, error)
	Get(time.Time, string) ([]*model.CustomCostResponse, error)
	Keys() ([]string, error)
	Put([]*model.CustomCostResponse) error
	Expire(time.Time) error
}
