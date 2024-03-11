package customcost

import (
	"context"
)

type Querier interface {
	QueryTotal(ctx context.Context, request CostTotalRequest) (*CostResponse, error)
	QueryTimeseries(ctx context.Context, request CostTimeseriesRequest) (*CostTimeseriesResponse, error)
}
