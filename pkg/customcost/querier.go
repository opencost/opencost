package customcost

import (
	"context"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/model"
	"github.com/opencost/opencost/core/pkg/opencost"
)

type Querier struct {
}

func NewQuerier() *Querier {
	return &Querier{}
}

type CostTotalRequest struct {
	Start       time.Time
	End         time.Time
	AggregateBy []string
	Filter      filter.Filter
}

type CostTotalResult struct {
	TotalCost float32
	Response  model.CustomCostResponse
}

type CostTimeseriesRequest struct {
	Start       time.Time
	End         time.Time
	AggregateBy []string
	Step        time.Duration
	Filter      filter.Filter
}

type CostTimeseriesResult struct {
	TotalCost float32
	Window    *opencost.Window
	Response  []model.CustomCostResponse
}

func (q *Querier) QueryTotal(request CostTotalRequest, ctx context.Context) (*CostTotalResult, error) {
	// TODO return dummy data
	return nil, nil
}

func (q *Querier) QueryTimeseries(request CostTimeseriesRequest, ctx context.Context) (*CostTimeseriesResult, error) {
	// TODO return dummy data
	return nil, nil
}
