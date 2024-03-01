package customcost

import (
	"context"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/model"
	"github.com/opencost/opencost/core/pkg/opencost"
)

type Querier struct {
	hourlyRepo Repository
	dailyRepo  Repository
}

func NewQuerier(hourlyRepo, dailyRepo Repository) *Querier {
	return &Querier{
		hourlyRepo: hourlyRepo,
		dailyRepo:  dailyRepo,
	}
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
	// TODO determine if hourly or daily

	// TODO don't hardcode these
	domain := "datadog"
	startString := "2024-02-27T00:00:00Z"

	start, err := time.Parse("2006-01-02T15:04:05Z", startString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse start string: %v", err)
	}

	response, err := q.hourlyRepo.Get(start, domain)
	if err != nil {
		return nil, fmt.Errorf("error querying total for \"%s\" domain: %v", domain, err)
	} else if response == nil {
		return nil, fmt.Errorf("nil response querying total for \"%s\" domain: %v", domain, err)
	}

	var totalCost float32 = 0
	for _, cost := range response.Costs {
		totalCost += cost.GetListCost()
	}

	result := &CostTotalResult{
		TotalCost: totalCost,
		Response:  *response,
	}

	return result, nil
}

func (q *Querier) QueryTimeseries(request CostTimeseriesRequest, ctx context.Context) (*CostTimeseriesResult, error) {
	// TODO determine if hourly or daily based on step
	// TODO split given window into step-sized sub-windows, get response for each sub-window

	return nil, nil
}
