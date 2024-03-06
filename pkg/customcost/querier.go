package customcost

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/model"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

type Querier struct {
	hourlyRepo     Repository
	dailyRepo      Repository
	hourlyDuration time.Duration
	dailyDuration  time.Duration
}

func NewQuerier(hourlyRepo, dailyRepo Repository, hourlyDuration, dailyDuration time.Duration) *Querier {
	return &Querier{
		hourlyRepo:     hourlyRepo,
		dailyRepo:      dailyRepo,
		hourlyDuration: hourlyDuration,
		dailyDuration:  dailyDuration,
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
	Response  []model.CustomCostResponse
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
	Window    opencost.Window
	Response  []model.CustomCostResponse
}

func (q *Querier) QueryTotal(request CostTotalRequest, ctx context.Context) (*CostTotalResult, error) {
	repo, queryTime, err := q.repoForTime(request.Start)
	if err != nil {
		return nil, fmt.Errorf("QueryTotal: %w", err)
	}

	keys, err := repo.Keys()
	if err != nil {
		return nil, fmt.Errorf("QueryTotal: %w", err)
	}

	result := &CostTotalResult{
		Response: []model.CustomCostResponse{},
	}

	for _, key := range keys {
		response, err := repo.Get(queryTime, key)
		if err != nil {
			return nil, fmt.Errorf("QueryTotal: %w", err)
		} else if response == nil {
			continue
		}

		for _, customCost := range response.GetCosts() {
			result.TotalCost += customCost.GetListCost()
		}
		result.Response = append(result.Response, *response)
	}

	return result, nil
}

func (q *Querier) QueryTimeseries(request CostTimeseriesRequest, ctx context.Context) (*CostTimeseriesResult, error) {
	window := opencost.NewClosedWindow(request.Start, request.End)
	if !request.Start.Before(request.End) {
		return nil, fmt.Errorf("invalid window: %s", window)
	}

	windows, err := opencost.GetWindows(request.Start, request.End, request.Step)
	if err != nil {
		return nil, fmt.Errorf("error getting timeseries windows: %w", err)
	}

	totals := make([]*CostTotalResult, len(windows))
	errors := make([]error, len(windows))

	// Query concurrently for each result, error
	var wg sync.WaitGroup
	wg.Add(len(windows))

	for i, w := range windows {
		go func(i int, window opencost.Window, res []*CostTotalResult) {
			defer wg.Done()
			totals[i], errors[i] = q.QueryTotal(CostTotalRequest{
				Start:       *window.Start(),
				End:         *window.End(),
				AggregateBy: request.AggregateBy,
				Filter:      request.Filter,
			}, ctx)
		}(i, w, totals)
	}

	wg.Wait()

	// Return an error if any errors occurred
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("one of %d errors: error querying costs for %s: %w", numErrors(errors), windows[i], err)
		}
	}

	result := &CostTimeseriesResult{
		Window:   window,
		Response: []model.CustomCostResponse{},
	}

	for _, total := range totals {
		if total.Response == nil {
			continue
		}

		result.Response = append(result.Response, total.Response...)
		result.TotalCost += total.TotalCost
	}

	return result, nil
}

func (q *Querier) repoForTime(t time.Time) (Repository, time.Time, error) {
	now := time.Now().UTC()
	if t.After(now.Add(-q.hourlyDuration)) {
		t = opencost.RoundBack(t, time.Hour)
		return q.hourlyRepo, t, nil
	} else {
		t = opencost.RoundBack(t, timeutil.Day)
		return q.dailyRepo, t, nil
	}
}

func numErrors(errors []error) int {
	numErrs := 0
	for i := range errors {
		if errors[i] != nil {
			numErrs++
		}
	}
	return numErrs
}
