package customcost

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

type RepositoryQuerier struct {
	hourlyRepo     Repository
	dailyRepo      Repository
	hourlyDuration time.Duration
	dailyDuration  time.Duration
}

func NewRepositoryQuerier(hourlyRepo, dailyRepo Repository, hourlyDuration, dailyDuration time.Duration) *RepositoryQuerier {
	return &RepositoryQuerier{
		hourlyRepo:     hourlyRepo,
		dailyRepo:      dailyRepo,
		hourlyDuration: hourlyDuration,
		dailyDuration:  dailyDuration,
	}
}

func (rq *RepositoryQuerier) QueryTotal(ctx context.Context, request CostTotalRequest) (*CostResponse, error) {
	repo := rq.dailyRepo
	step := timeutil.Day
	if request.Accumulate == opencost.AccumulateOptionHour {
		repo = rq.hourlyRepo
		step = time.Hour
	}
	domains, err := repo.Keys()
	if err != nil {
		return nil, fmt.Errorf("QueryTotal: %w", err)
	}

	requestWindow := opencost.NewClosedWindow(request.Start, request.End)
	ccs := NewCustomCostSet(requestWindow)
	queryStart := request.Start
	for queryStart.Before(request.End) {
		queryEnd := queryStart.Add(step)

		for _, domain := range domains {
			ccResponse, err := repo.Get(queryStart, domain)
			if err != nil {
				return nil, fmt.Errorf("QueryTotal: %w", err)
			} else if ccResponse == nil {
				continue
			}

			customCosts := ParseCustomCostResponse(ccResponse)
			ccs.Add(customCosts)
		}

		queryStart = queryEnd
	}

	err = ccs.Aggregate(request.AggregateBy)
	if err != nil {
		return nil, err
	}

	return NewCostResponse(ccs), nil
}

func (rq *RepositoryQuerier) QueryTimeseries(ctx context.Context, request CostTimeseriesRequest) (*CostTimeseriesResponse, error) {
	window, _ := opencost.NewClosedWindow(request.Start, request.End).GetAccumulateWindow(request.Accumulate)

	windows, err := window.GetAccumulateWindows(request.Accumulate)
	if err != nil {
		return nil, fmt.Errorf("error getting timeseries windows: %w", err)
	}

	totals := make([]*CostResponse, len(windows))
	errors := make([]error, len(windows))

	// Query concurrently for each result, error
	var wg sync.WaitGroup
	wg.Add(len(windows))

	for i, w := range windows {
		go func(i int, window opencost.Window, res []*CostResponse) {
			defer wg.Done()
			totals[i], errors[i] = rq.QueryTotal(ctx, CostTotalRequest{
				Start:       *window.Start(),
				End:         *window.End(),
				AggregateBy: request.AggregateBy,
				Filter:      request.Filter,
				Accumulate:  request.Accumulate,
			})
		}(i, w, totals)
	}

	wg.Wait()

	// Return an error if any errors occurred
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("one of %d errors: error querying costs for %s: %w", numErrors(errors), windows[i], err)
		}
	}

	result := &CostTimeseriesResponse{
		Window:     window,
		Timeseries: totals,
	}

	return result, nil
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
