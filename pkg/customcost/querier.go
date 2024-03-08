package customcost

import (
	"context"
	"fmt"
	"sync"
	"time"

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

func (q *Querier) QueryTotal(request CostTotalRequest, ctx context.Context) (*CostResponse, error) {
	repo, start, end, step := q.parseRequest(request.Start, request.End, request.Step)

	domains, err := repo.Keys()
	if err != nil {
		return nil, fmt.Errorf("QueryTotal: %w", err)
	}

	requestWindow := opencost.NewClosedWindow(request.Start, request.End)
	ccs := NewCustomCostSet(requestWindow)

	queryStart := start
	for queryStart.Before(end) {
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

func (q *Querier) QueryTimeseries(request CostTimeseriesRequest, ctx context.Context) (*CostTimeseriesResponse, error) {
	_, start, end, step := q.parseRequest(request.Start, request.End, request.Step)

	windows, err := opencost.GetWindows(start, end, step)
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
			totals[i], errors[i] = q.QueryTotal(CostTotalRequest{
				Start:       *window.Start(),
				End:         *window.End(),
				AggregateBy: request.AggregateBy,
				Filter:      request.Filter,
				Step:        step,
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

	result := &CostTimeseriesResponse{
		Window:     opencost.NewClosedWindow(start, end),
		Timeseries: totals,
	}

	return result, nil
}

func (q *Querier) parseRequest(requestStart, requestEnd time.Time, requestStep time.Duration) (Repository, time.Time, time.Time, time.Duration) {
	oldestHourlyData := time.Now().UTC().Add(-q.hourlyDuration)

	var step time.Duration
	var repo Repository
	if (requestStart.After(oldestHourlyData) || (requestStep == time.Hour)) &&
		(requestStep != timeutil.Day) {
		step = time.Hour
		repo = q.hourlyRepo
	} else {
		step = timeutil.Day
		repo = q.dailyRepo
	}
	start := opencost.RoundBack(requestStart, step)
	end := opencost.RoundBack(requestEnd, step)

	if requestStep != 0 {
		step = requestStep
	}

	return repo, start, end, step
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
