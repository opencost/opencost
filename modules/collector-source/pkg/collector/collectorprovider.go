package collector

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

// StoreProvider returns an appropriate collector for the given window. This is meant to bridge the mismatch of a system
// that was designed to make queries against a continuous datasource with now stores its data in discrete blocks
type StoreProvider interface {
	GetStore(start, end time.Time) metric.MetricStore
	GetDailyDataCoverage(limitDays int) (time.Time, time.Time, error)
}

// repoStoreProvider is a StoreProvider implementation which uses a Repository and the Intervals of its Resolutions that it is
// configured with to return the most appropriate time.
type repoStoreProvider struct {
	repo      *metric.MetricRepository
	intervals map[string]util.Interval
}

func newRepoStoreProvider(repo *metric.MetricRepository, resoluationConfigs []util.ResolutionConfiguration) *repoStoreProvider {
	intervals := make(map[string]util.Interval)
	for _, resConf := range resoluationConfigs {
		interval, err := util.NewInterval(resConf.Interval)
		if err != nil {
			continue
		}
		intervals[resConf.Interval] = interval
	}
	return &repoStoreProvider{
		repo:      repo,
		intervals: intervals,
	}
}

func (r *repoStoreProvider) GetStore(start, end time.Time) metric.MetricStore {
	resKey, start := r.getStoreKeys(start, end)
	store, err := r.repo.GetCollector(resKey, start)
	if err != nil {
		log.Debugf("failed to get Store for window '%s - %s': %s", start, end, err)
	}
	return store
}

// getStoreKeys compares the given start and end against each resolution by truncating the start time and
// add one interval to the truncated value. The duration between start and end is compared with the duration
// between the interval generated times, with the lowest
func (r *repoStoreProvider) getStoreKeys(start, end time.Time) (string, time.Time) {
	windowDuration := int64(end.Sub(start))
	var minDiff *int64
	var minKey string
	var minStart time.Time
	for key, interval := range r.intervals {
		intStart := interval.Truncate(start)
		intEnd := interval.Add(start, 1)
		intDuration := int64(intEnd.Sub(intStart))
		diffDuration := windowDuration - intDuration
		if diffDuration < 0 {
			diffDuration = -diffDuration
		}
		if minDiff == nil || diffDuration < *minDiff {
			minDiff = &diffDuration
			minKey = key
			minStart = intStart
		}
	}
	return minKey, minStart
}

// GetDailyDataCoverage this is a bit of a hacky add-on to help fulfill the metricsquerier interface
func (r *repoStoreProvider) GetDailyDataCoverage(limitDays int) (time.Time, time.Time, error) {
	coverage := r.repo.Coverage()
	dailyCoverage, ok := coverage["1d"]
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("daily resolution is not configured")
	}
	if len(dailyCoverage) == 0 {
		// If daily coverage is not available, fallback to a reasonable time range
		// This prevents CSV export from failing when the metric doesn't exist yet
		log.Warnf("GetDailyDataCoverage: daily coverage not available, using fallback time range")

		// Use a reasonable fallback: start from 1 day ago to account for metric collection delay
		fallbackEnd := time.Now().UTC().Truncate(timeutil.Day)
		fallbackStart := fallbackEnd.AddDate(0, 0, -1) // 1 day ago

		return fallbackStart, fallbackEnd, nil
	}
	start := dailyCoverage[0]
	end := dailyCoverage[0]
	for _, window := range dailyCoverage {
		if start.After(window) {
			start = window
		}
		if end.Before(window) {
			end = window
		}
	}
	limit := time.Now().UTC().Truncate(timeutil.Day).Add(-timeutil.Day * time.Duration(limitDays))
	if start.Before(limit) {
		start = limit
	}
	// since all times that we have been looking at are window start times,
	// add a day to end time to create the actual coverage
	end = end.Add(timeutil.Day)
	return start, end, nil
}
