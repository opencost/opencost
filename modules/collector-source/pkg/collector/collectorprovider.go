package collector

import (
	"fmt"
	"sort"
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
// adding one interval to the truncated value. The duration between start and end is compared with the
// duration between the interval-generated times, with the lowest diff selected.
func (r *repoStoreProvider) getStoreKeys(start, end time.Time) (string, time.Time) {
	windowDuration := int64(end.Sub(start))
	type candidate struct {
		diff     int64
		duration int64
		key      string
		start    time.Time
		set      bool
	}
	var best candidate
	var fallback candidate
	keys := make([]string, 0, len(r.intervals))
	for key := range r.intervals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		interval := r.intervals[key]
		intStart := interval.Truncate(start)
		intEnd := interval.Add(intStart, 1)
		intDuration := int64(intEnd.Sub(intStart))
		diffDuration := windowDuration - intDuration
		if diffDuration < 0 {
			diffDuration = -diffDuration
		}

		if !fallback.set || diffDuration < fallback.diff || diffDuration == fallback.diff && intDuration < fallback.duration {
			fallback = candidate{
				diff:     diffDuration,
				duration: intDuration,
				key:      key,
				start:    intStart,
				set:      true,
			}
		}

		if intDuration == windowDuration && !intStart.Equal(start) {
			continue
		}

		if !best.set || diffDuration < best.diff || diffDuration == best.diff && intDuration < best.duration {
			best = candidate{
				diff:     diffDuration,
				duration: intDuration,
				key:      key,
				start:    intStart,
				set:      true,
			}
		}
	}
	if best.set {
		return best.key, best.start
	}
	return fallback.key, fallback.start
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
