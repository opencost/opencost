package collector

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

// StoreProvider returns an appropriate collector for the given window. This is meant to bridge the mismatch of a system
// that was designed to make queries against a continuous datasource with now stores its data in discrete blocks
type StoreProvider interface {
	GetStore(start, end time.Time) metric.MetricStore
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
