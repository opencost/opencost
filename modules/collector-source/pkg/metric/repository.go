package metric

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

type RepositoryConfig struct {
	Resolutions []util.ResolutionConfiguration
}

// MetricRepository is an MetricUpdater which applies calls to update to all resolutions being tracked. It holds the
// MetricStore instances for each resolution.
type MetricRepository struct {
	lock             sync.Mutex
	resolutionStores map[string]*resolutionStores
}

func NewMetricRepository(config RepositoryConfig, factory MetricStoreFactory) *MetricRepository {
	resoluationCollectors := make(map[string]*resolutionStores)

	for _, resConf := range config.Resolutions {
		resCollector, err := newResolutionStores(resConf, factory)
		if err != nil {
			log.Errorf("NewMetricRepository: failed to init resolution metric: %s", err.Error())
			continue
		}
		resoluationCollectors[resConf.Interval] = resCollector
	}

	repo := &MetricRepository{
		resolutionStores: resoluationCollectors,
	}

	return repo
}

func (r *MetricRepository) GetCollector(interval string, t time.Time) (MetricStore, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	resCollector, ok := r.resolutionStores[interval]
	if !ok {
		return nil, fmt.Errorf("failed to find resolution for key %s", interval)
	}

	return resCollector.getCollector(t)
}

// Update calls Update on the collectors for each resolution
func (r *MetricRepository) Update(
	metricName string,
	labels map[string]string,
	value float64,
	timestamp *time.Time,
	additionalInformation map[string]string,
) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if timestamp == nil {
		timestamp = util.Ptr(time.Now().UTC())
	}
	t := *timestamp
	// Call update on the collectors for each resolution
	for _, resCollector := range r.resolutionStores {
		resCollector.update(metricName, labels, value, t, additionalInformation)
	}
}

// resolutionStores is a grouping of a resolution and the instances of MetricStore that it is used to manage
type resolutionStores struct {
	lock       sync.Mutex
	resolution *util.Resolution
	collectors map[int64]MetricStore
	factory    func() MetricStore
}

func newResolutionStores(resConf util.ResolutionConfiguration, factory MetricStoreFactory) (*resolutionStores, error) {
	resolution, err := util.NewResolution(resConf)
	if err != nil {
		return nil, fmt.Errorf("NewResolutionCollectors: %w", err)
	}

	resCol := &resolutionStores{
		resolution: resolution,
		collectors: map[int64]MetricStore{},
		factory:    factory,
	}

	// Start loop which will remove expired MetricStore
	go func() {
		for {
			time.Sleep(resCol.resolution.Next().Sub(time.Now().UTC()))
			resCol.clean()
		}
	}()

	return resCol, nil
}

func (r *resolutionStores) clean() {
	r.lock.Lock()
	defer r.lock.Unlock()
	limitKey := r.resolution.Limit().UnixMilli()
	for key := range r.collectors {
		if key < limitKey {
			delete(r.collectors, key)
		}
	}
}

func (r *resolutionStores) update(
	metricName string,
	labels map[string]string,
	value float64,
	timestamp time.Time,
	additionalInformation map[string]string,
) {
	r.lock.Lock()
	defer r.lock.Unlock()
	limit := r.resolution.Limit()
	if timestamp.Before(limit) {
		log.Debugf(
			"failed to call update on resolution '%s' because Timestamp '%s' is before the limit '%s",
			r.resolution.Interval(),
			timestamp.Format(time.RFC3339),
			limit.Format(time.RFC3339),
		)
		return
	}
	key := r.resolution.Get(timestamp).UnixMilli()
	collector, ok := r.collectors[key]
	if !ok {
		collector = r.factory()
		r.collectors[key] = collector
	}
	collector.Update(metricName, labels, value, &timestamp, additionalInformation)
}

func (r *resolutionStores) getCollector(t time.Time) (MetricStore, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if t.Before(r.resolution.Limit()) {
		return nil, fmt.Errorf(
			"request for metric at time '%s' for resolution '%s' is past limit of '%s'",
			t.Format(time.RFC3339),
			r.resolution.Interval(),
			r.resolution.Limit().Format(time.RFC3339),
		)
	}
	key := r.resolution.Get(t).UnixMilli()

	collector, ok := r.collectors[key]
	if !ok {
		return nil, fmt.Errorf("failed to find MetricCollector for interval '%s' for time '%s'", r.resolution.Interval(), t.Format(time.RFC3339))
	}

	return collector, nil
}
