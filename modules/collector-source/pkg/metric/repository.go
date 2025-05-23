package metric

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const ControllerEventName = "controller"

type RepositoryConfig struct {
}

// MetricRepository is an MetricUpdater which applies calls to update to all resolutions being tracked. It holds the
// MetricStore instances for each resolution.
type MetricRepository struct {
	lock             sync.Mutex
	resolutionStores map[string]*resolutionStores
	exporter         exporter.EventExporter[UpdateSet]
}

func NewMetricRepository(
	clusterID string,
	resolutions []util.ResolutionConfiguration,
	store storage.Storage,
	storeFactory MetricStoreFactory,
) *MetricRepository {
	resoluationCollectors := make(map[string]*resolutionStores)
	for _, resconf := range resolutions {
		resolution, err := util.NewResolution(resconf)
		if err != nil {
			log.Errorf("failed to create resolution %s", err.Error())
		}
		resCollector, err := newResolutionStores(resolution, storeFactory)
		if err != nil {
			log.Errorf("NewMetricRepository: failed to init resolution metric: %s", err.Error())
			continue
		}
		resoluationCollectors[resolution.Interval()] = resCollector
	}

	repo := &MetricRepository{
		resolutionStores: resoluationCollectors,
	}

	if store != nil {
		pathFormatter, err := pathing.NewEventStoragePathFormatter("", clusterID, ControllerEventName)
		if err != nil {
			log.Errorf("filed to create path formatter for scrape controller: %s", err.Error())
			return repo
		}
		encoder := exporter.NewJSONEncoder[UpdateSet]()
		repo.exporter = exporter.NewEventStorageExporter(
			pathFormatter,
			encoder,
			store,
		)
		// attempt to restore state from files
		// get path of saved files
		dirPath := path.Dir(pathFormatter.ToFullPath("", time.Time{}, ""))
		files, err := store.List(dirPath)
		if err != nil {
			log.Errorf("failed to list files in scrape controller: %s", err.Error())
		}
		// find oldest limit
		limit := time.Now().UTC()
		for _, resStore := range repo.resolutionStores {
			if limit.After(resStore.resolution.Limit()) {
				limit = resStore.resolution.Limit()
			}
		}

		// find files that are within limit
		var filesToRun []string
		for _, file := range files {
			fileName := path.Base(file.Name)
			timeString := strings.TrimSuffix(fileName, "."+encoder.FileExt())
			timestamp, err := time.Parse(pathing.EventStorageTimeFormat, timeString)
			if err != nil {
				log.Errorf("failed to parse fileName %s: %s", fileName, err.Error())
				continue
			}
			if timestamp.After(limit) {
				filesToRun = append(filesToRun, pathFormatter.ToFullPath("", timestamp, encoder.FileExt()))
			}
		}

		// sort files
		sort.Strings(filesToRun)

		// open files and run updates
		for _, fileName := range filesToRun {
			b, err := store.Read(fileName)
			if err != nil {
				log.Errorf("failed to load file contents for '%s': %s", fileName, err.Error())
				continue
			}
			updateSet := UpdateSet{}
			err = json.Unmarshal(b, &updateSet)
			if err != nil {
				log.Errorf("failed to unmarshal file %s: %s", fileName, err.Error())
				continue
			}
			filePrefix := path.Base(fileName)
			timeString := strings.TrimSuffix(filePrefix, "."+encoder.FileExt())
			timestamp, err := time.Parse(pathing.EventStorageTimeFormat, timeString)
			repo.Update(updateSet.Updates, timestamp)
		}
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
	updates []Update,
	timestamp time.Time,
) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, update := range updates {
		// Call update on the collectors for each resolution
		for _, resCollector := range r.resolutionStores {
			resCollector.update(update.Name, update.Labels, update.Value, timestamp, update.AdditionalInfo)
		}
	}

	if r.exporter != nil {
		err := r.exporter.Export(timestamp, &UpdateSet{
			Updates: updates,
		})
		if err != nil {
			log.Errorf("failed to export update results: %s", err.Error())
		}
	}
}

type UpdateSet struct {
	Updates []Update `json:"updates"`
}

type Update struct {
	Name           string            `json:"name"`
	Labels         map[string]string `json:"labels"`
	Value          float64           `json:"value"`
	AdditionalInfo map[string]string `json:"additionalInfo"`
}

func (r *MetricRepository) Coverage() map[string][]time.Time {
	r.lock.Lock()
	defer r.lock.Unlock()
	result := make(map[string][]time.Time)
	for resKey, resCollector := range r.resolutionStores {
		var windowStarts []time.Time
		for _, key := range resCollector.getKeys() {
			windowStarts = append(windowStarts, time.Unix(key, 0).UTC())
		}
		result[resKey] = windowStarts
	}
	return result
}

// resolutionStores is a grouping of a resolution and the instances of MetricStore that it is used to manage
type resolutionStores struct {
	lock       sync.Mutex
	resolution *util.Resolution
	collectors map[int64]MetricStore
	factory    func() MetricStore
}

func newResolutionStores(resolution *util.Resolution, factory MetricStoreFactory) (*resolutionStores, error) {
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
	collector.Update(metricName, labels, value, timestamp, additionalInformation)
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

func (r *resolutionStores) getKeys() []int64 {
	r.lock.Lock()
	defer r.lock.Unlock()
	var keys []int64
	for key := range r.collectors {
		keys = append(keys, key)
	}
	return keys
}
