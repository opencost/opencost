package pricingmodel

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	corestorage "github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud/aws"
)

// Pipeline manages a set of runners, one per PricingSource, exporting pricing
// model snapshots to bucket storage on a configured interval.
//
// Initially constructed with a fixed set of always-on sources. Additional
// sources can be registered dynamically via AddSource to support
// config-driven sources in the future (similar to the CloudCost ingestion
// manager's observer pattern).
type Pipeline struct {
	lock    sync.Mutex
	runners map[string]*runner
	store   *storageWriter
	config  PipelineConfig
}

// NewPipeline creates a Pipeline for the given sources and storage backend.
// If cfg is nil, DefaultPipelineConfig is used.
// The storage should be initialized by the caller via storage.InitializeStorage
// or storage.GetDefaultStorage, matching how CloudCost storage is wired up.
func NewPipeline(store corestorage.Storage, cfg PipelineConfig) (*Pipeline, error) {

	ps, err := newStorageWriter(store, cfg.AppName)
	if err != nil {
		return nil, fmt.Errorf("NewPipeline: %w", err)
	}

	p := &Pipeline{
		runners: make(map[string]*runner),
		store:   ps,
		config:  cfg,
	}
	lastUpdates, err := ps.LastUpdates()
	if err != nil {
		log.Warnf("NewPipeline: failed to load last update times, runners will start immediately: %s", err.Error())
		lastUpdates = map[string]time.Time{}
	}

	if cfg.AWSRunnerConfig.Enabled {
		src := &aws.PricingListPricingSource{}
		rc := runnerConfig{
			interval: cfg.AWSRunnerConfig.RefreshInterval,
		}
		if t, ok := lastUpdates[src.PricingSourceKey()]; ok {
			rc.lastRun = &t
		}
		p.addSource(src, rc)
	}
	return p, nil
}

// StartAll starts all registered runners.
func (p *Pipeline) StartAll() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, r := range p.runners {
		r.Start()
	}
}

// StopAll stops all registered runners.
func (p *Pipeline) StopAll() {
	p.lock.Lock()
	defer p.lock.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(p.runners))
	for _, r := range p.runners {
		go func(r *runner) {
			defer wg.Done()
			r.Stop()
		}(r)
	}
	wg.Wait()
}

// AddSource registers a new PricingSource and starts its runner. If a source
// with the same key already exists it is stopped and replaced.
func (p *Pipeline) AddSource(src pricingmodel.PricingSource, cfg runnerConfig) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.addSource(src, cfg)
}

// RemoveSource stops and removes the runner for the given source key.
func (p *Pipeline) RemoveSource(key string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.removeSource(key)
}

func (p *Pipeline) addSource(src pricingmodel.PricingSource, cfg runnerConfig) {
	key := src.PricingSourceKey()
	p.removeSource(key)
	log.Infof("PricingModel: pipeline: adding source %s", key)
	r := newRunner(src, p.store, cfg)
	r.Start()
	p.runners[key] = r
}

// Status returns the current status of all runners.
func (p *Pipeline) Status() []Status {
	p.lock.Lock()
	defer p.lock.Unlock()
	statuses := make([]Status, 0, len(p.runners))
	for _, r := range p.runners {
		statuses = append(statuses, r.Status())
	}
	return statuses
}

// Rebuild triggers an immediate export on all runners outside the scheduled tick.
func (p *Pipeline) Rebuild() {
	p.lock.Lock()
	runners := make([]*runner, 0, len(p.runners))
	for _, r := range p.runners {
		runners = append(runners, r)
	}
	p.lock.Unlock()

	for _, r := range runners {
		go r.export()
	}
}

// RebuildSource triggers an immediate export for the runner with the given source key.
func (p *Pipeline) RebuildSource(sourceKey string) error {
	p.lock.Lock()
	r, ok := p.runners[sourceKey]
	p.lock.Unlock()

	if !ok {
		return fmt.Errorf("PricingModel: no runner found for source key %q", sourceKey)
	}
	go r.export()
	return nil
}

func (p *Pipeline) removeSource(key string) {
	r, ok := p.runners[key]
	if !ok {
		return
	}
	log.Infof("PricingModel: pipeline: removing source %s", key)
	r.Stop()
	delete(p.runners, key)
}
