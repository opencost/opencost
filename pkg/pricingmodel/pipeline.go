package pricingmodel

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/storage"
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
	store   storage.Storage
	config  PipelineConfig
}

// NewPipeline creates a Pipeline for the given sources and storage backend.
// If cfg is nil, DefaultPipelineConfig is used.
// The storage should be initialized by the caller via storage.InitializeStorage
// or storage.GetDefaultStorage, matching how CloudCost storage is wired up.
func NewPipeline(store storage.Storage, sources []pricingmodel.PricingSource, cfg *PipelineConfig) *Pipeline {
	p := &Pipeline{
		runners: make(map[string]*runner),
		store:   store,
		config:  resolveConfig(cfg),
	}
	for _, src := range sources {
		p.addSource(src)
	}
	return p
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
// of the same type already exists it is stopped and replaced.
func (p *Pipeline) AddSource(src pricingmodel.PricingSource) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.addSource(src)
}

// RemoveSource stops and removes the runner for the given source type key.
// The key is the fully qualified type name, e.g. "aws.PublicAPIPricingSource".
func (p *Pipeline) RemoveSource(key string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.removeSource(key)
}

// sourceKey returns a stable map key for a PricingSource based on its concrete type.
func sourceKey(src pricingmodel.PricingSource) string {
	t := reflect.TypeOf(src)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
}

func (p *Pipeline) addSource(src pricingmodel.PricingSource) {
	key := sourceKey(src)
	p.removeSource(key)
	log.Infof("PricingModel: pipeline: adding source %s", key)
	r := newRunner(src, p.store, p.config)
	r.Start()
	p.runners[key] = r
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