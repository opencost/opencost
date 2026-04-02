package pricingmodel

import (
	"sync/atomic"

	"github.com/opencost/opencost/core/pkg/errors"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// runner periodically fetches pricing from a PricingSource and writes it to storage.
// The storage path is derived from PricingModelSet.Source set by the PricingSource implementation.
type runner struct {
	source     pricingmodel.PricingSource
	store      storage.Storage
	config     PipelineConfig
	isRunning  atomic.Bool
	isStopping atomic.Bool
	exitCh     chan struct{}
}

func newRunner(source pricingmodel.PricingSource, store storage.Storage, config PipelineConfig) *runner {
	return &runner{
		source: source,
		store:  store,
		config: config,
	}
}

func (r *runner) Start() {
	if !r.isRunning.CompareAndSwap(false, true) {
		return
	}
	r.exitCh = make(chan struct{})
	go r.run()
}

func (r *runner) Stop() {
	if !r.isStopping.CompareAndSwap(false, true) {
		return
	}
	close(r.exitCh)
	r.isRunning.Store(false)
	r.isStopping.Store(false)
}

func (r *runner) run() {
	defer errors.HandlePanic()

	ticker := timeutil.NewJobTicker()
	defer ticker.Close()
	ticker.TickIn(0)

	for {
		select {
		case <-r.exitCh:
			return
		case <-ticker.Ch:
		}

		r.export()
		ticker.TickIn(r.config.RefreshInterval)
	}
}

func (r *runner) export() {
	pms, err := r.source.GetPricing()
	if err != nil {
		log.Errorf("PricingModel: runner: failed to get pricing: %v", err)
		return
	}

	data, err := pms.MarshalBinary()
	if err != nil {
		log.Errorf("PricingModel[%s]: runner: failed to marshal pricing model set: %v", pms.Source, err)
		return
	}

	// TODO: finalize storage path structure, e.g. <environment-prefix>/<pms.Source>
	if err := r.store.Write(pms.Source, data); err != nil {
		log.Errorf("PricingModel[%s]: runner: failed to write pricing model set to storage: %v", pms.Source, err)
		return
	}

	log.Infof("PricingModel[%s]: runner: exported pricing model set (%d bytes)", pms.Source, len(data))
}