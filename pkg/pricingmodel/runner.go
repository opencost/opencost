package pricingmodel

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/opencost/opencost/core/pkg/errors"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

type runnerConfig struct {
	interval time.Duration
	lastRun  *time.Time
}

// runner periodically fetches pricing from a PricingSource and writes it to storage.
// The storage path is derived from PricingModelSet.Source set by the PricingSource implementation.
type runner struct {
	source     pricingmodel.PricingSource
	store      *storageWriter
	config     runnerConfig
	isRunning  atomic.Bool
	isStopping atomic.Bool
	exitCh     chan struct{}
	statusLock sync.RWMutex
	status     Status
}

func newRunner(source pricingmodel.PricingSource, store *storageWriter, config runnerConfig) *runner {
	return &runner{
		source: source,
		store:  store,
		config: config,
		status: Status{
			SourceKey:   source.PricingSourceKey(),
			CreatedAt:   time.Now().UTC(),
			RefreshRate: config.interval.String(),
		},
	}
}

// initialDelay computes how long to wait before the first tick.
// If lastRun is set and lastRun+interval is still in the future, wait until then.
// Otherwise run immediately.
func (r *runner) initialDelay() time.Duration {
	if r.config.lastRun == nil {
		return 0
	}
	next := r.config.lastRun.Add(r.config.interval)
	delay := time.Until(next)
	if delay <= 0 {
		return 0
	}
	return delay
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

func (r *runner) Status() Status {
	r.statusLock.RLock()
	defer r.statusLock.RUnlock()
	return r.status
}

func (r *runner) run() {
	defer errors.HandlePanic()

	ticker := timeutil.NewJobTicker()
	defer ticker.Close()
	ticker.TickIn(r.initialDelay())

	for {
		select {
		case <-r.exitCh:
			return
		case <-ticker.Ch:
		}

		r.export()

		r.statusLock.Lock()
		r.status.NextRun = time.Now().UTC().Add(r.config.interval)
		r.statusLock.Unlock()

		ticker.TickIn(r.config.interval)
	}
}

func (r *runner) export() {
	pms, err := r.source.GetPricing()
	if err != nil {
		log.Errorf("PricingModel: runner: failed to get pricing: %v", err)
		r.statusLock.Lock()
		r.status.LastError = err.Error()
		r.statusLock.Unlock()
		return
	}

	data, err := pms.MarshalBinary()
	if err != nil {
		log.Errorf("PricingModel[%s]: runner: failed to marshal pricing model set: %v", pms.Source, err)
		r.statusLock.Lock()
		r.status.LastError = err.Error()
		r.statusLock.Unlock()
		return
	}

	err = r.store.Write(pms.Source, data)
	if err != nil {
		log.Errorf("PricingModel[%s]: runner: failed to write pricing model set to storage: %v", pms.Source, err)
		r.statusLock.Lock()
		r.status.LastError = err.Error()
		r.statusLock.Unlock()
		return
	}

	log.Infof("PricingModel[%s]: runner: exported pricing model set (%d bytes)", pms.Source, len(data))

	r.statusLock.Lock()
	r.status.LastRun = time.Now().UTC()
	r.status.Runs++
	r.status.LastError = ""
	r.statusLock.Unlock()
}
