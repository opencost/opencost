package inferencecost

import (
	"context"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// Runner periodically drives the collect → calculate → export pipeline.
type Runner struct {
	collector  *Collector
	calculator *Calculator
	exporter   *Exporter
	interval   time.Duration
}

// NewRunner creates a Runner. The exporter must already be registered with
// Prometheus (call exporter.Register() before NewRunner).
func NewRunner(collector *Collector, calculator *Calculator, exporter *Exporter, interval time.Duration) *Runner {
	return &Runner{
		collector:  collector,
		calculator: calculator,
		exporter:   exporter,
		interval:   interval,
	}
}

// Start runs the collection loop until ctx is cancelled.
func (r *Runner) Start(ctx context.Context) {
	log.Infof("InferenceCost: starting collection loop (interval=%s)", r.interval)
	r.runOnce(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("InferenceCost: collection loop stopped")
			return
		case <-ticker.C:
			r.runOnce(ctx)
		}
	}
}

func (r *Runner) runOnce(ctx context.Context) {
	metrics, err := r.collector.CollectMetrics(ctx)
	if err != nil {
		log.Errorf("InferenceCost: collection failed: %v", err)
		return
	}
	r.calculator.CalculateCosts(metrics)
	r.exporter.Export(metrics)
	log.Debugf("InferenceCost: exported metrics for %d models", len(metrics))
}
