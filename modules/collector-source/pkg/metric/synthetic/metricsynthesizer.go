package synthetic

import (
	"time"

	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

// InstantMetric is a metric update that happened at a specific timestamp.
type InstantMetric struct {
	timestamp time.Time
	update    *metric.Update
}

// MetricSynthesizer is an implementation prototype for an object capable of processing
// a stream of metric updates, and then synthesizing new metric updates based on the processed
// data.
type MetricSynthesizer interface {
	// Process accepts individual Updates from an UpdateSet for processing. Once all Updates
	// have been processed, call Synthesize() to generate any additional updates.
	Process(t time.Time, update *metric.Update)

	// Synthesize will generate all synthetic Update instances after processing all existing updates
	// in a set.
	Synthesize() []metric.Update

	// Clear resets or cycles the current state of the processed metrics to prepare for the next scrape.
	Clear()
}

// MetricSynthesizers implements the `metric.Updater` interface, to accept a `metric.UpdateSet` of metric updates,
// pipes each `metric.Update` into the registered MetricSynthesizer instances for processing, and then synthesizes
// new metric updates to append.
type MetricSynthesizers struct {
	synthesizers []MetricSynthesizer
	next         metric.Updater
}

// NewMetricSynthesizers creates a new set of metric synthesizers, which acts as an updater decorator to append
// all newly synthesized metrics onto the existing update set before passing it along to the next updater.
func NewMetricSynthesizers(next metric.Updater, synthesizers ...MetricSynthesizer) *MetricSynthesizers {
	return &MetricSynthesizers{
		synthesizers: synthesizers,
		next:         next,
	}
}

func (ms *MetricSynthesizers) Update(set *metric.UpdateSet) {
	ts := set.Timestamp

	// first pass is to have all synthesizers process all updates
	for _, synthesizer := range ms.synthesizers {
		for i := range len(set.Updates) {
			update := set.Updates[i]
			synthesizer.Process(ts, &update)
		}
	}

	// second pass is to have the synthesizers generate all synthetic updates
	for _, synthesizer := range ms.synthesizers {
		updates := synthesizer.Synthesize()
		if len(updates) != 0 {
			set.Updates = append(set.Updates, updates...)
		}
		synthesizer.Clear()
	}

	ms.next.Update(set)
}
