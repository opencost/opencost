package exporter

import (
	"reflect"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/atomic"
)

// ComputeExportController[T] is a controller type which leverages a `ComputeSource[T]` and `Exporter[T]`
// to regularly compute the data for the current resolution and export it on a specific interval.
type ComputeExportController[T any] struct {
	runState   atomic.AtomicRunState
	source     ComputeSource[T]
	exporter   Exporter[T]
	resolution time.Duration
	typeName   string
}

// NewComputeExportController creates a new `ComputeExportController[T]` instance.
func NewComputeExportController[T any](source ComputeSource[T], exporter Exporter[T], resolution time.Duration) *ComputeExportController[T] {
	return &ComputeExportController[T]{
		source:     source,
		resolution: resolution,
		exporter:   exporter,
		typeName:   reflect.TypeOf((*T)(nil)).Elem().String(),
	}
}

// Start starts a background compute processing loop, which will compute the data for the current resolution and export it
// on the provided interval. This function will return `true` if the loop was started successfully, and `false` if it was
// already running.
func (cd *ComputeExportController[T]) Start(interval time.Duration) bool {
	// Before we attempt to start, we must ensure we are not in a stopping state
	cd.runState.WaitForReset()

	// This will atomically check the current state to ensure we can run, then advances the state.
	// If the state is already started, it will return false.
	if !cd.runState.Start() {
		return false
	}

	// our run state is advanced, let's execute our action on the interval
	// spawn a new goroutine which will loop and wait the interval each iteration
	go func() {
		for {
			// use a select statement to receive whichever channel receives data first
			select {
			// if our stop channel receives data, it means we have explicitly called
			// Stop(), and must reset our AtomicRunState to it's initial idle state
			case <-cd.runState.OnStop():
				cd.runState.Reset()
				return // exit go routine

			// After our interval elapses, fall through
			case <-time.After(interval):
			}

			start := time.Now().UTC().Truncate(cd.resolution)
			end := start.Add(cd.resolution)

			log.Debugf("[%s] Reporting for window: %s - %s", cd.typeName, start.UTC(), end.UTC())
			if !cd.source.CanCompute(start, end) {
				log.Errorf("[%s] Cannot compute window: [Start: %s, End: %s]", cd.typeName, start, end)
				continue
			}

			set, err := cd.source.Compute(start, end, cd.resolution)

			// If a NoDataError or ErrorCollection is returned, we expect that an empty set will
			// also be returned. Like an EOF error, this is an expected state
			// and indicates that we should still Insert and Save.
			if err != nil && !source.IsNoDataError(err) && !source.IsErrorCollection(err) {
				continue
			}

			// Check ErrorCollection to set Warnings and Errors
			if source.IsErrorCollection(err) {
				c := err.(source.QueryErrorCollection)
				errors, warnings := c.ToErrorAndWarningStrings()

				logErrors(start, end, warnings, errors)
				continue
			}

			err = cd.exporter.Export(opencost.NewClosedWindow(start, end), set)
			if err != nil {
				log.Warnf("[%s] Error during Write: %s", cd.typeName, err)
			}
		}
	}()

	return true
}

// Stops the compute processing loop
func (cd *ComputeExportController[T]) Stop() {
	cd.runState.Stop()
}

// temporary
func logErrors(start, end time.Time, warnings []string, errors []string) {

}
