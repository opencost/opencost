package exporter

import (
	"reflect"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// ExportController is a controller interface that is responsible for exporting data on a specific interval.
type ExportController interface {
	// Name returns the name of the controller
	Name() string

	// Start starts a background compute processing loop, which will compute the data for the current resolution and export it
	// on the provided interval. This function will return `true` if the loop was started successfully, and `false` if it was
	// already running.
	Start(interval time.Duration) bool

	// Stops the compute processing loop
	Stop()
}

// EventExportController[T] is used to export timestamped events of type T on a specific interval.
type EventExportController[T any] struct {
	runState atomic.AtomicRunState
	source   ExportSource[T]
	exporter Exporter[T]
	typeName string
}

// NewEventExportController creates a new `EventExportController[T]` instance which is used to export timestamped events of type T
// on a specific interval.
func NewEventExportController[T any](source ExportSource[T], exporter Exporter[T]) *EventExportController[T] {
	return &EventExportController[T]{
		source:   source,
		exporter: exporter,
		typeName: reflect.TypeOf((*T)(nil)).Elem().String(),
	}
}

// Name returns the name of the controller, which is the name of the T-type
func (cd *EventExportController[T]) Name() string {
	return cd.typeName
}

// Start starts a background export loop, which will create a new event instance for the current minute-truncated time
// and export it on the provided interval. This function will return `true` if the loop was started successfully, and
// `false` if it was already running.
func (cd *EventExportController[T]) Start(interval time.Duration) bool {
	cd.runState.WaitForReset()
	if !cd.runState.Start() {
		return false
	}

	go func() {
		for {
			select {
			case <-cd.runState.OnStop():
				cd.runState.Reset()
				return // exit go routine

			case <-time.After(interval):
			}

			// truncate the time to the minute to ensure broad enough coverage for event exports
			t := time.Now().UTC().Truncate(time.Second)

			err := cd.exporter.Export(cd.source.Make(t))
			if err != nil {
				log.Warnf("[%s] Error during Write: %s", cd.typeName, err)
			}
		}
	}()

	return true
}

// Stops the export loop
func (cd *EventExportController[T]) Stop() {
	cd.runState.Stop()
}

// ComputeExportController[T] is a controller type which leverages a `ComputeSource[T]` and `Exporter[T]`
// to regularly compute the data for the current resolution and export it on a specific interval.
type ComputeExportController[T any] struct {
	runState         atomic.AtomicRunState
	source           ComputeSource[T]
	exporter         ComputeExporter[T]
	resolution       time.Duration
	sourceResolution time.Duration
	typeName         string
}

// NewComputeExportController creates a new `ComputeExportController[T]` instance.
func NewComputeExportController[T any](
	source ComputeSource[T],
	exporter ComputeExporter[T],
	sourceResolution time.Duration,
) *ComputeExportController[T] {
	return &ComputeExportController[T]{
		source:           source,
		resolution:       exporter.Resolution(),
		sourceResolution: sourceResolution,
		exporter:         exporter,
		typeName:         reflect.TypeOf((*T)(nil)).Elem().String(),
	}
}

// Name returns the name of the controller, which is a combination of the type name and the resolution
func (cd *ComputeExportController[T]) Name() string {
	return cd.typeName + "-" + timeutil.FormatStoreResolution(cd.resolution)
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

			set, err := cd.source.Compute(start, end, cd.sourceResolution)

			// If a NoDataError or ErrorCollection is returned, we expect that an empty set will
			// also be returned. Like an EOF error, this is an expected state
			// and indicates that we should still Insert and Save.
			if err != nil && !source.IsNoDataError(err) && !source.IsErrorCollection(err) {
				log.Errorf("[%s] Error during Compute: %s", cd.typeName, err)
				continue
			}

			// Check ErrorCollection to set Warnings and Errors
			if source.IsErrorCollection(err) {
				c := err.(source.QueryErrorCollection)
				errors, warnings := c.ToErrorAndWarningStrings()

				cd.logErrors(start, end, warnings, errors)
				continue
			}

			log.Debugf("[%s] Exporting data for window: %s - %s", cd.typeName, start.UTC(), end.UTC())
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
func (cd *ComputeExportController[T]) logErrors(start, end time.Time, warnings []string, errors []string) {
	for _, w := range warnings {
		log.Warnf("[%s] (%s-%s) %s", cd.typeName, start.Format(time.RFC3339), end.Format(time.RFC3339), w)
	}

	for _, e := range errors {
		log.Errorf("[%s] (%s-%s) %s", cd.typeName, start.Format(time.RFC3339), end.Format(time.RFC3339), e)
	}
}

type ComputeExportControllerGroup[T any] struct {
	controllers []*ComputeExportController[T]
}

func NewComputeExportControllerGroup[T any](controllers ...*ComputeExportController[T]) *ComputeExportControllerGroup[T] {
	return &ComputeExportControllerGroup[T]{controllers: controllers}
}

func (g *ComputeExportControllerGroup[T]) Name() string {
	var sb strings.Builder
	sb.WriteRune('[')
	for i, c := range g.controllers {
		if i > 0 {
			sb.WriteRune('/')
		}
		sb.WriteString(c.Name())
	}
	sb.WriteRune(']')
	return sb.String()
}

func (g *ComputeExportControllerGroup[T]) Start(interval time.Duration) bool {
	for _, c := range g.controllers {
		if !c.Start(interval) {
			return false
		}
	}
	return true
}

func (g *ComputeExportControllerGroup[T]) Stop() {
	for _, c := range g.controllers {
		c.Stop()
	}
}
