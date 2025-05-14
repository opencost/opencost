package exporter

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
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
	exporter EventExporter[T]
	typeName string
}

// NewEventExportController creates a new `EventExportController[T]` instance which is used to export timestamped events of type T
// on a specific interval.
func NewEventExportController[T any](source ExportSource[T], exporter EventExporter[T]) *EventExportController[T] {
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

			// truncate the time to the second to ensure broad enough coverage for event exports
			t := time.Now().UTC().Truncate(time.Second)

			evt := cd.source.Make(t)
			if evt == nil {
				log.Debugf("[%s] No event data to export", cd.typeName)
				continue
			}

			err := cd.exporter.Export(t, evt)
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
	lastExport       time.Time
	typeName         string
}

// NewComputeExportController creates a new `ComputeExportController[T]` instance.
func NewComputeExportController[T any](
	source ComputeSource[T],
	exporter ComputeExporter[T],
	resolution time.Duration,
	sourceResolution time.Duration,
) *ComputeExportController[T] {
	return &ComputeExportController[T]{
		source:           source,
		resolution:       resolution,
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

			now := time.Now().UTC()
			windows := cd.exportWindowsFor(now)

			for _, window := range windows {
				err := cd.export(window)
				if err != nil {
					// Check ErrorCollection to set Warnings and Errors
					if source.IsErrorCollection(err) {
						c := err.(source.QueryErrorCollection)
						errors, warnings := c.ToErrorAndWarningStrings()

						cd.logErrors(window, warnings, errors)
						continue
					}

					log.Errorf("[%s] %s", cd.typeName, err)
				} else {
					cd.lastExport = now
				}
			}
		}
	}()

	return true
}

// exportWindows uses the last export time to determine the current time windows to
// export. This will, at most, return 2 windows: the previous resolution window and
// the current resolution window.
func (cd *ComputeExportController[T]) exportWindowsFor(now time.Time) []opencost.Window {
	start := now.Truncate(cd.resolution)
	end := start.Add(cd.resolution)

	if cd.lastExport.IsZero() {
		return []opencost.Window{
			opencost.NewClosedWindow(start, end),
		}
	}

	lastStart := cd.lastExport.Truncate(cd.resolution)
	if lastStart.Equal(start) {
		return []opencost.Window{
			opencost.NewClosedWindow(start, end),
		}
	}
	lastEnd := lastStart.Add(cd.resolution)

	// we've identified that the last export window is not the same as the current,
	// so we should export the previous resolution window as well as the current one
	return []opencost.Window{
		opencost.NewClosedWindow(lastStart, lastEnd),
		opencost.NewClosedWindow(start, end),
	}
}

// export computes and exports the data for a given time window
func (cd *ComputeExportController[T]) export(window opencost.Window) error {
	if window.IsOpen() {
		return fmt.Errorf("window is open: %s", window.String())
	}

	start, end := *window.Start(), *window.End()

	log.Debugf("[%s] Reporting for window: %s - %s", cd.typeName, start.UTC(), end.UTC())

	if !cd.source.CanCompute(start, end) {
		return fmt.Errorf("cannot compute window: [Start: %s, End: %s]", start, end)
	}

	set, err := cd.source.Compute(start, end, cd.sourceResolution)
	// all errors but NoDataError are considered a halt to the export
	if err != nil && !source.IsNoDataError(err) {
		return err
	}

	log.Debugf("[%s] Exporting data for window: %s - %s", cd.typeName, start.UTC(), end.UTC())
	err = cd.exporter.Export(window, set)
	if err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	return nil
}

// Stops the compute processing loop
func (cd *ComputeExportController[T]) Stop() {
	cd.runState.Stop()
}

// temporary
func (cd *ComputeExportController[T]) logErrors(window opencost.Window, warnings []string, errors []string) {
	start, end := window.Start(), window.End()
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
	if len(g.controllers) == 0 {
		log.Warnf("ComputeExportControllerGroup[%s] has no controllers to start", typeutil.TypeOf[T]())
		return false
	}

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

func (g *ComputeExportControllerGroup[T]) Resolutions() []time.Duration {
	resolutions := make([]time.Duration, 0, len(g.controllers))
	for _, c := range g.controllers {
		resolutions = append(resolutions, c.resolution)
	}
	return resolutions
}
