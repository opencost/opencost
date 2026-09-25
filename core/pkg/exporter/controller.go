package exporter

import (
	"fmt"
	"reflect"
	"slices"
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

const (
	// defaultMaxPendingWindows is the number of closed sub-daily windows retained for retry
	defaultMaxPendingWindows = 48

	// defaultMaxPendingDailyWindows is the number of closed daily (or longer) windows retained for retry
	defaultMaxPendingDailyWindows = 7

	// defaultMaxExportsPerTick caps how many closed windows are exported in a single tick, so that
	// draining a backlog after an outage doesn't compute and write every window at once
	defaultMaxExportsPerTick = 4

	// maxRetryBackoffTicks caps the number of ticks between retries of a failed closed window
	maxRetryBackoffTicks = 12
)

// pendingWindow is a closed window awaiting a successful export
type pendingWindow struct {
	start time.Time
	// attempts is the number of failed exports of this window since it closed
	attempts int
	// nextTick is the tick at which this window is next due to be retried
	nextTick uint64
}

// ComputeExportController[T] is a controller type which leverages a `ComputeSource[T]` and `Exporter[T]`
// to regularly compute the data for the current resolution and export it on a specific interval.
//
// Each tick exports the current (in-progress) window. When a window closes, it is added to a pending
// list and exported on the next tick. A window whose export fails stays pending and is retried with a
// per-window backoff until an export succeeds. Newly closed windows are exported before retries, and
// retries go oldest first; after a failed retry no further retries are attempted in that tick, so a
// storage outage costs at most one retry per tick. The pending list is bounded; when it overflows,
// the oldest window is dropped and counted.
type ComputeExportController[T any] struct {
	runState   atomic.AtomicRunState
	source     ComputeSource[T]
	exporter   ComputeExporter[T]
	resolution time.Duration
	typeName   string

	// tickCount is the number of ticks run
	tickCount uint64
	// lastTickWindow is the latest start of the current window seen by a tick
	lastTickWindow time.Time
	// pending holds closed windows awaiting a successful export, ascending by start
	pending []*pendingWindow
	// maxPendingWindows bounds pending; the oldest windows beyond this are dropped
	maxPendingWindows int
	// maxExportsPerTick bounds the closed-window exports (first attempts and retries) per tick
	maxExportsPerTick int
	// droppedWindows counts closed windows dropped from pending without a successful export
	droppedWindows uint64

	// now returns the current time; overridable for tests
	now func() time.Time
}

// NewComputeExportController creates a new `ComputeExportController[T]` instance.
func NewComputeExportController[T any](
	source ComputeSource[T],
	exporter ComputeExporter[T],
	resolution time.Duration,
) *ComputeExportController[T] {
	maxPending := defaultMaxPendingWindows
	if resolution >= timeutil.Day {
		maxPending = defaultMaxPendingDailyWindows
	}

	return &ComputeExportController[T]{
		source:            source,
		resolution:        resolution,
		exporter:          exporter,
		typeName:          reflect.TypeFor[T]().String(),
		maxPendingWindows: maxPending,
		maxExportsPerTick: defaultMaxExportsPerTick,
		now:               func() time.Time { return time.Now().UTC() },
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
				if n := cd.pendingCount(); n > 0 {
					log.Warnf("[%s] stopping with %d closed window(s) not yet exported", cd.Name(), n)
				}
				cd.runState.Reset()
				return // exit go routine

			// After our interval elapses, fall through
			case <-time.After(interval):
			}

			cd.tick(cd.now())
		}
	}()

	return true
}

// tick runs a single export pass for the provided time: newly closed windows first, then due retries
// oldest first (together at most maxExportsPerTick), followed by the current window.
func (cd *ComputeExportController[T]) tick(now time.Time) {
	cd.tickCount++

	start := now.Truncate(cd.resolution)
	cd.enqueueClosedWindows(start)
	// never move backwards, so a backwards clock step can't enqueue the same window twice
	if start.After(cd.lastTickWindow) {
		cd.lastTickWindow = start
	}

	attempts := 0
	for _, firstAttempt := range []bool{true, false} {
		for _, pw := range cd.pending {
			if attempts >= cd.maxExportsPerTick || cd.runState.IsStopping() {
				break
			}
			if (pw.attempts == 0) != firstAttempt || pw.nextTick > cd.tickCount {
				continue
			}
			attempts++

			if cd.exportAndLog(opencost.NewClosedWindow(pw.start, pw.start.Add(cd.resolution))) {
				pw.start = time.Time{} // exported; removed below
				continue
			}

			pw.attempts++
			pw.nextTick = cd.tickCount + retryBackoffTicks(pw.attempts)

			// a failed retry most likely means the next one will fail too (e.g. storage is down); stop
			// retrying until the next tick rather than recomputing more windows only to discard them
			if !firstAttempt {
				break
			}
		}
	}
	cd.pending = slices.DeleteFunc(cd.pending, func(pw *pendingWindow) bool { return pw.start.IsZero() })

	if cd.runState.IsStopping() {
		return
	}
	cd.exportAndLog(opencost.NewClosedWindow(start, start.Add(cd.resolution)))
}

// retryBackoffTicks returns the number of ticks to wait before retrying a window that has failed the
// given number of times: 1, 2, 4, 8, then maxRetryBackoffTicks.
func retryBackoffTicks(attempts int) uint64 {
	if attempts > 4 {
		return maxRetryBackoffTicks
	}
	return min(uint64(1)<<(attempts-1), maxRetryBackoffTicks)
}

// enqueueClosedWindows adds every window that has closed since the previous tick to the pending list,
// dropping the oldest pending windows if the list exceeds maxPendingWindows.
func (cd *ComputeExportController[T]) enqueueClosedWindows(currentStart time.Time) {
	// on the first tick there is no previous window; on a backwards clock step nothing has closed
	if cd.lastTickWindow.IsZero() || !currentStart.After(cd.lastTickWindow) {
		return
	}

	first := cd.lastTickWindow
	closed := int(currentStart.Sub(first) / cd.resolution)

	// if more windows closed than can be retained (e.g. a long stall), skip straight to the ones we
	// can keep rather than enqueueing and evicting each one
	if closed > cd.maxPendingWindows {
		skipped := closed - cd.maxPendingWindows
		cd.drop(first, first.Add(time.Duration(skipped)*cd.resolution), skipped)
		first = first.Add(time.Duration(skipped) * cd.resolution)
	}

	for ws := first; ws.Before(currentStart); ws = ws.Add(cd.resolution) {
		cd.pending = append(cd.pending, &pendingWindow{start: ws})
	}

	if over := len(cd.pending) - cd.maxPendingWindows; over > 0 {
		cd.drop(cd.pending[0].start, cd.pending[over-1].start.Add(cd.resolution), over)
		cd.pending = append([]*pendingWindow(nil), cd.pending[over:]...)
	}
}

// drop records count closed windows between start and end as dropped without a successful export
func (cd *ComputeExportController[T]) drop(start, end time.Time, count int) {
	cd.droppedWindows += uint64(count)
	log.Errorf("[%s] dropping %d closed window(s) between %s and %s that were never exported: pending limit of %d reached",
		cd.Name(), count, start.Format(time.RFC3339), end.Format(time.RFC3339), cd.maxPendingWindows)
}

// pendingCount returns the number of closed windows awaiting a successful export
func (cd *ComputeExportController[T]) pendingCount() int {
	return len(cd.pending)
}

// exportAndLog exports the window, logging any error, and returns true on success
func (cd *ComputeExportController[T]) exportAndLog(window opencost.Window) bool {
	err := cd.export(window)
	if err == nil {
		return true
	}

	// Check ErrorCollection to set Warnings and Errors
	if source.IsErrorCollection(err) {
		c := err.(source.QueryErrorCollection)
		errors, warnings := c.ToErrorAndWarningStrings()

		cd.logErrors(window, warnings, errors)
		return false
	}

	log.Errorf("[%s] %s", cd.typeName, err)
	return false
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

	set, err := cd.source.Compute(start, end)
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
		log.Debugf("ComputeExportControllerGroup[%s] has no controllers to start", typeutil.TypeOf[T]())
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
