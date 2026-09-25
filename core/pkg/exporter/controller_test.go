package exporter

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/exporter/validator"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// controllerTestSet is a trivial payload type for driving the controller.
type controllerTestSet struct {
	Start time.Time
	End   time.Time
	Seq   int
}

type computeCall struct {
	Start time.Time
	End   time.Time
}

// fakeComputeSource is a scripted ComputeSource[T]. By default Compute returns
// a non-nil *T built by makeFn; computeFn (if set) overrides that per call.
type fakeComputeSource[T any] struct {
	mu         sync.Mutex
	calls      []computeCall
	canCompute func(start, end time.Time) bool
	computeFn  func(start, end time.Time, callsForWindow int) (*T, error)
}

func (s *fakeComputeSource[T]) CanCompute(start, end time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.canCompute == nil {
		return true
	}
	return s.canCompute(start, end)
}

func (s *fakeComputeSource[T]) Compute(start, end time.Time) (*T, error) {
	s.mu.Lock()
	n := 0
	for _, c := range s.calls {
		if c.Start.Equal(start) && c.End.Equal(end) {
			n++
		}
	}
	s.calls = append(s.calls, computeCall{Start: start, End: end})
	fn := s.computeFn
	s.mu.Unlock()

	if fn == nil {
		return new(T), nil
	}
	return fn(start, end, n)
}

func (s *fakeComputeSource[T]) Name() string { return "fake-compute-source" }

func (s *fakeComputeSource[T]) Calls() []computeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]computeCall(nil), s.calls...)
}

// exportRecord captures one Export attempt.
type exportRecord[T any] struct {
	Tick    int       // index of the tick in which the attempt happened
	Now     time.Time // tick time the attempt happened at
	Start   time.Time
	End     time.Time
	Set     *T
	Success bool
}

func (r exportRecord[T]) postClose() bool { return !r.Now.Before(r.End) }

var errInjectedExport = errors.New("injected export failure")

// fakeComputeExporter records every Export attempt (in order) and can be
// scripted to fail via failIf. The test drives the "current tick" through
// beginTick so the exporter can attribute attempts to tick times.
type fakeComputeExporter[T any] struct {
	mu       sync.Mutex
	tick     int
	now      time.Time
	failIf   func(window opencost.Window, now time.Time) bool
	delegate ComputeExporter[T]
	records  []exportRecord[T]
}

func (e *fakeComputeExporter[T]) beginTick(i int, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tick = i
	e.now = now
}

func (e *fakeComputeExporter[T]) Export(window opencost.Window, set *T) error {
	e.mu.Lock()
	rec := exportRecord[T]{
		Tick:  e.tick,
		Now:   e.now,
		Start: *window.Start(),
		End:   *window.End(),
		Set:   set,
	}
	fail := e.failIf != nil && e.failIf(window, e.now)
	delegate := e.delegate
	e.mu.Unlock()

	var err error
	if fail {
		err = errInjectedExport
	} else if delegate != nil {
		err = delegate.Export(window, set)
	}
	rec.Success = err == nil

	e.mu.Lock()
	e.records = append(e.records, rec)
	e.mu.Unlock()
	return err
}

func (e *fakeComputeExporter[T]) Records() []exportRecord[T] {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]exportRecord[T](nil), e.records...)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var ctlBase = time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

func at(hh, mm, ss int) time.Time {
	return ctlBase.Add(time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second)
}

// ticksEvery returns tick times in [from, to] (inclusive) every step.
func ticksEvery(from, to time.Time, step time.Duration) []time.Time {
	var out []time.Time
	for t := from; !t.After(to); t = t.Add(step) {
		out = append(out, t)
	}
	return out
}

func runTicks[T any](c *ComputeExportController[T], exp *fakeComputeExporter[T], ticks []time.Time, afterTick func(i int, now time.Time)) {
	for i, now := range ticks {
		exp.beginTick(i, now)
		c.now = func() time.Time { return now }
		c.tick(now)
		if afterTick != nil {
			afterTick(i, now)
		}
	}
}

func hourWindow(start time.Time) string {
	return fmt.Sprintf("[%s,%s)", start.Format("15:04"), start.Add(time.Hour).Format("15:04"))
}

func dumpRecords[T any](t *testing.T, recs []exportRecord[T]) {
	t.Helper()
	for _, r := range recs {
		if r.Success && !r.postClose() {
			continue // routine in-progress refreshes are noise
		}
		t.Logf("tick=%02d now=%s window=%s success=%v postClose=%v",
			r.Tick, r.Now.Format("15:04:05"), hourWindow(r.Start), r.Success, r.postClose())
	}
}

// firstPostCloseSuccess returns the index into recs of the first successful
// export of the window starting at start whose tick time is >= window end.
func firstPostCloseSuccess[T any](recs []exportRecord[T], start time.Time) int {
	for i, r := range recs {
		if r.Start.Equal(start) && r.Success && r.postClose() {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// F-29: a failed closed-window export must be retried
// ---------------------------------------------------------------------------

func TestComputeExportController_RetriesFailedClosedWindow(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	nineAM := at(9, 0, 0)
	failAt := at(10, 0, 30)
	exp := &fakeComputeExporter[controllerTestSet]{
		// bucket 503 for the closed [09:00,10:00) window on the first
		// post-rollover tick only; the current window succeeds.
		failIf: func(w opencost.Window, now time.Time) bool {
			return w.Start().Equal(nineAM) && now.Equal(failAt)
		},
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	ticks := []time.Time{at(9, 59, 0), failAt}
	ticks = append(ticks, ticksEvery(at(10, 5, 0), at(12, 0, 0), 5*time.Minute)...)
	runTicks(c, exp, ticks, nil)

	recs := exp.Records()
	if firstPostCloseSuccess(recs, nineAM) < 0 {
		dumpRecords(t, recs)
		t.Fatalf("F-29: closed window %s was never successfully exported after it closed (at or after 10:00); "+
			"the failed export at 10:00:30 was never retried", hourWindow(nineAM))
	}
}

// ---------------------------------------------------------------------------
// Outage: every closed window drains, in order, exactly once
// ---------------------------------------------------------------------------

func TestComputeExportController_OutageDrainsInOrder(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	outageStart, outageEnd := at(9, 0, 0), at(14, 0, 0)
	exp := &fakeComputeExporter[controllerTestSet]{
		failIf: func(_ opencost.Window, now time.Time) bool {
			return !now.Before(outageStart) && now.Before(outageEnd)
		},
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	ticks := ticksEvery(at(8, 0, 0), at(16, 0, 0), 5*time.Minute)
	runTicks(c, exp, ticks, nil)
	recs := exp.Records()

	failed := false
	fail := func(format string, args ...any) {
		t.Helper()
		failed = true
		t.Errorf(format, args...)
	}

	// 1. every hourly window 08:00..14:00 got a final (post-close) export.
	var windows []time.Time
	for h := 8; h <= 14; h++ {
		windows = append(windows, at(h, 0, 0))
	}
	for _, w := range windows {
		if firstPostCloseSuccess(recs, w) < 0 {
			fail("window %s never got a successful export at or after its end (final export missing)", hourWindow(w))
		}
	}

	// 2. final exports occur in ascending window order, and 3. a closed window
	// is never exported again after its first post-close success.
	var order []time.Time
	done := map[time.Time]bool{}
	for _, r := range recs {
		if !r.postClose() {
			continue
		}
		if done[r.Start] {
			fail("window %s re-exported at %s after its final export already succeeded",
				hourWindow(r.Start), r.Now.Format("15:04:05"))
			continue
		}
		if r.Success {
			done[r.Start] = true
			order = append(order, r.Start)
		}
	}
	for i := 1; i < len(order); i++ {
		if !order[i].After(order[i-1]) {
			fail("final exports out of order: %s finalized after %s", hourWindow(order[i]), hourWindow(order[i-1]))
		}
	}

	// 4. the current window is attempted on every tick, including during the outage.
	for i, now := range ticks {
		cur := now.Truncate(time.Hour)
		found := false
		for _, r := range recs {
			if r.Tick == i && r.Start.Equal(cur) {
				found = true
				break
			}
		}
		if !found {
			fail("tick %s did not attempt the current window %s", now.Format("15:04:05"), hourWindow(cur))
		}
	}

	if failed {
		dumpRecords(t, recs)
	}
}

// ---------------------------------------------------------------------------
// Skeptic: a retried closed window that now computes empty must not overwrite
// ---------------------------------------------------------------------------

func TestComputeExportController_EmptyRetryDoesNotOverwrite(t *testing.T) {
	res := time.Hour
	store := storage.NewMemoryStorage()
	paths, err := pathing.NewDefaultStoragePathFormatter(TestAppName, TestClusterID, TestClusterName, pipelines.AllocationPipelineName, &res)
	if err != nil {
		t.Fatalf("failed to create path formatter: %v", err)
	}
	storeExp := NewComputeStorageExporter(
		paths,
		NewBingenEncoder[opencost.AllocationSet](),
		store,
		validator.NewSetValidator[opencost.AllocationSet](res),
		false,
	)

	nineAM, tenAM := at(9, 0, 0), at(10, 0, 0)
	failAt := at(10, 0, 30)

	// The first computation of [09:00,10:00) has data; every later computation
	// (e.g. data source restarted / retention expired) returns an empty set.
	src := &fakeComputeSource[opencost.AllocationSet]{
		computeFn: func(start, end time.Time, n int) (*opencost.AllocationSet, error) {
			if start.Equal(nineAM) && n == 0 {
				return opencost.GenerateMockAllocationSet(start), nil
			}
			return opencost.NewAllocationSet(start, end), nil
		},
	}
	exp := &fakeComputeExporter[opencost.AllocationSet]{
		delegate: storeExp,
		failIf: func(w opencost.Window, now time.Time) bool {
			return w.Start().Equal(nineAM) && now.Equal(failAt)
		},
	}
	c := NewComputeExportController[opencost.AllocationSet](src, exp, res)

	window := opencost.NewClosedWindow(nineAM, tenAM)
	path := paths.ToFullPath("", window, NewBingenEncoder[opencost.AllocationSet]().FileExt())

	// first write, with data
	runTicks(c, exp, []time.Time{at(9, 30, 0)}, nil)
	original, err := store.Read(path)
	if err != nil || len(original) == 0 {
		t.Fatalf("expected populated object at %s after first export: err=%v len=%d", path, err, len(original))
	}

	// fail the post-close export once, then keep ticking; any retry of
	// [09:00,10:00) will carry an empty set.
	ticks := []time.Time{failAt}
	ticks = append(ticks, ticksEvery(at(10, 5, 0), at(12, 0, 0), 5*time.Minute)...)
	runTicks(c, exp, ticks, nil)

	// and a direct re-export of the closed window with an empty set.
	if err := exp.Export(window, opencost.NewAllocationSet(nineAM, tenAM)); err != nil {
		t.Fatalf("direct empty re-export returned error: %v", err)
	}

	after, err := store.Read(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	if !bytes.Equal(original, after) {
		t.Fatalf("stored object for %s was overwritten by an empty set (before %d bytes, after %d bytes)",
			hourWindow(nineAM), len(original), len(after))
	}
}
