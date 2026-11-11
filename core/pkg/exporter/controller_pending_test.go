package exporter

// These tests exercise the bounded pending set introduced by OC-01. They rely
// on unexported controller members added by the fix:
//
//	maxPendingWindows  (integer field)  cap on closed windows awaiting export
//	maxExportsPerTick  (integer field)  cap on closed-window exports per tick
//	droppedWindows     (uint64 field)   count of windows evicted from pending
//	pendingCount() int                  closed windows currently pending
//	                                    (excludes the current, open window)

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputeExportController_EvictsBeyondMaxPending(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	// 6 closed windows fall in the outage: [08:00..13:00] (all close in [09:00,14:30)).
	outageStart, outageEnd := at(9, 0, 0), at(14, 30, 0)
	exp := &fakeComputeExporter[controllerTestSet]{
		failIf: func(_ opencost.Window, now time.Time) bool {
			return !now.Before(outageStart) && now.Before(outageEnd)
		},
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)
	c.maxPendingWindows = 3

	ticks := ticksEvery(at(8, 0, 0), at(16, 30, 0), 5*time.Minute)
	runTicks(c, exp, ticks, func(_ int, now time.Time) {
		if n := c.pendingCount(); n > 3 {
			t.Errorf("after tick %s pendingCount() = %d, want <= 3", now.Format("15:04:05"), n)
		}
	})
	recs := exp.Records()

	evicted := []time.Time{at(8, 0, 0), at(9, 0, 0), at(10, 0, 0)}
	kept := []time.Time{at(11, 0, 0), at(12, 0, 0), at(13, 0, 0)}

	for _, w := range kept {
		if firstPostCloseSuccess(recs, w) < 0 {
			t.Errorf("window %s is within maxPendingWindows but never got a post-close export", hourWindow(w))
		}
	}
	for _, w := range evicted {
		if i := firstPostCloseSuccess(recs, w); i >= 0 {
			t.Errorf("window %s should have been evicted but was exported post-close at %s",
				hourWindow(w), recs[i].Now.Format("15:04:05"))
		}
	}
	if got := uint64(c.droppedWindows); got != uint64(len(evicted)) {
		t.Errorf("droppedWindows = %d, want %d", got, len(evicted))
	}

	if t.Failed() {
		dumpRecords(t, recs)
	}
}

func TestComputeExportController_CapsExportsPerTick(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	// outage covers closes of [08:00..12:00] → 5 closed windows pending at 13:30.
	outageStart, recovery := at(9, 0, 0), at(13, 30, 0)
	exp := &fakeComputeExporter[controllerTestSet]{
		failIf: func(_ opencost.Window, now time.Time) bool {
			return !now.Before(outageStart) && now.Before(recovery)
		},
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)
	c.maxExportsPerTick = 2

	ticks := ticksEvery(at(8, 0, 0), at(15, 0, 0), 5*time.Minute)
	runTicks(c, exp, ticks, nil)
	recs := exp.Records()

	// per tick: at most 2 closed-window attempts, plus the current window.
	for i, now := range ticks {
		cur := now.Truncate(time.Hour)
		closed, current := 0, 0
		for _, r := range recs {
			if r.Tick != i {
				continue
			}
			if r.Start.Equal(cur) {
				current++
			} else {
				closed++
			}
		}
		if closed > 2 {
			t.Errorf("tick %s attempted %d closed windows, want <= 2", now.Format("15:04:05"), closed)
		}
		if current != 1 {
			t.Errorf("tick %s attempted the current window %d times, want 1", now.Format("15:04:05"), current)
		}
	}

	// every pending window drains within maxRetryBackoffTicks ticks of recovery
	drainedBy := recovery.Add(maxRetryBackoffTicks * 5 * time.Minute)
	for w := at(8, 0, 0); w.Before(at(13, 0, 0)); w = w.Add(time.Hour) {
		i := firstPostCloseSuccess(recs, w)
		if i < 0 {
			t.Errorf("window %s never got a post-close export", hourWindow(w))
			continue
		}
		if recs[i].Now.After(drainedBy) {
			t.Errorf("window %s finalized at %s, after the drain bound %s",
				hourWindow(w), recs[i].Now.Format("15:04:05"), drainedBy.Format("15:04:05"))
		}
	}

	if t.Failed() {
		dumpRecords(t, recs)
	}
}

// Without failures, every closed window gets exactly one post-close export, on the first tick after it
// closes: the same cadence as before pending retries existed.
func TestComputeExportController_NoFailuresExportsEachClosedWindowOnce(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	exp := &fakeComputeExporter[controllerTestSet]{}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	ticks := ticksEvery(at(8, 2, 0), at(12, 2, 0), 5*time.Minute)
	runTicks(c, exp, ticks, nil)

	for w := at(8, 0, 0); w.Before(at(12, 0, 0)); w = w.Add(time.Hour) {
		var postClose []exportRecord[controllerTestSet]
		for _, r := range exp.Records() {
			if r.Start.Equal(w) && r.postClose() {
				postClose = append(postClose, r)
			}
		}
		if len(postClose) != 1 {
			t.Fatalf("window %s got %d post-close exports, want 1", hourWindow(w), len(postClose))
		}
		if want := w.Add(time.Hour).Add(2 * time.Minute); !postClose[0].Now.Equal(want) {
			t.Errorf("window %s exported post-close at %s, want first tick after close %s", hourWindow(w), postClose[0].Now.Format("15:04:05"), want.Format("15:04:05"))
		}
	}
	if c.pendingCount() != 0 || c.droppedWindows != 0 {
		t.Errorf("expected nothing pending or dropped, got pending=%d dropped=%d", c.pendingCount(), c.droppedWindows)
	}
}

// A tick that arrives after many windows have closed (a long stall or a clock jump) enqueues only the
// most recent maxPendingWindows and counts the rest as dropped, without iterating each one.
func TestComputeExportController_StallBeyondMaxPending(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	exp := &fakeComputeExporter[controllerTestSet]{}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)
	c.maxPendingWindows = 3
	c.maxExportsPerTick = 10

	runTicks(c, exp, []time.Time{at(0, 30, 0), at(10, 30, 0)}, nil)

	// windows 00:00..09:00 closed between ticks: 07, 08 and 09 are retained and exported, 7 are dropped
	if c.droppedWindows != 7 {
		t.Errorf("droppedWindows = %d, want 7", c.droppedWindows)
	}
	for w := at(0, 0, 0); w.Before(at(10, 0, 0)); w = w.Add(time.Hour) {
		exported := firstPostCloseSuccess(exp.Records(), w) >= 0
		if want := !w.Before(at(7, 0, 0)); exported != want {
			t.Errorf("window %s post-close exported = %v, want %v", hourWindow(w), exported, want)
		}
	}
}

// A clock that steps backwards enqueues nothing and does not panic.
func TestComputeExportController_ClockStepsBackwards(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	exp := &fakeComputeExporter[controllerTestSet]{}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	runTicks(c, exp, []time.Time{at(10, 30, 0), at(9, 30, 0), at(10, 5, 0)}, nil)
	if c.pendingCount() != 0 || c.droppedWindows != 0 {
		t.Errorf("expected nothing pending or dropped, got pending=%d dropped=%d", c.pendingCount(), c.droppedWindows)
	}
}

// Stop() during a backlog stops draining closed windows within the current tick.
func TestComputeExportController_StopDuringBacklog(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	outageEnd := at(14, 0, 0)
	var c *ComputeExportController[controllerTestSet]
	exp := &fakeComputeExporter[controllerTestSet]{}
	exp.failIf = func(w opencost.Window, now time.Time) bool {
		if now.Equal(outageEnd) && !w.Start().Equal(outageEnd) {
			// first closed-window export of the recovery tick: stop the controller
			c.runState.Stop()
		}
		return now.Before(outageEnd)
	}
	c = NewComputeExportController[controllerTestSet](src, exp, time.Hour)
	c.maxExportsPerTick = 10
	if !c.runState.Start() {
		t.Fatalf("failed to start run state")
	}

	runTicks(c, exp, ticksEvery(at(8, 30, 0), outageEnd, 30*time.Minute), nil)

	closedAttempts := 0
	for _, r := range exp.Records() {
		if r.Now.Equal(outageEnd) && !r.Start.Equal(outageEnd) {
			closedAttempts++
		}
	}
	if closedAttempts != 1 {
		t.Errorf("expected draining to stop after the first closed window once stopped, got %d attempts", closedAttempts)
	}
	if c.pendingCount() != 5 {
		t.Errorf("expected the 5 un-attempted windows to remain pending, got %d", c.pendingCount())
	}
}

// Windows that always fail must not block newer closed windows from being exported (no head-of-line
// blocking), and are retried with backoff rather than on every tick.
func TestComputeExportController_PoisonedWindowsDoNotBlock(t *testing.T) {
	poisoned := map[time.Time]bool{at(8, 0, 0): true, at(9, 0, 0): true, at(10, 0, 0): true, at(11, 0, 0): true, at(12, 0, 0): true}
	src := &fakeComputeSource[controllerTestSet]{}
	exp := &fakeComputeExporter[controllerTestSet]{
		failIf: func(w opencost.Window, _ time.Time) bool { return poisoned[*w.Start()] },
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	ticks := ticksEvery(at(8, 2, 0), at(20, 2, 0), 5*time.Minute)
	runTicks(c, exp, ticks, nil)
	recs := exp.Records()

	// every healthy window gets its final export on the first tick after it closes
	for w := at(13, 0, 0); w.Before(at(20, 0, 0)); w = w.Add(time.Hour) {
		i := firstPostCloseSuccess(recs, w)
		if i < 0 {
			t.Errorf("healthy window %s was never exported after closing", hourWindow(w))
			continue
		}
		if want := w.Add(time.Hour).Add(2 * time.Minute); !recs[i].Now.Equal(want) {
			t.Errorf("healthy window %s finalized at %s, want %s", hourWindow(w), recs[i].Now.Format("15:04:05"), want.Format("15:04:05"))
		}
	}

	// the first poisoned window is retried with backoff: far fewer attempts than ticks since it closed
	attempts := 0
	for _, r := range recs {
		if r.Start.Equal(at(8, 0, 0)) && r.postClose() {
			attempts++
		}
	}
	ticksSinceClose := len(ticksEvery(at(9, 2, 0), at(20, 2, 0), 5*time.Minute))
	if attempts == 0 || attempts > ticksSinceClose/maxRetryBackoffTicks+5 {
		t.Errorf("poisoned window attempted %d times over %d ticks, want backoff", attempts, ticksSinceClose)
	}
	if t.Failed() {
		dumpRecords(t, recs)
	}
}

// A backwards clock step followed by a forward one must not enqueue the same window twice.
func TestComputeExportController_ClockStepDoesNotDuplicatePending(t *testing.T) {
	src := &fakeComputeSource[controllerTestSet]{}
	exp := &fakeComputeExporter[controllerTestSet]{
		failIf: func(w opencost.Window, _ time.Time) bool { return w.Start().Equal(at(9, 0, 0)) },
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	runTicks(c, exp, []time.Time{at(9, 30, 0), at(10, 30, 0), at(9, 45, 0), at(10, 35, 0)}, nil)
	if n := c.pendingCount(); n != 1 {
		t.Errorf("pendingCount() = %d, want 1", n)
	}
}

// A compute that fails with a QueryErrorCollection keeps the window pending until it succeeds.
func TestComputeExportController_ErrorCollectionKeepsWindowPending(t *testing.T) {
	failing := true
	src := &fakeComputeSource[controllerTestSet]{
		computeFn: func(start, _ time.Time, _ int) (*controllerTestSet, error) {
			if failing && start.Equal(at(9, 0, 0)) {
				errs := &source.QueryErrorCollector{}
				errs.AppendError(&source.QueryError{Query: "q", Error: fmt.Errorf("boom")})
				return nil, errs
			}
			return &controllerTestSet{}, nil
		},
	}
	exp := &fakeComputeExporter[controllerTestSet]{}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	runTicks(c, exp, []time.Time{at(9, 30, 0), at(10, 5, 0)}, nil)
	if n := c.pendingCount(); n != 1 {
		t.Fatalf("pendingCount() = %d after error collection, want 1", n)
	}
	failing = false
	runTicks(c, exp, []time.Time{at(10, 10, 0)}, nil)
	if n := c.pendingCount(); n != 0 {
		t.Errorf("pendingCount() = %d after recovery, want 0", n)
	}
}

// Many windows that always fail must not hold the retry slot ahead of a newer window that failed once:
// retries go to the fewest-attempted windows first.
func TestComputeExportController_ManyPoisonedWindowsDoNotBlockRetries(t *testing.T) {
	transient := at(14, 0, 0)
	src := &fakeComputeSource[controllerTestSet]{}
	exp := &fakeComputeExporter[controllerTestSet]{
		failIf: func(w opencost.Window, now time.Time) bool {
			s := *w.Start()
			if s.Before(at(14, 0, 0)) && !s.Before(at(1, 0, 0)) {
				return true // 13 windows that always fail
			}
			return s.Equal(transient) && now.Before(at(15, 10, 0))
		},
	}
	c := NewComputeExportController[controllerTestSet](src, exp, time.Hour)

	runTicks(c, exp, ticksEvery(at(0, 30, 0), at(18, 0, 0), 5*time.Minute), nil)

	i := firstPostCloseSuccess(exp.Records(), transient)
	if i < 0 {
		t.Fatalf("window %s was never exported after its transient failure cleared", hourWindow(transient))
	}
	// the transient failure clears at 15:10; with backoff it must be retried within maxRetryBackoffTicks
	if bound := at(15, 10, 0).Add(maxRetryBackoffTicks * 5 * time.Minute); exp.Records()[i].Now.After(bound) {
		t.Errorf("window %s finalized at %s, after %s", hourWindow(transient), exp.Records()[i].Now.Format("15:04:05"), bound.Format("15:04:05"))
	}
}
