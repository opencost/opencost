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
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
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

	// recovery drains oldest-first: 13:30 → 08,09; 13:35 → 10,11; 13:40 → 12.
	want := map[time.Time]time.Time{
		at(8, 0, 0):  recovery,
		at(9, 0, 0):  recovery,
		at(10, 0, 0): at(13, 35, 0),
		at(11, 0, 0): at(13, 35, 0),
		at(12, 0, 0): at(13, 40, 0),
	}
	for w, wantAt := range want {
		i := firstPostCloseSuccess(recs, w)
		if i < 0 {
			t.Errorf("window %s never got a post-close export", hourWindow(w))
			continue
		}
		if !recs[i].Now.Equal(wantAt) {
			t.Errorf("window %s finalized at %s, want %s (oldest-first, 2 per tick)",
				hourWindow(w), recs[i].Now.Format("15:04:05"), wantAt.Format("15:04:05"))
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
