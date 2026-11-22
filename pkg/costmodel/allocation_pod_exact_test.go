package costmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

// at returns a time inside the shared test window (2020-06-16), offset from its start.
func at(offset time.Duration) time.Time {
	return windowStart.Add(offset)
}

// samplesAt builds a running-series result with one sample per given time.
func samplesAt(times ...time.Time) []*util.Vector {
	out := make([]*util.Vector, 0, len(times))
	for _, t := range times {
		out = append(out, &util.Vector{Timestamp: float64(t.Unix()), Value: 1})
	}
	return out
}

func TestCalculatePodStartAndEnd(t *testing.T) {
	res := time.Minute
	h := 10 * time.Hour

	testCases := map[string]struct {
		samples       []*util.Vector
		starts, ends  []time.Time
		expectedStart time.Time
		expectedEnd   time.Time
	}{
		"no exact times: same as sampled": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + 2*time.Minute),
		},
		"2.5 minute pod seen by two samples is exact": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			starts:        []time.Time{at(h + 30*time.Second)},
			ends:          []time.Time{at(h + 3*time.Minute)},
			expectedStart: at(h + 30*time.Second),
			expectedEnd:   at(h + 3*time.Minute),
		},
		"2.5 minute pod seen by three samples is exact": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute), at(h+3*time.Minute)),
			starts:        []time.Time{at(h + 40*time.Second)},
			ends:          []time.Time{at(h + 3*time.Minute + 10*time.Second)},
			expectedStart: at(h + 40*time.Second),
			expectedEnd:   at(h + 3*time.Minute + 10*time.Second),
		},
		"45 second pod seen by one sample is exact": {
			samples:       samplesAt(at(h + time.Minute)),
			starts:        []time.Time{at(h + 50*time.Second)},
			ends:          []time.Time{at(h + time.Minute + 35*time.Second)},
			expectedStart: at(h + 50*time.Second),
			expectedEnd:   at(h + time.Minute + 35*time.Second),
		},
		"single sample, start only: one resolution from the exact start": {
			samples:       samplesAt(at(h + time.Minute)),
			starts:        []time.Time{at(h + 20*time.Second)},
			expectedStart: at(h + 20*time.Second),
			expectedEnd:   at(h + time.Minute + 20*time.Second),
		},
		"end only: start keeps the first sample": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			ends:          []time.Time{at(h + 2*time.Minute + 20*time.Second)},
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + 2*time.Minute + 20*time.Second),
		},
		"end shortly before the last sample (scrape delay) is used": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			ends:          []time.Time{at(h + time.Minute + 45*time.Second)},
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + time.Minute + 45*time.Second),
		},
		"end more than half a step before the last sample (sidecar) is ignored": {
			samples:       samplesAt(at(h+time.Minute), at(h+5*time.Minute)),
			ends:          []time.Time{at(h + 4*time.Minute + 10*time.Second)},
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + 5*time.Minute),
		},
		"start long before the first sample is ignored": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			starts:        []time.Time{at(h - 5*time.Minute)},
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + 2*time.Minute),
		},
		"start after the first sample (restart or clock skew) is ignored": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			starts:        []time.Time{at(h + time.Minute + 5*time.Second)},
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + 2*time.Minute),
		},
		"end long after the last sample is ignored": {
			samples:       samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			ends:          []time.Time{at(h + 10*time.Minute)},
			expectedStart: at(h + time.Minute),
			expectedEnd:   at(h + 2*time.Minute),
		},
		"closest candidates win": {
			samples: samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
			starts:  []time.Time{at(h - 30*time.Second), at(h + 20*time.Second)},
			ends: []time.Time{
				at(h + 3*time.Minute + 30*time.Second),
				at(h + 2*time.Minute + 10*time.Second),
			},
			expectedStart: at(h + 20*time.Second),
			expectedEnd:   at(h + 2*time.Minute + 10*time.Second),
		},
		"exact start before the window is clamped": {
			samples:       samplesAt(windowStart, windowStart.Add(time.Minute)),
			starts:        []time.Time{windowStart.Add(-30 * time.Second)},
			ends:          []time.Time{windowStart.Add(time.Minute + 15*time.Second)},
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute + 15*time.Second),
		},
		"exact end after the window is clamped": {
			samples:       samplesAt(windowEnd.Add(-2*time.Minute), windowEnd.Add(-time.Minute), windowEnd),
			ends:          []time.Time{windowEnd.Add(20 * time.Second)},
			expectedStart: windowEnd.Add(-2 * time.Minute),
			expectedEnd:   windowEnd,
		},
		"pod that ended before the window is skipped (no double count)": {
			samples: samplesAt(windowStart),
			starts:  []time.Time{windowStart.Add(-50 * time.Second)},
			ends:    []time.Time{windowStart.Add(-10 * time.Second)},
			// zero times: applyPodResults skips the pod
		},
		"completion before the window without an exact start is skipped": {
			samples: samplesAt(windowStart),
			ends:    []time.Time{windowStart.Add(-10 * time.Second)},
		},
		"completion before the window with an out-of-bounds start is skipped": {
			samples: samplesAt(windowStart),
			starts:  []time.Time{windowStart.Add(-10 * time.Minute)},
			ends:    []time.Time{windowStart.Add(-10 * time.Second)},
		},
		"pod that started at the window end is skipped": {
			samples: samplesAt(windowEnd),
			starts:  []time.Time{windowEnd},
		},
		"single sample, start two steps early: end never before the sample": {
			samples:       samplesAt(at(h + 10*time.Minute)),
			starts:        []time.Time{at(h + 10*time.Minute - 100*time.Second)},
			expectedStart: at(h + 10*time.Minute - 100*time.Second),
			expectedEnd:   at(h + 10*time.Minute),
		},
		"single sample after a missed scrape at the window start is kept": {
			samples:       samplesAt(windowStart.Add(time.Minute)),
			starts:        []time.Time{windowStart.Add(-time.Minute)},
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute),
		},
		"pod running across the window end keeps its share in this window": {
			samples:       samplesAt(windowEnd),
			starts:        []time.Time{windowEnd.Add(-50 * time.Second)},
			expectedStart: windowEnd.Add(-50 * time.Second),
			expectedEnd:   windowEnd,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			start, end := calculatePodStartAndEnd(tc.samples, res, window, tc.starts, tc.ends)
			if !start.Equal(tc.expectedStart) {
				t.Errorf("start: expected %v, got %v", tc.expectedStart, start)
			}
			if !end.Equal(tc.expectedEnd) {
				t.Errorf("end: expected %v, got %v", tc.expectedEnd, end)
			}
			if !start.IsZero() && !end.After(start) {
				t.Errorf("duration must be positive, got [%v, %v]", start, end)
			}
		})
	}
}

// A 40s pod whose only sample lands on the boundary between two windows must be counted
// once, for its real 40s, not in both windows.
func TestCalculatePodStartAndEndAcrossWindows(t *testing.T) {
	boundary := windowStart.Add(12 * time.Hour)
	before := opencost.NewClosedWindow(boundary.Add(-time.Hour), boundary)
	after := opencost.NewClosedWindow(boundary, boundary.Add(time.Hour))
	samples := samplesAt(boundary)
	starts := []time.Time{boundary.Add(-50 * time.Second)}
	ends := []time.Time{boundary.Add(-10 * time.Second)}

	s1, e1 := calculatePodStartAndEnd(samples, time.Minute, before, starts, ends)
	s2, e2 := calculatePodStartAndEnd(samples, time.Minute, after, starts, ends)

	total := e1.Sub(s1)
	if !s2.IsZero() {
		total += e2.Sub(s2)
	}
	if total != 40*time.Second {
		t.Errorf("expected 40s in total, got %v ([%v, %v] + [%v, %v])", total, s1, e1, s2, e2)
	}
}

func TestCalculatePodStartAndEndWithoutTimesMatchesSampled(t *testing.T) {
	samples := samplesAt(at(time.Hour), at(time.Hour+time.Minute), at(time.Hour+2*time.Minute))
	s1, e1 := calculateStartAndEnd(samples, time.Minute, window)
	s2, e2 := calculatePodStartAndEnd(samples, time.Minute, window, nil, nil)
	if !s1.Equal(s2) || !e1.Equal(e2) {
		t.Errorf("expected identical results, got [%v, %v] and [%v, %v]", s1, e1, s2, e2)
	}
}

func TestPodExactTimesLookup(t *testing.T) {
	exact := newPodExactTimes(
		[]*source.PodTimestampResult{
			{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1", Timestamp: at(time.Minute)},
			{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-2", Timestamp: at(time.Hour)},
			{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", Timestamp: at(2 * time.Hour)},
			{Cluster: "cluster1", Namespace: "ns", Pod: "zero"},
			{Cluster: "cluster1", Namespace: "", Pod: "no-namespace", Timestamp: at(time.Minute)},
			nil,
		},
		[]*source.PodTimestampResult{
			{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1", Timestamp: at(3 * time.Minute)},
		},
	)
	key := newPodKey("cluster1", "ns", "job-abc")

	starts, ends := exact.lookup(key, "uid-1")
	if len(starts) != 2 || !starts[0].Equal(at(time.Minute)) || !starts[1].Equal(at(2*time.Hour)) {
		t.Errorf("uid-1 starts: got %v", starts)
	}
	if len(ends) != 1 || !ends[0].Equal(at(3*time.Minute)) {
		t.Errorf("uid-1 ends: got %v", ends)
	}

	starts, ends = exact.lookup(key, "")
	if len(starts) != 3 || len(ends) != 1 {
		t.Errorf("no uid: expected every instance, got %d starts, %d ends", len(starts), len(ends))
	}

	if starts, _ := exact.lookup(newPodKey("cluster1", "ns", "zero"), ""); len(starts) != 0 {
		t.Errorf("zero timestamps must be dropped, got %v", starts)
	}

	var none *podExactTimes
	if starts, ends := none.lookup(key, "uid-1"); starts != nil || ends != nil {
		t.Errorf("nil index must return nothing")
	}
}

func TestApplyPodResultsUsesExactTimes(t *testing.T) {
	h := 10 * time.Hour
	resPods := []*source.PodsResult{{
		Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1",
		Data: samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
	}}
	exact := newPodExactTimes(
		[]*source.PodTimestampResult{{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1", Timestamp: at(h + 30*time.Second)}},
		[]*source.PodTimestampResult{{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1", Timestamp: at(h + 3*time.Minute)}},
	)

	podMap := map[podKey]*pod{}
	applyPodResults(window, time.Minute, podMap, resPods, false, map[podKey][]podKey{}, exact)
	p, ok := podMap[newPodKey("cluster1", "ns", "job-abc")]
	if !ok {
		t.Fatalf("pod not recorded")
	}
	if got := p.End.Sub(p.Start); got != 150*time.Second {
		t.Errorf("expected 2m30s, got %v", got)
	}

	// Without exact times the same samples give one minute.
	podMap = map[podKey]*pod{}
	applyPodResults(window, time.Minute, podMap, resPods, false, map[podKey][]podKey{}, nil)
	if got := podMap[newPodKey("cluster1", "ns", "job-abc")]; got.End.Sub(got.Start) != time.Minute {
		t.Errorf("expected sampled 1m, got %v", got.End.Sub(got.Start))
	}
}

// With UID ingestion, two instances of a reused pod name each get their own exact times.
func TestApplyPodResultsReusedNameWithUIDs(t *testing.T) {
	h := 10 * time.Hour
	resPods := []*source.PodsResult{
		{Cluster: "cluster1", Namespace: "ns", Pod: "worker-0", UID: "uid-a", Data: samplesAt(at(h+time.Minute), at(h+2*time.Minute))},
		{Cluster: "cluster1", Namespace: "ns", Pod: "worker-0", UID: "uid-b", Data: samplesAt(at(h+10*time.Minute), at(h+11*time.Minute))},
	}
	exact := newPodExactTimes(
		[]*source.PodTimestampResult{
			{Cluster: "cluster1", Namespace: "ns", Pod: "worker-0", UID: "uid-a", Timestamp: at(h + 30*time.Second)},
			{Cluster: "cluster1", Namespace: "ns", Pod: "worker-0", UID: "uid-b", Timestamp: at(h + 9*time.Minute + 40*time.Second)},
		},
		[]*source.PodTimestampResult{
			{Cluster: "cluster1", Namespace: "ns", Pod: "worker-0", UID: "uid-a", Timestamp: at(h + 2*time.Minute + 20*time.Second)},
			{Cluster: "cluster1", Namespace: "ns", Pod: "worker-0", UID: "uid-b", Timestamp: at(h + 11*time.Minute + 5*time.Second)},
		},
	)

	podMap := map[podKey]*pod{}
	applyPodResults(window, time.Minute, podMap, resPods, true, map[podKey][]podKey{}, exact)

	a := podMap[newPodKey("cluster1", "ns", "worker-0 uid-a")]
	b := podMap[newPodKey("cluster1", "ns", "worker-0 uid-b")]
	if a == nil || b == nil {
		t.Fatalf("expected both instances, got %v", podMap)
	}
	if !a.Start.Equal(at(h+30*time.Second)) || !a.End.Equal(at(h+2*time.Minute+20*time.Second)) {
		t.Errorf("uid-a: got [%v, %v]", a.Start, a.End)
	}
	if !b.Start.Equal(at(h+9*time.Minute+40*time.Second)) || !b.End.Equal(at(h+11*time.Minute+5*time.Second)) {
		t.Errorf("uid-b: got [%v, %v]", b.Start, b.End)
	}
}

// buildPodMap issues the timestamp queries and applies them; with no timestamp results the
// pods keep their sampled start and end.
func TestBuildPodMapExactTimes(t *testing.T) {
	h := 10 * time.Hour
	pods := []*source.PodsResult{{
		Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1",
		Data: samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
	}}

	build := func(withTimes bool) *pod {
		ds := source.NewMockOpenCostDataSource()
		ds.ResolutionValue = time.Minute
		ds.Querier.SetOverride(source.QueryPods, pods)
		if withTimes {
			ds.Querier.SetOverride(source.QueryPodContainerStartTime, []*source.PodTimestampResult{
				{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1", Timestamp: at(h + 30*time.Second)},
			})
			ds.Querier.SetOverride(source.QueryPodCompletionTime, []*source.PodTimestampResult{
				{Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1", Timestamp: at(h + 3*time.Minute)},
			})
		}
		cm := &CostModel{DataSource: ds}
		podMap := map[podKey]*pod{}
		if err := cm.buildPodMap(window, podMap, false, map[podKey][]podKey{}); err != nil {
			t.Fatalf("buildPodMap: %v", err)
		}
		return podMap[newPodKey("cluster1", "ns", "job-abc")]
	}

	if p := build(true); p == nil || p.End.Sub(p.Start) != 150*time.Second {
		t.Errorf("with timestamps: expected 2m30s, got %+v", p)
	}
	if p := build(false); p == nil || p.End.Sub(p.Start) != time.Minute {
		t.Errorf("without timestamps: expected sampled 1m, got %+v", p)
	}
}

// failingTimesQuerier returns errors from the two optional timestamp queries.
type failingTimesQuerier struct {
	*source.MockMetricsQuerier
}

func failingFuture(query string) *source.Future[source.PodTimestampResult] {
	ch := make(source.QueryResultsChan, 1)
	ch <- &source.QueryResults{Query: query, Error: fmt.Errorf("%s failed", query)}
	return source.NewFuture(source.DecodePodTimestampResult, ch)
}

func (q failingTimesQuerier) QueryPodContainerStartTime(start, end time.Time) *source.Future[source.PodTimestampResult] {
	return failingFuture(source.QueryPodContainerStartTime)
}

func (q failingTimesQuerier) QueryPodCompletionTime(start, end time.Time) *source.Future[source.PodTimestampResult] {
	return failingFuture(source.QueryPodCompletionTime)
}

type failingTimesDataSource struct {
	*source.MockOpenCostDataSource
	querier failingTimesQuerier
}

func (d failingTimesDataSource) Metrics() source.MetricsQuerier {
	return d.querier
}

// A failing optional query must not fail buildPodMap; pods keep their sampled start and end.
func TestBuildPodMapTimestampQueryFails(t *testing.T) {
	h := 10 * time.Hour
	mock := source.NewMockOpenCostDataSource()
	mock.ResolutionValue = time.Minute
	mock.Querier.SetOverride(source.QueryPods, []*source.PodsResult{{
		Cluster: "cluster1", Namespace: "ns", Pod: "job-abc", UID: "uid-1",
		Data: samplesAt(at(h+time.Minute), at(h+2*time.Minute)),
	}})
	cm := &CostModel{DataSource: failingTimesDataSource{MockOpenCostDataSource: mock, querier: failingTimesQuerier{mock.Querier}}}

	podMap := map[podKey]*pod{}
	if err := cm.buildPodMap(window, podMap, false, map[podKey][]podKey{}); err != nil {
		t.Fatalf("buildPodMap: %v", err)
	}
	p := podMap[newPodKey("cluster1", "ns", "job-abc")]
	if p == nil || p.End.Sub(p.Start) != time.Minute {
		t.Errorf("expected sampled 1m, got %+v", p)
	}
}
