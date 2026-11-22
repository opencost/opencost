package costmodel

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/config"
)

// ts builds one exact timestamp result for cluster1/ns.
func ts(pod, uid string, t time.Time) *source.PodTimestampResult {
	return &source.PodTimestampResult{Cluster: "cluster1", Namespace: "ns", Pod: pod, UID: uid, Timestamp: t}
}

func TestAddUnsampledPods(t *testing.T) {
	res := time.Minute
	h := 10 * time.Hour
	sampled := []*source.PodsResult{{Cluster: "cluster1", Namespace: "ns", Pod: "seen", UID: "uid-s", Data: samplesAt(at(h + time.Minute))}}

	cases := []struct {
		name      string
		starts    []*source.PodTimestampResult
		ends      []*source.PodTimestampResult
		uid       bool
		wantStart time.Time
		wantEnd   time.Time
		wantNone  bool
	}{
		{
			name:      "15s pod between two samples is added with its exact times",
			starts:    []*source.PodTimestampResult{ts("short", "uid-1", at(h+5*time.Second))},
			ends:      []*source.PodTimestampResult{ts("short", "uid-1", at(h+20*time.Second))},
			wantStart: at(h + 5*time.Second), wantEnd: at(h + 20*time.Second),
		},
		{
			name:     "pod returned by the running query is not added again",
			starts:   []*source.PodTimestampResult{ts("seen", "uid-s", at(h+30*time.Second))},
			ends:     []*source.PodTimestampResult{ts("seen", "uid-s", at(h+70*time.Second))},
			wantNone: true,
		},
		{
			name:     "completion before the window is left out",
			starts:   []*source.PodTimestampResult{ts("short", "uid-1", windowStart.Add(-40*time.Second))},
			ends:     []*source.PodTimestampResult{ts("short", "uid-1", windowStart.Add(-10*time.Second))},
			wantNone: true,
		},
		{
			name:      "pod across the window start is clamped to the window",
			starts:    []*source.PodTimestampResult{ts("short", "uid-1", windowStart.Add(-10*time.Second))},
			ends:      []*source.PodTimestampResult{ts("short", "uid-1", windowStart.Add(20*time.Second))},
			wantStart: windowStart, wantEnd: windowStart.Add(20 * time.Second),
		},
		{
			name:     "pod with no completion is left out",
			starts:   []*source.PodTimestampResult{ts("short", "uid-1", at(h))},
			wantNone: true,
		},
		{
			name:     "run longer than two resolution steps is not a sampling gap",
			starts:   []*source.PodTimestampResult{ts("short", "uid-1", at(h))},
			ends:     []*source.PodTimestampResult{ts("short", "uid-1", at(h+2*time.Minute+time.Second))},
			wantNone: true,
		},
		{
			name:     "completion before start is left out",
			starts:   []*source.PodTimestampResult{ts("short", "uid-1", at(h+20*time.Second))},
			ends:     []*source.PodTimestampResult{ts("short", "uid-1", at(h+5*time.Second))},
			wantNone: true,
		},
		{
			name:     "without UIDs, two instances of one name are ambiguous",
			starts:   []*source.PodTimestampResult{ts("short", "uid-1", at(h)), ts("short", "uid-2", at(h+5*time.Minute))},
			ends:     []*source.PodTimestampResult{ts("short", "uid-1", at(h+10*time.Second)), ts("short", "uid-2", at(h+5*time.Minute+10*time.Second))},
			wantNone: true,
		},
		{
			name:     "start and completion of different instances are not paired",
			starts:   []*source.PodTimestampResult{ts("short", "uid-1", at(h))},
			ends:     []*source.PodTimestampResult{ts("short", "uid-2", at(h+10*time.Second))},
			wantNone: true,
		},
		{
			name:      "with UIDs the instance gets its own key",
			starts:    []*source.PodTimestampResult{ts("short", "uid-1", at(h+5*time.Second))},
			ends:      []*source.PodTimestampResult{ts("short", "uid-1", at(h+20*time.Second))},
			uid:       true,
			wantStart: at(h + 5*time.Second), wantEnd: at(h + 20*time.Second),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			podMap := map[podKey]*pod{}
			uidMap := map[podKey][]podKey{}
			addUnsampledPods(window, res, podMap, sampled, tc.uid, uidMap, newPodExactTimes(tc.starts, tc.ends))

			if tc.wantNone {
				if len(podMap) != 0 {
					t.Fatalf("expected no pod, got %v", podMap)
				}
				return
			}
			key := newPodKey("cluster1", "ns", "short")
			if tc.uid {
				key = newPodKey("cluster1", "ns", "short uid-1")
				if got := uidMap[newPodKey("cluster1", "ns", "short")]; len(got) != 1 || got[0] != key {
					t.Errorf("podUIDKeyMap: got %v", got)
				}
			}
			p, ok := podMap[key]
			if !ok {
				t.Fatalf("pod %v not added: %v", key, podMap)
			}
			if !p.Start.Equal(tc.wantStart) || !p.End.Equal(tc.wantEnd) {
				t.Errorf("got [%v, %v], want [%v, %v]", p.Start, p.End, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

// With UIDs, two unsampled instances of one name are both added, each with its own times.
func TestAddUnsampledPodsReusedNameWithUIDs(t *testing.T) {
	h := 10 * time.Hour
	exact := newPodExactTimes(
		[]*source.PodTimestampResult{ts("worker-0", "uid-a", at(h)), ts("worker-0", "uid-b", at(h+5*time.Minute))},
		[]*source.PodTimestampResult{ts("worker-0", "uid-a", at(h+10*time.Second)), ts("worker-0", "uid-b", at(h+5*time.Minute+30*time.Second))},
	)
	podMap := map[podKey]*pod{}
	uidMap := map[podKey][]podKey{}
	addUnsampledPods(window, time.Minute, podMap, nil, true, uidMap, exact)

	a, b := podMap[newPodKey("cluster1", "ns", "worker-0 uid-a")], podMap[newPodKey("cluster1", "ns", "worker-0 uid-b")]
	if a == nil || b == nil {
		t.Fatalf("expected both instances, got %v", podMap)
	}
	if a.End.Sub(a.Start) != 10*time.Second || b.End.Sub(b.Start) != 30*time.Second {
		t.Errorf("got %v and %v", a.End.Sub(a.Start), b.End.Sub(b.Start))
	}
	if got := uidMap[newPodKey("cluster1", "ns", "worker-0")]; len(got) != 2 {
		t.Errorf("podUIDKeyMap: got %v", got)
	}
}

// With UID ingestion, a sampled instance is not added again, and a running result without a
// UID (buildPodMap's fallback) may be any instance, so nothing of that name is added.
func TestAddUnsampledPodsUIDSeen(t *testing.T) {
	h := 10 * time.Hour
	exact := newPodExactTimes(
		[]*source.PodTimestampResult{ts("short", "uid-1", at(h+5*time.Second))},
		[]*source.PodTimestampResult{ts("short", "uid-1", at(h+20*time.Second))},
	)
	for name, uid := range map[string]string{"same UID": "uid-1", "no UID": ""} {
		sampled := []*source.PodsResult{{Cluster: "cluster1", Namespace: "ns", Pod: "short", UID: uid, Data: samplesAt(at(h))}}
		podMap := map[podKey]*pod{}
		uidMap := map[podKey][]podKey{}
		addUnsampledPods(window, time.Minute, podMap, sampled, true, uidMap, exact)
		if len(podMap) != 0 || len(uidMap) != 0 {
			t.Errorf("%s: expected nothing added, got %v %v", name, podMap, uidMap)
		}
	}
}

// Splitting a window must not change a short pod's total runtime, whether the pod was
// sampled once (at the boundary) or never.
func TestShortPodsAcrossWindows(t *testing.T) {
	boundary := windowStart.Add(12 * time.Hour)
	before := opencost.NewClosedWindow(boundary.Add(-time.Hour), boundary)
	after := opencost.NewClosedWindow(boundary, boundary.Add(time.Hour))

	cases := []struct {
		name                        string
		samplesBefore, samplesAfter []*util.Vector
		start, end                  time.Time
		want                        time.Duration
	}{
		{
			// The completion is known in both windows (the timestamp query reads past the end).
			name:          "sampled once at the boundary, in the earlier window",
			samplesBefore: samplesAt(boundary),
			start:         boundary.Add(-50 * time.Second), end: boundary.Add(-10 * time.Second),
			want: 40 * time.Second,
		},
		{
			name:          "sampled once at the boundary, in both windows",
			samplesBefore: samplesAt(boundary),
			samplesAfter:  samplesAt(boundary),
			start:         boundary.Add(-50 * time.Second), end: boundary.Add(-10 * time.Second),
			want: 40 * time.Second,
		},
		{
			name:  "never sampled, across the boundary",
			start: boundary.Add(-10 * time.Second), end: boundary.Add(5 * time.Second),
			want: 15 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exact := newPodExactTimes(
				[]*source.PodTimestampResult{ts("short", "uid-1", tc.start)},
				[]*source.PodTimestampResult{ts("short", "uid-1", tc.end)},
			)
			var total time.Duration
			for _, w := range []struct {
				window  opencost.Window
				samples []*util.Vector
			}{{before, tc.samplesBefore}, {after, tc.samplesAfter}} {
				var resPods []*source.PodsResult
				if len(w.samples) > 0 {
					resPods = []*source.PodsResult{{Cluster: "cluster1", Namespace: "ns", Pod: "short", UID: "uid-1", Data: w.samples}}
				}
				podMap := map[podKey]*pod{}
				applyPodResults(w.window, time.Minute, podMap, resPods, false, map[podKey][]podKey{}, exact)
				addUnsampledPods(w.window, time.Minute, podMap, resPods, false, map[podKey][]podKey{}, exact)
				if p, ok := podMap[newPodKey("cluster1", "ns", "short")]; ok {
					total += p.End.Sub(p.Start)
				}
			}
			if total != tc.want {
				t.Errorf("expected %v in total, got %v", tc.want, total)
			}
		})
	}
}

// End to end: an unsampled 14s pod with a 0.25 core request is charged 0.25 * 14s of CPU on
// its node; one with no requests produces no allocation.
func TestComputeAllocationUnsampledPod(t *testing.T) {
	h := 10 * time.Hour
	start, end := at(h), at(h+time.Hour)

	ds := source.NewMockOpenCostDataSource()
	ds.ResolutionValue = time.Minute
	ds.Querier.SetOverride(source.QueryPodContainerStartTime, []*source.PodTimestampResult{
		ts("job-1", "uid-1", at(h+10*time.Minute)), ts("job-2", "uid-2", at(h+20*time.Minute)),
	})
	ds.Querier.SetOverride(source.QueryPodCompletionTime, []*source.PodTimestampResult{
		ts("job-1", "uid-1", at(h+10*time.Minute+14*time.Second)), ts("job-2", "uid-2", at(h+20*time.Minute+14*time.Second)),
	})
	ds.Querier.SetOverride(source.QueryCPURequests, []*source.CPURequestsResult{{
		Cluster: "cluster1", Namespace: "ns", Pod: "job-1", Container: "main", Node: "node-a",
		Data: []*util.Vector{{Timestamp: float64(end.Unix()), Value: 0.25}},
	}})

	confMan := config.NewConfigFileManager(storage.NewFileStorage("../../"))
	prov := &provider.CustomProvider{Config: provider.NewProviderConfig(confMan, "../../configs/default.json")}
	cm := &CostModel{DataSource: ds, Provider: prov, BatchDuration: 24 * time.Hour}
	set, _, err := cm.computeAllocation(start, end)
	if err != nil {
		t.Fatalf("computeAllocation: %v", err)
	}

	var job1 *opencost.Allocation
	for _, a := range set.Allocations {
		switch a.Properties.Pod {
		case "job-1":
			job1 = a
		case "job-2":
			t.Errorf("pod with no requests produced an allocation: %v", a.Name)
		}
	}
	if job1 == nil {
		t.Fatalf("no allocation for job-1; got %v", set.Allocations)
	}
	if want := 0.25 * 14.0 / 3600.0; math.Abs(job1.CPUCoreHours-want) > 1e-9 {
		t.Errorf("CPUCoreHours: got %v, want %v", job1.CPUCoreHours, want)
	}
	if job1.Properties.Node != "node-a" {
		t.Errorf("node: got %q", job1.Properties.Node)
	}
}
