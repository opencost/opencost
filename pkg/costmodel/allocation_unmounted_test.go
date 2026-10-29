package costmodel

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func unmountedTestWindow() opencost.Window {
	s := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	e := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	return opencost.NewWindow(&s, &e)
}

func unmountedAt(h, m int) time.Time {
	return time.Date(2026, 10, 29, h, m, 0, 0, time.UTC)
}

func unmountedIv(start, end time.Time) observedInterval {
	return observedInterval{Start: start, End: end, Valid: true}
}

func TestClampInterval(t *testing.T) {
	w := unmountedTestWindow()
	ws, we := *w.Start(), *w.End()

	cases := map[string]struct {
		iv    observedInterval
		start time.Time
		end   time.Time
	}{
		"inside window":                 {unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0)), unmountedAt(2, 0), unmountedAt(5, 0)},
		"clamped to window start":       {unmountedIv(ws.Add(-time.Hour), unmountedAt(5, 0)), ws, unmountedAt(5, 0)},
		"clamped to window end":         {unmountedIv(unmountedAt(20, 0), we.Add(time.Hour)), unmountedAt(20, 0), we},
		"exactly one minute is kept":    {unmountedIv(unmountedAt(2, 0), unmountedAt(2, 1)), unmountedAt(2, 0), unmountedAt(2, 1)},
		"not valid":                     {observedInterval{Start: unmountedAt(2, 0), End: unmountedAt(5, 0)}, ws, we},
		"zero start":                    {unmountedIv(time.Time{}, unmountedAt(5, 0)), ws, we},
		"zero end":                      {unmountedIv(unmountedAt(2, 0), time.Time{}), ws, we},
		"both zero":                     {unmountedIv(time.Time{}, time.Time{}), ws, we},
		"zero length":                   {unmountedIv(unmountedAt(2, 0), unmountedAt(2, 0)), ws, we},
		"negative length":               {unmountedIv(unmountedAt(5, 0), unmountedAt(2, 0)), ws, we},
		"sub-minute":                    {unmountedIv(unmountedAt(2, 0), unmountedAt(2, 0).Add(59*time.Second)), ws, we},
		"before window":                 {unmountedIv(ws.Add(-3*time.Hour), ws.Add(-time.Hour)), ws, we},
		"after window":                  {unmountedIv(we.Add(time.Hour), we.Add(3*time.Hour)), ws, we},
		"touches window start only":     {unmountedIv(ws.Add(-time.Hour), ws), ws, we},
		"sub-minute overlap with start": {unmountedIv(ws.Add(-time.Hour), ws.Add(30*time.Second)), ws, we},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			s, e := clampInterval(w, c.iv)
			if !s.Equal(c.start) || !e.Equal(c.end) {
				t.Errorf("expected [%s, %s], got [%s, %s]", c.start, c.end, s, e)
			}
		})
	}

	t.Run("nil window bounds do not clamp a usable interval", func(t *testing.T) {
		s, e := clampInterval(opencost.NewWindow(nil, nil), unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0)))
		if !s.Equal(unmountedAt(2, 0)) || !e.Equal(unmountedAt(5, 0)) {
			t.Errorf("expected [02:00, 05:00], got [%s, %s]", s, e)
		}
	})

	t.Run("one nil window bound clamps only the other side", func(t *testing.T) {
		start := unmountedAt(3, 0)
		s, e := clampInterval(opencost.NewWindow(&start, nil), unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0)))
		if !s.Equal(start) || !e.Equal(unmountedAt(5, 0)) {
			t.Errorf("expected [03:00, 05:00], got [%s, %s]", s, e)
		}
	})

	t.Run("nil window bounds and no usable interval give zero times", func(t *testing.T) {
		s, e := clampInterval(opencost.NewWindow(nil, nil), observedInterval{})
		if !s.IsZero() || !e.IsZero() {
			t.Errorf("expected zero times, got [%s, %s]", s, e)
		}
	})
}

// A zero time on a pod means "unset" and must never win a widen.
func TestWidenPod_IgnoresZeroTimes(t *testing.T) {
	newPod := func() *pod {
		return &pod{
			Start: unmountedAt(2, 0),
			End:   unmountedAt(5, 0),
			Allocations: map[string]*opencost.Allocation{
				opencost.UnmountedSuffix: {Properties: &opencost.AllocationProperties{Container: opencost.UnmountedSuffix}, Start: unmountedAt(2, 0), End: unmountedAt(5, 0)},
			},
		}
	}

	t.Run("zero arguments leave the pod alone", func(t *testing.T) {
		p := newPod()
		widenPod(p, time.Time{}, time.Time{})
		assertUnmountedPod(t, p, unmountedAt(2, 0), unmountedAt(5, 0))
	})

	t.Run("unset pod times are filled in", func(t *testing.T) {
		p := &pod{Allocations: map[string]*opencost.Allocation{opencost.UnmountedSuffix: {Properties: &opencost.AllocationProperties{Container: opencost.UnmountedSuffix}}}}
		widenPod(p, unmountedAt(2, 0), unmountedAt(5, 0))
		assertUnmountedPod(t, p, unmountedAt(2, 0), unmountedAt(5, 0))
	})

	t.Run("nil window through getUnmountedPodForCluster", func(t *testing.T) {
		nilWindow := opencost.NewWindow(nil, nil)
		m := map[podKey]*pod{}
		p := getUnmountedPodForCluster(nilWindow, m, "c1", observedInterval{})
		if !p.Start.IsZero() || !p.End.IsZero() {
			t.Fatalf("expected unset pod times, got [%s, %s]", p.Start, p.End)
		}
		p = getUnmountedPodForCluster(nilWindow, m, "c1", unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0)))
		assertUnmountedPod(t, p, unmountedAt(2, 0), unmountedAt(5, 0))
		p = getUnmountedPodForCluster(nilWindow, m, "c1", observedInterval{})
		assertUnmountedPod(t, p, unmountedAt(2, 0), unmountedAt(5, 0))
	})
}

// getLoadBalancerCosts extends lb.End by one resolution, which can pass now.
func TestLBCostInterval_CappedAtNow(t *testing.T) {
	now := unmountedAt(9, 0)

	t.Run("end past now is capped", func(t *testing.T) {
		lb := &lbCost{Start: unmountedAt(1, 0), End: now.Add(time.Minute)}
		got := lb.intervalAsOf(now)
		if !got.End.Equal(now) || !got.Start.Equal(unmountedAt(1, 0)) || !got.Valid {
			t.Errorf("expected [01:00, 09:00], got %+v", got)
		}
		if !lb.End.Equal(now.Add(time.Minute)) {
			t.Errorf("lb.End must not be modified, got %s", lb.End)
		}
	})

	t.Run("end before now is unchanged", func(t *testing.T) {
		lb := &lbCost{Start: unmountedAt(1, 0), End: unmountedAt(5, 0)}
		if got := lb.intervalAsOf(now); !got.End.Equal(unmountedAt(5, 0)) {
			t.Errorf("expected end 05:00, got %s", got.End)
		}
	})

	t.Run("nil lb", func(t *testing.T) {
		var lb *lbCost
		if lb.intervalAsOf(now).Valid {
			t.Errorf("nil lb must give an invalid interval")
		}
	})
}

func assertUnmountedPod(t *testing.T, p *pod, start, end time.Time) {
	t.Helper()
	if !p.Start.Equal(start) || !p.End.Equal(end) {
		t.Errorf("pod: expected [%s, %s], got [%s, %s]", start, end, p.Start, p.End)
	}
	alloc := p.Allocations[opencost.UnmountedSuffix]
	if alloc == nil {
		t.Fatalf("missing unmounted allocation")
	}
	if !alloc.Start.Equal(start) || !alloc.End.Equal(end) {
		t.Errorf("allocation: expected [%s, %s], got [%s, %s]", start, end, alloc.Start, alloc.End)
	}
	if alloc.Properties.Container != opencost.UnmountedSuffix {
		t.Errorf("expected container %q, got %q", opencost.UnmountedSuffix, alloc.Properties.Container)
	}
}

func TestGetUnmountedPod_UsesObservedInterval(t *testing.T) {
	w := unmountedTestWindow()

	t.Run("cluster", func(t *testing.T) {
		p := getUnmountedPodForCluster(w, map[podKey]*pod{}, "c1", unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0)))
		assertUnmountedPod(t, p, unmountedAt(2, 0), unmountedAt(5, 0))
		if p.Window.Start() == nil || !p.Window.Start().Equal(*w.Start()) {
			t.Errorf("pod window must stay the query window")
		}
	})

	t.Run("namespace", func(t *testing.T) {
		p := getUnmountedPodForNamespace(w, map[podKey]*pod{}, "c1", "ns1", unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0)))
		assertUnmountedPod(t, p, unmountedAt(2, 0), unmountedAt(5, 0))
		if p.Key.Pod != "ns1-unmounted-pvcs" || p.Allocations[opencost.UnmountedSuffix].Properties.Namespace != "ns1" {
			t.Errorf("unexpected namespace unmounted pod: %+v", p.Key)
		}
	})

	t.Run("namespace without interval uses window", func(t *testing.T) {
		p := getUnmountedPodForNamespace(w, map[podKey]*pod{}, "c1", "ns1", observedInterval{})
		assertUnmountedPod(t, p, *w.Start(), *w.End())
	})

	t.Run("unusable intervals give at least a minute", func(t *testing.T) {
		for name, in := range map[string]observedInterval{
			"zero-length": unmountedIv(unmountedAt(2, 0), unmountedAt(2, 0)),
			"negative":    unmountedIv(unmountedAt(5, 0), unmountedAt(2, 0)),
			"sub-minute":  unmountedIv(unmountedAt(2, 0), unmountedAt(2, 0).Add(10*time.Second)),
			"zero-value":  unmountedIv(time.Time{}, time.Time{}),
			"disjoint":    unmountedIv(w.Start().Add(-2*time.Hour), w.Start().Add(-time.Hour)),
		} {
			p := getUnmountedPodForCluster(w, map[podKey]*pod{}, "c1", in)
			if m := p.Allocations[opencost.UnmountedSuffix].Minutes(); m < 1 {
				t.Errorf("%s: expected Minutes() >= 1, got %f", name, m)
			}
		}
	})
}

func TestGetUnmountedPod_WidenIsOrderIndependent(t *testing.T) {
	w := unmountedTestWindow()
	a := unmountedIv(unmountedAt(2, 0), unmountedAt(5, 0))
	b := unmountedIv(unmountedAt(4, 0), unmountedAt(9, 0))
	whole := observedInterval{}

	t.Run("cluster", func(t *testing.T) {
		m1, m2 := map[podKey]*pod{}, map[podKey]*pod{}
		getUnmountedPodForCluster(w, m1, "c1", a)
		p1 := getUnmountedPodForCluster(w, m1, "c1", b)
		getUnmountedPodForCluster(w, m2, "c1", b)
		p2 := getUnmountedPodForCluster(w, m2, "c1", a)
		assertUnmountedPod(t, p1, unmountedAt(2, 0), unmountedAt(9, 0))
		assertUnmountedPod(t, p2, unmountedAt(2, 0), unmountedAt(9, 0))
	})

	t.Run("namespace", func(t *testing.T) {
		m1, m2 := map[podKey]*pod{}, map[podKey]*pod{}
		getUnmountedPodForNamespace(w, m1, "c1", "ns", a)
		p1 := getUnmountedPodForNamespace(w, m1, "c1", "ns", b)
		getUnmountedPodForNamespace(w, m2, "c1", "ns", b)
		p2 := getUnmountedPodForNamespace(w, m2, "c1", "ns", a)
		assertUnmountedPod(t, p1, unmountedAt(2, 0), unmountedAt(9, 0))
		assertUnmountedPod(t, p2, unmountedAt(2, 0), unmountedAt(9, 0))
	})

	t.Run("fallback to window widens to the window in either order", func(t *testing.T) {
		m1, m2 := map[podKey]*pod{}, map[podKey]*pod{}
		getUnmountedPodForCluster(w, m1, "c1", a)
		p1 := getUnmountedPodForCluster(w, m1, "c1", whole)
		getUnmountedPodForCluster(w, m2, "c1", whole)
		p2 := getUnmountedPodForCluster(w, m2, "c1", a)
		assertUnmountedPod(t, p1, *w.Start(), *w.End())
		assertUnmountedPod(t, p2, *w.Start(), *w.End())
	})

	t.Run("existing pod is never shrunk", func(t *testing.T) {
		m := map[podKey]*pod{}
		getUnmountedPodForCluster(w, m, "c1", whole)
		p := getUnmountedPodForCluster(w, m, "c1", a)
		assertUnmountedPod(t, p, *w.Start(), *w.End())
	})
}

// A window that is still open: PV and LB intervals end at "now", well before
// the window end. The unmounted allocation must span what was observed, while
// PV and LB dollars are unchanged.
func TestApplyUnmounted_InProgressWindow(t *testing.T) {
	w := unmountedTestWindow()
	now := unmountedAt(9, 0)
	const gib = 1024 * 1024 * 1024

	volume := &pv{Start: unmountedAt(0, 0), End: now, Bytes: 10 * gib, CostPerGiBHour: 0.1, Cluster: "c1", Name: "pv1"}
	claim := &pvc{Name: "claim", Namespace: "ns1", Cluster: "c1", Volume: &pv{Start: unmountedAt(1, 0), End: now, Bytes: 5 * gib, CostPerGiBHour: 0.2, Cluster: "c1", Name: "pv2"}, Start: unmountedAt(1, 0), End: now}
	lb := &lbCost{TotalCost: 3.0, Start: unmountedAt(0, 30), End: now}

	podMap := map[podKey]*pod{}
	applyUnmountedPVs(w, podMap, map[pvKey]*pv{newPVKey("c1", "pv1"): volume}, map[pvcKey]*pvc{})
	applyUnmountedPVCs(w, podMap, map[pvcKey]*pvc{newPVCKey("c1", "ns1", "claim"): claim})
	// A non-overlapping allocation keeps the LB cost on the unmounted pod, once.
	other := &opencost.Allocation{Name: "other", Properties: &opencost.AllocationProperties{}, Start: unmountedAt(10, 0), End: unmountedAt(11, 0)}
	applyLoadBalancersToPods(w, podMap, map[serviceKey]*lbCost{newServiceKey("c1", "ns1", "svc"): lb}, map[serviceKey][]*opencost.Allocation{newServiceKey("c1", "ns1", "svc"): {other}})

	cluster := podMap[getUnmountedPodKey("c1")]
	ns := podMap[newPodKey("c1", "ns1", "ns1-unmounted-pvcs")]
	if cluster == nil || ns == nil {
		t.Fatalf("expected unmounted pods for cluster and namespace")
	}

	// cluster pod: PV [00:00, 09:00] widened by the LB [00:30, 09:00]
	assertUnmountedPod(t, cluster, unmountedAt(0, 0), now)
	assertUnmountedPod(t, ns, unmountedAt(1, 0), now)

	// Dollars come from the resources' own minutes, not from the pod span.
	wantPV := 0.1 * 10 * 9
	gotPV := cluster.Allocations[opencost.UnmountedSuffix].PVCost()
	if math.Abs(gotPV-wantPV) > 1e-9 {
		t.Errorf("cluster PV cost: expected %f, got %f", wantPV, gotPV)
	}
	if got := cluster.Allocations[opencost.UnmountedSuffix].LoadBalancerCost; math.Abs(got-3.0) > 1e-9 {
		t.Errorf("LB cost: expected 3.0, got %f", got)
	}
	wantPVC := 0.2 * 5 * 8
	if got := ns.Allocations[opencost.UnmountedSuffix].PVCost(); math.Abs(got-wantPVC) > 1e-9 {
		t.Errorf("namespace PVC cost: expected %f, got %f", wantPVC, got)
	}
}

// The unmounted fallback in applyPVCsToPods must keep computing PVC hours
// over the whole window even though the unmounted pod is now narrower.
func TestApplyPVCsToPods_UnmountedFallbackCostUnchanged(t *testing.T) {
	w := unmountedTestWindow()
	const gib = 1024 * 1024 * 1024

	vol := &pv{Cluster: "c1", Name: "pv1", CostPerGiBHour: 0.1, Bytes: 10 * gib, Start: unmountedAt(0, 0), End: unmountedAt(12, 0)}
	claim := &pvc{Name: "claim", Namespace: "ns1", Cluster: "c1", Bytes: 10 * gib, Volume: vol, Mounted: true, Start: unmountedAt(0, 0), End: unmountedAt(12, 0)}

	// The pod mounts the pvc for the first 6 of the pvc's 12 hours, so half of
	// the pvc's cost goes to the namespace's unmounted pod.
	podKeyA := newPodKey("c1", "ns1", "pod-a")
	podA := &pod{
		Window: w.Clone(),
		Start:  unmountedAt(0, 0),
		End:    unmountedAt(6, 0),
		Key:    podKeyA,
		Allocations: map[string]*opencost.Allocation{
			"main": {Name: "a", Properties: &opencost.AllocationProperties{}, Window: w.Clone(), Start: unmountedAt(0, 0), End: unmountedAt(6, 0)},
		},
	}

	podMap := map[podKey]*pod{podKeyA: podA}
	applyPVCsToPods(w, podMap, map[podKey][]*pvc{podKeyA: {claim}}, map[pvcKey]*pvc{claim.key(): claim})

	ns := podMap[newPodKey("c1", "ns1", "ns1-unmounted-pvcs")]
	if ns == nil {
		t.Fatalf("expected namespace unmounted pod")
	}
	assertUnmountedPod(t, ns, unmountedAt(0, 0), unmountedAt(12, 0))

	// 24 window hours * 10 GiB * $0.1 * 0.5 coefficient, as before the change.
	want := 24 * 10 * 0.1 * 0.5
	got := ns.Allocations[opencost.UnmountedSuffix].PVCost()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("unmounted PVC cost: expected %f, got %f", want, got)
	}
}
