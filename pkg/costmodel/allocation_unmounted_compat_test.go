package costmodel

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// These tests only use APIs that are identical before and after KCM-6594, so
// the same file can be run against the previous implementation. Expected
// values were checked that way: dollars must match for every window, and
// Start/End must match for a completed window.

type unmountedSnapshot struct {
	start, end                  time.Time
	pvCost, lbCost, pvByteHours float64
}

func compatWindow() opencost.Window {
	s := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	e := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	return opencost.NewWindow(&s, &e)
}

func compatAt(h int) time.Time {
	return time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC).Add(time.Duration(h) * time.Hour)
}

func compatSnapshot(t *testing.T, p *pod) unmountedSnapshot {
	t.Helper()
	if p == nil {
		t.Fatalf("missing unmounted pod")
	}
	a := p.Allocations[opencost.UnmountedSuffix]
	if a == nil {
		t.Fatalf("missing unmounted allocation")
	}
	return unmountedSnapshot{
		start:       a.Start,
		end:         a.End,
		pvCost:      a.PVCost(),
		lbCost:      a.LoadBalancerCost,
		pvByteHours: a.PVByteHours(),
	}
}

// compatRun applies an unmounted PV, an unmounted PVC, an LB without
// allocations, and a PVC whose pod has no allocations, all over [from, to].
func compatRun(t *testing.T, from, to time.Time) (cluster, ns, fallback unmountedSnapshot) {
	t.Helper()
	w := compatWindow()
	const gib = 1024 * 1024 * 1024

	volume := &pv{Start: from, End: to, Bytes: 10 * gib, CostPerGiBHour: 0.1, Cluster: "c1", Name: "pv1"}
	claim := &pvc{Name: "claim", Namespace: "ns1", Cluster: "c1", Bytes: 5 * gib, Start: from, End: to,
		Volume: &pv{Start: from, End: to, Bytes: 5 * gib, CostPerGiBHour: 0.2, Cluster: "c1", Name: "pv2"}}
	lb := &lbCost{TotalCost: 3.0, Start: from, End: to}

	podMap := map[podKey]*pod{}
	applyUnmountedPVs(w, podMap, map[pvKey]*pv{newPVKey("c1", "pv1"): volume}, map[pvcKey]*pvc{})
	applyUnmountedPVCs(w, podMap, map[pvcKey]*pvc{newPVCKey("c1", "ns1", "claim"): claim})
	// An allocation that does not overlap the LB keeps its cost on the unmounted pod, once.
	other := &opencost.Allocation{Name: "other", Properties: &opencost.AllocationProperties{}, Start: w.End().Add(time.Hour), End: w.End().Add(2 * time.Hour)}
	applyLoadBalancersToPods(w, podMap, map[serviceKey]*lbCost{newServiceKey("c1", "ns1", "svc"): lb}, map[serviceKey][]*opencost.Allocation{newServiceKey("c1", "ns1", "svc"): {other}})

	cluster = compatSnapshot(t, podMap[getUnmountedPodKey("c1")])
	ns = compatSnapshot(t, podMap[newPodKey("c1", "ns1", "ns1-unmounted-pvcs")])

	// PVC fallback in applyPVCsToPods, in its own namespace so it is isolated.
	fvol := &pv{Cluster: "c1", Name: "pv3", CostPerGiBHour: 0.1, Bytes: 10 * gib, Start: from, End: to}
	fclaim := &pvc{Name: "fclaim", Namespace: "ns2", Cluster: "c1", Bytes: 10 * gib, Volume: fvol, Mounted: true, Start: from, End: to}
	mid := from.Add(to.Sub(from) / 2)
	keyA := newPodKey("c1", "ns2", "pod-a")
	podA := &pod{
		Window: w.Clone(), Start: from, End: mid, Key: keyA,
		Allocations: map[string]*opencost.Allocation{
			"main": {Name: "a", Properties: &opencost.AllocationProperties{}, Window: w.Clone(), Start: from, End: mid},
		},
	}
	fMap := map[podKey]*pod{keyA: podA}
	applyPVCsToPods(w, fMap, map[podKey][]*pvc{keyA: {fclaim}}, map[pvcKey]*pvc{fclaim.key(): fclaim})
	fallback = compatSnapshot(t, fMap[newPodKey("c1", "ns2", "ns2-unmounted-pvcs")])
	return
}

func compatClose(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s: expected %f, got %f", what, want, got)
	}
}

// Completed window: every resource covers the whole window. Nothing may move:
// not the Start/End, not a dollar.
func TestUnmountedCompat_CompletedWindowUnchanged(t *testing.T) {
	w := compatWindow()
	cluster, ns, fallback := compatRun(t, *w.Start(), *w.End())

	for name, s := range map[string]unmountedSnapshot{"cluster": cluster, "namespace": ns, "fallback": fallback} {
		if !s.start.Equal(*w.Start()) || !s.end.Equal(*w.End()) {
			t.Errorf("%s: expected span equal to the window, got [%s, %s]", name, s.start, s.end)
		}
	}
	compatClose(t, "cluster PV cost", cluster.pvCost, 0.1*10*24)
	compatClose(t, "cluster LB cost", cluster.lbCost, 3.0)
	compatClose(t, "namespace PVC cost", ns.pvCost, 0.2*5*24)
	compatClose(t, "fallback PVC cost", fallback.pvCost, 0.1*10*24*0.5)
}

// In-progress window: resources were observed only until 09:00. Dollars are
// the same as before the change; only the Start/End are allowed to differ.
func TestUnmountedCompat_InProgressWindowDollarsUnchanged(t *testing.T) {
	cluster, ns, fallback := compatRun(t, compatAt(0), compatAt(9))

	compatClose(t, "cluster PV cost", cluster.pvCost, 0.1*10*9)
	compatClose(t, "cluster LB cost", cluster.lbCost, 3.0)
	compatClose(t, "namespace PVC cost", ns.pvCost, 0.2*5*9)
	// The fallback has always priced the whole 24h window.
	compatClose(t, "fallback PVC cost", fallback.pvCost, 0.1*10*24*0.5)
}
