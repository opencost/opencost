package proptest

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"pgregory.net/rapid"
)

// Each test below asserts a generator honours its own documented contract. The
// point is not coverage of the generators; it is that a generator which quietly
// stops producing an interesting shape cannot weaken the properties that draw
// from it while leaving the suite green.

func TestGenInferenceEngineProducesValidEntries(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		e := GenInferenceEngine(rt, EngineOpts{})

		if err := e.ValidateInferenceEngine(); err != nil {
			rt.Fatalf("generated entry failed the production validator: %v", err)
		}
		if e.Engine != kubemodel.EngineVLLM {
			rt.Fatalf("default opts must produce the supported engine, got %q", e.Engine)
		}

		assertFractionTriple(rt, "kv", e.KVCacheUsageAvg, e.KVCacheUsageP95, e.KVCacheUsageMax)
		assertCountTriple(rt, "queue", e.QueueDepthAvg, e.QueueDepthP95, e.QueueDepthMax)
		assertCountTriple(rt, "running", e.RunningRequestsAvg, e.RunningRequestsP95, e.RunningRequestsMax)

		if e.Preemptions < 0 {
			rt.Fatalf("preemptions must be non-negative, got %v", e.Preemptions)
		}
	})
}

// The generator has to be able to produce avg > p95, or a wrong implementation
// that assumes avg <= p95 would pass every property drawing from it. This is
// the single most important generator self-test in the file: it guards against
// the suite going quietly blind to right-skewed gauges, which is the normal
// shape for a queue-depth series that is empty most of the window.
func TestGenInferenceEngineCanProduceAvgAboveP95(t *testing.T) {
	found := false
	for seed := 0; seed < 400 && !found; seed++ {
		func() {
			defer func() { _ = recover() }()
			rapid.Check(&nopT{T: t}, func(rt *rapid.T) {
				e := GenInferenceEngine(rt, EngineOpts{})
				if e.QueueDepthAvg > e.QueueDepthP95 || e.KVCacheUsageAvg > e.KVCacheUsageP95 {
					found = true
				}
			})
		}()
	}
	if !found {
		t.Fatal("generator never produced avg > p95; the max >= p95 invariant would be untestable " +
			"against right-skewed input, and an implementation asserting avg <= p95 would pass wrongly")
	}
}

func TestGenInferenceEngineUnclampedProducesValuesNeedingClamping(t *testing.T) {
	var sawOutOfRange, sawNegative, sawBadOrdering bool

	for seed := 0; seed < 200; seed++ {
		func() {
			defer func() { _ = recover() }()
			rapid.Check(&nopT{T: t}, func(rt *rapid.T) {
				e := GenInferenceEngineUnclamped(rt)
				if e.KVCacheUsageP95 < 0 || e.KVCacheUsageP95 > 1 {
					sawOutOfRange = true
				}
				if e.QueueDepthAvg < 0 || e.RunningRequestsP95 < 0 || e.Preemptions < 0 {
					sawNegative = true
				}
				if e.QueueDepthMax < e.QueueDepthP95 || e.RunningRequestsMax < e.RunningRequestsAvg {
					sawBadOrdering = true
				}
			})
		}()
	}

	if !sawOutOfRange {
		t.Error("unclamped generator never produced a KV value outside [0, 1]")
	}
	if !sawNegative {
		t.Error("unclamped generator never produced a negative count")
	}
	if !sawBadOrdering {
		t.Error("unclamped generator never produced a max below its own avg or p95")
	}
}

func TestGenInferenceEngineOptsShapes(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		unsupported := GenInferenceEngine(rt, EngineOpts{UnsupportedEngine: true})
		if unsupported.Engine == kubemodel.EngineVLLM {
			rt.Fatal("UnsupportedEngine must not produce the supported engine value")
		}

		pooling := GenInferenceEngine(rt, EngineOpts{PoolingMode: true})
		if pooling.KVCacheUsageMax != 0 || pooling.QueueDepthMax != 0 ||
			pooling.RunningRequestsMax != 0 || pooling.Preemptions != 0 {
			rt.Fatal("PoolingMode must zero every decode-stage gauge; a pooling model runs no decode loop")
		}

		missingKV := GenInferenceEngine(rt, EngineOpts{MissingKV: true})
		if missingKV.KVCacheUsageMax != 0 {
			rt.Fatal("MissingKV must zero the KV gauges")
		}
	})
}

func TestGenEngineIndexIncludesLexicallyMisleadingValues(t *testing.T) {
	// "10" sorts before "9" lexically. Anything picking a lowest engine index
	// has to parse numerically, and the generator is what exposes a lexical
	// sort, so the alphabet must actually contain the trap.
	seen := map[string]bool{}
	for seed := 0; seed < 200; seed++ {
		func() {
			defer func() { _ = recover() }()
			rapid.Check(&nopT{T: t}, func(rt *rapid.T) {
				seen[GenEngineIndex(rt)] = true
			})
		}()
	}
	for _, want := range []string{"", "9", "10"} {
		if !seen[want] {
			t.Errorf("engine index alphabet never produced %q", want)
		}
	}
}

func TestGenGaugeSampleSeriesIsOrderedAndInsideTheWindow(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		w := GenWindow(rt)
		samples := GenGaugeSampleSeries(rt, w, SeriesOpts{Gaps: true, MinSamples: 1})

		for i, s := range samples {
			if s.Time.Before(*w.Start()) || !s.Time.Before(*w.End()) {
				rt.Fatalf("sample %d at %s falls outside the half-open window %s", i, s.Time, w)
			}
			if i > 0 && s.Time.Before(samples[i-1].Time) {
				rt.Fatalf("sample %d is out of order: %s before %s", i, s.Time, samples[i-1].Time)
			}
		}
	})
}

func TestGenGaugeSampleSeriesMonotonicResetsAreObservable(t *testing.T) {
	// A reset has to show up as a strict decrease between consecutive samples,
	// because that is the only signal available to detect an engine restart.
	sawDecrease := false
	for seed := 0; seed < 200 && !sawDecrease; seed++ {
		func() {
			defer func() { _ = recover() }()
			rapid.Check(&nopT{T: t}, func(rt *rapid.T) {
				w := GenWindow(rt)
				samples := GenGaugeSampleSeries(rt, w, SeriesOpts{
					Monotonic:  true,
					Resets:     2,
					MinSamples: 8,
					MaxSamples: 32,
				})
				for i := 1; i < len(samples); i++ {
					if samples[i].Value < samples[i-1].Value {
						sawDecrease = true
					}
				}
			})
		}()
	}
	if !sawDecrease {
		t.Fatal("Resets never produced a strict decrease, so counter-reset handling would be untested")
	}
}

func TestGenGaugeSampleSeriesAllZeroIsDistinctFromEmpty(t *testing.T) {
	// The pooling-model and idle-engine case is a populated series of zeros,
	// which must not be conflated with an absent series. If the generator
	// returned nil here, the two would be indistinguishable in every test that
	// draws from it, and that conflation is exactly what Requirement 16 exists
	// to prevent from reaching a user.
	rapid.Check(t, func(rt *rapid.T) {
		w := GenWindow(rt)
		samples := GenGaugeSampleSeries(rt, w, SeriesOpts{AllZero: true, MinSamples: 3, MaxSamples: 10})
		if len(samples) == 0 {
			rt.Fatal("AllZero with MinSamples >= 3 must still produce samples")
		}
		for _, s := range samples {
			if s.Value != 0 {
				rt.Fatalf("AllZero produced a non-zero value %v", s.Value)
			}
		}
	})
}

func TestGenWindowPartitionCoversTheWindowExactly(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		w, subs := GenWindowPartition(rt)

		if len(subs) == 0 {
			rt.Fatal("partition must not be empty; an empty partition makes the additivity properties vacuous")
		}
		if !subs[0].Start().Equal(*w.Start()) {
			rt.Fatalf("partition starts at %s, window starts at %s", subs[0].Start(), w.Start())
		}
		if !subs[len(subs)-1].End().Equal(*w.End()) {
			rt.Fatalf("partition ends at %s, window ends at %s", subs[len(subs)-1].End(), w.End())
		}

		var total time.Duration
		for i, s := range subs {
			if !s.End().After(*s.Start()) {
				rt.Fatalf("sub-window %d is empty or inverted: %s", i, s)
			}
			if i > 0 && !s.Start().Equal(*subs[i-1].End()) {
				rt.Fatalf("sub-window %d starts at %s but %d ended at %s: partition is not contiguous",
					i, s.Start(), i-1, subs[i-1].End())
			}
			total += s.End().Sub(*s.Start())
		}

		if total != w.End().Sub(*w.Start()) {
			rt.Fatalf("sub-window durations sum to %s, window is %s", total, w.End().Sub(*w.Start()))
		}
	})
}

func TestGenWindowPartitionProducesUnevenPartitions(t *testing.T) {
	// Equal-sized sub-windows would never produce the truncated trailing step a
	// real timeseries request generates, and that step is where
	// duration-weighted accumulation goes wrong.
	sawUneven := false
	for seed := 0; seed < 200 && !sawUneven; seed++ {
		func() {
			defer func() { _ = recover() }()
			rapid.Check(&nopT{T: t}, func(rt *rapid.T) {
				_, subs := GenWindowPartition(rt)
				if len(subs) < 2 {
					return
				}
				first := subs[0].End().Sub(*subs[0].Start())
				for _, s := range subs[1:] {
					if s.End().Sub(*s.Start()) != first {
						sawUneven = true
					}
				}
			})
		}()
	}
	if !sawUneven {
		t.Fatal("partitions were always evenly sized, so truncated trailing steps would be untested")
	}
}

func assertFractionTriple(rt *rapid.T, label string, avg, p95, max float64) {
	for name, v := range map[string]float64{"avg": avg, "p95": p95, "max": max} {
		if v < 0 || v > 1 {
			rt.Fatalf("%s %s must be a fraction in [0, 1], got %v", label, name, v)
		}
	}
	if max < p95 || max < avg {
		rt.Fatalf("%s max %v is below avg %v or p95 %v", label, max, avg, p95)
	}
}

func assertCountTriple(rt *rapid.T, label string, avg, p95, max float64) {
	for name, v := range map[string]float64{"avg": avg, "p95": p95, "max": max} {
		if v < 0 {
			rt.Fatalf("%s %s must be non-negative, got %v", label, name, v)
		}
	}
	if max < p95 || max < avg {
		rt.Fatalf("%s max %v is below avg %v or p95 %v", label, max, avg, p95)
	}
}

// nopT lets a self-test sample the generator many times to confirm a shape is
// reachable, without a single unlucky draw failing the build. rapid.Check needs
// a TB; swallowing Fatalf is what makes "did the generator ever produce X?"
// expressible.
type nopT struct {
	*testing.T
}

func (n *nopT) Errorf(string, ...any) {}
func (n *nopT) Fatalf(string, ...any) { panic("nopT") }
func (n *nopT) FailNow()              { panic("nopT") }
