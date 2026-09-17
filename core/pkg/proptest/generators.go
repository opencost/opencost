// Package proptest holds property-based test generators shared across
// OpenCost modules.
//
// It lives in core, and is exported rather than internal, because the same
// generators are needed by tests in core (KubeModel codec round-trips) and by
// tests in the root module (the inference efficiency calculator). The root
// module can import core; core cannot import the root module, and
// core/internal/... is not visible outside core. An exported package is the
// only arrangement that lets one definition serve both, and it follows the
// precedent already set by the exported test helpers in
// core/pkg/model/kubemodel/mock.go and core/pkg/source/mock.go.
//
// Nothing here is imported by production code. The generators exist to feed
// rapid.Check, and every one of them is itself covered by a self-test that
// asserts it produces values satisfying its stated contract. A silently broken
// generator is worse than a missing one: it weakens every property that draws
// from it while leaving the suite green.
package proptest

import (
	"time"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	"pgregory.net/rapid"
)

// EngineOpts controls which shapes GenInferenceEngine may produce. The zero
// value produces ordinary valid entries.
type EngineOpts struct {
	// UnsupportedEngine allows an Engine value other than vllm, for the
	// engine_unsupported degradation path.
	UnsupportedEngine bool
	// PoolingMode forces the decode-stage gauges to zero, which is how a
	// pooling model (embedding, classification, reward) reports: it runs no
	// decode loop.
	PoolingMode bool
	// MissingKV forces the KV-cache gauges to zero while leaving the batch
	// gauges populated. This is the OD-2 shape: a broken KV gauge on an
	// engine whose batch occupancy is measurable.
	MissingKV bool
}

// GenInferenceEngine produces a valid InferenceEngine, valid in the sense
// Requirement 13.2 defines: fractional fields in [0, 1], counts non-negative,
// enumeration fields drawn from their permitted values, identity fields
// non-empty.
//
// The summary triples are drawn max-first, then avg and p95 independently
// within [0, max]. That is deliberate and it is the one thing about this
// generator worth reading twice. The invariant the requirements state is
// `max >= p95` and `max >= avg` — it is NOT `avg <= p95`, because that is
// false for a right-skewed gauge: nineteen samples of 0 and one of 1000 give a
// mean of 50 and a p95 near 0. Drawing avg and p95 independently means the
// generator produces avg > p95 cases, so an implementation that wrongly
// assumes avg <= p95 fails here instead of passing.
func GenInferenceEngine(t *rapid.T, opts EngineOpts) *kubemodel.InferenceEngine {
	engine := kubemodel.EngineVLLM
	if opts.UnsupportedEngine {
		engine = rapid.SampledFrom([]string{"sglang", "trtllm", "unknown-engine"}).Draw(t, "engine")
	}

	kvAvg, kvP95, kvMax := genFractionTriple(t, "kv")
	if opts.PoolingMode || opts.MissingKV {
		kvAvg, kvP95, kvMax = 0, 0, 0
	}

	queueAvg, queueP95, queueMax := genCountTriple(t, "queue")
	runAvg, runP95, runMax := genCountTriple(t, "running")
	preemptions := rapid.Float64Range(0, 1e6).Draw(t, "preemptions")

	if opts.PoolingMode {
		queueAvg, queueP95, queueMax = 0, 0, 0
		runAvg, runP95, runMax = 0, 0, 0
		preemptions = 0
	}

	return &kubemodel.InferenceEngine{
		PodUID:             GenUID(t, "podUID"),
		EngineIndex:        GenEngineIndex(t),
		NamespaceUID:       GenUID(t, "namespaceUID"),
		ModelName:          GenModelName(t),
		Engine:             engine,
		KVCacheUsageAvg:    kvAvg,
		KVCacheUsageP95:    kvP95,
		KVCacheUsageMax:    kvMax,
		QueueDepthAvg:      queueAvg,
		QueueDepthP95:      queueP95,
		QueueDepthMax:      queueMax,
		RunningRequestsAvg: runAvg,
		RunningRequestsP95: runP95,
		RunningRequestsMax: runMax,
		Preemptions:        preemptions,
	}
}

// GenInferenceEngineUnclamped produces an InferenceEngine that deliberately
// violates the ordering and range invariants: KV values outside [0, 1],
// negative counts, and maxima below their own avg or p95.
//
// This generator exists because the clamp in Requirement 2.10 is only
// meaningfully tested by input that needs clamping. Feeding the clamp valid
// values would prove nothing beyond that it is a no-op.
func GenInferenceEngineUnclamped(t *rapid.T) *kubemodel.InferenceEngine {
	// Range chosen to straddle every boundary the clamp cares about: below 0,
	// inside [0, 1], and above 1.
	loose := func(label string) float64 {
		return rapid.Float64Range(-5, 5).Draw(t, label)
	}

	return &kubemodel.InferenceEngine{
		PodUID:             GenUID(t, "podUID"),
		EngineIndex:        GenEngineIndex(t),
		NamespaceUID:       GenUID(t, "namespaceUID"),
		ModelName:          GenModelName(t),
		Engine:             kubemodel.EngineVLLM,
		KVCacheUsageAvg:    loose("kvAvg"),
		KVCacheUsageP95:    loose("kvP95"),
		KVCacheUsageMax:    loose("kvMax"),
		QueueDepthAvg:      loose("queueAvg") * 10,
		QueueDepthP95:      loose("queueP95") * 10,
		QueueDepthMax:      loose("queueMax") * 10,
		RunningRequestsAvg: loose("runAvg") * 10,
		RunningRequestsP95: loose("runP95") * 10,
		RunningRequestsMax: loose("runMax") * 10,
		Preemptions:        loose("preemptions") * 100,
	}
}

// genFractionTriple draws an (avg, p95, max) summary bounded to [0, 1].
func genFractionTriple(t *rapid.T, label string) (avg, p95, max float64) {
	max = rapid.Float64Range(0, 1).Draw(t, label+"Max")
	avg = rapid.Float64Range(0, max).Draw(t, label+"Avg")
	p95 = rapid.Float64Range(0, max).Draw(t, label+"P95")
	return avg, p95, max
}

// genCountTriple draws an (avg, p95, max) summary of a non-negative count.
// The max is integral, because a maximum over integer gauge samples is itself
// an integer, while avg and p95 may be fractional.
func genCountTriple(t *rapid.T, label string) (avg, p95, max float64) {
	max = float64(rapid.IntRange(0, 4096).Draw(t, label+"Max"))
	avg = rapid.Float64Range(0, max).Draw(t, label+"Avg")
	p95 = rapid.Float64Range(0, max).Draw(t, label+"P95")
	return avg, p95, max
}

// GenUID produces a non-empty Kubernetes-shaped UID.
func GenUID(t *rapid.T, label string) string {
	return rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`).Draw(t, label)
}

// GenEngineIndex produces a vLLM engine index. vLLM emits the stringified core
// index, so "0" is the single-engine case and data-parallel deployments report
// one series per rank.
//
// The alphabet includes two-digit values on purpose. Engine indices are
// strings, so ordering them lexically puts "10" before "9"; anything selecting
// a "lowest" engine index must parse numerically, and this generator is what
// catches a lexical sort.
func GenEngineIndex(t *rapid.T) string {
	return rapid.SampledFrom([]string{"", "0", "1", "2", "7", "9", "10", "11", "15"}).Draw(t, "engineIndex")
}

// GenModelName produces a served model name. Half the alphabet carries a "/"
// because fully-qualified names like "org/model" are what make the aggregation
// key escaping rule load-bearing, and what the short-name reconciliation has
// to match against.
func GenModelName(t *rapid.T) string {
	return rapid.SampledFrom([]string{
		"llama-3-8b",
		"gemma-4-31B",
		"mistral-7b-instruct",
		"meta-llama/Llama-3-8B",
		"google/gemma-4-31B",
		"org/nested/model",
		"model-with-dash",
	}).Draw(t, "modelName")
}

// Sample is one timestamped gauge or counter observation.
type Sample struct {
	Time  time.Time
	Value float64
}

// SeriesOpts controls which awkward shapes GenGaugeSampleSeries injects. Each
// field targets a specific failure mode rather than adding generic noise.
type SeriesOpts struct {
	// Resets is the number of monotonic-counter resets to inject. A reset is
	// an engine restart, and an unqualified delta over a series containing
	// one reports a negative value.
	Resets int
	// Gaps injects intervals with no sample, which is how a scrape failure
	// looks and what distinguishes "measured zero" from "not measured".
	Gaps bool
	// BoundarySamples places a sample exactly on both window edges, which is
	// where a half-open window either double-counts or drops a sample.
	BoundarySamples bool
	// AllZero forces every value to zero, the pooling-model and idle-engine
	// shape.
	AllZero bool
	// Monotonic makes the series a non-decreasing counter rather than a
	// free-moving gauge.
	Monotonic bool
	// MinSamples and MaxSamples bound the series length. A zero MaxSamples
	// defaults to 64. MinSamples of 0 permits the empty series, and a
	// single-sample series is permitted whenever MinSamples <= 1, because a
	// delta needs two samples and a one-sample series is the degenerate case
	// that reveals whether the code knows it.
	MinSamples int
	MaxSamples int
}

// GenGaugeSampleSeries produces an ordered sample series over the given
// window. Samples are ascending by time and every timestamp falls inside
// [window.Start, window.End), except for the deliberate boundary samples.
func GenGaugeSampleSeries(t *rapid.T, w opencost.Window, opts SeriesOpts) []Sample {
	maxSamples := opts.MaxSamples
	if maxSamples == 0 {
		maxSamples = 64
	}
	n := rapid.IntRange(opts.MinSamples, maxSamples).Draw(t, "sampleCount")
	if n == 0 {
		return nil
	}

	start := *w.Start()
	span := w.End().Sub(start)
	step := span / time.Duration(n)
	if step <= 0 {
		step = time.Second
	}

	// Reset positions are drawn up front so they are distinct and ordered,
	// rather than decided per sample where two could collide.
	resetAt := map[int]bool{}
	for i := 0; i < opts.Resets && n > 1; i++ {
		resetAt[rapid.IntRange(1, n-1).Draw(t, "resetIndex")] = true
	}

	var (
		samples []Sample
		running float64
	)
	for i := 0; i < n; i++ {
		if opts.Gaps && rapid.Float64Range(0, 1).Draw(t, "gap") < 0.2 {
			continue
		}

		ts := start.Add(time.Duration(i) * step)
		if opts.BoundarySamples {
			if i == 0 {
				ts = start
			}
			if i == n-1 {
				// The window is half-open, so the last in-window instant is
				// one nanosecond before the end. This pins a sample there,
				// which is where an inclusive-end implementation double-counts
				// across adjacent sub-windows. A sample exactly at End is
				// deliberately not generated: it belongs to the next window,
				// and generating it here would put an out-of-window sample in
				// a series this generator documents as in-window.
				ts = w.End().Add(-time.Nanosecond)
			}
		}

		var v float64
		switch {
		case opts.AllZero:
			v = 0
		case opts.Monotonic:
			if resetAt[i] {
				running = 0
			}
			running += rapid.Float64Range(0, 100).Draw(t, "increment")
			v = running
		default:
			v = rapid.Float64Range(0, 1000).Draw(t, "value")
		}

		samples = append(samples, Sample{Time: ts, Value: v})
	}

	return samples
}

// GenWindow produces a window with a duration between one minute and 30 days,
// anchored at a fixed epoch so failures reproduce identically rather than
// drifting with wall-clock time.
func GenWindow(t *rapid.T) opencost.Window {
	anchor := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	offset := time.Duration(rapid.IntRange(0, 720).Draw(t, "windowOffsetHours")) * time.Hour
	minutes := rapid.IntRange(1, 30*24*60).Draw(t, "windowMinutes")

	start := anchor.Add(offset)
	return opencost.NewClosedWindow(start, start.Add(time.Duration(minutes)*time.Minute))
}

// GenWindowPartition produces a window together with a contiguous,
// non-overlapping partition of it that exactly covers it.
//
// The partition is deliberately uneven. Equal-sized sub-windows would never
// produce the truncated trailing step a real timeseries request generates when
// its duration is not an integer multiple of the step, and the trailing step is
// exactly where duration-weighted accumulation goes wrong.
func GenWindowPartition(t *rapid.T) (opencost.Window, []opencost.Window) {
	w := GenWindow(t)
	parts := rapid.IntRange(1, 12).Draw(t, "partitionCount")

	start := *w.Start()
	end := *w.End()
	total := end.Sub(start)

	// Draw cut weights, then scale them to the window so the partition covers
	// it exactly. Working in weights rather than absolute offsets is what
	// guarantees exact coverage: the last sub-window is pinned to the window
	// end rather than computed and hoped over.
	weights := make([]float64, parts)
	var sum float64
	for i := range weights {
		weights[i] = rapid.Float64Range(0.05, 1).Draw(t, "partitionWeight")
		sum += weights[i]
	}

	subs := make([]opencost.Window, 0, parts)
	cursor := start
	acc := 0.0
	for i := 0; i < parts; i++ {
		acc += weights[i]
		next := start.Add(time.Duration(float64(total) * acc / sum))
		if i == parts-1 || !next.Before(end) {
			next = end
		}
		if next.After(cursor) {
			subs = append(subs, opencost.NewClosedWindow(cursor, next))
			cursor = next
		}
		if !cursor.Before(end) {
			break
		}
	}

	// A degenerate draw can collapse every cut onto the start; fall back to
	// the whole window rather than returning an empty partition, since an
	// empty partition does not cover the window and would make the additivity
	// properties vacuously true.
	if len(subs) == 0 {
		subs = append(subs, opencost.NewClosedWindow(start, end))
	}

	return w, subs
}
