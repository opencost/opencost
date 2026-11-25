package prom

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

func newPodTimestampQueryResult(cluster, namespace, pod, uid string, evalTs, valueTs float64) *source.QueryResult {
	metric := map[string]any{
		"cluster_id": cluster,
		"namespace":  namespace,
		"pod":        pod,
		"uid":        uid,
	}
	return source.NewQueryResult(metric, []*util.Vector{{Timestamp: evalTs, Value: valueTs}}, nil)
}

func requireTimestamps(t *testing.T, qr *source.QueryResult, wantStart, wantEnd float64) {
	t.Helper()
	if len(qr.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(qr.Values))
	}
	if qr.Values[0].Timestamp != wantStart {
		t.Errorf("expected start timestamp %f, got %f", wantStart, qr.Values[0].Timestamp)
	}
	if qr.Values[1].Timestamp != wantEnd {
		t.Errorf("expected end timestamp %f, got %f", wantEnd, qr.Values[1].Timestamp)
	}
}

func TestMergePodStartEndResults(t *testing.T) {
	t.Run("pod present in both start and end results", func(t *testing.T) {
		resStart := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "kube-system", "pod-abc", "uid-1", 5000, 1000),
		}
		resEnd := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "kube-system", "pod-abc", "uid-1", 5000, 2000),
		}

		merged := mergePodStartEndResults(resStart, resEnd)
		if len(merged) != 1 {
			t.Fatalf("expected 1 merged result, got %d", len(merged))
		}
		requireTimestamps(t, merged[0], 1000, 2000)

		namespace, err := merged[0].GetNamespace()
		if err != nil || namespace != "kube-system" {
			t.Errorf("expected namespace to be preserved, got %q (err: %v)", namespace, err)
		}
	})

	t.Run("timestamps are rounded to the nearest 10 seconds", func(t *testing.T) {
		resStart := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "default", "pod-abc", "uid-1", 5000, 1003.2),
		}
		resEnd := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "default", "pod-abc", "uid-1", 5000, 1997.5),
		}

		merged := mergePodStartEndResults(resStart, resEnd)
		if len(merged) != 1 {
			t.Fatalf("expected 1 merged result, got %d", len(merged))
		}
		requireTimestamps(t, merged[0], 1000, 2000)
	})

	t.Run("pod missing from end results uses start timestamp for both", func(t *testing.T) {
		resStart := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "default", "pod-abc", "uid-1", 5000, 1000),
		}

		merged := mergePodStartEndResults(resStart, nil)
		if len(merged) != 1 {
			t.Fatalf("expected 1 merged result, got %d", len(merged))
		}
		requireTimestamps(t, merged[0], 1000, 1000)
	})

	t.Run("pod missing from start results uses end timestamp for both", func(t *testing.T) {
		resEnd := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "default", "pod-abc", "uid-1", 5000, 2000),
		}

		merged := mergePodStartEndResults(nil, resEnd)
		if len(merged) != 1 {
			t.Fatalf("expected 1 merged result, got %d", len(merged))
		}
		requireTimestamps(t, merged[0], 2000, 2000)
	})

	t.Run("pods are matched by cluster, namespace, pod, and uid", func(t *testing.T) {
		resStart := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "ns-a", "pod-abc", "uid-1", 5000, 1000),
			newPodTimestampQueryResult("cluster-one", "ns-b", "pod-abc", "uid-2", 5000, 1200),
		}
		resEnd := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "ns-b", "pod-abc", "uid-2", 5000, 2200),
			newPodTimestampQueryResult("cluster-one", "ns-a", "pod-abc", "uid-1", 5000, 2000),
		}

		merged := mergePodStartEndResults(resStart, resEnd)
		if len(merged) != 2 {
			t.Fatalf("expected 2 merged results, got %d", len(merged))
		}

		for _, qr := range merged {
			namespace, _ := qr.GetNamespace()
			switch namespace {
			case "ns-a":
				requireTimestamps(t, qr, 1000, 2000)
			case "ns-b":
				requireTimestamps(t, qr, 1200, 2200)
			default:
				t.Errorf("unexpected namespace %q in merged results", namespace)
			}
		}
	})

	t.Run("end timestamp earlier than start is clamped to start", func(t *testing.T) {
		resStart := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "default", "pod-abc", "uid-1", 5000, 2000),
		}
		resEnd := []*source.QueryResult{
			newPodTimestampQueryResult("cluster-one", "default", "pod-abc", "uid-1", 5000, 1990),
		}

		merged := mergePodStartEndResults(resStart, resEnd)
		if len(merged) != 1 {
			t.Fatalf("expected 1 merged result, got %d", len(merged))
		}
		requireTimestamps(t, merged[0], 2000, 2000)
	})

	t.Run("results with no values are dropped", func(t *testing.T) {
		resStart := []*source.QueryResult{
			source.NewQueryResult(map[string]any{"namespace": "default", "pod": "pod-abc"}, nil, nil),
		}
		resEnd := []*source.QueryResult{
			source.NewQueryResult(map[string]any{"namespace": "default", "pod": "pod-abc"}, nil, nil),
		}

		merged := mergePodStartEndResults(resStart, resEnd)
		if len(merged) != 0 {
			t.Fatalf("expected 0 merged results, got %d", len(merged))
		}
	})

	t.Run("empty inputs produce an empty non-nil result", func(t *testing.T) {
		merged := mergePodStartEndResults(nil, nil)
		if merged == nil {
			t.Fatal("expected non-nil result")
		}
		if len(merged) != 0 {
			t.Fatalf("expected 0 merged results, got %d", len(merged))
		}
	})
}
