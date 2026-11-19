package opencost

import (
	"testing"
	"time"
)

// Merging sets grouped by the same dimensions keeps the keys derived from the
// caller's label config, which the merged set does not have.
func TestAssetSetRange_KeepsLabelConfigKeys(t *testing.T) {
	day := 24 * time.Hour
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	aggregateBy := []string{string(AssetClusterProp), string(AssetTeamProp)}

	labelConfig := NewLabelConfig()
	labelConfig.TeamExternalLabel = "my_team"

	daySet := func(dayStart time.Time, cost float64) *AssetSet {
		dayEnd := dayStart.Add(day)
		window := NewWindow(&dayStart, &dayEnd)

		node := NewNode("node1", "cluster1", "i-node1", dayStart, dayEnd, window)
		node.SetLabels(AssetLabels{"my_team": "infra"})
		node.CPUCost = cost

		return NewAssetSet(dayStart, dayEnd, node)
	}

	asr := NewAssetSetRange(daySet(start, 10), daySet(start.Add(day), 20))
	if err := asr.AggregateBy(aggregateBy, &AssetAggregationOptions{LabelConfig: labelConfig}); err != nil {
		t.Fatalf("AggregateBy: %v", err)
	}

	for _, as := range asr.Assets {
		if _, ok := as.Assets["cluster1/infra"]; !ok {
			t.Fatalf("daily set is not keyed under cluster1/infra: %v", keysOfSet(as))
		}
	}

	t.Run("InsertRange", func(t *testing.T) {
		dst := NewAssetSetRange(daySet(start, 10))
		src := NewAssetSetRange(daySet(start, 20))
		for _, r := range []*AssetSetRange{dst, src} {
			if err := r.AggregateBy(aggregateBy, &AssetAggregationOptions{LabelConfig: labelConfig}); err != nil {
				t.Fatalf("AggregateBy: %v", err)
			}
		}

		if err := dst.InsertRange(src); err != nil {
			t.Fatalf("InsertRange: %v", err)
		}

		assertKeptKey(t, dst.Assets[0])
	})

	t.Run("AccumulateToAssetSet", func(t *testing.T) {
		as, err := asr.AccumulateToAssetSet()
		if err != nil {
			t.Fatalf("AccumulateToAssetSet: %v", err)
		}

		assertKeptKey(t, as)
	})

	t.Run("Accumulate all", func(t *testing.T) {
		accumulated, err := asr.Accumulate(AccumulateOptionAll)
		if err != nil {
			t.Fatalf("Accumulate: %v", err)
		}

		if len(accumulated.Assets) != 1 {
			t.Fatalf("expected one accumulated set, got %d", len(accumulated.Assets))
		}

		assertKeptKey(t, accumulated.Assets[0])
	})
}

func assertKeptKey(t *testing.T, as *AssetSet) {
	t.Helper()

	node, ok := as.Assets["cluster1/infra"]
	if !ok {
		t.Fatalf("result lost the label config key: %v", keysOfSet(as))
	}

	if got := node.TotalCost(); got != 30 {
		t.Fatalf("cost = %v, want 30", got)
	}
}

func keysOfSet(as *AssetSet) []string {
	keys := make([]string, 0, len(as.Assets))
	for k := range as.Assets {
		keys = append(keys, k)
	}

	return keys
}

func TestAssetSetRange_MixedAggregationKeys(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	cluster := string(AssetClusterProp)
	name := string(AssetNameProp)
	cases := []struct {
		name                string
		destination, source []string
		wantKey             string
	}{
		{"grouped and raw", []string{cluster}, nil, "cluster1"},
		{"different dimension order", []string{cluster, name}, []string{name, cluster}, "cluster1/node1"},
		{"explicit empty and default", []string{}, nil, ""},
	}

	newSet := func(t *testing.T, start time.Time, keys []string, cost float64) *AssetSet {
		t.Helper()
		end := start.Add(day)
		node := NewNode("node1", "cluster1", "i-node1", start, end, NewClosedWindow(start, end))
		node.CPUCost = cost
		as := NewAssetSet(start, end, node)
		if keys != nil {
			if err := as.AggregateBy(keys, nil); err != nil {
				t.Fatalf("AggregateBy: %v", err)
			}
		}
		return as
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			check := func(t *testing.T, got *AssetSet) {
				t.Helper()
				asset, ok := got.Assets[tc.wantKey]
				if !ok || len(got.Assets) != 1 {
					t.Fatalf("keys = %q, want only %q", keysOfSet(got), tc.wantKey)
				}
				if got := asset.TotalCost(); got != 30 {
					t.Fatalf("cost = %v, want 30", got)
				}
			}

			t.Run("InsertRange", func(t *testing.T) {
				dst := newSet(t, start, tc.destination, 10)
				src := newSet(t, start, tc.source, 20)
				if err := NewAssetSetRange(dst).InsertRange(NewAssetSetRange(src)); err != nil {
					t.Fatalf("InsertRange: %v", err)
				}
				check(t, dst)
			})

			t.Run("AccumulateToAssetSet", func(t *testing.T) {
				left := newSet(t, start, tc.destination, 10)
				right := newSet(t, start.Add(day), tc.source, 20)
				got, err := NewAssetSetRange(left, right).AccumulateToAssetSet()
				if err != nil {
					t.Fatalf("AccumulateToAssetSet: %v", err)
				}
				check(t, got)
			})

			t.Run("Accumulate all", func(t *testing.T) {
				left := newSet(t, start, tc.destination, 10)
				right := newSet(t, start.Add(day), tc.source, 20)
				got, err := NewAssetSetRange(left, right).Accumulate(AccumulateOptionAll)
				if err != nil {
					t.Fatalf("Accumulate: %v", err)
				}
				if len(got.Assets) != 1 {
					t.Fatalf("sets = %d, want 1", len(got.Assets))
				}
				check(t, got.Assets[0])
			})
		})
	}
}
