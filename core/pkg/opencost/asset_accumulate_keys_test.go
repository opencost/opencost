package opencost

import (
	"testing"
	"time"
)

// Merging sets grouped by the same dimensions keeps the keys derived from the
// caller's label config.
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
	team := string(AssetTeamProp)
	labelConfig := NewLabelConfig()
	labelConfig.TeamExternalLabel = "my_team"
	opts := &AssetAggregationOptions{LabelConfig: labelConfig}
	cases := []struct {
		name                string
		destination, source []string
		wantKey             string
	}{
		{"grouped and raw", []string{cluster}, nil, "cluster1"},
		{"different dimension order", []string{cluster, name}, []string{name, cluster}, "cluster1/node1"},
		{"explicit empty and default", []string{}, nil, ""},
		{"explicit empty and grouped", []string{}, []string{cluster}, ""},
		{"custom label and raw", []string{cluster, team}, nil, "cluster1/infra"},
		{"different custom label order", []string{cluster, team}, []string{team, cluster}, "cluster1/infra"},
	}

	newSet := func(t *testing.T, start time.Time, keys []string, cost float64) *AssetSet {
		t.Helper()
		end := start.Add(day)
		node := NewNode("node1", "cluster1", "i-node1", start, end, NewClosedWindow(start, end))
		node.SetLabels(AssetLabels{"my_team": "infra"})
		node.CPUCost = cost
		as := NewAssetSet(start, end, node)
		if keys != nil {
			if err := as.AggregateBy(keys, opts); err != nil {
				t.Fatalf("AggregateBy: %v", err)
			}
		}
		return as
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("InsertRange", func(t *testing.T) {
				dst := newSet(t, start, tc.destination, 10)
				src := newSet(t, start, tc.source, 20)
				if err := NewAssetSetRange(dst).InsertRange(NewAssetSetRange(src)); err != nil {
					t.Fatalf("InsertRange: %v", err)
				}
				assertAssetSetKeyCost(t, dst, tc.wantKey, 30)
			})

			t.Run("AccumulateToAssetSet", func(t *testing.T) {
				left := newSet(t, start, tc.destination, 10)
				right := newSet(t, start.Add(day), tc.source, 20)
				got, err := NewAssetSetRange(left, right).AccumulateToAssetSet()
				if err != nil {
					t.Fatalf("AccumulateToAssetSet: %v", err)
				}
				assertAssetSetKeyCost(t, got, tc.wantKey, 30)
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
				assertAssetSetKeyCost(t, got.Assets[0], tc.wantKey, 30)
			})
		})
	}

	t.Run("populated default grouping", func(t *testing.T) {
		left := newSet(t, start, nil, 10)
		wantKey := keysOfSet(left)[0]
		right := newSet(t, start.Add(day), []string{cluster}, 20)
		got, err := NewAssetSetRange(left, right).AccumulateToAssetSet()
		if err != nil {
			t.Fatalf("AccumulateToAssetSet: %v", err)
		}
		assertAssetSetKeyCost(t, got, wantKey, 30)
	})

	t.Run("inherit after an unconfigured empty set", func(t *testing.T) {
		left := NewAssetSet(start, start.Add(day))
		grouped := newSet(t, start.Add(day), []string{cluster, team}, 10)
		raw := newSet(t, start.Add(2*day), nil, 20)
		got, err := NewAssetSetRange(left, grouped, raw).AccumulateToAssetSet()
		if err != nil {
			t.Fatalf("AccumulateToAssetSet: %v", err)
		}
		assertAssetSetKeyCost(t, got, "cluster1/infra", 30)
	})

	t.Run("keep grouping across consecutive empty sets", func(t *testing.T) {
		left := NewAssetSet(start, start.Add(day))
		if err := left.AggregateBy([]string{}, nil); err != nil {
			t.Fatalf("AggregateBy: %v", err)
		}
		empty := NewAssetSet(start.Add(day), start.Add(2*day))
		right := newSet(t, start.Add(2*day), []string{cluster}, 20)
		got, err := NewAssetSetRange(left, empty, right).AccumulateToAssetSet()
		if err != nil {
			t.Fatalf("AccumulateToAssetSet: %v", err)
		}
		assertAssetSetKeyCost(t, got, "", 20)
	})
}

func assertAssetSetKeyCost(t *testing.T, as *AssetSet, key string, wantCost float64) {
	t.Helper()
	asset, ok := as.Assets[key]
	if !ok || len(as.Assets) != 1 {
		t.Fatalf("keys = %q, want only %q", keysOfSet(as), key)
	}
	if got := asset.TotalCost(); got != wantCost {
		t.Fatalf("cost = %v, want %v", got, wantCost)
	}
}

func TestAssetSetRange_RekeysTeamAliases(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := NewClosedWindow(start, end)
	labelConfig := NewLabelConfig()
	labelConfig.TeamExternalLabel = "my_team,other_team"
	opts := &AssetAggregationOptions{LabelConfig: labelConfig}

	mkNode := func(name, label string, cost float64) *Node {
		node := NewNode(name, "clusters/cluster1", name, start, end, window)
		node.SetLabels(AssetLabels{label: "infra"})
		node.CPUCost = cost
		return node
	}
	aggregate := func(t *testing.T, as *AssetSet, keys ...string) {
		t.Helper()
		if err := as.AggregateBy(keys, opts); err != nil {
			t.Fatalf("AggregateBy: %v", err)
		}
	}
	roundTrip := func(t *testing.T, as *AssetSet) *AssetSet {
		t.Helper()
		data, err := as.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		decoded := &AssetSet{}
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		return decoded
	}

	t.Run("use destination aliases after a binary round trip", func(t *testing.T) {
		dst := NewAssetSet(start, end, mkNode("node1", "my_team", 10))
		aggregate(t, dst, "cluster", "team")
		dst = roundTrip(t, dst)
		src := NewAssetSet(start, end, mkNode("node2", "other_team", 20))

		got, err := NewAssetSetRange(dst, src).AccumulateToAssetSet()
		if err != nil {
			t.Fatalf("AccumulateToAssetSet: %v", err)
		}
		assertAssetSetKeyCost(t, got, "clusters/cluster1/infra", 30)
	})

	for _, binaryRoundTrip := range []bool{false, true} {
		name := "use source group after merging different aliases"
		if binaryRoundTrip {
			name += " and a binary round trip"
		}
		t.Run(name, func(t *testing.T) {
			dst := NewAssetSet(start, end, mkNode("node1", "my_team", 10))
			aggregate(t, dst, "cluster", "team")
			src := NewAssetSet(start, end,
				mkNode("node2", "my_team", 20),
				mkNode("node3", "other_team", 30),
			)
			aggregate(t, src, "team", "cluster")
			assertAssetSetKeyCost(t, src, "infra/clusters/cluster1", 50)
			if len(src.Assets["infra/clusters/cluster1"].GetLabels()) != 0 {
				t.Fatal("merged node should have no shared labels")
			}
			if binaryRoundTrip {
				src = roundTrip(t, src)
			}

			if err := NewAssetSetRange(dst).InsertRange(NewAssetSetRange(src)); err != nil {
				t.Fatalf("InsertRange: %v", err)
			}
			assertAssetSetKeyCost(t, dst, "clusters/cluster1/infra", 60)
		})
	}

	t.Run("use destination alias precedence", func(t *testing.T) {
		dst := NewAssetSet(start, end, mkNode("node1", "my_team", 10))
		dstConfig := NewLabelConfig()
		dstConfig.TeamExternalLabel = "other_team,my_team"
		if err := dst.AggregateBy([]string{"cluster", "team"}, &AssetAggregationOptions{LabelConfig: dstConfig}); err != nil {
			t.Fatalf("AggregateBy: %v", err)
		}
		node := mkNode("node2", "my_team", 20)
		node.SetLabels(AssetLabels{"my_team": "platform", "other_team": "infra"})
		src := NewAssetSet(start, end, node)
		aggregate(t, src, "team", "cluster")
		assertAssetSetKeyCost(t, src, "platform/clusters/cluster1", 20)

		if err := NewAssetSetRange(dst).InsertRange(NewAssetSetRange(src)); err != nil {
			t.Fatalf("InsertRange: %v", err)
		}
		assertAssetSetKeyCost(t, dst, "clusters/cluster1/infra", 30)
	})
}
