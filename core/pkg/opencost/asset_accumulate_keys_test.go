package opencost

import (
	"testing"
	"time"
)

// Accumulating a range must keep the keys the sets were aggregated under. Those keys came from
// the caller's label config, which the accumulated set does not have.
func TestAssetSetRange_AccumulateKeepsLabelConfigKeys(t *testing.T) {
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
		t.Fatalf("accumulated set lost the label config key: %v", keysOfSet(as))
	}

	if got := node.TotalCost(); got != 30 {
		t.Fatalf("accumulated cost = %v, want 30", got)
	}
}

func keysOfSet(as *AssetSet) []string {
	keys := make([]string, 0, len(as.Assets))
	for k := range as.Assets {
		keys = append(keys, k)
	}

	return keys
}
