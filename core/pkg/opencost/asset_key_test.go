package opencost

import (
	"testing"
	"time"
)

func TestGetAssetKeyWithLabelConfig(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := NewWindow(&start, &end)
	aggregateBy := []string{string(AssetClusterProp), string(AssetTeamProp)}

	node := NewNode("node1", "cluster1", "i-node1", start, end, window)
	node.SetLabels(AssetLabels{"my_team": "infra"})

	labelConfig := NewLabelConfig()
	labelConfig.TeamExternalLabel = "my_team"

	t.Run("matches the key an AssetSet stores the asset under", func(t *testing.T) {
		set := NewAssetSet(start, end, node)
		opts := &AssetAggregationOptions{LabelConfig: labelConfig}
		if err := set.AggregateBy(aggregateBy, opts); err != nil {
			t.Fatalf("AggregateBy: %v", err)
		}

		got, err := GetAssetKeyWithLabelConfig(node, aggregateBy, labelConfig)
		if err != nil {
			t.Fatalf("GetAssetKeyWithLabelConfig: %v", err)
		}

		if want := "cluster1/infra"; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if set.Assets[got] != node {
			t.Fatalf("set does not hold node under %q", got)
		}
	})

	t.Run("nil config means the default label names", func(t *testing.T) {
		got, err := GetAssetKeyWithLabelConfig(node, aggregateBy, nil)
		if err != nil {
			t.Fatalf("GetAssetKeyWithLabelConfig: %v", err)
		}

		// my_team is not the default team label, so the node is unallocated under nil.
		if want := "cluster1/" + UnallocatedSuffix; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}

		defaultNode := NewNode("node2", "cluster1", "i-node2", start, end, window)
		defaultNode.SetLabels(AssetLabels{NewLabelConfig().TeamExternalLabel: "platform"})

		got, err = GetAssetKeyWithLabelConfig(defaultNode, aggregateBy, nil)
		if err != nil {
			t.Fatalf("GetAssetKeyWithLabelConfig: %v", err)
		}

		if want := "cluster1/platform"; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}
