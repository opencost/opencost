package opencost

import (
	"testing"
	"time"
)

func TestGetAssetKeyWithLabelConfig(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	window := NewWindow(&start, &end)

	node := NewNode("node1", "cluster1", "i-node1", start, end, window)
	node.SetLabels(AssetLabels{"my_team": "infra"})

	labelConfig := NewLabelConfig()
	labelConfig.TeamExternalLabel = "my_team"

	t.Run("matches the key an AssetSet stores the asset under", func(t *testing.T) {
		aggregateBy := []string{string(AssetClusterProp), string(AssetTeamProp)}

		set := NewAssetSet(start, end)
		set.AggregationKeys = aggregateBy
		if err := set.Insert(node, labelConfig); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		got, err := GetAssetKeyWithLabelConfig(node, aggregateBy, labelConfig)
		if err != nil {
			t.Fatalf("GetAssetKeyWithLabelConfig: %v", err)
		}

		if _, ok := set.Assets[got]; !ok {
			t.Fatalf("key %q is not the key the set used; set has %v", got, keysOf(set))
		}

		if want := "cluster1/infra"; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	t.Run("nil config behaves like GetAssetKey", func(t *testing.T) {
		aggregateBy := []string{string(AssetClusterProp), string(AssetNameProp)}

		withConfig, err := GetAssetKeyWithLabelConfig(node, aggregateBy, nil)
		if err != nil {
			t.Fatalf("GetAssetKeyWithLabelConfig: %v", err)
		}

		plain, err := GetAssetKey(node, aggregateBy)
		if err != nil {
			t.Fatalf("GetAssetKey: %v", err)
		}

		if withConfig != plain {
			t.Fatalf("nil config gave %q, GetAssetKey gave %q", withConfig, plain)
		}
	})
}

func keysOf(set *AssetSet) []string {
	keys := make([]string, 0, len(set.Assets))
	for key := range set.Assets {
		keys = append(keys, key)
	}

	return keys
}
