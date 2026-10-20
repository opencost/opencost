package scaleway

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/carbon"
	v1 "k8s.io/api/core/v1"
)

// scalewayNode builds a one-hour Scaleway node asset for the given region and
// instance type, mirroring how the cost model labels nodes.
func scalewayNode(region, instanceType string) *opencost.Node {
	start := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	n := opencost.NewNode("node", "cluster", "scaleway://fr-par/instance/scw-1234", start, end, opencost.NewWindow(&start, &end))
	n.Properties.Provider = opencost.ScalewayProvider
	n.Labels = opencost.AssetLabels{
		v1.LabelTopologyRegion:     region,
		v1.LabelInstanceTypeStable: instanceType,
	}
	return n
}

func assertOneRow(t *testing.T, as *opencost.AssetSet) carbon.CarbonRow {
	t.Helper()
	rows, err := carbon.RelateCarbonAssets(as)
	if err != nil {
		t.Fatalf("RelateCarbonAssets: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	var row carbon.CarbonRow
	for _, r := range rows {
		row = r
	}
	return row
}

// TestRegisterCatalogCarbonCoefficients_EndToEnd verifies the full path from
// store carbon section (kg CO2e/hour, R11) through the runtime registry to a
// carbon estimate: a one-hour node emits kg/h ÷ 1000 tonnes (US3).
func TestRegisterCatalogCarbonCoefficients_EndToEnd(t *testing.T) {
	t.Cleanup(func() { carbon.RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, nil) })

	const kgPerHour = 0.0005840293 // 2026-09-24 reference value
	store := newCatalogStore()
	store.carbon[carbonKey{Region: "fr-par", OfferID: "BASIC3-X2C-4G"}] = kgPerHour
	registerCatalogCarbonCoefficients(store)

	n := scalewayNode("fr-par", "BASIC3-X2C-4G")
	row := assertOneRow(t, opencost.NewAssetSet(*n.Window.Start(), *n.Window.End(), n))

	// One hour of runtime at t/h coefficient -> tonnes.
	want := kgPerHour / 1000.0
	if math.Abs(row.Co2e-want) > 1e-18+1e-12*math.Max(math.Abs(row.Co2e), math.Abs(want)) {
		t.Fatalf("Co2e = %g, want %g (kg/h converted to t/h)", row.Co2e, want)
	}
}

// TestRegisterCatalogCarbonCoefficients_Noop verifies FR-013: when the fetched
// catalog carries no impact data, the previous coefficient set is preserved
// intact (data-model.md §5) — nothing is cleared or zeroed.
func TestRegisterCatalogCarbonCoefficients_Noop(t *testing.T) {
	t.Cleanup(func() { carbon.RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, nil) })

	const seeded = 1e-6
	carbon.RegisterRuntimeNodeCoefficients(opencost.ScalewayProvider, map[carbon.NodeCoefficient]float64{
		{Region: "fr-par", InstanceType: "SEED"}: seeded,
	})

	registerCatalogCarbonCoefficients(newCatalogStore())
	registerCatalogCarbonCoefficients(nil)

	n := scalewayNode("fr-par", "SEED")
	row := assertOneRow(t, opencost.NewAssetSet(*n.Window.Start(), *n.Window.End(), n))
	if row.Co2e != seeded {
		t.Fatalf("Co2e = %g, want %g (empty re-registration must keep the previous set)", row.Co2e, seeded)
	}
}
