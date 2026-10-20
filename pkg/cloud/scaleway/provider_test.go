package scaleway

import (
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	instance "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
)

func nodeKey(zone, instanceType string) models.Key {
	return &scalewayKey{
		Labels: map[string]string{
			"topology.kubernetes.io/zone":      zone,
			"node.kubernetes.io/instance-type": instanceType,
		},
	}
}

func pvKey(zone, storageClass string) models.PVKey {
	return &scalewayPVKey{
		Zone:             zone,
		StorageClassName: storageClass,
	}
}

// newProviderWithCatalog builds a provider whose catalog store was populated
// by the reference fixture set, as if a fetch had succeeded.
func newProviderWithCatalog(t *testing.T) *Scaleway {
	t.Helper()
	return &Scaleway{
		Catalog:        buildTestStore(t),
		catalogFetched: true,
		Pricing: map[string]*ScalewayPricing{
			"fr-par-1": {PVCost: 0.00011, NodesInfos: map[string]*instance.ServerType{}},
		},
	}
}

func TestPricingSourceStatusStates(t *testing.T) {
	t.Run("never fetched", func(t *testing.T) {
		c := &Scaleway{}
		status := c.PricingSourceStatus()
		src := status[ProductCatalogPricing]
		if src == nil {
			t.Fatal("catalog source missing from status")
		}
		if src.Enabled != true || src.Available != false || src.Error != "" {
			t.Fatalf("never fetched = %+v, want Enabled=true Available=false Error=\"\"", src)
		}
		if legacy := status[InstanceAPIPricing]; legacy == nil || legacy.Available != true {
			t.Fatalf("legacy source missing or unavailable: %+v", legacy)
		}
	})

	t.Run("fetch succeeded", func(t *testing.T) {
		c := &Scaleway{
			Catalog:        newCatalogStore(),
			catalogFetched: true,
		}
		src := c.PricingSourceStatus()[ProductCatalogPricing]
		if src.Enabled != true || src.Available != true || src.Error != "" {
			t.Fatalf("fetch succeeded = %+v, want Enabled=true Available=true Error=\"\"", src)
		}
	})

	t.Run("fetch failed with last-good data (SC-003: observable without logs)", func(t *testing.T) {
		c := &Scaleway{
			Catalog:        newCatalogStore(),
			catalogFetched: false,
			catalogError:   "catalog fetch failed: connection refused",
		}
		src := c.PricingSourceStatus()[ProductCatalogPricing]
		if src.Enabled != true || src.Available != false {
			t.Fatalf("degraded = %+v, want Enabled=true Available=false", src)
		}
		if src.Error == "" {
			t.Fatal("degraded state must carry a non-empty error")
		}
	})

	t.Run("fetch failed, no data ever fetched", func(t *testing.T) {
		c := &Scaleway{
			catalogFetched: false,
			catalogError:   "catalog fetch failed: connection refused",
		}
		src := c.PricingSourceStatus()[ProductCatalogPricing]
		if src.Available != false || src.Error == "" {
			t.Fatalf("no data = %+v, want Available=false + non-empty error", src)
		}
	})
}

func TestPricingSourceSummaryCatalogSection(t *testing.T) {
	c := newProviderWithCatalog(t)
	summary, ok := c.PricingSourceSummary().(map[string]interface{})
	if !ok {
		t.Fatalf("summary type = %T, want map[string]interface{}", c.PricingSourceSummary())
	}

	// Existing per-zone pricing is preserved (additive contract).
	if _, ok := summary["fr-par-1"]; !ok {
		t.Fatal("legacy zone pricing missing from summary")
	}

	cat, ok := summary["catalog"].(*catalogSummary)
	if !ok {
		t.Fatalf("catalog section type = %T, want *catalogSummary", summary["catalog"])
	}
	if cat.Source != ProductCatalogPricing {
		t.Fatalf("catalog.source = %q", cat.Source)
	}
	// The fixture set holds 11 GA products (2 instance, 4 block_storage,
	// 3 load_balancer, 2 kubernetes).
	if cat.Products != 11 {
		t.Fatalf("catalog.products = %d, want 11", cat.Products)
	}
	for _, typ := range []string{"instance", "block_storage", "load_balancer", "kubernetes"} {
		if _, ok := cat.ByType[typ]; !ok {
			t.Errorf("catalog.byType missing %q", typ)
		}
	}
	if !contains(cat.Zones, "fr-par-1") {
		t.Errorf("catalog.zones = %v, want to contain fr-par-1", cat.Zones)
	}
	if !contains(cat.Regions, "fr-par") {
		t.Errorf("catalog.regions = %v, want to contain fr-par", cat.Regions)
	}
	if cat.LastFetched == "" {
		t.Error("catalog.lastFetched is empty")
	}

	// No catalog → no catalog section.
	plain := &Scaleway{}
	if s, ok := plain.PricingSourceSummary().(map[string]interface{}); ok {
		if _, has := s["catalog"]; has {
			t.Error("catalog section must be absent without a store")
		}
	}
}

func TestNodePricingCatalogFirst(t *testing.T) {
	c := newProviderWithCatalog(t)

	// Catalog hit: the catalog price is used (FR-002).
	node, meta, err := c.NodePricing(nodeKey("fr-par-1", "BASIC3-X2C-4G"))
	if err != nil || node == nil {
		t.Fatalf("NodePricing = (%+v, %v, %v)", node, meta, err)
	}
	if !strings.HasPrefix(node.Cost, "0.039449") {
		t.Errorf("node.Cost = %q, want catalog price 0.039449", node.Cost)
	}
	if node.PricingType != models.DefaultPrices || node.InstanceType != "BASIC3-X2C-4G" || node.Region != "fr-par-1" {
		t.Errorf("node = %+v", node)
	}

	// A type absent from the catalog (and from the legacy map) errors,
	// preserving current behavior.
	if _, _, err := c.NodePricing(nodeKey("fr-par-1", "MISSING-TYPE")); err == nil {
		t.Fatal("expected error for unknown type")
	}

	// Unknown zone errors.
	if _, _, err := c.NodePricing(nodeKey("xx-unknown-1", "BASIC3-X2C-4G")); err == nil {
		t.Fatal("expected error for unknown zone")
	}
}

func TestPVPricingCatalogFirst(t *testing.T) {
	c := newProviderWithCatalog(t)

	// Catalog hit (per GB-hour).
	pv, err := c.PVPricing(pvKey("fr-par-1", "volume-bssd"))
	if err != nil || pv == nil {
		t.Fatalf("PVPricing = (%+v, %v)", pv, err)
	}
	if !strings.HasPrefix(pv.Cost, "0.000129999") {
		t.Errorf("pv.Cost = %q, want catalog price 0.000129999", pv.Cost)
	}
	if pv.Class != "volume-bssd" {
		t.Errorf("pv.Class = %q", pv.Class)
	}

	// Catalog miss (unmapped class) → legacy per-zone PVCost (edge case 3).
	pv, err = c.PVPricing(pvKey("fr-par-1", "volume-lssd"))
	if err != nil || pv == nil {
		t.Fatalf("PVPricing fallback = (%+v, %v)", pv, err)
	}
	if !strings.HasPrefix(pv.Cost, "0.00011") {
		t.Errorf("pv.Cost = %q, want legacy PVCost 0.00011", pv.Cost)
	}

	// Zone not in the legacy map and not in the catalog → empty PV (current behavior).
	pv, err = c.PVPricing(pvKey("xx-unknown-1", "volume-bssd"))
	if err != nil || pv == nil {
		t.Fatalf("PVPricing unknown zone = (%+v, %v)", pv, err)
	}
	if pv.Cost != "" {
		t.Errorf("pv.Cost = %q, want empty", pv.Cost)
	}
}

func TestLoadBalancerPricingFallback(t *testing.T) {
	// No clientset → no zone → static fallback (FR-007).
	c := newProviderWithCatalog(t)
	lb, err := c.LoadBalancerPricing()
	if err != nil || lb == nil {
		t.Fatalf("LoadBalancerPricing = (%+v, %v)", lb, err)
	}
	if !closeEnough(lb.Cost, 0.014) {
		t.Errorf("lb.Cost = %v, want static fallback 0.014", lb.Cost)
	}
}

func TestClusterManagementPricingNotKapsule(t *testing.T) {
	// No nodes → platform unknown → current zero-cost behavior.
	c := newProviderWithCatalog(t)
	provisioner, price, err := c.ClusterManagementPricing()
	if err != nil {
		t.Fatalf("ClusterManagementPricing = (%q, %v, %v)", provisioner, price, err)
	}
	if provisioner != "" || price != 0.0 {
		t.Fatalf("ClusterManagementPricing = (%q, %v), want (\"\", 0.0)", provisioner, price)
	}
}
