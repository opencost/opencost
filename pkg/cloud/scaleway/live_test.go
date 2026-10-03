package scaleway

import (
	"os"
	"testing"
)

// TestFetchCatalogLive hits the real public catalog (unauthenticated). It is
// skipped unless LIVE=1 so the offline suite never depends on the network.
func TestFetchCatalogLive(t *testing.T) {
	if os.Getenv("LIVE") != "1" {
		t.Skip("set LIVE=1 to run against the live catalog")
	}
	store, err := fetchCatalog("EUR")
	if err != nil {
		t.Fatalf("fetchCatalog: %v", err)
	}

	for _, typ := range []string{"instance", "block_storage", "load_balancer", "kubernetes"} {
		if n := store.byType[typ]; n == 0 {
			t.Errorf("byType[%s] = 0, want > 0", typ)
		}
	}
	if len(store.zones) == 0 {
		t.Error("no zones covered by the live catalog")
	}
	if len(store.regions) == 0 {
		t.Error("no regions covered by the live catalog")
	}

	// Spot-check the 2026-09-24 reference values (prices may drift).
	if p, ok := store.instancePrice("fr-par-1", "BASIC3-X2C-4G"); ok {
		t.Logf("live BASIC3-X2C-4G @ fr-par-1 = %v €/h", p)
	} else {
		t.Error("BASIC3-X2C-4G not found in fr-par-1")
	}
	if p, ok := store.volumePrice("fr-par-1", "volume-bssd"); ok {
		t.Logf("live volume-bssd @ fr-par-1 = %v €/GB-h", p)
	} else {
		t.Error("volume-bssd not found in fr-par-1")
	}
	if p, ok := store.loadBalancerPrice("fr-par-1"); ok {
		t.Logf("live cheapest LB node @ fr-par-1 = %v €/h", p)
	} else {
		t.Error("no LB node price in fr-par-1")
	}
	if p, ok := store.controlPlanePrice("fr-par"); ok {
		t.Logf("live Kapsule mutualized @ fr-par = %v €/h (0 is valid)", p)
	} else {
		t.Error("Kapsule mutualized not found in fr-par")
	}

	t.Logf("summary: products=%d byType=%v zones=%d regions=%d dropped=%v",
		store.summary().Products, store.byType, len(store.zones), len(store.regions), store.dropped)
}
