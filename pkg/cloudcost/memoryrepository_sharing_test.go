package cloudcost

import (
	"context"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// newResourceCloudCost builds a fresh resource-level CloudCost with its own
// Properties instance, mirroring what the billing integrations construct per
// row. Each call allocates new (but equal) properties and labels.
func newResourceCloudCost(start, end time.Time, providerID string, labels map[string]string) *opencost.CloudCost {
	ccLabels := opencost.CloudCostLabels{}
	for k, v := range labels {
		ccLabels[k] = v
	}
	return &opencost.CloudCost{
		Window: opencost.NewClosedWindow(start, end),
		Properties: &opencost.CloudCostProperties{
			Provider:        opencost.AWSProvider,
			ProviderID:      providerID,
			AccountID:       "account1",
			InvoiceEntityID: "invoiceEntity1",
			RegionID:        "us-east-1",
			Service:         "AmazonEC2",
			Category:        opencost.ComputeCategory,
			Labels:          ccLabels,
		},
		ListCost: opencost.CostMetric{Cost: 100},
		NetCost:  opencost.CostMetric{Cost: 90},
	}
}

func newResourceCloudCostSet(start, end time.Time, integration string, costs ...*opencost.CloudCost) *opencost.CloudCostSet {
	ccs := opencost.NewCloudCostSet(start, end)
	ccs.Integration = integration
	for _, cc := range costs {
		ccs.Insert(cc)
	}
	return ccs
}

func soleCloudCost(t *testing.T, ccs *opencost.CloudCostSet) *opencost.CloudCost {
	t.Helper()
	if ccs == nil {
		t.Fatal("expected non-nil CloudCostSet")
	}
	if len(ccs.CloudCosts) != 1 {
		t.Fatalf("expected 1 CloudCost in set, got %d", len(ccs.CloudCosts))
	}
	for _, cc := range ccs.CloudCosts {
		return cc
	}
	return nil
}

// TestMemoryRepository_Get_ReturnsSharedSet checks that Get does not deep-copy
// the stored set. Deep-copying every entry of a resource-level billing day on
// every query dominated the transient memory of the cloud cost query path.
func TestMemoryRepository_Get_ReturnsSharedSet(t *testing.T) {
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(timeutil.Day)
	labels := map[string]string{"team": "payments"}

	repo := NewMemoryRepository()
	set := newResourceCloudCostSet(start, end, "key-1", newResourceCloudCost(start, end, "i-1", labels))
	if err := repo.Put(set); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}

	got, err := repo.Get(start, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}
	if got != set {
		t.Errorf("expected Get to return the stored set without deep-copying it")
	}
}

// TestMemoryRepository_Put_SharesPropertiesAcrossDays checks that when equal
// resource properties are stored for adjacent days, the stored entries share a
// single CloudCostProperties instance rather than each day retaining its own
// copy of the same strings and label map.
func TestMemoryRepository_Put_SharesPropertiesAcrossDays(t *testing.T) {
	day1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(timeutil.Day)
	labels := map[string]string{"team": "payments", "env": "prod"}

	repo := NewMemoryRepository()
	if err := repo.Put(newResourceCloudCostSet(day1, day2, "key-1", newResourceCloudCost(day1, day2, "i-1", labels))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}
	if err := repo.Put(newResourceCloudCostSet(day2, day2.Add(timeutil.Day), "key-1", newResourceCloudCost(day2, day2.Add(timeutil.Day), "i-1", labels))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}

	got1, err := repo.Get(day1, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}
	got2, err := repo.Get(day2, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}

	cc1 := soleCloudCost(t, got1)
	cc2 := soleCloudCost(t, got2)
	if cc1.Properties != cc2.Properties {
		t.Errorf("expected equal properties stored on adjacent days to share one CloudCostProperties instance")
	}
}

// TestMemoryRepository_Put_SharesPropertiesOnRebuild checks that re-ingesting a
// window that is already stored (which the ingestor does on every refresh and
// month-to-date run) reuses the previously stored properties instead of
// retaining a fresh copy of every string and label map.
func TestMemoryRepository_Put_SharesPropertiesOnRebuild(t *testing.T) {
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(timeutil.Day)
	labels := map[string]string{"team": "payments"}

	repo := NewMemoryRepository()
	if err := repo.Put(newResourceCloudCostSet(start, end, "key-1", newResourceCloudCost(start, end, "i-1", labels))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}
	before, err := repo.Get(start, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}
	beforeProps := soleCloudCost(t, before).Properties

	// Rebuild the same day from freshly parsed (equal but distinct) objects
	if err := repo.Put(newResourceCloudCostSet(start, end, "key-1", newResourceCloudCost(start, end, "i-1", labels))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}
	after, err := repo.Get(start, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}

	if soleCloudCost(t, after).Properties != beforeProps {
		t.Errorf("expected rebuild of an existing window to reuse the previously stored CloudCostProperties instance")
	}
}

// TestMemoryRepository_Put_SharesPropertiesBackward checks that property
// sharing also applies when windows are ingested newest-first, which is the
// order the ingestor uses for its initial backfill.
func TestMemoryRepository_Put_SharesPropertiesBackward(t *testing.T) {
	day1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(timeutil.Day)
	labels := map[string]string{"team": "payments"}

	repo := NewMemoryRepository()
	if err := repo.Put(newResourceCloudCostSet(day2, day2.Add(timeutil.Day), "key-1", newResourceCloudCost(day2, day2.Add(timeutil.Day), "i-1", labels))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}
	if err := repo.Put(newResourceCloudCostSet(day1, day2, "key-1", newResourceCloudCost(day1, day2, "i-1", labels))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}

	got1, err := repo.Get(day1, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}
	got2, err := repo.Get(day2, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}

	if soleCloudCost(t, got1).Properties != soleCloudCost(t, got2).Properties {
		t.Errorf("expected properties to be shared when days are ingested newest-first")
	}
}

// TestMemoryRepository_Put_KeepsChangedPropertiesSeparate checks that when a
// resource's properties change between ingests, the stored entry keeps its own
// new values rather than adopting the stale stored instance.
func TestMemoryRepository_Put_KeepsChangedPropertiesSeparate(t *testing.T) {
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(timeutil.Day)

	repo := NewMemoryRepository()
	if err := repo.Put(newResourceCloudCostSet(start, end, "key-1", newResourceCloudCost(start, end, "i-1", map[string]string{"env": "prod"}))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}
	if err := repo.Put(newResourceCloudCostSet(start, end, "key-1", newResourceCloudCost(start, end, "i-1", map[string]string{"env": "staging"}))); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}

	got, err := repo.Get(start, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}
	cc := soleCloudCost(t, got)
	if cc.Properties.Labels["env"] != "staging" {
		t.Errorf("expected rebuilt entry to keep its new label values, got %q", cc.Properties.Labels["env"])
	}
}

// TestRepositoryQuerier_Query_DoesNotMutateRepository checks that querying and
// aggregating does not corrupt the data stored in the repository. This is the
// safety contract that allows Get to return the stored set without cloning.
func TestRepositoryQuerier_Query_DoesNotMutateRepository(t *testing.T) {
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(timeutil.Day)

	repo := NewMemoryRepository()
	set := newResourceCloudCostSet(start, end, "key-1",
		newResourceCloudCost(start, end, "i-1", map[string]string{"team": "payments"}),
		newResourceCloudCost(start, end, "i-2", map[string]string{"team": "search"}),
	)
	if err := repo.Put(set); err != nil {
		t.Fatalf("unexpected error on Put: %s", err)
	}

	querier := NewRepositoryQuerier(repo)
	request := QueryRequest{
		Start:       start,
		End:         end,
		AggregateBy: []string{"service"},
		Accumulate:  opencost.AccumulateOptionNone,
	}

	queryTotal := func() float64 {
		result, err := querier.Query(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error on Query: %s", err)
		}
		total := 0.0
		for _, ccs := range result.CloudCostSets {
			for _, cc := range ccs.CloudCosts {
				total += cc.ListCost.Cost
			}
		}
		return total
	}

	firstTotal := queryTotal()
	if firstTotal != 200 {
		t.Fatalf("expected first query total of 200, got %f", firstTotal)
	}

	// The stored resource-level data must be untouched by the aggregation
	stored, err := repo.Get(start, "key-1")
	if err != nil {
		t.Fatalf("unexpected error on Get: %s", err)
	}
	if len(stored.CloudCosts) != 2 {
		t.Fatalf("expected 2 stored CloudCosts after query, got %d", len(stored.CloudCosts))
	}
	for _, cc := range stored.CloudCosts {
		if cc.Properties.ProviderID == "" {
			t.Errorf("expected stored ProviderID to be preserved after aggregation query")
		}
		if len(cc.Properties.Labels) != 1 {
			t.Errorf("expected stored labels to be preserved after aggregation query, got %v", cc.Properties.Labels)
		}
		if cc.ListCost.Cost != 100 {
			t.Errorf("expected stored cost to be unchanged after aggregation query, got %f", cc.ListCost.Cost)
		}
	}

	// A repeated query must produce identical results
	if secondTotal := queryTotal(); secondTotal != firstTotal {
		t.Errorf("expected repeated query to return %f, got %f", firstTotal, secondTotal)
	}
}
