package opencost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// TestCloudCostSetRange_LoadCloudCost_TakesOwnership checks that LoadCloudCost
// inserts the given CloudCost directly rather than deep-copying it. Ingestion
// integrations construct a fresh CloudCost per billing row and never touch it
// again, so cloning each one doubles the allocation cost of ingesting large
// billing exports.
func TestCloudCostSetRange_LoadCloudCost_TakesOwnership(t *testing.T) {
	end := RoundBack(time.Now().UTC(), timeutil.Day)
	start := end.Add(-timeutil.Day)
	ccsr, err := NewCloudCostSetRange(start, end, AccumulateOptionDay, "integration")
	if err != nil {
		t.Fatalf("unexpected error creating CloudCostSetRange: %s", err)
	}

	cc := &CloudCost{
		Properties: &CloudCostProperties{
			Provider:   AWSProvider,
			ProviderID: "i-00000001",
			AccountID:  "account1",
			Service:    "AmazonEC2",
			Category:   ComputeCategory,
			Labels: CloudCostLabels{
				"team": "payments",
			},
		},
		Window:   NewClosedWindow(start, end),
		ListCost: CostMetric{Cost: 100},
		NetCost:  CostMetric{Cost: 90},
	}

	ccsr.LoadCloudCost(cc)

	if len(ccsr.CloudCostSets) != 1 {
		t.Fatalf("expected 1 CloudCostSet, got %d", len(ccsr.CloudCostSets))
	}
	set := ccsr.CloudCostSets[0]
	if len(set.CloudCosts) != 1 {
		t.Fatalf("expected 1 CloudCost in set, got %d", len(set.CloudCosts))
	}
	for _, stored := range set.CloudCosts {
		if stored != cc {
			t.Errorf("expected LoadCloudCost to insert the given CloudCost without cloning it")
		}
	}
}
