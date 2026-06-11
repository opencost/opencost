package pricing

import (
	"encoding/json"
	"testing"

	"github.com/opencost/opencost/core/pkg/unit"
)

func TestPricingSetSort(t *testing.T) {
	// Create a pricing set with items in non-deterministic order
	ps1 := &PricingSet{
		Nodes: []*NodePricing{
			{
				Properties: NodePricingProperties{
					Provider:     AzureProvider,
					Region:       "eastus",
					InstanceType: "Standard_D2s_v3",
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.096}},
				},
			},
			{
				Properties: NodePricingProperties{
					Provider:     AWSProvider,
					Region:       "us-east-1",
					InstanceType: "t3.medium",
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.0416}},
				},
			},
			{
				Properties: NodePricingProperties{
					Provider:     AWSProvider,
					Region:       "us-east-1",
					InstanceType: "t3.large",
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.0832}},
				},
			},
		},
		Volumes: []*VolumePricing{
			{
				Properties: VolumePricingProperties{
					Provider:   AzureProvider,
					Region:     "eastus",
					VolumeType: VolumeTypePremiumLRS,
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.15}},
				},
			},
			{
				Properties: VolumePricingProperties{
					Provider:   AWSProvider,
					Region:     "us-east-1",
					VolumeType: VolumeTypeGP3,
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.08}},
				},
			},
		},
	}

	// Create a second pricing set with the same items in different order
	ps2 := &PricingSet{
		Nodes: []*NodePricing{
			{
				Properties: NodePricingProperties{
					Provider:     AWSProvider,
					Region:       "us-east-1",
					InstanceType: "t3.large",
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.0832}},
				},
			},
			{
				Properties: NodePricingProperties{
					Provider:     AWSProvider,
					Region:       "us-east-1",
					InstanceType: "t3.medium",
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.0416}},
				},
			},
			{
				Properties: NodePricingProperties{
					Provider:     AzureProvider,
					Region:       "eastus",
					InstanceType: "Standard_D2s_v3",
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.096}},
				},
			},
		},
		Volumes: []*VolumePricing{
			{
				Properties: VolumePricingProperties{
					Provider:   AWSProvider,
					Region:     "us-east-1",
					VolumeType: VolumeTypeGP3,
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.08}},
				},
			},
			{
				Properties: VolumePricingProperties{
					Provider:   AzureProvider,
					Region:     "eastus",
					VolumeType: VolumeTypePremiumLRS,
				},
				Prices: Prices{
					unit.USD: []Price{{Currency: unit.USD, Unit: unit.Hour, Price: 0.15}},
				},
			},
		},
	}

	// Sort both pricing sets
	ps1.Sort()
	ps2.Sort()

	// Serialize both to JSON
	json1, err := json.Marshal(ps1)
	if err != nil {
		t.Fatalf("Failed to marshal ps1: %v", err)
	}

	json2, err := json.Marshal(ps2)
	if err != nil {
		t.Fatalf("Failed to marshal ps2: %v", err)
	}

	// They should produce identical JSON
	if string(json1) != string(json2) {
		t.Errorf("Sorted pricing sets produced different JSON output.\nps1: %s\nps2: %s", string(json1), string(json2))
	}

	// Verify the sort order is correct (AWS before Azure alphabetically)
	if ps1.Nodes[0].Properties.Provider != AWSProvider {
		t.Errorf("Expected first node to be AWS, got %s", ps1.Nodes[0].Properties.Provider)
	}
	if ps1.Nodes[2].Properties.Provider != AzureProvider {
		t.Errorf("Expected third node to be Azure, got %s", ps1.Nodes[2].Properties.Provider)
	}

	// Verify instance types are sorted within same provider/region
	if ps1.Nodes[0].Properties.InstanceType != "t3.large" {
		t.Errorf("Expected first AWS node to be t3.large, got %s", ps1.Nodes[0].Properties.InstanceType)
	}
	if ps1.Nodes[1].Properties.InstanceType != "t3.medium" {
		t.Errorf("Expected second AWS node to be t3.medium, got %s", ps1.Nodes[1].Properties.InstanceType)
	}
}

func TestPricingSetSortNil(t *testing.T) {
	var ps *PricingSet
	// Should not panic
	ps.Sort()
}

func TestPricingSetSortEmpty(t *testing.T) {
	ps := &PricingSet{
		Nodes:   []*NodePricing{},
		Volumes: []*VolumePricing{},
	}
	// Should not panic
	ps.Sort()
}

// Made with Bob
