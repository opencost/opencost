package pricingmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/model/shared"
)

func TestNodeKeyRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		key  NodeKey
	}{
		{
			name: "full GPU key",
			key: NodeKey{
				Provider:   shared.ProviderGCP,
				Region:     "us-central1",
				NodeType:   "n2-standard-4",
				UsageType:  shared.UsageTypeOnDemand,
				Family:     "n2",
				DeviceType: "nvidia-tesla-t4",
				Type:       NodePricingTypeDevice,
			},
		},
		{
			name: "on-demand CPU key",
			key: NodeKey{
				Provider:  shared.ProviderAWS,
				Region:    "us-east-1",
				NodeType:  "m5.xlarge",
				UsageType: shared.UsageTypeOnDemand,
				Family:    "m5",
				Type:      NodePricingTypeCPUCore,
			},
		},
		{
			name: "spot total key",
			key: NodeKey{
				Provider:  shared.ProviderAzure,
				Region:    "eastus",
				NodeType:  "Standard_D4s_v3",
				UsageType: shared.UsageTypeSpot,
				Type:      NodePricingTypeTotal,
			},
		},
		{
			name: "RAM key",
			key: NodeKey{
				Provider:  shared.ProviderGCP,
				Region:    "europe-west1",
				Family:    "n1",
				UsageType: shared.UsageTypeOnDemand,
				Type:      NodePricingTypeRamGB,
			},
		},
		{
			name: "empty key",
			key:  NodeKey{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.key.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error: %v", err)
			}

			var got NodeKey
			if err := got.UnmarshalBinary(data); err != nil {
				t.Fatalf("UnmarshalBinary() error: %v", err)
			}

			if got != tc.key {
				t.Errorf("roundtrip mismatch:\n  got  %+v\n  want %+v", got, tc.key)
			}
		})
	}
}

func TestNodePricingRoundtrip(t *testing.T) {
	cases := []float64{0, 0.048, 0.192, 1.5, 2.0, 99.99}

	for _, rate := range cases {
		np := NodePricing{HourlyRate: rate}
		data, err := np.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary(%v) error: %v", rate, err)
		}

		var got NodePricing
		if err := got.UnmarshalBinary(data); err != nil {
			t.Fatalf("UnmarshalBinary(%v) error: %v", rate, err)
		}

		if got.HourlyRate != np.HourlyRate {
			t.Errorf("HourlyRate roundtrip: got %v, want %v", got.HourlyRate, np.HourlyRate)
		}
	}
}
