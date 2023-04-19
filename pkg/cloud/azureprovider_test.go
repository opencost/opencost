package cloud

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/services/preview/commerce/mgmt/2015-06-01-preview/commerce"
	"github.com/opencost/opencost/pkg/cloud/types"
	"github.com/stretchr/testify/require"
)

func TestParseAzureSubscriptionID(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "azure:///subscriptions/0badafdf-1234-abcd-wxyz-123456789/...",
			expected: "0badafdf-1234-abcd-wxyz-123456789",
		},
		{
			input:    "azure:/subscriptions/0badafdf-1234-abcd-wxyz-123456789/...",
			expected: "",
		},
		{
			input:    "azure:///subscriptions//",
			expected: "",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, test := range cases {
		result := parseAzureSubscriptionID(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestConvertMeterToPricings(t *testing.T) {
	regions := map[string]string{
		"useast":             "US East",
		"japanwest":          "Japan West",
		"australiasoutheast": "Australia Southeast",
		"norwaywest":         "Norway West",
	}
	baseCPUPrice := "0.30000"

	meterInfo := func(category, subcategory, name, region string, rate float64) commerce.MeterInfo {
		return commerce.MeterInfo{
			MeterCategory:    &category,
			MeterSubCategory: &subcategory,
			MeterName:        &name,
			MeterRegion:      &region,
			MeterRates:       map[string]*float64{"0": &rate},
		}
	}

	t.Run("windows", func(t *testing.T) {
		info := meterInfo("Virtual Machines", "D2 Series Windows", "D2s v3", "AU Southeast", 0.3)
		results, err := convertMeterToPricings(info, regions, baseCPUPrice)
		require.NoError(t, err)
		require.Nil(t, results)
	})

	t.Run("storage", func(t *testing.T) {
		info := meterInfo("Storage", "Some SSD type", "P4 are good", "US East", 2000)
		results, err := convertMeterToPricings(info, regions, baseCPUPrice)
		require.NoError(t, err)

		expected := map[string]*AzurePricing{
			"useast,premium_ssd": {
				PV: &types.PV{Cost: "0.085616", Region: "useast"},
			},
		}
		require.Equal(t, expected, results)
	})

	t.Run("virtual machines", func(t *testing.T) {
		info := meterInfo("Virtual Machines", "Eav4/Easv4 Series", "E96a v4/E96as v4 Low Priority", "JA West", 10)
		results, err := convertMeterToPricings(info, regions, baseCPUPrice)
		require.NoError(t, err)

		expected := map[string]*AzurePricing{
			"japanwest,Standard_E96a_v4,preemptible": {
				Node: &types.Node{Cost: "10.000000", BaseCPUPrice: "0.30000", UsageType: "preemptible"},
			},
			"japanwest,Standard_E96as_v4,preemptible": {
				Node: &types.Node{Cost: "10.000000", BaseCPUPrice: "0.30000", UsageType: "preemptible"},
			},
		}
		require.Equal(t, expected, results)
	})
}
