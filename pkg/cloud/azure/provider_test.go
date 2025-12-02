package azure

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/commerce/armcommerce"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/util/mathutil"
	"github.com/opencost/opencost/pkg/cloud/models"
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
		result := ParseAzureSubscriptionID(test.input)
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

	meterInfo := func(category, subcategory, name, region string, rate float32) armcommerce.MeterInfo {
		return armcommerce.MeterInfo{
			MeterCategory:    &category,
			MeterSubCategory: &subcategory,
			MeterName:        &name,
			MeterRegion:      &region,
			MeterRates:       map[string]*float32{"0": &rate},
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
				PV: &models.PV{Cost: "0.085616", Region: "useast"},
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
				Node: &models.Node{Cost: "10.000000", BaseCPUPrice: "0.30000", UsageType: "preemptible"},
			},
			"japanwest,Standard_E96as_v4,preemptible": {
				Node: &models.Node{Cost: "10.000000", BaseCPUPrice: "0.30000", UsageType: "preemptible"},
			},
		}
		require.Equal(t, expected, results)
	})
}

func TestAzure_findCostForDisk(t *testing.T) {
	var loc string = "location"
	var size int32 = 1

	az := &Azure{
		Pricing: map[string]*AzurePricing{
			"location,nil": nil,
			"location,nilpv": {
				PV: nil,
			},
			"location,ssd": {
				PV: &models.PV{
					Cost: "1",
				},
			},
		},
	}

	testCases := []struct {
		name   string
		disk   *armcompute.Disk
		exp    float64
		expErr error
	}{
		{
			"disk is nil",
			nil,
			0.0,
			fmt.Errorf("disk is empty"),
		},
		{
			"nil location",
			&armcompute.Disk{
				Location: nil,
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("ssd")),
				},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("failed to find pricing for key: ,ssd"),
		},
		{
			"nil disk properties",
			&armcompute.Disk{
				Location: &loc,
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("ssd")),
				},
				Properties: nil,
			},
			0.0,
			fmt.Errorf("disk properties are nil"),
		},
		{
			"nil disk size",
			&armcompute.Disk{
				Location: &loc,
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("ssd")),
				},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: nil,
				},
			},
			0.0,
			fmt.Errorf("disk size is nil"),
		},
		{
			"sku does not exist",
			&armcompute.Disk{
				Location: &loc,
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("doesnotexist")),
				},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("failed to find pricing for key: location,doesnotexist"),
		},
		{
			"pricing is nil",
			&armcompute.Disk{
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("nil")),
				},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("failed to find pricing for key: location,nil"),
		},
		{
			"pricing.PV is nil",
			&armcompute.Disk{
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("nilpv")),
				},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("pricing for key 'location,nilpv' has nil PV"),
		},
		{
			"valid (ssd)",
			&armcompute.Disk{
				Location: &loc,
				SKU: &armcompute.DiskSKU{
					Name: to.Ptr(armcompute.DiskStorageAccountTypes("ssd")),
				},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			730.0,
			nil,
		},
		{
			"nil sku",
			&armcompute.Disk{
				Location:   nil,
				SKU:        nil,
				Properties: nil,
			},
			0.0,
			fmt.Errorf("disk sku is nil"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			act, actErr := az.findCostForDisk(tc.disk)
			if actErr != nil && tc.expErr == nil {
				t.Fatalf("unexpected error: %s", actErr)
			}
			if tc.expErr != nil && actErr == nil {
				t.Fatalf("missing expected error: %s", tc.expErr)
			}
			if !mathutil.Approximately(tc.exp, act) {
				t.Fatalf("expected value %f; got %f", tc.exp, act)
			}
		})
	}
}
