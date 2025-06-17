package azure

import (
	"fmt"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2021-11-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/preview/commerce/mgmt/2015-06-01-preview/commerce"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/util/mathutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
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
		disk   *compute.Disk
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
			&compute.Disk{
				Location: nil,
				Sku: &compute.DiskSku{
					Name: "ssd",
				},
				DiskProperties: &compute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("failed to find pricing for key: ,ssd"),
		},
		{
			"nil disk properties",
			&compute.Disk{
				Location: &loc,
				Sku: &compute.DiskSku{
					Name: "ssd",
				},
				DiskProperties: nil,
			},
			0.0,
			fmt.Errorf("disk properties are nil"),
		},
		{
			"nil disk size",
			&compute.Disk{
				Location: &loc,
				Sku: &compute.DiskSku{
					Name: "ssd",
				},
				DiskProperties: &compute.DiskProperties{
					DiskSizeGB: nil,
				},
			},
			0.0,
			fmt.Errorf("disk size is nil"),
		},
		{
			"sku does not exist",
			&compute.Disk{
				Location: &loc,
				Sku: &compute.DiskSku{
					Name: "doesnotexist",
				},
				DiskProperties: &compute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("failed to find pricing for key: location,doesnotexist"),
		},
		{
			"pricing is nil",
			&compute.Disk{
				Sku: &compute.DiskSku{
					Name: "nil",
				},
				DiskProperties: &compute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("failed to find pricing for key: location,nil"),
		},
		{
			"pricing.PV is nil",
			&compute.Disk{
				Sku: &compute.DiskSku{
					Name: "nilpv",
				},
				DiskProperties: &compute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			0.0,
			fmt.Errorf("pricing for key 'location,nilpv' has nil PV"),
		},
		{
			"valid (ssd)",
			&compute.Disk{
				Location: &loc,
				Sku: &compute.DiskSku{
					Name: "ssd",
				},
				DiskProperties: &compute.DiskProperties{
					DiskSizeGB: &size,
				},
			},
			730.0,
			nil,
		},
		{
			"nil sku",
			&compute.Disk{
				Location:       nil,
				Sku:            nil,
				DiskProperties: nil,
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

func TestGetAzureRateCardAuth(t *testing.T) {
	// Save original env vars
	origSubID := os.Getenv(env.AzureSubscriptionIDEnvVar)
	origClientID := os.Getenv(env.AzureClientIDEnvVar)
	origClientSecret := os.Getenv(env.AzureClientSecretEnvVar)
	origTenantID := os.Getenv(env.AzureTenantIDEnvVar)

	// Restore original env vars after test
	defer func() {
		os.Setenv(env.AzureSubscriptionIDEnvVar, origSubID)
		os.Setenv(env.AzureClientIDEnvVar, origClientID)
		os.Setenv(env.AzureClientSecretEnvVar, origClientSecret)
		os.Setenv(env.AzureTenantIDEnvVar, origTenantID)
	}()

	az := &Azure{}
	cp := &models.CustomPricing{}

	t.Run("environment variables", func(t *testing.T) {
		// Set environment variables
		os.Setenv(env.AzureSubscriptionIDEnvVar, "test-sub-id")
		os.Setenv(env.AzureClientIDEnvVar, "test-client-id")
		os.Setenv(env.AzureClientSecretEnvVar, "test-client-secret")
		os.Setenv(env.AzureTenantIDEnvVar, "test-tenant-id")

		subID, clientID, clientSecret, tenantID := az.getAzureRateCardAuth(false, cp)

		require.Equal(t, "test-sub-id", subID)
		require.Equal(t, "test-client-id", clientID)
		require.Equal(t, "test-client-secret", clientSecret)
		require.Equal(t, "test-tenant-id", tenantID)
	})

	t.Run("partial environment variables", func(t *testing.T) {
		// Set only some environment variables
		os.Setenv(env.AzureSubscriptionIDEnvVar, "test-sub-id")
		os.Setenv(env.AzureClientIDEnvVar, "test-client-id")
		os.Unsetenv(env.AzureClientSecretEnvVar)
		os.Unsetenv(env.AzureTenantIDEnvVar)

		// Set custom pricing values
		cp.AzureSubscriptionID = "config-sub-id"
		cp.AzureClientID = "config-client-id"
		cp.AzureClientSecret = "config-client-secret"
		cp.AzureTenantID = "config-tenant-id"

		subID, clientID, clientSecret, tenantID := az.getAzureRateCardAuth(false, cp)

		// Should fall back to config values since not all env vars are set
		require.Equal(t, "config-sub-id", subID)
		require.Equal(t, "config-client-id", clientID)
		require.Equal(t, "config-client-secret", clientSecret)
		require.Equal(t, "config-tenant-id", tenantID)
	})

	t.Run("no environment variables", func(t *testing.T) {
		// Unset all environment variables
		os.Unsetenv(env.AzureSubscriptionIDEnvVar)
		os.Unsetenv(env.AzureClientIDEnvVar)
		os.Unsetenv(env.AzureClientSecretEnvVar)
		os.Unsetenv(env.AzureTenantIDEnvVar)

		// Set custom pricing values
		cp.AzureSubscriptionID = "config-sub-id"
		cp.AzureClientID = "config-client-id"
		cp.AzureClientSecret = "config-client-secret"
		cp.AzureTenantID = "config-tenant-id"

		subID, clientID, clientSecret, tenantID := az.getAzureRateCardAuth(false, cp)

		require.Equal(t, "config-sub-id", subID)
		require.Equal(t, "config-client-id", clientID)
		require.Equal(t, "config-client-secret", clientSecret)
		require.Equal(t, "config-tenant-id", tenantID)
	})
}
