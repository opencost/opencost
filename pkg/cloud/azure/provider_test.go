package azure

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2021-11-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/preview/commerce/mgmt/2015-06-01-preview/commerce"
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

func Test_buildAzureRetailPricesURL(t *testing.T) {
	testCases := []struct {
		name         string
		region       string
		skuName      string
		currencyCode string
		expected     string
	}{
		{
			name:         "all parameters provided",
			region:       "eastus",
			skuName:      "Standard_D8ds_v5",
			currencyCode: "USD",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&currencyCode='USD'&$filter=armRegionName+eq+%27eastus%27+and+armSkuName+eq+%27Standard_D8ds_v5%27+and+serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "no currency code",
			region:       "westus",
			skuName:      "Standard_D4s_v3",
			currencyCode: "",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&$filter=armRegionName+eq+%27westus%27+and+armSkuName+eq+%27Standard_D4s_v3%27+and+serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "no region",
			region:       "",
			skuName:      "Standard_D8s_v3",
			currencyCode: "EUR",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&currencyCode='EUR'&$filter=armSkuName+eq+%27Standard_D8s_v3%27+and+serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "no sku name",
			region:       "northeurope",
			skuName:      "",
			currencyCode: "GBP",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&currencyCode='GBP'&$filter=armRegionName+eq+%27northeurope%27+and+serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "only currency code",
			region:       "",
			skuName:      "",
			currencyCode: "JPY",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&currencyCode='JPY'&$filter=serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "no parameters",
			region:       "",
			skuName:      "",
			currencyCode: "",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&$filter=serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "region with special characters",
			region:       "south-central-us",
			skuName:      "Standard_B2s",
			currencyCode: "USD",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&currencyCode='USD'&$filter=armRegionName+eq+%27south-central-us%27+and+armSkuName+eq+%27Standard_B2s%27+and+serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
		{
			name:         "sku name with underscores",
			region:       "eastus2",
			skuName:      "Standard_E16_v3",
			currencyCode: "CAD",
			expected:     "https://prices.azure.com/api/retail/prices?$skip=0&currencyCode='CAD'&$filter=armRegionName+eq+%27eastus2%27+and+armSkuName+eq+%27Standard_E16_v3%27+and+serviceFamily+eq+%27Compute%27+and+type+eq+%27Consumption%27+and+contains%28meterName%2C%27Low+Priority%27%29+eq+false",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildAzureRetailPricesURL(tc.region, tc.skuName, tc.currencyCode)
			require.Equal(t, tc.expected, result, "URL mismatch for test case: %s", tc.name)
		})
	}
}

func Test_extractAzureVMRetailAndSpotPrices(t *testing.T) {
	testCases := []struct {
		name             string
		jsonResponse     string
		expectedRetail   string
		expectedSpot     string
		expectedError    bool
		expectedErrorMsg string
	}{
		{
			name: "valid response with retail and spot prices",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"CustomerEntityId": "Default",
				"CustomerEntityType": "Retail",
				"Items": [
					{
						"currencyCode": "USD",
						"tierMinimumUnits": 0,
						"retailPrice": 0.384,
						"unitPrice": 0.384,
						"armRegionName": "eastus2",
						"location": "US East 2",
						"effectiveStartDate": "2023-01-01T00:00:00Z",
						"meterId": "abc-123",
						"meterName": "D8ds v5",
						"productId": "DZH318Z0BQ4B",
						"skuId": "DZH318Z0BQ4B/00G1",
						"productName": "Virtual Machines Ddsv5 Series",
						"skuName": "D8ds v5",
						"serviceName": "Virtual Machines",
						"serviceId": "DZH313Z7MMC8",
						"serviceFamily": "Compute",
						"unitOfMeasure": "1 Hour",
						"type": "Consumption",
						"isPrimaryMeterRegion": true,
						"armSkuName": "Standard_D8ds_v5"
					},
					{
						"currencyCode": "USD",
						"tierMinimumUnits": 0,
						"retailPrice": 0.0768,
						"unitPrice": 0.0768,
						"armRegionName": "eastus2",
						"location": "US East 2",
						"effectiveStartDate": "2023-01-01T00:00:00Z",
						"meterId": "def-456",
						"meterName": "D8ds v5 Spot",
						"productId": "DZH318Z0BQ4B",
						"skuId": "DZH318Z0BQ4B/00G2",
						"productName": "Virtual Machines Ddsv5 Series",
						"skuName": "D8ds v5 Spot",
						"serviceName": "Virtual Machines",
						"serviceId": "DZH313Z7MMC8",
						"serviceFamily": "Compute",
						"unitOfMeasure": "1 Hour",
						"type": "Consumption",
						"isPrimaryMeterRegion": true,
						"armSkuName": "Standard_D8ds_v5"
					}
				],
				"NextPageLink": "",
				"Count": 2
			}`,
			expectedRetail: "0.384000",
			expectedSpot:   "0.076800",
			expectedError:  false,
		},
		{
			name: "only retail price available",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"CustomerEntityId": "Default",
				"CustomerEntityType": "Retail",
				"Items": [
					{
						"currencyCode": "USD",
						"retailPrice": 0.192,
						"armRegionName": "westus",
						"productName": "Virtual Machines Dsv3 Series",
						"skuName": "D4s v3",
						"armSkuName": "Standard_D4s_v3"
					}
				],
				"Count": 1
			}`,
			expectedRetail: "0.192000",
			expectedSpot:   "",
			expectedError:  false,
		},
		{
			name: "only spot price available",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"CustomerEntityId": "Default",
				"CustomerEntityType": "Retail",
				"Items": [
					{
						"currencyCode": "USD",
						"retailPrice": 0.0384,
						"armRegionName": "eastus",
						"productName": "Virtual Machines Dsv3 Series",
						"skuName": "D4s v3 Spot",
						"armSkuName": "Standard_D4s_v3"
					}
				],
				"Count": 1
			}`,
			expectedRetail: "",
			expectedSpot:   "0.038400",
			expectedError:  false,
		},
		{
			name: "filters out Windows instances",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"CustomerEntityId": "Default",
				"CustomerEntityType": "Retail",
				"Items": [
					{
						"currencyCode": "USD",
						"retailPrice": 0.5,
						"armRegionName": "eastus",
						"productName": "Virtual Machines Dsv3 Series Windows",
						"skuName": "D4s v3",
						"armSkuName": "Standard_D4s_v3"
					},
					{
						"currencyCode": "USD",
						"retailPrice": 0.192,
						"armRegionName": "eastus",
						"productName": "Virtual Machines Dsv3 Series",
						"skuName": "D4s v3",
						"armSkuName": "Standard_D4s_v3"
					}
				],
				"Count": 2
			}`,
			expectedRetail: "0.192000",
			expectedSpot:   "",
			expectedError:  false,
		},
		{
			name: "filters out low priority instances",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"CustomerEntityId": "Default",
				"CustomerEntityType": "Retail",
				"Items": [
					{
						"currencyCode": "USD",
						"retailPrice": 0.05,
						"armRegionName": "eastus",
						"productName": "Virtual Machines Dsv3 Series",
						"skuName": "D4s v3 Low Priority",
						"armSkuName": "Standard_D4s_v3"
					},
					{
						"currencyCode": "USD",
						"retailPrice": 0.192,
						"armRegionName": "eastus",
						"productName": "Virtual Machines Dsv3 Series",
						"skuName": "D4s v3",
						"armSkuName": "Standard_D4s_v3"
					}
				],
				"Count": 2
			}`,
			expectedRetail: "0.192000",
			expectedSpot:   "",
			expectedError:  false,
		},
		{
			name: "empty items array",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"CustomerEntityId": "Default",
				"CustomerEntityType": "Retail",
				"Items": [],
				"Count": 0
			}`,
			expectedRetail: "",
			expectedSpot:   "",
			expectedError:  false,
		},
		{
			name: "invalid JSON",
			jsonResponse: `{
				"BillingCurrency": "USD",
				"Items": [
					{
						"retailPrice": "invalid"
					}
				]
			`,
			expectedRetail:   "",
			expectedSpot:     "",
			expectedError:    true,
			expectedErrorMsg: "error unmarshalling data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock http.Response with the JSON response as the body
			resp := &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(tc.jsonResponse)),
			}

			retailPrice, spotPrice, err := extractAzureVMRetailAndSpotPrices(resp)

			if tc.expectedError {
				require.Error(t, err)
				if tc.expectedErrorMsg != "" {
					require.Contains(t, err.Error(), tc.expectedErrorMsg)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedRetail, retailPrice, "Retail price mismatch")
				require.Equal(t, tc.expectedSpot, spotPrice, "Spot price mismatch")
			}
		})
	}
}
