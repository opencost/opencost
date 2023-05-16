package gcp

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
)

func TestParseGCPInstanceTypeLabel(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "n1-standard-2",
			expected: "n1standard",
		},
		{
			input:    "e2-medium",
			expected: "e2medium",
		},
		{
			input:    "k3s",
			expected: "unknown",
		},
		{
			input:    "custom-n1-standard-2",
			expected: "custom",
		},
		{
			input:    "n2d-highmem-8",
			expected: "n2dstandard",
		},
	}

	for _, test := range cases {
		result := parseGCPInstanceTypeLabel(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestParseGCPProjectID(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "gce://guestbook-12345/...",
			expected: "guestbook-12345",
		},
		{
			input:    "gce:/guestbook-12345/...",
			expected: "",
		},
		{
			input:    "asdfa",
			expected: "",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, test := range cases {
		result := ParseGCPProjectID(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestGetUsageType(t *testing.T) {
	cases := []struct {
		input    map[string]string
		expected string
	}{
		{
			input: map[string]string{
				GKEPreemptibleLabel: "true",
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				GKESpotLabel: "true",
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				models.KarpenterCapacityTypeLabel: models.KarpenterCapacitySpotTypeValue,
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				"someotherlabel": "true",
			},
			expected: "ondemand",
		},
		{
			input:    map[string]string{},
			expected: "ondemand",
		},
	}

	for _, test := range cases {
		result := getUsageType(test.input)
		if result != test.expected {
			t.Errorf("Input: %v, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

// tests basic parsing of GCP pricing API responses
// Load a reader object on a portion of a GCP api response
// Confirm that the resting *GCP object contains the correctly parsed pricing info
func TestParsePage(t *testing.T) {

	gcpSkuString := `
	{
		"skus": [
			{
				"name": "services/6F81-5844-456A/skus/039F-D0DA-4055",
				"skuId": "039F-D0DA-4055",
				"description": "Nvidia Tesla A100 GPU running in Americas",
				"category": {
				  "serviceDisplayName": "Compute Engine",
				  "resourceFamily": "Compute",
				  "resourceGroup": "GPU",
				  "usageType": "OnDemand"
				},
				"serviceRegions": [
				  "us-central1",
				  "us-east1",
				  "us-west1"
				],
				"pricingInfo": [
				  {
					"summary": "",
					"pricingExpression": {
					  "usageUnit": "h",
					  "displayQuantity": 1,
					  "tieredRates": [
						{
						  "startUsageAmount": 0,
						  "unitPrice": {
							"currencyCode": "USD",
							"units": "2",
							"nanos": 933908000
						  }
						}
					  ],
					  "usageUnitDescription": "hour",
					  "baseUnit": "s",
					  "baseUnitDescription": "second",
					  "baseUnitConversionFactor": 3600
					},
					"currencyConversionRate": 1,
					"effectiveTime": "2023-03-24T10:52:50.681Z"
				  }
				],
				"serviceProviderName": "Google",
				"geoTaxonomy": {
				  "type": "MULTI_REGIONAL",
				  "regions": [
					"us-central1",
					"us-east1",
					"us-west1"
				  ]
				}
			},
			{
				"name": "services/6F81-5844-456A/skus/2390-DCAF-DA38",
				"skuId": "2390-DCAF-DA38",
				"description": "A2 Instance Ram running in Americas",
				"category": {
				  "serviceDisplayName": "Compute Engine",
				  "resourceFamily": "Compute",
				  "resourceGroup": "RAM",
				  "usageType": "OnDemand"
				},
				"serviceRegions": [
				  "us-central1",
				  "us-east1",
				  "us-west1"
				],
				"pricingInfo": [
				  {
					"summary": "",
					"pricingExpression": {
					  "usageUnit": "GiBy.h",
					  "displayQuantity": 1,
					  "tieredRates": [
						{
						  "startUsageAmount": 0,
						  "unitPrice": {
							"currencyCode": "USD",
							"units": "0",
							"nanos": 4237000
						  }
						}
					  ],
					  "usageUnitDescription": "gibibyte hour",
					  "baseUnit": "By.s",
					  "baseUnitDescription": "byte second",
					  "baseUnitConversionFactor": 3865470566400
					},
					"currencyConversionRate": 1,
					"effectiveTime": "2023-03-24T10:52:50.681Z"
				  }
				],
				"serviceProviderName": "Google",
				"geoTaxonomy": {
				  "type": "MULTI_REGIONAL",
				  "regions": [
					"us-central1",
					"us-east1",
					"us-west1"
				  ]
				}
			},
			{
				"name": "services/6F81-5844-456A/skus/2922-40C5-B19F",
				"skuId": "2922-40C5-B19F",
				"description": "A2 Instance Core running in Americas",
				"category": {
				  "serviceDisplayName": "Compute Engine",
				  "resourceFamily": "Compute",
				  "resourceGroup": "CPU",
				  "usageType": "OnDemand"
				},
				"serviceRegions": [
				  "us-central1",
				  "us-east1",
				  "us-west1"
				],
				"pricingInfo": [
				  {
					"summary": "",
					"pricingExpression": {
					  "usageUnit": "h",
					  "displayQuantity": 1,
					  "tieredRates": [
						{
						  "startUsageAmount": 0,
						  "unitPrice": {
							"currencyCode": "USD",
							"units": "0",
							"nanos": 31611000
						  }
						}
					  ],
					  "usageUnitDescription": "hour",
					  "baseUnit": "s",
					  "baseUnitDescription": "second",
					  "baseUnitConversionFactor": 3600
					},
					"currencyConversionRate": 1,
					"effectiveTime": "2023-03-24T10:52:50.681Z"
				  }
				],
				"serviceProviderName": "Google",
				"geoTaxonomy": {
				  "type": "MULTI_REGIONAL",
				  "regions": [
					"us-central1",
					"us-east1",
					"us-west1"
				  ]
				}
			}
		],
			"nextPageToken": "APKCS1HVa0YpwgyTFbqbJ1eGwzKZmsPwLqzMZPTSNia5ck1Hc54Tx_Kz3oBxwSnRIdGVxXoSPdf-XlDpyNBf4QuxKcIEgtrQ1LDLWAgZowI0ns7HjrGta2s="
		}
	`
	reader := ioutil.NopCloser(bytes.NewBufferString(gcpSkuString))

	testGcp := &GCP{}

	inputKeys := map[string]models.Key{
		"us-central1,a2highgpu,ondemand,gpu": &gcpKey{
			Labels: map[string]string{
				"node.kubernetes.io/instance-type": "a2-highgpu-1g",
				"cloud.google.com/gke-gpu":         "true",
				"cloud.google.com/gke-accelerator": "nvidia-tesla-a100",
				"topology.kubernetes.io/region":    "us-central1",
			},
		},
	}

	pvKeys := map[string]models.PVKey{}

	actualPrices, token, err := testGcp.parsePage(reader, inputKeys, pvKeys)
	if err != nil {
		t.Fatalf("got error parsing page: %v", err)
	}

	const expectedToken = "APKCS1HVa0YpwgyTFbqbJ1eGwzKZmsPwLqzMZPTSNia5ck1Hc54Tx_Kz3oBxwSnRIdGVxXoSPdf-XlDpyNBf4QuxKcIEgtrQ1LDLWAgZowI0ns7HjrGta2s="
	if token != expectedToken {
		t.Fatalf("error parsing GCP next page token, parsed %s but expected %s", token, expectedToken)
	}

	expectedActualPrices := map[string]*GCPPricing{
		"us-central1,a2highgpu,ondemand,gpu": {
			Name:        "services/6F81-5844-456A/skus/039F-D0DA-4055",
			SKUID:       "039F-D0DA-4055",
			Description: "Nvidia Tesla A100 GPU running in Americas",
			Category: &GCPResourceInfo{
				ServiceDisplayName: "Compute Engine",
				ResourceFamily:     "Compute",
				ResourceGroup:      "GPU",
				UsageType:          "OnDemand",
			},
			ServiceRegions: []string{"us-central1", "us-east1", "us-west1"},
			PricingInfo: []*PricingInfo{
				{
					Summary: "",
					PricingExpression: &PricingExpression{
						UsageUnit:                "h",
						UsageUnitDescription:     "hour",
						BaseUnit:                 "s",
						BaseUnitConversionFactor: 0,
						DisplayQuantity:          1,
						TieredRates: []*TieredRates{
							{
								StartUsageAmount: 0,
								UnitPrice: &UnitPriceInfo{
									CurrencyCode: "USD",
									Units:        "2",
									Nanos:        933908000,
								},
							},
						},
					},
					CurrencyConversionRate: 1,
					EffectiveTime:          "2023-03-24T10:52:50.681Z",
				},
			},
			ServiceProviderName: "Google",
			Node: &models.Node{
				VCPUCost:         "0.031611",
				RAMCost:          "0.004237",
				UsesBaseCPUPrice: false,
				GPU:              "1",
				GPUName:          "nvidia-tesla-a100",
				GPUCost:          "2.933908",
			},
		},
		"us-central1,a2highgpu,ondemand": {
			Node: &models.Node{
				VCPUCost:         "0.031611",
				RAMCost:          "0.004237",
				UsesBaseCPUPrice: false,
				UsageType:        "ondemand",
			},
		},
	}

	if !reflect.DeepEqual(actualPrices, expectedActualPrices) {
		t.Fatalf("error parsing GCP prices. parsed %v but expected %v", actualPrices, expectedActualPrices)
	}
}
