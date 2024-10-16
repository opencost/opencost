package aws

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
)

func Test_awsKey_getUsageType(t *testing.T) {
	type fields struct {
		Labels     map[string]string
		ProviderID string
	}
	type args struct {
		labels map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			// test with no labels should return false
			name: "Label does not have the capacityType label associated with it",
			args: args{
				labels: map[string]string{},
			},
			want: "",
		},
		{
			name: "EKS label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: EKSCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
		{
			name: "Karpenter label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					models.KarpenterCapacityTypeLabel: models.KarpenterCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &awsKey{
				Labels:     tt.fields.Labels,
				ProviderID: tt.fields.ProviderID,
			}
			if got := k.getUsageType(tt.args.labels); got != tt.want {
				t.Errorf("getUsageType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_PricingData_Regression
//
// Objective: To test the pricing data download and validate the schema is still
// as expected
//
// These tests may take a long time to complete. It is downloading AWS Pricing
// data files (~500MB) for each region.
func Test_PricingData_Regression(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("skipping integration tests, set environment variable INTEGRATION")
	}

	awsRegions := []string{"us-east-1", "eu-west-1"}

	// Check pricing data produced for each region
	for _, region := range awsRegions {

		awsTest := AWS{}
		res, _, err := awsTest.getRegionPricing([]*clustercache.Node{
			{
				Labels: map[string]string{"topology.kubernetes.io/region": region},
			}})
		if err != nil {
			t.Errorf("Failed to download pricing data for region %s: %v", region, err)
		}

		// Unmarshal pricing data into AWSPricing
		var pricingData AWSPricing
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Errorf("Failed to read pricing data for region %s: %v", region, err)
		}
		err = json.Unmarshal(body, &pricingData)
		if err != nil {
			t.Errorf("Failed to unmarshal pricing data for region %s: %v", region, err)
		}

		// ASSERTION. We only anticipate "OnDemand" or "CapacityBlock" in the
		// pricing data.
		//
		// Failing this test does not necessarily mean we have regressed. Just
		// that we need to revisit this code to ensure OnDemand pricing is still
		// functioning as expected.
		for _, product := range pricingData.Products {
			if product.Attributes.MarketOption != "OnDemand" && product.Attributes.MarketOption != "CapacityBlock" && product.Attributes.MarketOption != "" {
				t.Errorf("Invalid marketOption for product %s: %s", product.Sku, product.Attributes.MarketOption)
			}
		}
	}
}

// Test_populate_pricing
//
// Objective: To test core pricing population logic for AWS
//
// Case 0: US endpoints
// Take a portion of json returned from ondemand terms in us endpoints load the
// request into the http response and give it to the function inspect the
// resulting aws object after the function returns and validate fields
//
// Case 1: Ensure marketOption=OnDemand
// AWS introduced the field marketOption. We need to further filter for
// marketOption=OnDemand to ensure we are not getting pricing from a line item
// such as marketOption=CapacityBlock
//
// Case 2: Chinese endpoints
// Same as above US test case, except using CN PV offer codes. Validate
// populated fields in AWS object
func Test_populate_pricing(t *testing.T) {
	awsTest := AWS{
		ValidPricingKeys: map[string]bool{},
	}
	inputkeys := map[string]bool{
		"us-east-2,m5.large,linux": true,
	}
	// Case 0
	awsUSEastString := `
	{
		"formatVersion" : "v1.0",
		"disclaimer" : "This pricing list is for informational purposes only. All prices are subject to the additional terms included in the pricing pages on http://aws.amazon.com. All Free Tier prices are also subject to the terms included at https://aws.amazon.com/free/",
		"offerCode" : "AmazonEC2",
		"version" : "20230322145651",
		"publicationDate" : "2023-03-22T14:56:51Z",
		"products" : {
			"8D49XP354UEYTHGM" : {
				"sku" : "8D49XP354UEYTHGM",
				"productFamily" : "Compute Instance",
				"attributes" : {
				  "servicecode" : "AmazonEC2",
				  "location" : "US East (Ohio)",
				  "locationType" : "AWS Region",
				  "instanceType" : "m5.large",
				  "currentGeneration" : "Yes",
				  "instanceFamily" : "General purpose",
				  "vcpu" : "2",
				  "physicalProcessor" : "Intel Xeon Platinum 8175",
				  "clockSpeed" : "3.1 GHz",
				  "memory" : "8 GiB",
				  "storage" : "EBS only",
				  "networkPerformance" : "Up to 10 Gigabit",
				  "processorArchitecture" : "64-bit",
				  "tenancy" : "Shared",
				  "operatingSystem" : "Linux",
				  "licenseModel" : "No License required",
				  "usagetype" : "USE2-BoxUsage:m5.large",
				  "operation" : "RunInstances",
				  "availabilityzone" : "NA",
				  "capacitystatus" : "Used",
				  "classicnetworkingsupport" : "false",
				  "dedicatedEbsThroughput" : "Up to 2120 Mbps",
				  "ecu" : "10",
				  "enhancedNetworkingSupported" : "Yes",
				  "gpuMemory" : "NA",
				  "intelAvxAvailable" : "Yes",
				  "intelAvx2Available" : "Yes",
				  "intelTurboAvailable" : "Yes",
				  "marketoption" : "OnDemand",
				  "normalizationSizeFactor" : "4",
				  "preInstalledSw" : "NA",
				  "processorFeatures" : "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo",
				  "regionCode" : "us-east-2",
				  "servicename" : "Amazon Elastic Compute Cloud",
				  "vpcnetworkingsupport" : "true"
				}
			},
			"9ZEEN7WWWQKAG292" : {
				"sku" : "9ZEEN7WWWQKAG292",
				"productFamily" : "Compute Instance",
				"attributes" : {
				  "servicecode" : "AmazonEC2",
				  "location" : "US East (Ohio)",
				  "locationType" : "AWS Region",
				  "instanceType" : "p3.8xlarge",
				  "currentGeneration" : "Yes",
				  "instanceFamily" : "GPU instance",
				  "vcpu" : "32",
				  "physicalProcessor" : "Intel Xeon E5-2686 v4 (Broadwell)",
				  "clockSpeed" : "2.3 GHz",
				  "memory" : "244 GiB",
				  "storage" : "EBS only",
				  "networkPerformance" : "10 Gigabit",
				  "processorArchitecture" : "64-bit",
				  "tenancy" : "Shared",
				  "operatingSystem" : "Windows",
				  "licenseModel" : "Bring your own license",
				  "usagetype" : "USE2-BoxUsage:p3.8xlarge",
				  "operation" : "RunInstances:0800",
				  "availabilityzone" : "NA",
				  "capacitystatus" : "Used",
				  "classicnetworkingsupport" : "false",
				  "dedicatedEbsThroughput" : "7000 Mbps",
				  "ecu" : "97",
				  "enhancedNetworkingSupported" : "Yes",
				  "gpu" : "4",
				  "gpuMemory" : "NA",
				  "intelAvxAvailable" : "Yes",
				  "intelAvx2Available" : "Yes",
				  "intelTurboAvailable" : "Yes",
				  "marketoption" : "OnDemand",
				  "normalizationSizeFactor" : "64",
				  "preInstalledSw" : "NA",
				  "processorFeatures" : "Intel AVX; Intel AVX2; Intel Turbo",
				  "regionCode" : "us-east-2",
				  "servicename" : "Amazon Elastic Compute Cloud",
				  "vpcnetworkingsupport" : "true"
				}
			},
			"M6UGCCQ3CDJQAA37" : {
				"sku" : "M6UGCCQ3CDJQAA37",
				"productFamily" : "Storage",
				"attributes" : {
				  "servicecode" : "AmazonEC2",
				  "location" : "US East (Ohio)",
				  "locationType" : "AWS Region",
				  "storageMedia" : "SSD-backed",
				  "volumeType" : "General Purpose",
				  "maxVolumeSize" : "16 TiB",
				  "maxIopsvolume" : "16000",
				  "maxThroughputvolume" : "1000 MiB/s",
				  "usagetype" : "USE2-EBS:VolumeUsage.gp3",
				  "operation" : "",
				  "regionCode" : "us-east-2",
				  "servicename" : "Amazon Elastic Compute Cloud",
				  "volumeApiName" : "gp3"
				}
			}
		},
		"terms" : {
			"OnDemand" : {
				"M6UGCCQ3CDJQAA37" : {
					"M6UGCCQ3CDJQAA37.JRTCKXETXF" : {
					  "offerTermCode" : "JRTCKXETXF",
					  "sku" : "M6UGCCQ3CDJQAA37",
					  "effectiveDate" : "2023-03-01T00:00:00Z",
					  "priceDimensions" : {
						"M6UGCCQ3CDJQAA37.JRTCKXETXF.6YS6EN2CT7" : {
						  "rateCode" : "M6UGCCQ3CDJQAA37.JRTCKXETXF.6YS6EN2CT7",
						  "description" : "$0.08 per GB-month of General Purpose (gp3) provisioned storage - US East (Ohio)",
						  "beginRange" : "0",
						  "endRange" : "Inf",
						  "unit" : "GB-Mo",
						  "pricePerUnit" : {
							"USD" : "0.0800000000"
						  },
						  "appliesTo" : [ ]
						}
					  },
					  "termAttributes" : { }
					}
				},
				"9ZEEN7WWWQKAG292" : {
					"9ZEEN7WWWQKAG292.JRTCKXETXF" : {
					  "offerTermCode" : "JRTCKXETXF",
					  "sku" : "9ZEEN7WWWQKAG292",
					  "effectiveDate" : "2023-03-01T00:00:00Z",
					  "priceDimensions" : {
						"9ZEEN7WWWQKAG292.JRTCKXETXF.6YS6EN2CT7" : {
						  "rateCode" : "9ZEEN7WWWQKAG292.JRTCKXETXF.6YS6EN2CT7",
						  "description" : "$12.24 per On Demand Windows BYOL p3.8xlarge Instance Hour",
						  "beginRange" : "0",
						  "endRange" : "Inf",
						  "unit" : "Hrs",
						  "pricePerUnit" : {
							"USD" : "12.2400000000"
						  },
						  "appliesTo" : [ ]
						}
					  },
					  "termAttributes" : { }
					}
				},
				"8D49XP354UEYTHGM" : {
					"8D49XP354UEYTHGM.MZU6U2429S" : {
					  "offerTermCode" : "MZU6U2429S",
					  "sku" : "8D49XP354UEYTHGM",
					  "effectiveDate" : "2019-01-01T00:00:00Z",
					  "priceDimensions" : {
						"8D49XP354UEYTHGM.MZU6U2429S.2TG2D8R56U" : {
						  "rateCode" : "8D49XP354UEYTHGM.MZU6U2429S.2TG2D8R56U",
						  "description" : "Upfront Fee",
						  "unit" : "Quantity",
						  "pricePerUnit" : {
							"USD" : "1161"
						  },
						  "appliesTo" : [ ]
						},
					  },
					  "termAttributes" : {
						"LeaseContractLength" : "3yr",
						"OfferingClass" : "convertible",
						"PurchaseOption" : "All Upfront"
					  }
					}
				}
			}
		},
		"attributesList" : { }
	}
	`

	testResponse := http.Response{
		Body: io.NopCloser(bytes.NewBufferString(awsUSEastString)),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponse, inputkeys)

	expectedProdTermsDisk := &AWSProductTerms{
		Sku:     "M6UGCCQ3CDJQAA37",
		Memory:  "",
		Storage: "",
		VCpu:    "",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "M6UGCCQ3CDJQAA37",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"M6UGCCQ3CDJQAA37.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "GB-Mo",
					PricePerUnit: AWSCurrencyCode{
						USD: "0.0800000000",
						CNY: "",
					},
				},
			},
		},
		PV: &models.PV{
			Cost:       "0.00010958904109589041",
			CostPerIO:  "",
			Class:      "gp3",
			Size:       "",
			Region:     "us-east-2",
			ProviderID: "",
		},
	}

	expectedProdTermsInstanceOndemand := &AWSProductTerms{
		Sku:     "8D49XP354UEYTHGM",
		Memory:  "8 GiB",
		Storage: "EBS only",
		VCpu:    "2",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:             "",
			OfferTermCode:   "",
			PriceDimensions: nil,
		},
	}

	expectedProdTermsInstanceSpot := &AWSProductTerms{
		Sku:     "8D49XP354UEYTHGM",
		Memory:  "8 GiB",
		Storage: "EBS only",
		VCpu:    "2",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:             "",
			OfferTermCode:   "",
			PriceDimensions: nil,
		},
	}

	expectedPricing := map[string]*AWSProductTerms{
		"us-east-2,EBS:VolumeUsage.gp3":             expectedProdTermsDisk,
		"us-east-2,EBS:VolumeUsage.gp3,preemptible": expectedProdTermsDisk,
		"us-east-2,m5.large,linux":                  expectedProdTermsInstanceOndemand,
		"us-east-2,m5.large,linux,preemptible":      expectedProdTermsInstanceSpot,
	}

	if !reflect.DeepEqual(expectedPricing, awsTest.Pricing) {
		t.Fatalf("expected parsed pricing did not match actual parsed result (us-east-1)")
	}

	// Case 1 - Only accept `"marketoption":"OnDemand"`
	inputkeysCase1 := map[string]bool{
		"us-east-1,p4d.24xlarge,linux": true,
	}
	pricingCase1 := `
	{
		"formatVersion" : "v1.0",
		"disclaimer" : "This pricing list is for informational purposes only. All prices are subject to the additional terms included in the pricing pages on http://aws.amazon.com. All Free Tier prices are also subject to the terms included at https://aws.amazon.com/free/",
		"offerCode" : "AmazonEC2",
		"version" : "20240528203522",
		"publicationDate" : "2024-05-28T20:35:22Z",
		"products" : {
			"H7NGEAC6UEHNTKSJ" : {
				"sku" : "H7NGEAC6UEHNTKSJ",
				"productFamily" : "Compute Instance",
				"attributes" : {
					"servicecode" : "AmazonEC2",
					"location" : "US East (N. Virginia)",
					"locationType" : "AWS Region",
					"instanceType" : "p4d.24xlarge",
					"currentGeneration" : "Yes",
					"instanceFamily" : "GPU instance",
					"vcpu" : "96",
					"physicalProcessor" : "Intel Xeon Platinum 8275L",
					"clockSpeed" : "3 GHz",
					"memory" : "1152 GiB",
					"storage" : "8 x 1000 SSD",
					"networkPerformance" : "400 Gigabit",
					"processorArchitecture" : "64-bit",
					"tenancy" : "Shared",
					"operatingSystem" : "Linux",
					"licenseModel" : "No License required",
					"usagetype" : "BoxUsage:p4d.24xlarge",
					"operation" : "RunInstances",
					"availabilityzone" : "NA",
					"capacitystatus" : "Used",
					"classicnetworkingsupport" : "false",
					"dedicatedEbsThroughput" : "19000 Mbps",
					"ecu" : "345",
					"enhancedNetworkingSupported" : "No",
					"gpu" : "8",
					"gpuMemory" : "NA",
					"intelAvxAvailable" : "Yes",
					"intelAvx2Available" : "Yes",
					"intelTurboAvailable" : "Yes",
					"marketoption" : "OnDemand",
					"normalizationSizeFactor" : "192",
					"preInstalledSw" : "NA",
					"processorFeatures" : "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo",
					"regionCode" : "us-east-1",
					"servicename" : "Amazon Elastic Compute Cloud",
					"vpcnetworkingsupport" : "true"
				}
			},
			"YSXJGN78QTXNVGDQ" : {
				"sku" : "YSXJGN78QTXNVGDQ",
				"productFamily" : "Compute Instance",
				"attributes" : {
					"servicecode" : "AmazonEC2",
					"location" : "US East (N. Virginia)",
					"locationType" : "AWS Region",
					"instanceType" : "p4d.24xlarge",
					"currentGeneration" : "Yes",
					"instanceFamily" : "GPU instance",
					"vcpu" : "96",
					"physicalProcessor" : "Intel Xeon Platinum 8275L",
					"clockSpeed" : "3 GHz",
					"memory" : "1152 GiB",
					"storage" : "8 x 1000 SSD",
					"networkPerformance" : "400 Gigabit",
					"processorArchitecture" : "64-bit",
					"tenancy" : "Shared",
					"operatingSystem" : "Linux",
					"licenseModel" : "No License required",
					"usagetype" : "BoxUsage:p4d.24xlarge",
					"operation" : "RunInstances:CB",
					"availabilityzone" : "NA",
					"capacitystatus" : "Used",
					"classicnetworkingsupport" : "false",
					"dedicatedEbsThroughput" : "19000 Mbps",
					"ecu" : "345",
					"enhancedNetworkingSupported" : "No",
					"gpu" : "8",
					"gpuMemory" : "NA",
					"intelAvxAvailable" : "Yes",
					"intelAvx2Available" : "Yes",
					"intelTurboAvailable" : "Yes",
					"marketoption" : "CapacityBlock",
					"normalizationSizeFactor" : "192",
					"preInstalledSw" : "NA",
					"processorFeatures" : "Intel AVX; Intel AVX2; Intel AVX512; Intel Turbo",
					"regionCode" : "us-east-1",
					"servicename" : "Amazon Elastic Compute Cloud",
					"vpcnetworkingsupport" : "true"
				}
			}
		},
		"terms" : {
			"OnDemand" : {
				"H7NGEAC6UEHNTKSJ" : {
					"H7NGEAC6UEHNTKSJ.JRTCKXETXF" : {
						"offerTermCode" : "JRTCKXETXF",
						"sku" : "H7NGEAC6UEHNTKSJ",
						"effectiveDate" : "2024-05-01T00:00:00Z",
						"priceDimensions" : {
							"H7NGEAC6UEHNTKSJ.JRTCKXETXF.6YS6EN2CT7" : {
								"rateCode" : "H7NGEAC6UEHNTKSJ.JRTCKXETXF.6YS6EN2CT7",
								"description" : "$32.7726 per On Demand Linux p4d.24xlarge Instance Hour",
								"beginRange" : "0",
								"endRange" : "Inf",
								"unit" : "Hrs",
								"pricePerUnit" : {
									"USD" : "32.7726000000"
								},
								"appliesTo" : [ ]
							}
						},
						"termAttributes" : { }
					}
				},
				"YSXJGN78QTXNVGDQ" : {
					"YSXJGN78QTXNVGDQ.JRTCKXETXF" : {
						"offerTermCode" : "JRTCKXETXF",
						"sku" : "YSXJGN78QTXNVGDQ",
						"effectiveDate" : "2024-05-01T00:00:00Z",
						"priceDimensions" : {
							"YSXJGN78QTXNVGDQ.JRTCKXETXF.6YS6EN2CT7" : {
							"rateCode" : "YSXJGN78QTXNVGDQ.JRTCKXETXF.6YS6EN2CT7",
							"description" : "$0.00 per Capacity Block Linux p4d.24xlarge Instance Hour",
							"beginRange" : "0",
							"endRange" : "Inf",
							"unit" : "Hrs",
							"pricePerUnit" : {
								"USD" : "0.0000000000"
							},
							"appliesTo" : [ ]
						}
					},
					"termAttributes" : { }
					}
				},
			}
		},
		"attributesList" : { }
	}
	`

	testResponseCase1 := http.Response{
		Body: io.NopCloser(bytes.NewBufferString(pricingCase1)),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponseCase1, inputkeysCase1)

	expectedProdTermsInstanceOndemandCase1 := &AWSProductTerms{
		Sku:     "H7NGEAC6UEHNTKSJ",
		Memory:  "1152 GiB",
		Storage: "8 x 1000 SSD",
		VCpu:    "96",
		GPU:     "8",
		OnDemand: &AWSOfferTerm{
			Sku:           "H7NGEAC6UEHNTKSJ",
			OfferTermCode: "JRTCKXETXF",
			PriceDimensions: map[string]*AWSRateCode{
				"H7NGEAC6UEHNTKSJ.JRTCKXETXF.6YS6EN2CT7": {
					Unit: "Hrs",
					PricePerUnit: AWSCurrencyCode{
						USD: "32.7726000000",
					},
				},
			},
		},
	}

	expectedPricingCase1 := map[string]*AWSProductTerms{
		"us-east-1,p4d.24xlarge,linux":             expectedProdTermsInstanceOndemandCase1,
		"us-east-1,p4d.24xlarge,linux,preemptible": expectedProdTermsInstanceOndemandCase1,
	}

	if !reflect.DeepEqual(expectedPricingCase1, awsTest.Pricing) {
		expectedJsonString, _ := json.MarshalIndent(expectedPricingCase1, "", "  ")
		resultJsonString, _ := json.MarshalIndent(awsTest.Pricing, "", "  ")
		t.Logf("Expected: %s", string(expectedJsonString))
		t.Logf("Result: %s", string(resultJsonString))
		t.Fatalf("expected parsed pricing did not match actual parsed result (us-east-1)")
	}

	// Case 2
	awsCnString := `
	{
		"formatVersion" : "v1.0",
		"disclaimer" : "This pricing list is for informational purposes only. All prices are subject to the additional terms included in the pricing pages on http://www.amazonaws.cn.",
		"offerCode" : "AmazonEC2",
		"version" : "20230314154740",
		"publicationDate" : "2023-03-14T15:47:40Z",
		"products" : {
			"R83VXG9NAPDASEGN" : {
				"sku" : "R83VXG9NAPDASEGN",
				"productFamily" : "Storage",
				"attributes" : {
				  "servicecode" : "AmazonEC2",
				  "location" : "China (Ningxia)",
				  "locationType" : "AWS Region",
				  "storageMedia" : "SSD-backed",
				  "volumeType" : "General Purpose",
				  "maxVolumeSize" : "16 TiB",
				  "maxIopsvolume" : "16000",
				  "maxThroughputvolume" : "1000 MiB/s",
				  "usagetype" : "CNW1-EBS:VolumeUsage.gp3",
				  "operation" : "",
				  "regionCode" : "cn-northwest-1",
				  "servicename" : "Amazon Elastic Compute Cloud",
				  "volumeApiName" : "gp3"
				}
			}
		},
		"terms" : {
			"OnDemand" : {
			  "R83VXG9NAPDASEGN" : {
				"R83VXG9NAPDASEGN.5Y9WH78GDR" : {
				  "offerTermCode" : "5Y9WH78GDR",
				  "sku" : "R83VXG9NAPDASEGN",
				  "effectiveDate" : "2023-03-01T00:00:00Z",
				  "priceDimensions" : {
					"R83VXG9NAPDASEGN.5Y9WH78GDR.Q7UJUT2CE6" : {
					  "rateCode" : "R83VXG9NAPDASEGN.5Y9WH78GDR.Q7UJUT2CE6",
					  "description" : "0.5312 CNY per GB-month of General Purpose (gp3) provisioned storage - China (Ningxia)",
					  "beginRange" : "0",
					  "endRange" : "Inf",
					  "unit" : "GB-Mo",
					  "pricePerUnit" : {
						"CNY" : "0.5312000000"
					  },
					  "appliesTo" : [ ]
					}
				  },
				  "termAttributes" : { }
				}
			  }
			}
	    },
	  "attributesList" : { }
	}
	`
	awsTest = AWS{
		ValidPricingKeys: map[string]bool{},
	}

	testResponse = http.Response{
		Body: io.NopCloser(bytes.NewBufferString(awsCnString)),
		Request: &http.Request{
			URL: &url.URL{
				Scheme: "https",
				Host:   "test-aws-http-endpoint:443",
			},
		},
	}

	awsTest.populatePricing(&testResponse, inputkeys)

	expectedProdTermsDisk = &AWSProductTerms{
		Sku:     "R83VXG9NAPDASEGN",
		Memory:  "",
		Storage: "",
		VCpu:    "",
		GPU:     "",
		OnDemand: &AWSOfferTerm{
			Sku:           "R83VXG9NAPDASEGN",
			OfferTermCode: "5Y9WH78GDR",
			PriceDimensions: map[string]*AWSRateCode{
				"R83VXG9NAPDASEGN.5Y9WH78GDR.Q7UJUT2CE6": {
					Unit: "GB-Mo",
					PricePerUnit: AWSCurrencyCode{
						USD: "",
						CNY: "0.5312000000",
					},
				},
			},
		},
		PV: &models.PV{
			Cost:       "0.0007276712328767123",
			CostPerIO:  "",
			Class:      "gp3",
			Size:       "",
			Region:     "cn-northwest-1",
			ProviderID: "",
		},
	}

	expectedPricing = map[string]*AWSProductTerms{
		"cn-northwest-1,EBS:VolumeUsage.gp3":             expectedProdTermsDisk,
		"cn-northwest-1,EBS:VolumeUsage.gp3,preemptible": expectedProdTermsDisk,
	}

	if !reflect.DeepEqual(expectedPricing, awsTest.Pricing) {
		t.Fatalf("expected parsed pricing did not match actual parsed result (cn)")
	}
}

func TestFeatures(t *testing.T) {
	testCases := map[string]struct {
		aws      awsKey
		expected string
	}{
		"Spot from custom labels": {
			aws: awsKey{
				SpotLabelName:  "node-type",
				SpotLabelValue: "node-spot",
				Labels: map[string]string{
					"node-type":                "node-spot",
					v1.LabelOSStable:           "linux",
					v1.LabelHostname:           "my-hostname",
					v1.LabelTopologyRegion:     "us-west-2",
					v1.LabelTopologyZone:       "us-west-2b",
					v1.LabelInstanceTypeStable: "m5.large",
				},
			},
			expected: "us-west-2,m5.large,linux,preemptible",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features := tc.aws.Features()
			if features != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, features)
			}
		})
	}
}

func Test_getStorageClassTypeFrom(t *testing.T) {
	tests := []struct {
		name        string
		provisioner string
		want        string
	}{
		{
			name:        "empty-provisioner",
			provisioner: "",
			want:        "",
		},
		{
			name:        "ebs-default-provisioner",
			provisioner: "kubernetes.io/aws-ebs",
			want:        "gp2",
		},
		{
			name:        "ebs-csi-provisioner",
			provisioner: "ebs.csi.aws.com",
			want:        "gp3",
		},
		{
			name:        "unknown-provisioner",
			provisioner: "unknown",
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getStorageClassTypeFrom(tt.provisioner); got != tt.want {
				t.Errorf("getStorageClassTypeFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}
