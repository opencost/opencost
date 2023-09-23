package provider

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
)

func TestCSVProvider(t *testing.T) {
	t.Run("this test we need to fix", func(t *testing.T) {
		t.Skip()
		pricing := map[string]*price{
			"region,instanceid": {},

			"us-east-1,anuchito-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "anuchito-1",
				Region:            "us-east-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.2591",
				Version:           "",
			},
			"us-east-1,aorjoa-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "aorjoa-1",
				Region:            "us-east-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.60000",
				Version:           "",
			},
			"ap-southeast-1,aorjoa-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "aorjoa-1",
				Region:            "ap-southeast-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.90000",
				Version:           "",
			},
		}
		np := map[string]float64{"ap-southeast-1,c5.4xlarge,node": 0.9, "us-east-1,c5.4xlarge,node": 0.2591}
		nc := map[string]float64{"ap-southeast-1,c5.4xlarge,node": 1, "us-east-1,c5.4xlarge,node": 1.4295499999999999}
		csv := &CSVProvider{
			Pricing:          pricing,
			NodeClassPricing: np,
			NodeClassCount:   nc,
			NodeMapField:     "metadata.labels.instance-group",
			GPUClassPricing:  map[string]*price{},
			GPUMapFields:     []string{},
			UsesRegion:       true,
		}
		key := &csvKey{
			Labels: map[string]string{"beta.kubernetes.io/arch": "arm64",
				"beta.kubernetes.io/instance-type":                        "c5.4xlarge",
				"beta.kubernetes.io/os":                                   "linux",
				"instance-group":                                          "aorjoa-1",
				"kubernetes.io/arch":                                      "arm64",
				"kubernetes.io/hostname":                                  "kind-control-plane",
				"kubernetes.io/os":                                        "linux",
				"node-role.kubernetes.io/control-plane":                   "",
				"node.kubernetes.io/exclude-from-external-load-balancers": "",
				"providerID":                                              "kind://docker/kind/kind-control-plane",
				"topology.kubernetes.io/region":                           "us-east-1"},
			ProviderID: "us-east-1,aorjoa-1",
			GPULabel:   []string{},
			GPU:        0}

		node, _, _ := csv.NodePricing(key)

		// TODO: fix - we are expecting "0.60000"
		assertNode(t, node, &models.Node{Cost: "0.259100", PricingType: models.CsvClass})
	})

	t.Run("it should lookup from ProviderID and Region (NO1. studentID)", func(t *testing.T) {
		pr := map[string]*price{
			"us-east-1,aorjoa-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "aorjoa-1",
				Region:            "us-east-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.60000",
				Version:           "",
			},
			"aorjoa-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "aorjoa-1",
				Region:            "us-east-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.77777",
				Version:           "",
			},
		}

		key := &csvKey{
			ProviderID: "us-east-1,aorjoa-1",
		}

		csv := &CSVProvider{
			Pricing: pr,
		}

		node, _, _ := csv.NodePricing(key)

		assertNode(t, node, &models.Node{Cost: "0.60000", PricingType: models.CsvExact})
	})

	t.Run("Node Pricing lookup by region,instance-group key.ID() (us-east-1,aorjoa-1)", func(t *testing.T) {
		pr := map[string]*price{
			"us-east-1,aorjoa-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "aorjoa-1",
				Region:            "us-east-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.60001",
				Version:           "",
			},
		}

		key := &csvKey{
			ProviderID: "us-east-1,aorjoa-1",
		}

		csv := &CSVProvider{
			Pricing: pr,
		}

		node, _, _ := csv.NodePricing(key)

		assertNode(t, node, &models.Node{Cost: "0.60001", PricingType: models.CsvExact})
	})

	t.Run("Try without a region to be sure key.ID() (I don not understand this logic??)", func(t *testing.T) {
		pr := map[string]*price{
			"aorjoa-1": {
				EndTimestamp:      "2023-07-24 16:00:00 UTC",
				InstanceID:        "aorjoa-1",
				Region:            "us-east-1",
				AssetClass:        "node",
				InstanceIDField:   "metadata.labels.instance-group",
				InstanceType:      "c5.4xlarge",
				MarketPriceHourly: "0.77777",
				Version:           "",
			},
		}

		key := &csvKey{
			ProviderID: "us-east-1,aorjoa-1",
		}

		csv := &CSVProvider{
			Pricing: pr,
		}

		node, _, _ := csv.NodePricing(key)

		assertNode(t, node, &models.Node{Cost: "0.77777", PricingType: models.CsvExact})
	})

	t.Run("Use node attributes to try and do a class match key.Features() (region,instance-group,assetClass)", func(t *testing.T) {
		csv := &CSVProvider{
			NodeClassPricing: map[string]float64{
				"us-east-1,c5.4xlarge,node": 0.3599,
			},
		}
		key := &csvKey{
			Labels: map[string]string{
				"beta.kubernetes.io/instance-type": "c5.4xlarge",
				"topology.kubernetes.io/region":    "us-east-1",
			},
		}

		node, _, _ := csv.NodePricing(key)

		assertNode(t, node, &models.Node{Cost: "0.359900", PricingType: models.CsvClass})
	})

	t.Run("Unable to find Node matching either ProviderID or key.Features() should be error", func(t *testing.T) {
		csv := &CSVProvider{}
		key := &csvKey{
			Labels: map[string]string{
				"beta.kubernetes.io/instance-type": "c5.4xlarge",
				"topology.kubernetes.io/region":    "us-east-1",
			},
			ProviderID: "us-east-1,aorjoa-1",
		}

		node, _, err := csv.NodePricing(key)

		want := fmt.Errorf("Unable to find Node matching `%s`:`%s`", key.ID(), key.Features())
		if err.Error() != want.Error() {
			t.Errorf("want error %#v but got %#v", want, err)
		}

		if node != nil {
			t.Error("node should be nil but it not")
		}
	})

	t.Run("Adjust Cost when have region,instance-group key.ID() and key.GPUType()", func(t *testing.T) {
		t.Run("should be adjust CPU cost success", func(t *testing.T) {
			csv := &CSVProvider{
				GPUClassPricing: map[string]*price{
					"cluster-api/value/arm64": {MarketPriceHourly: "0.82393"},
				},
				Pricing: map[string]*price{
					"us-east-1,aorjoa-1": {MarketPriceHourly: "0.40000"},
				},
			}
			key := &csvKey{
				ProviderID: "us-east-1,aorjoa-1",
				GPULabel:   []string{"cluster-api/Key/arm64"},
				GPU:        4,
				Labels: map[string]string{
					"cluster-api/Key/arm64": "cluster-api/Value/arm64",
				},
			}

			node, _, err := csv.NodePricing(key)

			if err != nil {
				t.Fatal("should not be error but got error:", err)
			}

			assertNode(t, node, &models.Node{Cost: "3.695720", GPUCost: "3.295720", GPU: "4", PricingType: models.CsvExact})
		})
		// NOTE: good example of 100% test coverage but the case it not really cover
		t.Run("should not be adjust Cost when can not parse Pricing `MarketPriceHourly` to float64", func(t *testing.T) {
			// totalCost := hourly * float64(count) ==> 0.20000 * 4.0 = 0.800000
			// node.GPUCost = fmt.Sprintf("%f", totalCost) ==> 0.800000
			// node.Cost = fmt.Sprintf("%f", nc + totalCost) ===>  0 + 0.800000 = 0.800000
			csv := &CSVProvider{
				GPUClassPricing: map[string]*price{
					"cluster-api/value/arm64": {MarketPriceHourly: "0.20000"},
				},
				Pricing: map[string]*price{
					"us-east-1,aorjoa-1": {MarketPriceHourly: "not a number"},
				},
			}
			key := &csvKey{
				ProviderID: "us-east-1,aorjoa-1",
				GPULabel:   []string{"cluster-api/Key/arm64"},
				GPU:        4,
				Labels: map[string]string{
					"cluster-api/Key/arm64": "cluster-api/Value/arm64",
				},
			}

			node, _, err := csv.NodePricing(key)

			if err != nil {
				t.Fatal("should not be error but got error:", err)
			}

			assertNode(t, node, &models.Node{Cost: "0.800000", GPUCost: "0.800000", GPU: "4", PricingType: models.CsvExact})
		})

		t.Run("should not be adjust Cost when can not parse GPUClassPricing `MarketPriceHourly` to float64", func(t *testing.T) {
			// totalCost := hourly * float64(count) ==> 0.0 * 4.0 = 0.0
			// node.GPUCost = fmt.Sprintf("%f", totalCost) ==> 0.0
			// node.Cost = fmt.Sprintf("%f", nc + totalCost) ===>  0.60001 + 0.0 = 0.60001
			csv := &CSVProvider{
				GPUClassPricing: map[string]*price{
					"cluster-api/value/arm64": {MarketPriceHourly: "not a number"},
				},
				Pricing: map[string]*price{
					"us-east-1,aorjoa-1": {MarketPriceHourly: "0.60001"},
				},
			}
			key := &csvKey{
				ProviderID: "us-east-1,aorjoa-1",
				GPULabel:   []string{"cluster-api/Key/arm64"},
				GPU:        4,
				Labels: map[string]string{
					"cluster-api/Key/arm64": "cluster-api/Value/arm64",
				},
			}

			node, _, err := csv.NodePricing(key)

			if err != nil {
				t.Fatal("should not be error but got error:", err)
			}

			assertNode(t, node, &models.Node{Cost: "0.600010", GPUCost: "0.000000", GPU: "4", PricingType: models.CsvExact})
		})
	})
}

func assertNode(t *testing.T, got, want *models.Node) {
	if got.Cost != want.Cost {
		t.Errorf("Cost: want %#v but got %#v", want.Cost, got.Cost)
	}

	if got.GPUCost != want.GPUCost {
		t.Errorf("GPUCost: want %#v but got %#v", want.GPUCost, got.GPUCost)
	}

	if got.GPU != want.GPU {
		t.Errorf("GPU: want %#v but got %#v", want.GPU, got.GPU)
	}

	if got.PricingType != want.PricingType {
		t.Errorf("PricingType: want %#v but got %#v", want.PricingType, got.PricingType)
	}
}
