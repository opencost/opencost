package provider

import (
	"testing"
)

func TestCSVProvider(t *testing.T) {
	t.Run("this test we need to fix", func(t *testing.T) {
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

		if node.Cost != "0.259100" {
			t.Errorf("want: %#v, got: %#v\n", "0.259100", node.Cost)
		}
	})

}
