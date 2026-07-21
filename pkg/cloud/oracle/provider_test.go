package oracle

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestGetKey(t *testing.T) {
	var testCases = map[string]struct {
		isVirtual bool
		gpus      int
	}{
		"virtual-node": {
			true,
			0,
		},
		"gpu": {
			false,
			3,
		},
		"node": {
			false,
			0,
		},
	}
	for instanceType, testCase := range testCases {
		t.Run(instanceType, func(t *testing.T) {
			labels := map[string]string{
				v1.LabelInstanceTypeStable: instanceType,
			}
			if testCase.isVirtual {
				labels[virtualNodeLabel] = ""
			}
			key := (&Oracle{}).GetKey(labels, testNode(testCase.gpus))
			assert.NotEmpty(t, key.ID())
			features := strings.Split(key.Features(), ",")
			assert.Len(t, features, 3)
			assert.Equal(t, instanceType, features[0])
			assert.Equal(t, strconv.FormatBool(testCase.isVirtual), features[1])
			assert.Equal(t, testCase.gpus, key.GPUCount())
			if testCase.gpus > 0 {
				assert.Equal(t, "nvidia.com/gpu", key.GPUType())
			} else {
				assert.Equal(t, "", key.GPUType())
			}
		})
	}
}

func TestGetKeyFallsBackToOCIInstanceShapeLabel(t *testing.T) {
	labels := map[string]string{
		ociInstanceShapeLabel: "VM.Standard.E3.Flex",
	}

	key := (&Oracle{}).GetKey(labels, testNode(0))
	features := strings.Split(key.Features(), ",")

	assert.Len(t, features, 3)
	assert.Equal(t, "VM.Standard.E3.Flex", features[0])
}

func TestGetKeyPrefersKubernetesInstanceTypeLabel(t *testing.T) {
	labels := map[string]string{
		v1.LabelInstanceTypeStable: "VM.Standard.E3.Flex.2o.32g.1_1b",
		ociInstanceShapeLabel:      "VM.Standard.E3.Flex",
	}

	key := (&Oracle{}).GetKey(labels, testNode(0))
	features := strings.Split(key.Features(), ",")

	assert.Len(t, features, 3)
	assert.Equal(t, "VM.Standard.E3.Flex.2o.32g.1_1b", features[0])
}

func TestGetPVKey(t *testing.T) {
	storageClass := "xyz"
	providerID := "ocid.abc"
	pv := &clustercache.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			StorageClassName: storageClass,
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: providerID,
					Driver:       driverOCIBV,
				},
			},
		},
	}
	pvkey := (&Oracle{}).GetPVKey(pv, map[string]string{}, "")
	assert.Equal(t, blockVolumePartNumber, pvkey.Features())
	assert.Equal(t, storageClass, pvkey.GetStorageClass())
	assert.Equal(t, providerID, pvkey.ID())
}

func TestRegions(t *testing.T) {
	regions := (&Oracle{}).Regions()
	assert.Len(t, regions, 39)
}

func TestNodePricing_Preemptible(t *testing.T) {
	oracle := &Oracle{
		RateCardStore: NewRateCardStore("", "USD"),
		DefaultPricing: DefaultPricing{
			OCPU:    "0.2",
			Memory:  "0.1",
			GPU:     "0.3",
			Storage: "0.25",
		},
	}

	testCases := []struct {
		name        string
		labels      map[string]string
		expectUsage string
	}{
		{
			name: "preemptible node",
			labels: map[string]string{
				v1.LabelInstanceTypeStable: "VM.Standard.E4.Flex",
				preemptibleLabel:           "true",
			},
			expectUsage: "preemptible",
		},
		{
			name: "non-preemptible node",
			labels: map[string]string{
				v1.LabelInstanceTypeStable: "VM.Standard.E4.Flex",
			},
			expectUsage: "",
		},
		{
			name: "preemptible label false",
			labels: map[string]string{
				v1.LabelInstanceTypeStable: "VM.Standard.E4.Flex",
				preemptibleLabel:           "false",
			},
			expectUsage: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := &oracleKey{
				instanceType: "VM.Standard.E4.Flex",
				labels:       tc.labels,
				providerID:   "ocid.test",
			}

			node, _, err := oracle.NodePricing(key)
			assert.NoError(t, err)
			assert.NotNil(t, node)
			assert.Equal(t, tc.expectUsage, node.UsageType)
		})
	}
}

func testNode(gpus int) *clustercache.Node {
	capacity := map[v1.ResourceName]resource.Quantity{}
	if gpus > 0 {
		capacity["nvidia.com/gpu"] = resource.MustParse(fmt.Sprintf("%d", gpus))
	}
	return &clustercache.Node{
		SpecProviderID: "ocid.abc",
		Status: v1.NodeStatus{
			Capacity: capacity,
		},
	}
}

// TestOracleNetworkPricing verifies that NetworkPricing returns a non-nil Network
// using pre-populated pricing data and that a NetworkKey with an explicit region
// overrides the provider's ClusterRegion for egress resolution.
func TestOracleNetworkPricing(t *testing.T) {
	const egressPrice = 0.0085

	newOracle := func(clusterRegion string) *Oracle {
		return &Oracle{
			ClusterRegion: clusterRegion,
			RateCardStore: &RateCardStore{
				prices: map[string]Price{
					egress1PartNumber: {UnitPrice: egressPrice},
				},
			},
			DefaultPricing: DefaultPricing{},
		}
	}

	t.Run("uses ClusterRegion when key region is empty", func(t *testing.T) {
		o := newOracle("us-ashburn-1")
		key := &oracleNetworkKey{region: ""}
		network, err := o.NetworkPricing(key)
		assert.NoError(t, err)
		assert.NotNil(t, network)
		assert.Equal(t, egressPrice, network.RegionNetworkEgressCost)
		assert.Equal(t, egressPrice, network.InternetNetworkEgressCost)
	})

	t.Run("overrides region from NetworkKey when non-empty", func(t *testing.T) {
		// ClusterRegion is ap-sydney-1 (egress2), but key specifies a us-* region (egress1)
		o := newOracle("ap-sydney-1")
		key := &oracleNetworkKey{region: "us-phoenix-1"}
		network, err := o.NetworkPricing(key)
		assert.NoError(t, err)
		assert.NotNil(t, network)
		assert.Equal(t, egressPrice, network.RegionNetworkEgressCost,
			"should use key region (us-phoenix-1 → egress1), not ClusterRegion (ap-sydney-1 → egress2)")
	})

	t.Run("nil key falls back to ClusterRegion", func(t *testing.T) {
		o := newOracle("us-ashburn-1")
		network, err := o.NetworkPricing(nil)
		assert.NoError(t, err)
		assert.NotNil(t, network)
		assert.Equal(t, egressPrice, network.RegionNetworkEgressCost)
	})

	t.Run("unknown region returns zero egress cost", func(t *testing.T) {
		o := newOracle("unknown-region-1")
		network, err := o.NetworkPricing(nil)
		assert.NoError(t, err)
		assert.NotNil(t, network)
		assert.Equal(t, 0.0, network.RegionNetworkEgressCost)
		assert.Equal(t, 0.0, network.ZoneNetworkEgressCost)
	})
}

// TestOracleGetNetworkKey verifies that GetNetworkKey extracts zone and region
// from Kubernetes node labels and returns a valid NetworkKey.
func TestOracleGetNetworkKey(t *testing.T) {
	o := &Oracle{}

	t.Run("extracts region from topology label", func(t *testing.T) {
		labels := map[string]string{
			"topology.kubernetes.io/region": "us-ashburn-1",
			"topology.kubernetes.io/zone":   "AD-1",
		}
		key := o.GetNetworkKey(labels, "cluster-1")
		assert.Equal(t, "AD-1", key.GetZone())
		assert.Equal(t, "us-ashburn-1", key.GetRegion())
		assert.NotEmpty(t, key.ID())
		assert.Contains(t, key.Features(), "us-ashburn-1")
		assert.Contains(t, key.Features(), "AD-1")
	})

	t.Run("handles empty labels gracefully", func(t *testing.T) {
		key := o.GetNetworkKey(map[string]string{}, "cluster-2")
		assert.Empty(t, key.GetZone())
		assert.Empty(t, key.GetRegion())
		assert.Empty(t, key.ID())
		assert.Empty(t, key.Features())
	})
}

// oracleNetworkKey is a minimal NetworkKey stub for unit testing.
type oracleNetworkKey struct{ region string }

func (k *oracleNetworkKey) ID() string        { return k.region }
func (k *oracleNetworkKey) Features() string  { return k.region }
func (k *oracleNetworkKey) GetZone() string   { return "" }
func (k *oracleNetworkKey) GetRegion() string { return k.region }
