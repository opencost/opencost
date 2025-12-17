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
