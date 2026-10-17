package oracle

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// oracleMockConfig implements models.ProviderConfig for testing RefreshCustomPricing.
type oracleMockConfig struct {
	customPricing *models.CustomPricing
}

func (m *oracleMockConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	return m.customPricing, nil
}

func (m *oracleMockConfig) Update(_ func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *oracleMockConfig) UpdateFromMap(_ map[string]string) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *oracleMockConfig) ConfigFileManager() *config.ConfigFileManager {
	return nil
}

func TestOracleRefreshCustomPricing(t *testing.T) {
	cp := &models.CustomPricing{
		CPU:                   "0.03",
		RAM:                   "0.004",
		GPU:                   "1.5",
		Storage:               "0.00023",
		InternetNetworkEgress: "0.085",
		DefaultLBPrice:        "0.025",
	}
	o := &Oracle{Config: &oracleMockConfig{customPricing: cp}}
	if err := o.RefreshCustomPricing(); err != nil {
		t.Fatalf("RefreshCustomPricing() unexpected error: %v", err)
	}
	if o.DefaultPricing.OCPU != cp.CPU {
		t.Errorf("DefaultPricing.OCPU = %q, want %q", o.DefaultPricing.OCPU, cp.CPU)
	}
	if o.DefaultPricing.Memory != cp.RAM {
		t.Errorf("DefaultPricing.Memory = %q, want %q", o.DefaultPricing.Memory, cp.RAM)
	}
	if o.DefaultPricing.Storage != cp.Storage {
		t.Errorf("DefaultPricing.Storage = %q, want %q", o.DefaultPricing.Storage, cp.Storage)
	}
}

func TestNodePricing_PreemptibleIsHalfPrice(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()
	o := &Oracle{RateCardStore: rcs, nextRefresh: time.Now().Add(time.Hour)}

	labels := map[string]string{
		v1.LabelInstanceTypeStable: "VM.Standard.E5.Flex",
		"kubernetes.io/arch":       "amd64",
	}
	onDemand, _, err := o.NodePricing(o.GetKey(labels, testNode(0)))
	require.NoError(t, err)

	// "Preemptible capacity costs 50% less than on-demand capacity in all regions":
	// https://docs.oracle.com/en-us/iaas/Content/Compute/Concepts/preemptible.htm
	labels[preemptibleLabel] = "true"
	preemptible, _, err := o.NodePricing(o.GetKey(labels, testNode(0)))
	require.NoError(t, err)

	assert.Equal(t, "preemptible", preemptible.UsageType)
	assert.InDelta(t, mustParseFloat(t, onDemand.Cost)/2, mustParseFloat(t, preemptible.Cost), 0.000001)
	assert.InDelta(t, mustParseFloat(t, onDemand.VCPUCost)/2, mustParseFloat(t, preemptible.VCPUCost), 0.000001)
	assert.InDelta(t, mustParseFloat(t, onDemand.RAMCost)/2, mustParseFloat(t, preemptible.RAMCost), 0.000001)
}

func TestEnsurePricingData_RefreshesStalePrices(t *testing.T) {
	pricesUSDBytes, err := os.ReadFile("test/prices_usd.json")
	require.NoError(t, err)
	var hits atomic.Int32
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(pricesUSDBytes)
	}))
	defer server.Close()

	o := &Oracle{RateCardStore: NewRateCardStore(server.URL, currencyCodeUSD)}
	_, err = o.RateCardStore.Refresh()
	require.NoError(t, err)
	hits.Store(0)

	// Fresh: no download.
	o.nextRefresh = time.Now().Add(time.Hour)
	require.NoError(t, o.ensurePricingData())
	assert.Equal(t, int32(0), hits.Load())

	// Stale: downloaded again.
	o.nextRefresh = time.Now().Add(-time.Second)
	require.NoError(t, o.ensurePricingData())
	assert.Equal(t, int32(1), hits.Load())
	assert.True(t, o.nextRefresh.After(time.Now()))

	// Stale and the price list unavailable: the prices already held are kept, and the next
	// attempt is not on the very next call.
	fail.Store(true)
	o.nextRefresh = time.Now().Add(-time.Second)
	require.NoError(t, o.ensurePricingData())
	assert.Equal(t, int32(2), hits.Load())
	assert.NotEmpty(t, o.RateCardStore.Store())
	assert.True(t, o.nextRefresh.After(time.Now()))
}
