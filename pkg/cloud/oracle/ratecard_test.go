package oracle

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRCSForKey(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	testCases := map[string]struct {
		cost string
		arm  bool
	}{
		"VM.DenseIO.E4.Flex": {
			"0.014061",
			false,
		},
		"BM.GPU3.8": {
			"3.01455",
			false,
		},
		"Pod.Standard.A1.Flex": {
			"0.0115",
			true,
		},
		"VM.Standard.E4.Flex": {
			"0.014000",
			false,
		},
		"VM.Standard.E3.Flex": {
			"0.014000",
			false,
		},
		"unknown-shape.2o.32g.1_2b": {
			"0.600000",
			false,
		},
		"unknown-shape": {
			"0.600000",
			false,
		},
	}

	for instanceType, testCase := range testCases {
		t.Run(instanceType, func(t *testing.T) {
			key := &oracleKey{
				instanceType: instanceType,
				labels:       make(map[string]string),
			}
			if testCase.arm {
				key.labels["kubernetes.io/arch"] = "arm64"
			}
			node, _, err := rcs.ForKey(key, DefaultPricing{
				OCPU:   "0.2",
				Memory: "0.1",
				GPU:    "0.3",
			})
			require.NoError(t, err)
			assertFloatStrings(t, testCase.cost, node.Cost, 0.001)
		})
	}
}

func TestRCSForKey_KarpenterFlexShape(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	defaultPricing := DefaultPricing{
		OCPU:   "0.2",
		Memory: "0.1",
		GPU:    "0.3",
	}

	testCases := map[string]struct {
		baseShape     string
		flexShape     string
		cpuMultiplier float64
		assertCost    bool
	}{
		"baseline-1_1": {
			baseShape:     "VM.Standard.E3.Flex",
			flexShape:     "VM.Standard.E3.Flex.2o.32g.1_1b",
			cpuMultiplier: 1,
			assertCost:    true,
		},
		"baseline-1_2": {
			baseShape:     "VM.Standard.E4.Flex",
			flexShape:     "VM.Standard.E4.Flex.8o.32g.1_2b",
			cpuMultiplier: 0.5,
		},
		"baseline-1_8": {
			baseShape:     "VM.Standard.E4.Flex",
			flexShape:     "VM.Standard.E4.Flex.8o.32g.1_8b",
			cpuMultiplier: 0.125,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			baseNode, _, err := rcs.ForKey(&oracleKey{instanceType: testCase.baseShape, labels: make(map[string]string)}, defaultPricing)
			require.NoError(t, err)

			flexNode, _, err := rcs.ForKey(&oracleKey{instanceType: testCase.flexShape, labels: make(map[string]string)}, defaultPricing)
			require.NoError(t, err)

			assertFloatStrings(t, baseNode.RAMCost, flexNode.RAMCost, 0.000001)
			assert.InDelta(t, mustParseFloat(t, baseNode.VCPUCost)*testCase.cpuMultiplier, mustParseFloat(t, flexNode.VCPUCost), 0.000001)
			if testCase.assertCost {
				assertFloatStrings(t, baseNode.Cost, flexNode.Cost, 0.000001)
			}
		})
	}
}

func TestRCSForPVK(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	var testCases = map[string]struct {
		cost string
	}{
		driverOCIBV: {
			"0.000034",
		},
		driverOCI: {
			"0.000034",
		},
		"unknown": {
			"0.25",
		},
	}

	for driver, testCase := range testCases {
		t.Run(driver, func(t *testing.T) {
			pvk := &oraclePVKey{
				driver: driver,
			}
			pv, err := rcs.ForPVK(pvk, DefaultPricing{
				Storage: "0.25",
			})
			require.NoError(t, err)
			assertFloatStrings(t, testCase.cost, pv.Cost, 0.00001)
		})
	}
}

func TestRCSEgressForRegion(t *testing.T) {
	rcs, server := testSetupRateCardStore(t)
	defer server.Close()

	var testCases = map[string]struct {
		cost float64
	}{
		"ap-mumbai-1": {
			0.025,
		},
		"sa-saopaulo-1": {
			0.025,
		},
		"me-dubai-1": {
			0.05,
		},
		"af-johannesburg-1": {
			0.05,
		},
		"il-jerusalem-1": {
			0.05,
		},
		"eu-madrid-1": {
			0.0085,
		},
		"uk-cardiff-1": {
			0.0085,
		},
		"mx-monterrey-1": {
			0.0085,
		},
		"us-chicago-1": {
			0.0085,
		},
		"ca-montreal-1": {
			0.0085,
		},
		"unknown": {
			0.000123,
		},
	}
	for region, testCase := range testCases {
		net, err := rcs.ForEgressRegion(region, DefaultPricing{
			Egress: "0.000123",
		})
		assert.NoError(t, err)
		assert.InDelta(t, float64(0), net.ZoneNetworkEgressCost, 0.1)
		assert.InDelta(t, testCase.cost, net.RegionNetworkEgressCost, 0.001)
		assert.InDelta(t, testCase.cost, net.InternetNetworkEgressCost, 0.001)
	}
}

func testSetupRateCardStore(t *testing.T) (*RateCardStore, *httptest.Server) {
	pricesUSDBytes, err := os.ReadFile("test/prices_usd.json")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(pricesUSDBytes)
	}))

	rcs := NewRateCardStore(server.URL, currencyCodeUSD)
	store, err := rcs.Refresh()
	require.NoError(t, err)
	require.NotEmpty(t, store)
	return rcs, server
}

func assertFloatStrings(t *testing.T, s1, s2 string, delta float64) {
	t.Helper()
	f1, err := strconv.ParseFloat(s1, 64)
	require.NoError(t, err)
	f2, err := strconv.ParseFloat(s2, 64)
	require.NoError(t, err)
	assert.InDelta(t, f1, f2, delta)
}

func mustParseFloat(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	require.NoError(t, err)
	return f
}
