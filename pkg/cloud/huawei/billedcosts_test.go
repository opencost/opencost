package huawei

import (
	"math"
	"testing"

	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"

	"github.com/opencost/opencost/pkg/cloud/models"
)

const testOtherCCEClusterID = "9f8e7d6c-aaaa-bbbb-cccc-ddddeeeeffff"

func cceResourceID(id, name string) string {
	return cceCloudServiceType + ":hws.resource.type.cce:" + id + ":" + name
}

func costRow(resourceID string, dailyCosts map[string]string) bssintlmodel.CostDataByDimension {
	dims := []bssintlmodel.DimensionGroup{{Key: strPtr("RESOURCE_ID"), Value: strPtr(resourceID)}}
	costs := make([]bssintlmodel.Cost, 0, len(dailyCosts))
	for day, amount := range dailyCosts {
		costs = append(costs, bssintlmodel.Cost{TimeDimensionValue: strPtr(day), Amount: strPtr(amount)})
	}
	return bssintlmodel.CostDataByDimension{Dimensions: &dims, Costs: &costs}
}

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func newBilledTestProvider(t *testing.T) *Huawei {
	t.Helper()
	return &Huawei{
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{}},
		ClusterRegion: "la-south-2",
	}
}

func TestBilledResourceID(t *testing.T) {
	cases := []struct {
		name, resourceID, wantID string
		wantOK                   bool
	}{
		{"cce cluster", cceResourceID(testCCEClusterID, "cce-prod"), testCCEClusterID, true},
		{"cce cluster without name", cceResourceID(testCCEClusterID, "null"), testCCEClusterID, true},
		{"not a cce cluster", "hws.service.type.ec2:hws.resource.type.vm:" + testCCEClusterID + ":node-1", "", false},
		{"null id", cceResourceID("null", "cce"), "", false},
		{"not composite", testCCEClusterID, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := billedResourceID(tc.resourceID)
			if id != tc.wantID || ok != tc.wantOK {
				t.Fatalf("got (%q, %v), want (%q, %v)", id, ok, tc.wantID, tc.wantOK)
			}
		})
	}
}

func TestBilledHourlyCosts(t *testing.T) {
	rows := []bssintlmodel.CostDataByDimension{
		// Billed for three days; the first (partial) day is dropped.
		costRow(cceResourceID(testCCEClusterID, "cce-prod"), map[string]string{
			"2026-09-20": "1.00",
			"2026-09-21": "4.80",
			"2026-09-22": "2.40",
		}),
		// A second row for the same cluster adds to the same day.
		costRow(cceResourceID(testCCEClusterID, "cce-prod"), map[string]string{
			"2026-09-22": "2.40",
		}),
		// Billed for a single day: kept, nothing to drop.
		costRow(cceResourceID(testOtherCCEClusterID, "cce-staging"), map[string]string{
			"2026-09-22": "2.40",
		}),
		// Not a CCE cluster: ignored.
		costRow("hws.service.type.ec2:hws.resource.type.vm:"+testCCEClusterID+":node-1", map[string]string{
			"2026-09-22": "100",
		}),
	}

	hourly := billedHourlyCosts(rows)

	// (4.80 + 4.80) / (2 days * 24h)
	if got := hourly[testCCEClusterID]; !approxEqual(got, 0.2) {
		t.Fatalf("expected 0.2/h for %s, got %v", testCCEClusterID, got)
	}
	if got := hourly[testOtherCCEClusterID]; !approxEqual(got, 0.1) {
		t.Fatalf("expected 0.1/h for %s, got %v", testOtherCCEClusterID, got)
	}
	if len(hourly) != 2 {
		t.Fatalf("expected only the two clusters, got %v", hourly)
	}
}
