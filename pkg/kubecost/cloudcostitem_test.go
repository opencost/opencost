package kubecost

import (
	"github.com/opencost/opencost/pkg/util/timeutil"
	"testing"
	"time"
)

var cciProperties1 = CloudCostItemProperties{
	ProviderID:  "providerid1",
	Provider:    "provider1",
	WorkGroupID: "workgroup1",
	BillingID:   "billing1",
	Service:     "service1",
	Category:    "category1",
	Labels: map[string]string{
		"label1": "value1",
		"label2": "value2",
	},
}

// TestCloudCostItem_LoadCloudCostItem checks that loaded CloudCostItems end up in the correct set in the
// correct proportions
func TestCloudCostItem_LoadCloudCostItem(t *testing.T) {
	// create values for 3 day Range tests
	end := RoundBack(time.Now().UTC(), timeutil.Day)
	start := end.Add(-3 * timeutil.Day)
	dayWindows, _ := GetWindows(start, end, timeutil.Day)
	emtpyCCISR, _ := NewCloudCostItemSetRange(start, end, timeutil.Day, "integration")
	testCases := map[string]struct {
		cci      []*CloudCostItem
		ccisr    *CloudCostItemSetRange
		expected []*CloudCostItemSet
	}{
		"Load Single Day On Grid": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       dayWindows[0],
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       dayWindows[0],
							IsKubernetes: true,
							Cost:         100,
							NetCost:      80,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
		"Load Single Day Off Grid": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       NewClosedWindow(start.Add(12*time.Hour), start.Add(36*time.Hour)),
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(start.Add(12*time.Hour), start.Add(24*time.Hour)),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(start.Add(24*time.Hour), start.Add(36*time.Hour)),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
		"Load Single Day Off Grid Before Range Window": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       NewClosedWindow(start.Add(-12*time.Hour), start.Add(12*time.Hour)),
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration: "integration",
					Window:      dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(start, start.Add(12*time.Hour)),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
		"Load Single Day Off Grid After Range Window": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       NewClosedWindow(end.Add(-12*time.Hour), end.Add(12*time.Hour)),
					IsKubernetes: true,
					Cost:         100,
					NetCost:      80,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration:    "integration",
					Window:         dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       NewClosedWindow(end.Add(-12*time.Hour), end),
							IsKubernetes: true,
							Cost:         50,
							NetCost:      40,
						},
					},
				},
			},
		},
		"Single Day Kubecost Percent": {
			cci: []*CloudCostItem{
				{
					Properties:   cciProperties1,
					Window:       dayWindows[1],
					IsKubernetes: true,
					Cost:         75,
					NetCost:      60,
				},
				{
					Properties:   cciProperties1,
					Window:       dayWindows[1],
					IsKubernetes: true,
					Cost:         25,
					NetCost:      20,
				},
			},
			ccisr: emtpyCCISR.Clone(),
			expected: []*CloudCostItemSet{
				{
					Integration:    "integration",
					Window:         dayWindows[0],
					CloudCostItems: map[string]*CloudCostItem{},
				},
				{
					Integration: "integration",
					Window:      dayWindows[1],
					CloudCostItems: map[string]*CloudCostItem{
						cciProperties1.Key(): {
							Properties:   cciProperties1,
							Window:       dayWindows[1],
							IsKubernetes: true,
							Cost:         100,
							NetCost:      80,
						},
					},
				},
				{
					Integration:    "integration",
					Window:         dayWindows[2],
					CloudCostItems: map[string]*CloudCostItem{},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// load Cloud Cost Items
			for _, cci := range tc.cci {
				tc.ccisr.LoadCloudCostItem(cci)
			}

			if len(tc.ccisr.CloudCostItemSets) != len(tc.expected) {
				t.Errorf("the CloudCostItemSetRanges did not have the expected length")
			}

			for i, ccis := range tc.ccisr.CloudCostItemSets {
				if !ccis.Equal(tc.expected[i]) {
					t.Errorf("CloudCostItemSet at index: %d did not match expected", i)
				}
			}
		})
	}

}
