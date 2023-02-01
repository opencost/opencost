package kubecost

import (
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/util/timeutil"
)

var ccaProperties1 = CloudCostAggregateProperties{
	Provider:    "provider1",
	WorkGroupID: "workgroup1",
	BillingID:   "billing1",
	Service:     "service1",
	LabelValue:  "labelValue1",
}

func TestCloudCostAggregatePropertiesIntersection(t *testing.T) {
	testCases := map[string]struct {
		baseCCAP     CloudCostAggregateProperties
		intCCAP      CloudCostAggregateProperties
		expectedCCAP CloudCostAggregateProperties
	}{
		"When properties match between both CloudCostAggregateProperties": {
			baseCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "Service1",
				LabelValue:  "Label1",
			},
			intCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "Service1",
				LabelValue:  "Label1",
			},
			expectedCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "Service1",
				LabelValue:  "Label1",
			},
		},
		"When one of the properties differ in the two CloudCostAggregateProperties": {
			baseCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "Service1",
				LabelValue:  "Label1",
			},
			intCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "Service2",
				LabelValue:  "Label1",
			},
			expectedCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "",
				LabelValue:  "Label1",
			},
		},
		"When two of the properties differ in the two CloudCostAggregateProperties": {
			baseCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID1",
				BillingID:   "BillingID1",
				Service:     "Service1",
				LabelValue:  "Label1",
			},
			intCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "WorkGroupID2",
				BillingID:   "BillingID1",
				Service:     "Service2",
				LabelValue:  "Label1",
			},
			expectedCCAP: CloudCostAggregateProperties{
				Provider:    "CustomProvider",
				WorkGroupID: "",
				BillingID:   "BillingID1",
				Service:     "",
				LabelValue:  "Label1",
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actualCCAP := tc.baseCCAP.Intersection(tc.intCCAP)
			if actualCCAP.Provider != tc.expectedCCAP.Provider {
				t.Errorf("Case %s: Provider properties dont match with expected CloudCostAggregateProperties: %v actual %v", name, tc.expectedCCAP, actualCCAP)
			}
			if actualCCAP.WorkGroupID != tc.expectedCCAP.WorkGroupID {
				t.Errorf("Case %s: WorkGroupID properties dont match with expected CloudCostAggregateProperties: %v actual %v", name, tc.expectedCCAP, actualCCAP)
			}
			if actualCCAP.BillingID != tc.expectedCCAP.BillingID {
				t.Errorf("Case %s: BillingID properties dont match with expected CloudCostAggregateProperties: %v actual %v", name, tc.expectedCCAP, actualCCAP)
			}
			if actualCCAP.Service != tc.expectedCCAP.Service {
				t.Errorf("Case %s: Service properties dont match with expected CloudCostAggregateProperties: %v actual %v", name, tc.expectedCCAP, actualCCAP)
			}
			if actualCCAP.LabelValue != tc.expectedCCAP.LabelValue {
				t.Errorf("Case %s: LabelValue properties dont match with expected CloudCostAggregateProperties: %v actual %v", name, tc.expectedCCAP, actualCCAP)
			}
		})
	}
}

// TestCloudCostAggregate_LoadCloudCostAggregate checks that loaded CloudCostAggregates end up in the correct set in the
// correct proportions
func TestCloudCostAggregate_LoadCloudCostAggregate(t *testing.T) {
	// create values for 3 day Range tests
	end := RoundBack(time.Now().UTC(), timeutil.Day)
	start := end.Add(-3 * timeutil.Day)
	dayWindows, _ := GetWindows(start, end, timeutil.Day)
	emtpyCASSR, _ := NewCloudCostAggregateSetRange(start, end, timeutil.Day, "integration", "label")
	testCases := map[string]struct {
		cca      []*CloudCostAggregate
		windows  []Window
		ccasr    *CloudCostAggregateSetRange
		expected []*CloudCostAggregateSet
	}{
		"Load Single Day On Grid": {
			cca: []*CloudCostAggregate{
				{
					Properties:        ccaProperties1,
					KubernetesPercent: 1,
					Cost:              100,
					NetCost:           80,
				},
			},
			windows: []Window{
				dayWindows[0],
			},
			ccasr: emtpyCASSR.Clone(),
			expected: []*CloudCostAggregateSet{
				{
					Integration: "integration",
					LabelName:   "label",
					Window:      dayWindows[0],
					CloudCostAggregates: map[string]*CloudCostAggregate{
						ccaProperties1.Key(nil): {
							Properties:        ccaProperties1,
							KubernetesPercent: 1,
							Cost:              100,
							NetCost:           80,
						},
					},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[1],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[2],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
			},
		},
		"Load Single Day Off Grid": {
			cca: []*CloudCostAggregate{
				{
					Properties:        ccaProperties1,
					KubernetesPercent: 1,
					Cost:              100,
					NetCost:           80,
				},
			},
			windows: []Window{
				NewClosedWindow(start.Add(12*time.Hour), start.Add(36*time.Hour)),
			},
			ccasr: emtpyCASSR.Clone(),
			expected: []*CloudCostAggregateSet{
				{
					Integration: "integration",
					LabelName:   "label",
					Window:      dayWindows[0],
					CloudCostAggregates: map[string]*CloudCostAggregate{
						ccaProperties1.Key(nil): {
							Properties:        ccaProperties1,
							KubernetesPercent: 1,
							Cost:              50,
							NetCost:           40,
						},
					},
				},
				{
					Integration: "integration",
					LabelName:   "label",
					Window:      dayWindows[1],
					CloudCostAggregates: map[string]*CloudCostAggregate{
						ccaProperties1.Key(nil): {
							Properties:        ccaProperties1,
							KubernetesPercent: 1,
							Cost:              50,
							NetCost:           40,
						},
					},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[2],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
			},
		},
		"Load Single Day Off Grid Before Range Window": {
			cca: []*CloudCostAggregate{
				{
					Properties:        ccaProperties1,
					KubernetesPercent: 1,
					Cost:              100,
					NetCost:           80,
				},
			},
			windows: []Window{
				NewClosedWindow(start.Add(-12*time.Hour), start.Add(12*time.Hour)),
			},
			ccasr: emtpyCASSR.Clone(),
			expected: []*CloudCostAggregateSet{
				{
					Integration: "integration",
					LabelName:   "label",
					Window:      dayWindows[0],
					CloudCostAggregates: map[string]*CloudCostAggregate{
						ccaProperties1.Key(nil): {
							Properties:        ccaProperties1,
							KubernetesPercent: 1,
							Cost:              50,
							NetCost:           40,
						},
					},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[1],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[2],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
			},
		},
		"Load Single Day Off Grid After Range Window": {
			cca: []*CloudCostAggregate{
				{
					Properties:        ccaProperties1,
					KubernetesPercent: 1,
					Cost:              100,
					NetCost:           80,
				},
			},
			windows: []Window{
				NewClosedWindow(end.Add(-12*time.Hour), end.Add(12*time.Hour)),
			},
			ccasr: emtpyCASSR.Clone(),
			expected: []*CloudCostAggregateSet{
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[0],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[1],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
				{
					Integration: "integration",
					LabelName:   "label",
					Window:      dayWindows[2],
					CloudCostAggregates: map[string]*CloudCostAggregate{
						ccaProperties1.Key(nil): {
							Properties:        ccaProperties1,
							KubernetesPercent: 1,
							Cost:              50,
							NetCost:           40,
						},
					},
				},
			},
		},
		"Single Day Kubecost Percent": {
			cca: []*CloudCostAggregate{
				{
					Properties:        ccaProperties1,
					KubernetesPercent: 1,
					Cost:              75,
					NetCost:           60,
				},
				{
					Properties:        ccaProperties1,
					KubernetesPercent: 0,
					Cost:              25,
					NetCost:           20,
				},
			},
			windows: []Window{
				dayWindows[1],
				dayWindows[1],
			},
			ccasr: emtpyCASSR.Clone(),
			expected: []*CloudCostAggregateSet{
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[0],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
				{
					Integration: "integration",
					LabelName:   "label",
					Window:      dayWindows[1],
					CloudCostAggregates: map[string]*CloudCostAggregate{
						ccaProperties1.Key(nil): {
							Properties:        ccaProperties1,
							KubernetesPercent: 0.75,
							Cost:              100,
							NetCost:           80,
						},
					},
				},
				{
					Integration:         "integration",
					LabelName:           "label",
					Window:              dayWindows[2],
					CloudCostAggregates: map[string]*CloudCostAggregate{},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// load Cloud Cost Aggregates
			for i, cca := range tc.cca {
				tc.ccasr.LoadCloudCostAggregate(tc.windows[i], cca)
			}

			if len(tc.ccasr.CloudCostAggregateSets) != len(tc.expected) {
				t.Errorf("the CloudCostAggregateSetRanges did not have the expected length")
			}

			for i, ccas := range tc.ccasr.CloudCostAggregateSets {
				if !ccas.Equal(tc.expected[i]) {
					t.Errorf("CloudCostAggregateSet at index: %d did not match expected", i)
				}
			}
		})
	}

}
