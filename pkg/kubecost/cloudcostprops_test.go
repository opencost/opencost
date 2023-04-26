package kubecost

import "testing"

func TestCloudCostPropertiesIntersection(t *testing.T) {
	testCases := map[string]struct {
		baseCCP     *CloudCostProperties
		intCCP      *CloudCostProperties
		expectedCCP *CloudCostProperties
	}{
		"When properties match between both CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
		},
		"When one of the properties differ in the two CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service2",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
		},
		"When two of the properties differ in the two CloudCostProperties": {
			baseCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID2",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service2",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
				},
			},
		},
		"When labels differ": {
			baseCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				},
			},
			intCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key1": "value2",
					"key2": "value2",
					"key4": "value4",
				},
			},
			expectedCCP: &CloudCostProperties{
				Provider:        "CustomProvider",
				ProviderID:      "ProviderID1",
				AccountID:       "WorkGroupID1",
				InvoiceEntityID: "InvoiceEntityID1",
				Service:         "Service1",
				Category:        "Category1",
				Labels: map[string]string{
					"key2": "value2",
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actualCCP := tc.baseCCP.Intersection(tc.intCCP)

			if !actualCCP.Equal(tc.expectedCCP) {
				t.Errorf("Case %s: properties dont match with expected CloudCostProperties: %v actual %v", name, tc.expectedCCP, actualCCP)
			}
		})
	}
}
