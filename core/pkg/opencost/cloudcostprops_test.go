package opencost

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

func TestCloudCostProperties_hashKey(t *testing.T) {

	tests := map[string]struct {
		props *CloudCostProperties
		want  string
	}{
		"enpty props": {
			props: &CloudCostProperties{},
			want:  "cbf29ce484222325",
		},
		"All props no labels": {
			props: &CloudCostProperties{
				ProviderID:      "providerid1",
				Provider:        "provider1",
				AccountID:       "workgroup1",
				InvoiceEntityID: "billing1",
				Service:         "service1",
				Category:        "category1",
				Labels:          map[string]string{},
			},
			want: "a19b7dddf0032572",
		},
		"All props": {
			props: &CloudCostProperties{
				ProviderID:      "providerid1",
				Provider:        "provider1",
				AccountID:       "workgroup1",
				InvoiceEntityID: "billing1",
				Service:         "service1",
				Category:        "category1",
				Labels: map[string]string{
					"label1": "value1",
					"label2": "value2",
				},
			},
			want: "9d54403e40ad4db6",
		},
		"All props swap labels": {
			props: &CloudCostProperties{
				ProviderID:      "providerid1",
				Provider:        "provider1",
				AccountID:       "workgroup1",
				InvoiceEntityID: "billing1",
				Service:         "service1",
				Category:        "category1",
				Labels: map[string]string{
					"label2": "value2",
					"label1": "value1",
				},
			},
			want: "9d54403e40ad4db6",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := tt.props.hashKey(); got != tt.want {
				t.Errorf("hashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
