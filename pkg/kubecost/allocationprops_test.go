package kubecost

import (
	"reflect"
	"testing"
)

func TestGenerateKey(t *testing.T) {

	cases := map[string]struct {
		aggregate       []string
		allocationProps *AllocationProperties
		expected        string
	}{
		"aggregateregate by owner without owner labels": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "cost-analyzer"},
				Annotations: map[string]string{"owner": "test owner 123"},
			},
			expected: "test owner 123",
		},
		"aggregate by owner without labels": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Annotations: map[string]string{"owner": "test owner 123"},
			},
			expected: "test owner 123",
		},
		"aggregate by owner with owner label and annotation": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"owner": "owner-label"},
				Annotations: map[string]string{"owner": "owner-annotation"},
			},
			expected: "owner-label",
		},
		"aggregate by environment with environment label and annotation": {
			aggregate: []string{"environment"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"env": "environment-label"},
				Annotations: map[string]string{"env": "environment-annotation"},
			},
			expected: "environment-label",
		},
		"aggregate by department with department label and annotation": {
			aggregate: []string{"department"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"department": "department-label"},
				Annotations: map[string]string{"department": "department-annotation"},
			},
			expected: "department-label",
		},
		"aggregate by team with team label and annotation": {
			aggregate: []string{"team"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"team": "team-label"},
				Annotations: map[string]string{"team": "team-annotation"},
			},
			expected: "team-label",
		},
		"aggregate by product with product label and annotation": {
			aggregate: []string{"product"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "product-label"},
				Annotations: map[string]string{"app": "product-annotation"},
			},
			expected: "product-label",
		},
		"aggregate by product and owner with multiple labels and annotations": {
			aggregate: []string{"product", "owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "product-label", "owner": "owner-label", "team": "team-label"},
				Annotations: map[string]string{"app": "product-annotation", "owner": "owner-annotation", "team": "team-annotation"},
			},
			expected: "product-label/owner-label",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {

			lc := NewLabelConfig()

			result := tc.allocationProps.GenerateKey(tc.aggregate, lc)

			if !reflect.DeepEqual(result, tc.expected) {
				t.Fatalf("expected %+v; got %+v", tc.expected, result)
			}
		})
	}
}
