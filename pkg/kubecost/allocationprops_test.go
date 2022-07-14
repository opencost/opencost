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
		"agg by owner with labels": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Labels:      map[string]string{"app": "cost-analyzer"},
				Annotations: map[string]string{"owner": "test owner 123"},
			},
			expected: "test owner 123",
		},
		"agg by owner without labels": {
			aggregate: []string{"owner"},
			allocationProps: &AllocationProperties{
				Annotations: map[string]string{"owner": "test owner 123"},
			},
			expected: "test owner 123",
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
