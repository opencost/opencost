package costmodel

import (
	"reflect"
	"testing"
)

func TestMergeTypeMaps(t *testing.T) {
	cases := []struct {
		name     string
		map1     map[nodeIdentifierNoProviderID]string
		map2     map[nodeIdentifierNoProviderID]string
		expected map[nodeIdentifierNoProviderID]string
	}{
		{
			name:     "both empty",
			map1:     map[nodeIdentifierNoProviderID]string{},
			map2:     map[nodeIdentifierNoProviderID]string{},
			expected: map[nodeIdentifierNoProviderID]string{},
		},
		{
			name: "map2 empty",
			map1: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
			map2: map[nodeIdentifierNoProviderID]string{},
			expected: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
		},
		{
			name: "map2 empty",
			map1: map[nodeIdentifierNoProviderID]string{},
			map2: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
			expected: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
		},
		{
			name: "no overlap",
			map1: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
			map2: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: "type2",
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node4",
				}: "type4",
			},
			expected: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: "type2",
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node4",
				}: "type4",
			},
		},
		{
			name: "with overlap",
			map1: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
			map2: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: "type2",
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type4",
			},
			expected: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: "type2",
			},
		},
	}

	for _, testCase := range cases {
		result := mergeTypeMaps(testCase.map1, testCase.map2)

		if !reflect.DeepEqual(result, testCase.expected) {
			t.Errorf("mergeTypeMaps case %s failed. Got %+v but expected %+v", testCase.name, result, testCase.expected)
		}
	}
}
