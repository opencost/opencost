package costmodel

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
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

func TestBuildNodeMap(t *testing.T) {
	cases := []struct {
		name                 string
		cpuCostMap           map[NodeIdentifier]float64
		ramCostMap           map[NodeIdentifier]float64
		gpuCostMap           map[NodeIdentifier]float64
		cpuCoresMap          map[nodeIdentifierNoProviderID]float64
		ramBytesMap          map[nodeIdentifierNoProviderID]float64
		ramUserPctMap        map[nodeIdentifierNoProviderID]float64
		ramSystemPctMap      map[nodeIdentifierNoProviderID]float64
		cpuBreakdownMap      map[nodeIdentifierNoProviderID]*ClusterCostsBreakdown
		activeDataMap        map[NodeIdentifier]activeData
		preemptibleMap       map[NodeIdentifier]bool
		clusterAndNameToType map[nodeIdentifierNoProviderID]string
		expected             map[NodeIdentifier]*Node
	}{
		{
			name:     "empty",
			expected: map[NodeIdentifier]*Node{},
		},
		{
			name: "just cpu cost",
			cpuCostMap: map[NodeIdentifier]float64{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1",
				}: 0.048,
			},
			clusterAndNameToType: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
			},
			expected: map[NodeIdentifier]*Node{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1",
				}: &Node{
					Cluster:      "cluster1",
					Name:         "node1",
					ProviderID:   "prov_node1",
					NodeType:     "type1",
					CPUCost:      0.048,
					CPUBreakdown: &ClusterCostsBreakdown{},
					RAMBreakdown: &ClusterCostsBreakdown{},
				},
			},
		},
	}

	for _, testCase := range cases {

		result := buildNodeMap(
			testCase.cpuCostMap, testCase.ramCostMap, testCase.gpuCostMap,
			testCase.cpuCoresMap, testCase.ramBytesMap, testCase.ramUserPctMap,
			testCase.ramSystemPctMap,
			testCase.cpuBreakdownMap,
			testCase.activeDataMap,
			testCase.preemptibleMap,
			testCase.clusterAndNameToType,
		)

		if !reflect.DeepEqual(result, testCase.expected) {
			t.Errorf("buildNodeMap case %s failed. Got %+v but expected %+v", testCase.name, result, testCase.expected)

			// Use spew because we have to follow pointers to figure out
			// what isn't matching up
			t.Logf("Got: %s", spew.Sdump(result))
			t.Logf("Expected: %s", spew.Sdump(testCase.expected))
		}
	}
}
