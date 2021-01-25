package costmodel

import (
	"reflect"
	"testing"
	"time"

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
		{
			name: "just cpu cost with empty provider ID",
			cpuCostMap: map[NodeIdentifier]float64{
				NodeIdentifier{
					Cluster: "cluster1",
					Name:    "node1",
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
					Cluster: "cluster1",
					Name:    "node1",
				}: &Node{
					Cluster:      "cluster1",
					Name:         "node1",
					NodeType:     "type1",
					CPUCost:      0.048,
					CPUBreakdown: &ClusterCostsBreakdown{},
					RAMBreakdown: &ClusterCostsBreakdown{},
				},
			},
		},
		{
			name: "cpu cost with overlapping node names",
			cpuCostMap: map[NodeIdentifier]float64{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: 0.048,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: 0.087,
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
					ProviderID: "prov_node1_A",
				}: &Node{
					Cluster:      "cluster1",
					Name:         "node1",
					ProviderID:   "prov_node1_A",
					NodeType:     "type1",
					CPUCost:      0.048,
					CPUBreakdown: &ClusterCostsBreakdown{},
					RAMBreakdown: &ClusterCostsBreakdown{},
				},
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: &Node{
					Cluster:      "cluster1",
					Name:         "node1",
					ProviderID:   "prov_node1_B",
					NodeType:     "type1",
					CPUCost:      0.087,
					CPUBreakdown: &ClusterCostsBreakdown{},
					RAMBreakdown: &ClusterCostsBreakdown{},
				},
			},
		},
		{
			name: "all fields + overlapping node names",
			cpuCostMap: map[NodeIdentifier]float64{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: 0.048,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: 0.087,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
				}: 0.033,
			},
			ramCostMap: map[NodeIdentifier]float64{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: 0.09,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: 0.3,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
				}: 0.024,
			},
			gpuCostMap: map[NodeIdentifier]float64{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: 0.8,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: 1.4,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
				}: 3.1,
			},
			cpuCoresMap: map[nodeIdentifierNoProviderID]float64{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: 2.0,
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: 5.0,
			},
			ramBytesMap: map[nodeIdentifierNoProviderID]float64{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: 2048.0,
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: 6303.0,
			},
			ramUserPctMap: map[nodeIdentifierNoProviderID]float64{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: 30.0,
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: 42.6,
			},
			ramSystemPctMap: map[nodeIdentifierNoProviderID]float64{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: 15.0,
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: 20.1,
			},
			cpuBreakdownMap: map[nodeIdentifierNoProviderID]*ClusterCostsBreakdown{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: &ClusterCostsBreakdown{
					System: 20.2,
					User:   68.0,
				},
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: &ClusterCostsBreakdown{
					System: 28.9,
					User:   34.0,
				},
			},
			activeDataMap: map[NodeIdentifier]activeData{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: activeData{
					start:   time.Date(2020, 6, 16, 3, 45, 28, 0, time.UTC),
					end:     time.Date(2020, 6, 16, 9, 20, 39, 0, time.UTC),
					minutes: 5*60 + 35 + (11.0 / 60.0),
				},
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: activeData{
					start:   time.Date(2020, 6, 16, 3, 45, 28, 0, time.UTC),
					end:     time.Date(2020, 6, 16, 9, 21, 39, 0, time.UTC),
					minutes: 5*60 + 36 + (11.0 / 60.0),
				},
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
				}: activeData{
					start:   time.Date(2020, 6, 16, 3, 45, 28, 0, time.UTC),
					end:     time.Date(2020, 6, 16, 9, 10, 39, 0, time.UTC),
					minutes: 5*60 + 25 + (11.0 / 60.0),
				},
			},
			preemptibleMap: map[NodeIdentifier]bool{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: true,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: false,
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
				}: false,
			},
			clusterAndNameToType: map[nodeIdentifierNoProviderID]string{
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node1",
				}: "type1",
				nodeIdentifierNoProviderID{
					Cluster: "cluster1",
					Name:    "node2",
				}: "type2",
			},
			expected: map[NodeIdentifier]*Node{
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
				}: &Node{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_A",
					NodeType:   "type1",
					CPUCost:    0.048,
					RAMCost:    0.09,
					GPUCost:    0.8,
					CPUCores:   2.0,
					RAMBytes:   2048.0,
					RAMBreakdown: &ClusterCostsBreakdown{
						User:   30.0,
						System: 15.0,
					},
					CPUBreakdown: &ClusterCostsBreakdown{
						System: 20.2,
						User:   68.0,
					},
					Start:       time.Date(2020, 6, 16, 3, 45, 28, 0, time.UTC),
					End:         time.Date(2020, 6, 16, 9, 20, 39, 0, time.UTC),
					Minutes:     5*60 + 35 + (11.0 / 60.0),
					Preemptible: true,
				},
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
				}: &Node{
					Cluster:    "cluster1",
					Name:       "node1",
					ProviderID: "prov_node1_B",
					NodeType:   "type1",
					CPUCost:    0.087,
					RAMCost:    0.3,
					GPUCost:    1.4,
					CPUCores:   2.0,
					RAMBytes:   2048.0,
					RAMBreakdown: &ClusterCostsBreakdown{
						User:   30.0,
						System: 15.0,
					},
					CPUBreakdown: &ClusterCostsBreakdown{
						System: 20.2,
						User:   68.0,
					},
					Start:       time.Date(2020, 6, 16, 3, 45, 28, 0, time.UTC),
					End:         time.Date(2020, 6, 16, 9, 21, 39, 0, time.UTC),
					Minutes:     5*60 + 36 + (11.0 / 60.0),
					Preemptible: false,
				},
				NodeIdentifier{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
				}: &Node{
					Cluster:    "cluster1",
					Name:       "node2",
					ProviderID: "prov_node2_A",
					NodeType:   "type2",
					CPUCost:    0.033,
					RAMCost:    0.024,
					GPUCost:    3.1,
					CPUCores:   5.0,
					RAMBytes:   6303.0,
					RAMBreakdown: &ClusterCostsBreakdown{
						User:   42.6,
						System: 20.1,
					},
					CPUBreakdown: &ClusterCostsBreakdown{
						System: 28.9,
						User:   34.0,
					},
					Start:       time.Date(2020, 6, 16, 3, 45, 28, 0, time.UTC),
					End:         time.Date(2020, 6, 16, 9, 10, 39, 0, time.UTC),
					Minutes:     5*60 + 25 + (11.0 / 60.0),
					Preemptible: false,
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
