package costmodel

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
)

const Ki = 1024
const Mi = Ki * 1024
const Gi = Mi * 1024

const second = 1.0
const minute = second * 60.0
const hour = minute * 60.0

var windowStart = time.Date(2020, 6, 16, 0, 0, 0, 0, time.UTC)
var windowEnd = time.Date(2020, 6, 17, 0, 0, 0, 0, time.UTC)
var window = opencost.NewWindow(&windowStart, &windowEnd)

var startFloat = float64(windowStart.Unix())
var endFloat = float64(windowEnd.Unix())

var podKey1 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster1",
		Namespace: "namespace1",
	},
	Pod: "pod1",
}
var podKey2 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster1",
		Namespace: "namespace1",
	},
	Pod: "pod2",
}
var podKey3 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster2",
		Namespace: "namespace2",
	},
	Pod: "pod3",
}

var podKey4 = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster2",
		Namespace: "namespace2",
	},
	Pod: "pod4",
}

var podKeyUnmounted = podKey{
	namespaceKey: namespaceKey{
		Cluster:   "cluster2",
		Namespace: opencost.UnmountedSuffix,
	},
	Pod: opencost.UnmountedSuffix,
}

var kcPVKey1 = opencost.PVKey{
	Cluster: "cluster1",
	Name:    "pv1",
}

var kcPVKey2 = opencost.PVKey{
	Cluster: "cluster1",
	Name:    "pv2",
}

var kcPVKey3 = opencost.PVKey{
	Cluster: "cluster2",
	Name:    "pv3",
}

var kcPVKey4 = opencost.PVKey{
	Cluster: "cluster2",
	Name:    "pv4",
}

var podMap1 = map[podKey]*pod{
	podKey1: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 0, 0, 0, 0, time.UTC),
		Key:         podKey1,
		Allocations: nil,
	},
	podKey2: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 12, 0, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 0, 0, 0, 0, time.UTC),
		Key:         podKey2,
		Allocations: nil,
	},
	podKey3: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 6, 30, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 18, 12, 33, 0, time.UTC),
		Key:         podKey3,
		Allocations: nil,
	},
	podKey4: {
		Window:      window.Clone(),
		Start:       time.Date(2020, 6, 16, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2020, 6, 17, 13, 0, 0, 0, time.UTC),
		Key:         podKey4,
		Allocations: nil,
	},
	podKeyUnmounted: {
		Window: window.Clone(),
		Start:  *window.Start(),
		End:    *window.End(),
		Key:    podKeyUnmounted,
		Allocations: map[string]*opencost.Allocation{
			opencost.UnmountedSuffix: {
				Name: fmt.Sprintf("%s/%s/%s/%s", podKeyUnmounted.Cluster, podKeyUnmounted.Namespace, podKeyUnmounted.Pod, opencost.UnmountedSuffix),
				Properties: &opencost.AllocationProperties{
					Cluster:   podKeyUnmounted.Cluster,
					Node:      "",
					Container: opencost.UnmountedSuffix,
					Namespace: podKeyUnmounted.Namespace,
					Pod:       podKeyUnmounted.Pod,
					Services:  []string{"LB1"},
				},
				Window:                     window,
				Start:                      *window.Start(),
				End:                        *window.End(),
				LoadBalancerCost:           0.60,
				LoadBalancerCostAdjustment: 0,
				PVs: opencost.PVAllocations{
					kcPVKey2: &opencost.PVAllocation{
						ByteHours: 24 * Gi,
						Cost:      2.25,
					},
				},
			},
		},
	},
}

var pvKey1 = pvKey{
	Cluster:          "cluster1",
	PersistentVolume: "pv1",
}

var pvKey2 = pvKey{
	Cluster:          "cluster1",
	PersistentVolume: "pv2",
}

var pvKey3 = pvKey{
	Cluster:          "cluster2",
	PersistentVolume: "pv3",
}

var pvKey4 = pvKey{
	Cluster:          "cluster2",
	PersistentVolume: "pv4",
}

var pvMap1 = map[pvKey]*pv{
	pvKey1: {
		Start:          windowStart,
		End:            windowEnd.Add(time.Hour * -6),
		Bytes:          20 * Gi,
		CostPerGiBHour: 0.05,
		Cluster:        "cluster1",
		Name:           "pv1",
		StorageClass:   "class1",
	},
	pvKey2: {
		Start:          windowStart,
		End:            windowEnd,
		Bytes:          100 * Gi,
		CostPerGiBHour: 0.05,
		Cluster:        "cluster1",
		Name:           "pv2",
		StorageClass:   "class1",
	},
	pvKey3: {
		Start:          windowStart.Add(time.Hour * 6),
		End:            windowEnd.Add(time.Hour * -6),
		Bytes:          50 * Gi,
		CostPerGiBHour: 0.03,
		Cluster:        "cluster2",
		Name:           "pv3",
		StorageClass:   "class2",
	},
	pvKey4: {
		Start:          windowStart,
		End:            windowEnd.Add(time.Hour * -6),
		Bytes:          30 * Gi,
		CostPerGiBHour: 0.05,
		Cluster:        "cluster2",
		Name:           "pv4",
		StorageClass:   "class1",
	},
}

func TestBuildPVMap(t *testing.T) {
	pvMap1NoBytes := make(map[pvKey]*pv, len(pvMap1))
	for thisPVKey, thisPV := range pvMap1 {
		clonePV := thisPV.clone()
		clonePV.Bytes = 0.0
		clonePV.StorageClass = ""
		pvMap1NoBytes[thisPVKey] = clonePV
	}

	testCases := map[string]struct {
		resolution              time.Duration
		resultsPVCostPerGiBHour []*source.QueryResult
		resultsActiveMinutes    []*source.QueryResult
		expected                map[pvKey]*pv
	}{
		"pvMap1": {
			resolution: time.Hour * 6,
			resultsPVCostPerGiBHour: []*source.QueryResult{
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id": "cluster1",
						"volumename": "pv1",
					},
					[]*util.Vector{
						{
							Value: 0.05,
						},
					},
					source.DefaultResultKeys(),
				),
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id": "cluster1",
						"volumename": "pv2",
					},
					[]*util.Vector{
						{
							Value: 0.05,
						},
					},
					source.DefaultResultKeys(),
				),
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id": "cluster2",
						"volumename": "pv3",
					},
					[]*util.Vector{
						{
							Value: 0.03,
						},
					},
					source.DefaultResultKeys(),
				),
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id": "cluster2",
						"volumename": "pv4",
					},
					[]*util.Vector{
						{
							Value: 0.05,
						},
					},
					source.DefaultResultKeys(),
				),
			},
			resultsActiveMinutes: []*source.QueryResult{
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id":       "cluster1",
						"persistentvolume": "pv1",
					},
					[]*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
					source.DefaultResultKeys(),
				),
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id":       "cluster1",
						"persistentvolume": "pv2",
					},
					[]*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
						{
							Timestamp: startFloat + (hour * 24),
						},
					},
					source.DefaultResultKeys(),
				),
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id":       "cluster2",
						"persistentvolume": "pv3",
					},
					[]*util.Vector{
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
					source.DefaultResultKeys(),
				),
				source.NewQueryResult(
					map[string]interface{}{
						"cluster_id":       "cluster2",
						"persistentvolume": "pv4",
					},
					[]*util.Vector{
						{
							Timestamp: startFloat,
						},
						{
							Timestamp: startFloat + (hour * 6),
						},
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
					source.DefaultResultKeys(),
				),
			},
			expected: pvMap1NoBytes,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			pvMap := make(map[pvKey]*pv)
			pvCostResults := source.DecodeAll(testCase.resultsPVCostPerGiBHour, source.DecodePVPricePerGiBHourResult)
			pvActiveMinsResults := source.DecodeAll(testCase.resultsActiveMinutes, source.DecodePVActiveMinutesResult)
			pvInfoResult := []*source.PVInfoResult{}

			buildPVMap(testCase.resolution, pvMap, pvCostResults, pvActiveMinsResults, pvInfoResult, window)
			if len(pvMap) != len(testCase.expected) {
				t.Errorf("pv map does not have the expected length %d : %d", len(pvMap), len(testCase.expected))
			}

			for thisPVKey, expectedPV := range testCase.expected {
				actualPV, ok := pvMap[thisPVKey]
				if !ok {
					t.Errorf("pv map is missing key %s", thisPVKey)
				}
				if !actualPV.equal(expectedPV) {
					t.Errorf("pv does not match with key %s: %s != %s", thisPVKey, opencost.NewClosedWindow(actualPV.Start, actualPV.End), opencost.NewClosedWindow(expectedPV.Start, expectedPV.End))
				}
			}
		})
	}
}

func TestGetUnmountedPodForCluster(t *testing.T) {
	testCases := map[string]struct {
		window   opencost.Window
		podMap   map[podKey]*pod
		cluster  string
		expected *pod
	}{
		"create new": {
			window:  window.Clone(),
			podMap:  podMap1,
			cluster: "cluster1",
			expected: &pod{
				Window: window.Clone(),
				Start:  *window.Start(),
				End:    *window.End(),
				Key:    getUnmountedPodKey("cluster1"),
				Allocations: map[string]*opencost.Allocation{
					opencost.UnmountedSuffix: {
						Name: fmt.Sprintf("%s/%s/%s/%s", "cluster1", opencost.UnmountedSuffix, opencost.UnmountedSuffix, opencost.UnmountedSuffix),
						Properties: &opencost.AllocationProperties{
							Cluster:   "cluster1",
							Node:      "",
							Container: opencost.UnmountedSuffix,
							Namespace: opencost.UnmountedSuffix,
							Pod:       opencost.UnmountedSuffix,
						},
						Window: window,
						Start:  *window.Start(),
						End:    *window.End(),
					},
				},
			},
		},
		"get existing": {
			window:  window.Clone(),
			podMap:  podMap1,
			cluster: "cluster2",
			expected: &pod{
				Window: window.Clone(),
				Start:  *window.Start(),
				End:    *window.End(),
				Key:    getUnmountedPodKey("cluster2"),
				Allocations: map[string]*opencost.Allocation{
					opencost.UnmountedSuffix: {
						Name: fmt.Sprintf("%s/%s/%s/%s", "cluster2", opencost.UnmountedSuffix, opencost.UnmountedSuffix, opencost.UnmountedSuffix),
						Properties: &opencost.AllocationProperties{
							Cluster:   "cluster2",
							Node:      "",
							Container: opencost.UnmountedSuffix,
							Namespace: opencost.UnmountedSuffix,
							Pod:       opencost.UnmountedSuffix,
							Services:  []string{"LB1"},
						},
						Window:                     window,
						Start:                      *window.Start(),
						End:                        *window.End(),
						LoadBalancerCost:           .60,
						LoadBalancerCostAdjustment: 0,
						PVs: opencost.PVAllocations{
							kcPVKey2: &opencost.PVAllocation{
								ByteHours: 24 * Gi,
								Cost:      2.25,
							},
						},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := getUnmountedPodForCluster(testCase.window, testCase.podMap, testCase.cluster)
			if !actual.equal(testCase.expected) {
				t.Errorf("Unmounted pod does not match expectation")
			}
		})
	}
}

func TestCalculateStartAndEnd(t *testing.T) {
	testCases := map[string]struct {
		resolution    time.Duration // User defined config when querying Prometheus
		expectedStart time.Time
		expectedEnd   time.Time
		result        *source.QueryResult
	}{
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1h:1h]
		"1 hour resolution, 1 hour window": {
			resolution:    time.Hour,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Hour),
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat,
					},
					{
						Timestamp: startFloat + (minute * 60),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1h:30m]
		"30 minute resolution, 1 hour window": {
			resolution:    time.Minute * 30,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Hour),
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat,
					},
					{
						Timestamp: startFloat + (minute * 30),
					},
					{
						Timestamp: startFloat + (minute * 60),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[45m:15m]
		"15 minute resolution, 45 minute window": {
			resolution:    time.Minute * 15,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute * 45),
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 0),
					},
					{
						Timestamp: startFloat + (minute * 15),
					},
					{
						Timestamp: startFloat + (minute * 30),
					},
					{
						Timestamp: startFloat + (minute * 45),
					},
				},
			},
		},
		"1 minute resolution, 5 minute window": {
			resolution:    time.Minute,
			expectedStart: windowStart.Add(time.Minute * 15),
			expectedEnd:   windowStart.Add(time.Minute * 20),
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 15),
					},
					{
						Timestamp: startFloat + (minute * 16),
					},
					{
						Timestamp: startFloat + (minute * 17),
					},
					{
						Timestamp: startFloat + (minute * 18),
					},
					{
						Timestamp: startFloat + (minute * 19),
					},
					{
						Timestamp: startFloat + (minute * 20),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1m:1m]
		"1 minute resolution, 1 minute window": {
			resolution:    time.Minute,
			expectedStart: windowStart.Add(time.Minute * 15),
			expectedEnd:   windowStart.Add(time.Minute * 16),
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 15),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1m:1m]
		"1 minute resolution, 1 minute window, at window start": {
			resolution:    time.Minute,
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute),
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat,
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1m:1m]
		"1 minute resolution, 1 minute window, at window end": {
			resolution:    time.Minute,
			expectedStart: windowEnd,
			expectedEnd:   windowEnd,
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: endFloat,
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1m:1m]
		"1 minute resolution, 1 minute window, near window end": {
			resolution:    time.Minute,
			expectedStart: windowEnd.Add(-time.Second * 15),
			expectedEnd:   windowEnd,
			result: &source.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: endFloat - 15.0,
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			start, end := calculateStartAndEnd(testCase.result.Values, testCase.resolution, window)
			if !start.Equal(testCase.expectedStart) {
				t.Errorf("start does not match: expected %v; got %v", testCase.expectedStart, start)
			}
			if !end.Equal(testCase.expectedEnd) {
				t.Errorf("end does not match: expected %v; got %v", testCase.expectedEnd, end)
			}
		})
	}
}

func TestApplyNodesToPod_CarbonAllocation(t *testing.T) {
	cm := &CostModel{
		Provider: &carbonMockProvider{},
	}

	// Create pod map
	pKey := podKey{
		namespaceKey: namespaceKey{
			Cluster:   "cluster1",
			Namespace: "namespace1",
		},
		Pod: "pod1",
	}

	alloc := &opencost.Allocation{
		Name: "cluster1/node1/namespace1/pod1/container1",
		Properties: &opencost.AllocationProperties{
			Cluster:   "cluster1",
			Node:      "node1",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
		},
		CPUCoreHours: 2.0,
		RAMByteHours: 8.0 * 1024.0 * 1024.0 * 1024.0, // 8 GiB hours
	}

	podMap := map[podKey]*pod{
		pKey: {
			Key: pKey,
			Allocations: map[string]*opencost.Allocation{
				"container1": alloc,
			},
		},
	}

	// Create node pricing map
	nKey := newNodeKey("cluster1", "node1")
	nodeMap := map[nodeKey]*nodePricing{
		nKey: {
			Name:            "node1",
			NodeType:        "t4g.nano",
			ProviderID:      "aws:///us-east-1a/i-1",
			CostPerCPUHr:    0.01,
			CostPerRAMGiBHr: 0.002,
		},
	}

	// Create node labels map
	nodeLabels := map[nodeKey]map[string]string{
		nKey: {
			"topology_kubernetes_io_region": "us-east-1",
		},
	}

	cm.applyNodesToPod(podMap, nodeMap, nodeLabels)

	if alloc.CarbonKilograms == 0 {
		t.Errorf("expected non-zero CarbonKilograms")
	}

	// Math verification:
	// nodeCoeff for AWS us-east-1 t4g.nano is 4.84769853777516e-06 tonnes/hour = 0.00484769853777516 kg/hour.
	// default capacity fallback is 4 cores, 16 GiB RAM.
	// totalNodeCostRate = 0.01 * 4 + 0.002 * 16 = 0.072.
	// alloc.CPUCost = 2 * 0.01 = 0.02
	// alloc.RAMCost = 8 * 0.002 = 0.016
	// totalAllocCost = 0.036
	// carbonKilograms = 0.036 * (0.00484769853777516 / 0.072) = 0.00242384926888758 kg.
	expectedCarbon := 0.036 * (0.00484769853777516 / 0.072)
	if math.Abs(alloc.CarbonKilograms-expectedCarbon) > 1e-9 {
		t.Errorf("unexpected CarbonKilograms: expected %g, got %g", expectedCarbon, alloc.CarbonKilograms)
	}
}

type carbonMockProvider struct {
	models.Provider
}

func (mp *carbonMockProvider) GetConfig() (*models.CustomPricing, error) {
	return &models.CustomPricing{
		CustomPricesEnabled: "false",
		CPU:                 "0.01",
		RAM:                 "0.002",
	}, nil
}
