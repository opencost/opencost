package costmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/prom"
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

	// These test cases are mocking behavior from Prometheus v3+
	prometheusVersion = "3.0.0"

	testCases := map[string]struct {
		resolution              time.Duration
		resultsPVCostPerGiBHour []*prom.QueryResult
		resultsActiveMinutes    []*prom.QueryResult
		expected                map[pvKey]*pv
	}{
		"pvMap1": {
			resolution: time.Hour * 6,
			resultsPVCostPerGiBHour: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"volumename": "pv1",
					},
					Values: []*util.Vector{
						{
							Value: 0.05,
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster1",
						"volumename": "pv2",
					},
					Values: []*util.Vector{
						{
							Value: 0.05,
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster2",
						"volumename": "pv3",
					},
					Values: []*util.Vector{
						{
							Value: 0.03,
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id": "cluster2",
						"volumename": "pv4",
					},
					Values: []*util.Vector{
						{
							Value: 0.05,
						},
					},
				},
			},
			resultsActiveMinutes: []*prom.QueryResult{
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster1",
						"persistentvolume": "pv1",
					},
					Values: []*util.Vector{
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
				},
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster1",
						"persistentvolume": "pv2",
					},
					Values: []*util.Vector{
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
				},
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster2",
						"persistentvolume": "pv3",
					},
					Values: []*util.Vector{
						{
							Timestamp: startFloat + (hour * 12),
						},
						{
							Timestamp: startFloat + (hour * 18),
						},
					},
				},
				{
					Metric: map[string]interface{}{
						"cluster_id":       "cluster2",
						"persistentvolume": "pv4",
					},
					Values: []*util.Vector{
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
				},
			},
			expected: pvMap1NoBytes,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			pvMap := make(map[pvKey]*pv)
			buildPVMap(testCase.resolution, pvMap, testCase.resultsPVCostPerGiBHour, testCase.resultsActiveMinutes, []*prom.QueryResult{}, window)
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
	// These test cases are mocking behavior from Prometheus v3+
	prometheusVersion = "3.0.0"

	testCases := map[string]struct {
		resolution    time.Duration   // User defined config when querying Prometheus
		window        opencost.Window // User defined config when querying Allocations/Assets
		expectedStart time.Time
		expectedEnd   time.Time
		result        *prom.QueryResult
	}{
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1h:1h]
		"1 hour resolution, 1 hour window": {
			resolution:    time.Hour,
			window:        opencost.NewClosedWindow(windowStart, windowStart.Add(time.Hour)),
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Hour),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 60),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1h:30m]
		"30 minute resolution, 1 hour window": {
			resolution:    time.Minute * 30,
			window:        opencost.NewClosedWindow(windowStart, windowStart.Add(time.Hour)),
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Hour),
			result: &prom.QueryResult{
				Values: []*util.Vector{
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
			window:        opencost.NewClosedWindow(windowStart, windowStart.Add(time.Minute*45)),
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute * 45),
			result: &prom.QueryResult{
				Values: []*util.Vector{
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
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[30m:5m]
		"5 minute resolution, 30 minute window": {
			resolution:    time.Minute * 5,
			window:        opencost.NewClosedWindow(windowStart, windowStart.Add(time.Minute*30)),
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Minute * 30),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 5),
					},
					{
						Timestamp: startFloat + (minute * 10),
					},
					{
						Timestamp: startFloat + (minute * 15),
					},
					{
						Timestamp: startFloat + (minute * 20),
					},
					{
						Timestamp: startFloat + (minute * 25),
					},
					{
						Timestamp: startFloat + (minute * 30),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[30m:5m]
		"5 minute resolution, 30 minute window, partial data": {
			resolution:    time.Minute * 5,
			window:        opencost.NewClosedWindow(windowStart, windowStart.Add(time.Minute*30)),
			expectedStart: windowStart.Add(time.Minute * 5),
			expectedEnd:   windowStart.Add(time.Minute * 20),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 10),
					},
					{
						Timestamp: startFloat + (minute * 15),
					},
					{
						Timestamp: startFloat + (minute * 20),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[5m:1m]
		"1 minute resolution, 5 minute window": {
			resolution:    time.Minute,
			window:        opencost.NewClosedWindow(windowStart.Add(time.Minute*15), windowStart.Add(time.Minute*20)),
			expectedStart: windowStart.Add(time.Minute * 15),
			expectedEnd:   windowStart.Add(time.Minute * 20),
			result: &prom.QueryResult{
				Values: []*util.Vector{
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
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[5m:1m]
		"1 minute resolution, 5 minute window, partial data": {
			resolution:    time.Minute,
			window:        opencost.NewClosedWindow(windowStart.Add(time.Minute*15), windowStart.Add(time.Minute*20)),
			expectedStart: windowStart.Add(time.Minute * 18),
			expectedEnd:   windowStart.Add(time.Minute * 20),
			result: &prom.QueryResult{
				Values: []*util.Vector{
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
			window:        opencost.NewClosedWindow(windowStart.Add(time.Minute*14).Add(time.Second*30), windowStart.Add(time.Minute*15).Add(time.Second*30)),
			expectedStart: windowStart.Add(time.Minute * 14).Add(time.Second * 30),
			expectedEnd:   windowStart.Add(time.Minute * 15).Add(time.Second * 30),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (minute * 15) + (second * 30),
					},
				},
			},
		},
		// Example: avg(node_total_hourly_cost{}) by (node, provider_id)[1m:1m]
		"1 minute resolution, 1 minute window, at window start": {
			resolution:    time.Minute,
			window:        opencost.NewClosedWindow(windowStart, windowStart.Add(time.Second*30)),
			expectedStart: windowStart,
			expectedEnd:   windowStart.Add(time.Second * 30),
			result: &prom.QueryResult{
				Values: []*util.Vector{
					{
						Timestamp: startFloat + (second * 30),
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			start, end := calculateStartAndEnd(testCase.result, testCase.resolution, testCase.window)
			if !start.Equal(testCase.expectedStart) {
				t.Errorf("start does not match: expected %v; got %v", testCase.expectedStart, start)
			}
			if !end.Equal(testCase.expectedEnd) {
				t.Errorf("end does not match: expected %v; got %v", testCase.expectedEnd, end)
			}
		})
	}
}
