package collector

import (
	"cmp"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
)

var Start1Str = "2025-01-01T00:00:00Z"
var End1Str = "2025-01-01T01:00:00Z"

type MockStoreProvider struct {
	metricsCollector metric.MetricStore
}

func (m *MockStoreProvider) GetStore(start, end time.Time) metric.MetricStore {
	return m.metricsCollector
}

// QueryDataCoverage is not implemented for this  mock
func (m *MockStoreProvider) GetDailyDataCoverage(limitDays int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, nil
}

func GetMockCollectorProvider() StoreProvider {
	collector := NewOpenCostMetricStore()

	start, _ := time.Parse(time.RFC3339, Start1Str)
	time1 := time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC)
	end, _ := time.Parse(time.RFC3339, End1Str)

	node1Info := map[string]string{
		"node":        "node1",
		"provider_id": "node1",
	}

	localStorage1Info := map[string]string{
		source.InstanceLabel: "node1",
		source.DeviceLabel:   "local",
	}

	cluster1Info := map[string]string{
		"provisioner_name": "GKE",
	}

	gpu1Info := map[string]string{
		source.NamespaceLabel: "namespace1",
		source.PodLabel:       "pod1",
		"container":           "container1",
		"gpu":                 "0",
		"UUID":                "GPU-1",
		"pci_bus_id":          "00000000:00:0A.0",
		"device":              "nvidia0",
		"modelName":           "Tesla T4",
		"Hostname":            "localhost",
	}

	pod1Info := map[string]string{
		source.NamespaceLabel: "namespace1",
		source.NodeLabel:      "node1",
		source.InstanceLabel:  "node1",
		source.PodLabel:       "pod1",
		source.UIDLabel:       "pod-uuid1",
	}

	container1Info := map[string]string{
		source.NamespaceLabel: "namespace1",
		source.NodeLabel:      "node1",
		source.InstanceLabel:  "node1",
		source.PodLabel:       "pod1",
		source.UIDLabel:       "pod-uuid1",
		source.ContainerLabel: "container1",
	}

	container2Info := map[string]string{
		source.NamespaceLabel: "kube-system",
		source.NodeLabel:      "node1",
		source.InstanceLabel:  "node1",
		source.PodLabel:       "pod2",
		source.UIDLabel:       "pod-uuid2",
		source.ContainerLabel: "container2",
	}

	networkZone1Info := map[string]string{
		source.PodNameLabel:    "pod1",
		source.NamespaceLabel:  "namespace1",
		source.InternetLabel:   "false",
		source.SameRegionLabel: "true",
		source.SameZoneLabel:   "false",
		source.ServiceLabel:    "service1",
	}

	networkRegion1Info := map[string]string{
		source.PodNameLabel:    "pod1",
		source.NamespaceLabel:  "namespace1",
		source.InternetLabel:   "false",
		source.SameRegionLabel: "false",
		source.SameZoneLabel:   "false",
		source.ServiceLabel:    "service1",
	}

	networkInternet1Info := map[string]string{
		source.PodNameLabel:    "pod1",
		source.NamespaceLabel:  "namespace1",
		source.InternetLabel:   "true",
		source.SameRegionLabel: "false",
		source.SameZoneLabel:   "false",
		source.ServiceLabel:    "service1",
	}

	networkInternet2Info := map[string]string{
		source.PodNameLabel:    "pod1",
		source.NamespaceLabel:  "namespace1",
		source.InternetLabel:   "true",
		source.SameRegionLabel: "false",
		source.SameZoneLabel:   "false",
		source.ServiceLabel:    "service2",
	}

	collector.Update(scrape.NodeTotalHourlyCost, node1Info, 0, start, nil)
	collector.Update(scrape.NodeTotalHourlyCost, node1Info, 0, end, nil)

	collector.Update(scrape.NodeFSCapacityBytes, localStorage1Info, 2*GiB, start, nil)
	collector.Update(scrape.ContainerFSUsageBytes, localStorage1Info, 1*GiB, start, nil)
	collector.Update(scrape.ContainerFSUsageBytes, localStorage1Info, 1*GiB, end, nil)

	collector.Update(scrape.KubeNodeStatusCapacityMemoryBytes, node1Info, 4*GiB, start, nil)
	collector.Update(scrape.ContainerMemoryWorkingSetBytes, container1Info, 1*GiB, start, nil)
	collector.Update(scrape.ContainerMemoryWorkingSetBytes, container2Info, 2*GiB, start, nil)

	collector.Update(scrape.ContainerCPUUsageSecondsTotal, container1Info, 0, start, nil)
	collector.Update(scrape.ContainerCPUUsageSecondsTotal, container1Info, 60*60*4, time1, nil)
	collector.Update(scrape.ContainerCPUUsageSecondsTotal, container1Info, 60*60*10, end, nil)

	collector.Update(scrape.KubecostClusterManagementCost, cluster1Info, 0.1, start, nil)
	collector.Update(scrape.KubecostClusterManagementCost, cluster1Info, 0.1, end, nil)

	collector.Update(scrape.DCGMFIDEVDECUTIL, gpu1Info, 0, start, nil)
	collector.Update(scrape.DCGMFIPROFGRENGINEACTIVE, gpu1Info, 0, start, nil)
	collector.Update(scrape.DCGMFIPROFGRENGINEACTIVE, gpu1Info, 1, end, nil)

	collector.Update(scrape.KubecostNetworkZoneEgressCost, nil, 1, start, nil)
	collector.Update(scrape.KubecostNetworkRegionEgressCost, nil, 2, start, nil)
	collector.Update(scrape.KubecostNetworkInternetEgressCost, nil, 3, start, nil)

	collector.Update(scrape.ContainerNetworkTransmitBytesTotal, pod1Info, 3*GiB, start, nil)
	collector.Update(scrape.ContainerNetworkTransmitBytesTotal, pod1Info, 13*GiB, end, nil)

	collector.Update(scrape.ContainerNetworkReceiveBytesTotal, pod1Info, 30*GiB, start, nil)
	collector.Update(scrape.ContainerNetworkReceiveBytesTotal, pod1Info, 130*GiB, end, nil)

	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkRegion1Info, 1*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkZone1Info, 0*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkInternet1Info, 1*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkInternet2Info, 1*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkRegion1Info, 2*GiB, end, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkZone1Info, 2*GiB, end, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkInternet1Info, 4*GiB, end, nil)
	collector.Update(scrape.KubecostPodNetworkEgressBytesTotal, networkInternet2Info, 5*GiB, end, nil)

	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkRegion1Info, 10*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkZone1Info, 0*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkInternet1Info, 10*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkInternet2Info, 10*GiB, start, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkRegion1Info, 20*GiB, end, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkZone1Info, 20*GiB, end, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkInternet1Info, 40*GiB, end, nil)
	collector.Update(scrape.KubecostPodNetworkIngressBytesTotal, networkInternet2Info, 50*GiB, end, nil)

	return &MockStoreProvider{
		metricsCollector: collector,
	}
}

func TestCollectorMetricsQuerier_QueryLocalStorageCost(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryLocalStorageCost(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.LocalStorageCostResult{
		{
			Cluster:  "",
			Instance: "node1",
			Device:   "local",
			Data: []*util.Vector{
				{
					Value: LocalStorageCostPerGiBHr * 2,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryLocalStorageUsedCost(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryLocalStorageUsedCost(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.LocalStorageUsedCostResult{
		{
			Cluster:  "",
			Instance: "node1",
			Device:   "local",
			Data: []*util.Vector{
				{
					Value: LocalStorageCostPerGiBHr,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryNodeActiveMinutes(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNodeActiveMinutes(time.Now(), time.Now())
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NodeActiveMinutesResult{
		{
			Cluster:    "",
			Node:       "node1",
			ProviderID: "node1",
			Data: []*util.Vector{
				{
					Timestamp: float64(start1.Unix()),
					Value:     1,
				},
				{
					Timestamp: float64(end1.Unix()),
					Value:     1,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryNodeRAMSystemPercent(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNodeRAMSystemPercent(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NodeRAMSystemPercentResult{
		{
			Cluster:  "",
			Instance: "node1",
			Data: []*util.Vector{
				{
					Value: .5,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryNodeRAMUserPercent(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNodeRAMUserPercent(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NodeRAMUserPercentResult{
		{
			Cluster:  "",
			Instance: "node1",
			Data: []*util.Vector{
				{
					Value: .25,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryClusterManagementDuration(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryClusterManagementDuration(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.ClusterManagementDurationResult{
		{
			Cluster:     "",
			Provisioner: "GKE",
			Data: []*util.Vector{
				{
					Timestamp: float64(start1.Unix()),
					Value:     1,
				},
				{
					Timestamp: float64(end1.Unix()),
					Value:     1,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryCPUUsageAvg(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryCPUUsageAvg(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.CPUUsageAvgResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Node:      "node1",
			Instance:  "node1",
			Pod:       "pod1",
			Container: "container1",
			Data: []*util.Vector{
				{
					Value: 10,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryCPUUsageMax(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryCPUUsageMax(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.CPUUsageMaxResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Node:      "node1",
			Instance:  "node1",
			Pod:       "pod1",
			Container: "container1",
			Data: []*util.Vector{
				{
					Value: 12,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryGPUsUsageAvg(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryGPUsUsageAvg(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.GPUsUsageAvgResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
			Data: []*util.Vector{
				{
					Value: 0.5,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryGPUsUsageMax(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryGPUsUsageMax(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.GPUsUsageMaxResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
			Data: []*util.Vector{
				{
					Value: 1.0,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func TestCollectorMetricsQuerier_QueryGPUInfo(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryGPUInfo(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.GPUInfoResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Container: "container1",
			Device:    "nvidia0",
			ModelName: "Tesla T4",
			UUID:      "GPU-1",
			Data: []*util.Vector{
				{
					Value: 1,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetZoneGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetZoneGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetZoneGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 2,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetZonePricePerGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetZonePricePerGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetZonePricePerGiBResult{
		{
			Cluster: "",
			Data: []*util.Vector{
				{
					Value: 1,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetRegionGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetRegionGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetRegionGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 1,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetRegionPricePerGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetRegionPricePerGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetRegionPricePerGiBResult{
		{
			Cluster: "",
			Data: []*util.Vector{
				{
					Value: 2,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetInternetGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetInternetGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetInternetGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 7,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetInternetPricePerGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetInternetPricePerGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetInternetPricePerGiBResult{
		{
			Cluster: "",
			Data: []*util.Vector{
				{
					Value: 3,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetInternetServiceGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetInternetServiceGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetInternetServiceGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Service:   "service1",
			Data: []*util.Vector{
				{
					Value: 3,
				},
			},
		},
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Service:   "service2",
			Data: []*util.Vector{
				{
					Value: 4,
				},
			},
		},
	}

	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}

	slices.SortFunc(res, func(a, b *source.NetInternetServiceGiBResult) int {
		return cmp.Compare(a.Service, b.Service)
	})

	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetTransferBytes(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetTransferBytes(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetTransferBytesResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 10 * GiB,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetZoneIngressGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetZoneIngressGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetZoneIngressGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 20,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetRegionIngressGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetRegionIngressGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetRegionIngressGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 10,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetInternetIngressGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetInternetIngressGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetInternetIngressGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 70,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetInternetServiceIngressGiB(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetInternetServiceIngressGiB(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetInternetServiceIngressGiBResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Service:   "service1",
			Data: []*util.Vector{
				{
					Value: 30,
				},
			},
		},
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Service:   "service2",
			Data: []*util.Vector{
				{
					Value: 40,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}

	slices.SortFunc(res, func(a, b *source.NetInternetServiceIngressGiBResult) int {
		return cmp.Compare(a.Service, b.Service)
	})

	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}

func Test_collectorMetricsQuerier_QueryNetReceiveBytes(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resCh := c.QueryNetReceiveBytes(start1, end1)
	res, err := resCh.Await()
	if err != nil {
		t.Errorf("unexpected error: %v", err.Error())
	}
	expected := []*source.NetReceiveBytesResult{
		{
			Cluster:   "",
			Namespace: "namespace1",
			Pod:       "pod1",
			Data: []*util.Vector{
				{
					Value: 100 * GiB,
				},
			},
		},
	}
	if len(res) != len(expected) {
		t.Errorf("length of result was not as expected: got = %d, want %d", len(res), len(expected))
	}
	for i, got := range res {
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("result at index %d did not match: got = %v, want %v", i, got, expected[i])
		}
	}
}
