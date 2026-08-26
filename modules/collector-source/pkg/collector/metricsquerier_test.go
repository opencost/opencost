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
		source.PodUIDLabel:    "pod-uuid1",
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

	collector.Update(metric.KubeNodeLabels, node1Info, 0, start, nil)
	collector.Update(metric.KubeNodeLabels, node1Info, 0, end, nil)

	collector.Update(metric.NodeTotalHourlyCost, node1Info, 0, start, nil)
	collector.Update(metric.NodeTotalHourlyCost, node1Info, 0, end, nil)

	collector.Update(metric.NodeFSCapacityBytes, localStorage1Info, 2*GiB, start, nil)
	collector.Update(metric.ContainerFSUsageBytes, localStorage1Info, 1*GiB, start, nil)
	collector.Update(metric.ContainerFSUsageBytes, localStorage1Info, 1*GiB, end, nil)

	collector.Update(metric.KubeNodeStatusCapacityMemoryBytes, node1Info, 4*GiB, start, nil)
	collector.Update(metric.ContainerMemoryWorkingSetBytes, container1Info, 1*GiB, start, nil)
	collector.Update(metric.ContainerMemoryWorkingSetBytes, container2Info, 2*GiB, start, nil)

	collector.Update(metric.ContainerCPUUsageSecondsTotal, container1Info, 0, start, nil)
	collector.Update(metric.ContainerCPUUsageSecondsTotal, container1Info, 60*60*4, time1, nil)
	collector.Update(metric.ContainerCPUUsageSecondsTotal, container1Info, 60*60*10, end, nil)

	collector.Update(metric.KubecostClusterManagementCost, cluster1Info, 0.1, start, nil)
	collector.Update(metric.KubecostClusterManagementCost, cluster1Info, 0.1, end, nil)

	collector.Update(metric.DCGMFIDEVDECUTIL, gpu1Info, 0, start, nil)
	collector.Update(metric.DCGMFIPROFGRENGINEACTIVE, gpu1Info, 0, start, nil)
	collector.Update(metric.DCGMFIPROFGRENGINEACTIVE, gpu1Info, 1, end, nil)

	inference1Info := map[string]string{
		source.InferenceModelNameLabel: "Qwen3-32B",
		source.PodUIDLabel:             "pod1-uid",
		source.NamespaceUIDLabel:       "namespace1-uid",
	}
	collector.Update(metric.VLLMKVCacheUsagePerc, inference1Info, 0.2, start, nil)
	collector.Update(metric.VLLMKVCacheUsagePerc, inference1Info, 0.8, end, nil)
	collector.Update(metric.VLLMNumRequestsWaiting, inference1Info, 0, start, nil)
	collector.Update(metric.VLLMNumRequestsWaiting, inference1Info, 4, end, nil)
	collector.Update(metric.VLLMNumRequestsRunning, inference1Info, 10, start, nil)
	collector.Update(metric.VLLMNumRequestsRunning, inference1Info, 30, end, nil)
	collector.Update(metric.VLLMNumPreemptionsTotal, inference1Info, 2, start, nil)
	collector.Update(metric.VLLMNumPreemptionsTotal, inference1Info, 6, end, nil)

	// Inference cost counters are measured per model-server pod and rolled up
	// to (model_name, namespace) in the querier, so their label set carries
	// pod identity. TWO replicas of the same model are seeded deliberately: a
	// single replica produces identical results under per-pod and per-model
	// grouping, so a one-replica fixture certifies nothing about either.
	inferenceCost1Info := map[string]string{
		source.InferenceModelNameLabel: "Qwen3-32B",
		source.PodUIDLabel:             "pod1-uid",
		source.NamespaceLabel:          "namespace1",
		source.NamespaceUIDLabel:       "namespace1-uid",
	}
	inferenceCost2Info := map[string]string{
		source.InferenceModelNameLabel: "Qwen3-32B",
		source.PodUIDLabel:             "pod2-uid",
		source.NamespaceLabel:          "namespace1",
		source.NamespaceUIDLabel:       "namespace1-uid",
	}
	// Ordered by scrape cycle, not by pod. The scrape controller applies a
	// whole cycle as one UpdateSet sharing a timestamp, so interleaving a
	// second replica's start sample after the first replica's end sample would
	// be an artifact of the fixture and would make these tests pass for the
	// wrong reason.
	collector.Update(metric.VLLMPromptTokensTotal, inferenceCost1Info, 1000, start, nil)
	collector.Update(metric.VLLMPromptTokensTotal, inferenceCost2Info, 300, start, nil)
	collector.Update(metric.VLLMPromptTokensTotal, inferenceCost1Info, 5000, end, nil)
	collector.Update(metric.VLLMPromptTokensTotal, inferenceCost2Info, 800, end, nil)
	collector.Update(metric.VLLMGenerationTokensTotal, inferenceCost1Info, 200, start, nil)
	collector.Update(metric.VLLMGenerationTokensTotal, inferenceCost2Info, 50, start, nil)
	collector.Update(metric.VLLMGenerationTokensTotal, inferenceCost1Info, 900, end, nil)
	collector.Update(metric.VLLMGenerationTokensTotal, inferenceCost2Info, 150, end, nil)
	collector.Update(metric.VLLMPrefixCacheHitsTotal, inferenceCost1Info, 10, start, nil)
	collector.Update(metric.VLLMPrefixCacheHitsTotal, inferenceCost2Info, 5, start, nil)
	collector.Update(metric.VLLMPrefixCacheHitsTotal, inferenceCost1Info, 60, end, nil)
	collector.Update(metric.VLLMPrefixCacheHitsTotal, inferenceCost2Info, 25, end, nil)
	collector.Update(metric.VLLMRequestPrefillTimeSecondsSum, inferenceCost1Info, 2, start, nil)
	collector.Update(metric.VLLMRequestPrefillTimeSecondsSum, inferenceCost2Info, 1, start, nil)
	collector.Update(metric.VLLMRequestPrefillTimeSecondsSum, inferenceCost1Info, 8, end, nil)
	collector.Update(metric.VLLMRequestPrefillTimeSecondsSum, inferenceCost2Info, 4, end, nil)
	collector.Update(metric.VLLMRequestTimePerOutputTokenSecondsSum, inferenceCost1Info, 5, start, nil)
	collector.Update(metric.VLLMRequestTimePerOutputTokenSecondsSum, inferenceCost2Info, 3, start, nil)
	collector.Update(metric.VLLMRequestTimePerOutputTokenSecondsSum, inferenceCost1Info, 20, end, nil)
	collector.Update(metric.VLLMRequestTimePerOutputTokenSecondsSum, inferenceCost2Info, 9, end, nil)

	// cache_config_info is an info metric: the payload rides on AdditionalInfo.
	// Both replicas report the same setting here; disagreement is covered by a
	// dedicated test.
	cacheConfig1Info := map[string]string{
		source.InferenceModelNameLabel:  "Qwen3-32B",
		source.PodUIDLabel:              "pod1-uid",
		source.NamespaceLabel:           "namespace1",
		source.NamespaceUIDLabel:        "namespace1-uid",
		source.EnablePrefixCachingLabel: "true",
	}
	cacheConfig2Info := map[string]string{
		source.InferenceModelNameLabel:  "Qwen3-32B",
		source.PodUIDLabel:              "pod2-uid",
		source.NamespaceLabel:           "namespace1",
		source.NamespaceUIDLabel:        "namespace1-uid",
		source.EnablePrefixCachingLabel: "true",
	}
	collector.Update(metric.VLLMCacheConfigInfo, cacheConfig1Info, 1, start, cacheConfig1Info)
	collector.Update(metric.VLLMCacheConfigInfo, cacheConfig2Info, 1, start, cacheConfig2Info)

	collector.Update(metric.KubecostNetworkZoneEgressCost, nil, 1, start, nil)
	collector.Update(metric.KubecostNetworkRegionEgressCost, nil, 2, start, nil)
	collector.Update(metric.KubecostNetworkInternetEgressCost, nil, 3, start, nil)

	collector.Update(metric.ContainerNetworkTransmitBytesTotal, pod1Info, 3*GiB, start, nil)
	collector.Update(metric.ContainerNetworkTransmitBytesTotal, pod1Info, 13*GiB, end, nil)

	collector.Update(metric.ContainerNetworkReceiveBytesTotal, pod1Info, 30*GiB, start, nil)
	collector.Update(metric.ContainerNetworkReceiveBytesTotal, pod1Info, 130*GiB, end, nil)

	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkRegion1Info, 1*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkZone1Info, 0*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkInternet1Info, 1*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkInternet2Info, 1*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkRegion1Info, 2*GiB, end, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkZone1Info, 2*GiB, end, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkInternet1Info, 4*GiB, end, nil)
	collector.Update(metric.KubecostPodNetworkEgressBytesTotal, networkInternet2Info, 5*GiB, end, nil)

	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkRegion1Info, 10*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkZone1Info, 0*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkInternet1Info, 10*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkInternet2Info, 10*GiB, start, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkRegion1Info, 20*GiB, end, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkZone1Info, 20*GiB, end, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkInternet1Info, 40*GiB, end, nil)
	collector.Update(metric.KubecostPodNetworkIngressBytesTotal, networkInternet2Info, 50*GiB, end, nil)

	return &MockStoreProvider{
		metricsCollector: collector,
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
			UID:      "pod-uuid2",
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
			UID:      "pod-uuid1",
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
			UID:       "pod-uuid1",
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
			UID:       "pod-uuid1",
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
			UID:       "pod-uuid1",
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
			UID:       "pod-uuid1",
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
			UID:       "pod-uuid1",
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
			UID:       "pod-uuid1",
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
			UID:       "pod-uuid1",
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

func TestCollectorMetricsQuerier_QueryInferenceSaturation(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}

	tests := map[string]struct {
		query func(start, end time.Time) *source.Future[source.InferenceEngineMetricResult]
		want  float64
	}{
		"kv cache usage avg": {
			query: c.QueryInferenceKVCacheUsageAvg,
			want:  0.5,
		},
		"kv cache usage max": {
			query: c.QueryInferenceKVCacheUsageMax,
			want:  0.8,
		},
		"queue depth avg": {
			query: c.QueryInferenceQueueDepthAvg,
			want:  2,
		},
		"queue depth max": {
			query: c.QueryInferenceQueueDepthMax,
			want:  4,
		},
		"running requests avg": {
			query: c.QueryInferenceRunningRequestsAvg,
			want:  20,
		},
		"preemptions delta": {
			query: c.QueryInferencePreemptions,
			want:  4,
		},
		"kv cache usage p95": {
			query: c.QueryInferenceKVCacheUsageP95,
			// Prometheus-style linear interpolation between the two
			// samples (0.2 and 0.8) at rank 0.95.
			want: 0.2 + (0.8-0.2)*0.95,
		},
		"queue depth p95": {
			query: c.QueryInferenceQueueDepthP95,
			// Interpolation between the two samples (0 and 4) at rank 0.95.
			want: 0 + (4-0)*0.95,
		},
		"running requests max": {
			query: c.QueryInferenceRunningRequestsMax,
			want:  30,
		},
		"running requests p95": {
			query: c.QueryInferenceRunningRequestsP95,
			// Interpolation between the two samples (10 and 30) at rank 0.95.
			want: 10 + (30-10)*0.95,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := tt.query(start1, end1).Await()
			if err != nil {
				t.Fatalf("unexpected error: %v", err.Error())
			}
			if len(res) != 1 {
				t.Fatalf("length of result was not as expected: got = %d, want 1", len(res))
			}
			got := res[0]
			if got.ModelName != "Qwen3-32B" || got.PodUID != "pod1-uid" || got.NamespaceUID != "namespace1-uid" {
				t.Errorf("result identity did not match: got = %+v", got)
			}
			if got.Value != tt.want {
				t.Errorf("result value did not match: got = %v, want %v", got.Value, tt.want)
			}
		})
	}
}

// TestCollectorMetricsQuerier_QueryInferenceCost exercises the inference cost
// queries that the collector source previously answered with "inference
// metrics not supported by collector source". They roll up per
// (model_name, namespace) to match the Prometheus source's
// `sum by (model_name, namespace)` and the "model:namespace" result keying.
func TestCollectorMetricsQuerier_QueryInferenceCost(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := collectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}

	const key = "Qwen3-32B:namespace1"

	t.Run("token counters return window deltas", func(t *testing.T) {
		tests := map[string]struct {
			query func(start, end time.Time) *source.Future[source.InferenceTokensResult]
			want  float64
		}{
			// seeded 1000 -> 5000 over the window
			"prompt tokens": {query: c.QueryInferencePromptTokens, want: 4500},
			// seeded 200 -> 900
			"generation tokens": {query: c.QueryInferenceGenerationTokens, want: 800},
			// seeded 10 -> 60
			"cached tokens": {query: c.QueryInferenceCachedTokens, want: 70},
		}

		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				res, err := tt.query(start1, end1).Await()
				if err != nil {
					t.Fatalf("unexpected error: %v", err.Error())
				}
				if len(res) != 1 {
					t.Fatalf("length of result was not as expected: got = %d, want 1", len(res))
				}
				if got := res[0].Values[key]; got != tt.want {
					t.Errorf("value for %q did not match: got = %v, want %v", key, got, tt.want)
				}
			})
		}
	})

	t.Run("processing times return window deltas", func(t *testing.T) {
		tests := map[string]struct {
			query func(start, end time.Time) *source.Future[source.InferenceProcessingTimeResult]
			want  float64
		}{
			// seeded 2 -> 8
			"input processing time": {query: c.QueryInferenceInputProcessingTime, want: 9},
			// seeded 5 -> 20
			"output processing time": {query: c.QueryInferenceOutputProcessingTime, want: 21},
		}

		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				res, err := tt.query(start1, end1).Await()
				if err != nil {
					t.Fatalf("unexpected error: %v", err.Error())
				}
				if len(res) != 1 {
					t.Fatalf("length of result was not as expected: got = %d, want 1", len(res))
				}
				if got := res[0].Values[key]; got != tt.want {
					t.Errorf("value for %q did not match: got = %v, want %v", key, got, tt.want)
				}
			})
		}
	})

	t.Run("cache config reads the enable_prefix_caching label", func(t *testing.T) {
		res, err := c.QueryInferenceCacheConfig(end1).Await()
		if err != nil {
			t.Fatalf("unexpected error: %v", err.Error())
		}
		if len(res) != 1 {
			t.Fatalf("length of result was not as expected: got = %d, want 1", len(res))
		}
		config, ok := res[0].Configs[key]
		if !ok {
			t.Fatalf("no cache config for key %q, got %+v", key, res[0].Configs)
		}
		if !config.PrefixCachingEnabled {
			t.Errorf("PrefixCachingEnabled = false, want true")
		}
	})
}

// TestCollectorInferenceCost_ReplicaResetDoesNotEraseOtherReplicas is the test
// that actually distinguishes per-pod measurement from per-model measurement.
// A two-replica fixture with no reset returns the same total either way, so it
// certifies nothing on its own; only a reset separates them.
//
// The Increase aggregator credits an increase only when the pooled total rises
// (aggregator/increase.go). With both replicas in one aggregator, replica A
// restarting drags the pooled total below its previous value and the whole
// cycle's increase is discarded, including replica B's growth, which never
// reset.
//
// Scrape cycles, one timestamp each, both replicas per cycle:
//
//	          t0     t1            t2
//	pod A    1000   1500   200 (restarted)
//	pod B     100    200   300
//
// Per replica: A contributes 500 across t0 to t1, then its post-reset 200 is
// not credited as growth because 200 < 1500, so A totals 500. B rises
// 100 -> 200 -> 300, so B totals 200. The model total is 700.
func TestCollectorInferenceCost_ReplicaResetDoesNotEraseOtherReplicas(t *testing.T) {
	t0, _ := time.Parse(time.RFC3339, Start1Str)
	t1 := t0.Add(20 * time.Minute)
	t2 := t0.Add(40 * time.Minute)

	store := NewOpenCostMetricStore()

	podA := map[string]string{
		source.InferenceModelNameLabel: "Qwen3-32B",
		source.PodUIDLabel:             "pod-a-uid",
		source.NamespaceLabel:          "llm-d",
		source.NamespaceUIDLabel:       "llm-d-uid",
	}
	podB := map[string]string{
		source.InferenceModelNameLabel: "Qwen3-32B",
		source.PodUIDLabel:             "pod-b-uid",
		source.NamespaceLabel:          "llm-d",
		source.NamespaceUIDLabel:       "llm-d-uid",
	}

	store.Update(metric.VLLMPromptTokensTotal, podA, 1000, t0, nil)
	store.Update(metric.VLLMPromptTokensTotal, podB, 100, t0, nil)
	store.Update(metric.VLLMPromptTokensTotal, podA, 1500, t1, nil)
	store.Update(metric.VLLMPromptTokensTotal, podB, 200, t1, nil)
	// pod A restarts: its counter drops to 200.
	store.Update(metric.VLLMPromptTokensTotal, podA, 200, t2, nil)
	store.Update(metric.VLLMPromptTokensTotal, podB, 300, t2, nil)

	c := &collectorMetricsQuerier{collectorProvider: &MockStoreProvider{metricsCollector: store}}

	res, err := c.QueryInferencePromptTokens(t0, t2).Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err.Error())
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 result, got %d", len(res))
	}

	const key = "Qwen3-32B:llm-d"
	const want = 700.0
	if got := res[0].Values[key]; got != want {
		t.Errorf("prompt tokens = %v, want %v (500 from the replica that reset plus 200 from the one that did not); "+
			"a lower value means one replica's reset erased the other's growth", got, want)
	}
}
