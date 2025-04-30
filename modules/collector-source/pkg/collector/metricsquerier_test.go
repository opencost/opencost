package collector

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape"
)

var Start1Str = "2025-01-01T00:00:00Z00:00"
var End1Str = "2025-01-01T00:01:00Z00:00"

type MockCollectorProvider struct {
	metricsCollector metric.MetricStore
}

func (m *MockCollectorProvider) GetStore(start, end time.Time) metric.MetricStore {
	return m.metricsCollector
}

func GetMockCollectorProvider() StoreProvider {
	collector := NewOpenCostMetricStore()

	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	node1Info := map[string]string{
		"node":        "node1",
		"provider_id": "node1",
	}

	cluster1Info := map[string]string{
		"provisioner_name": "GKE",
	}

	gpu1Info := map[string]string{
		"namespace":  "namespace1",
		"pod":        "pod1",
		"container":  "container1",
		"gpu":        "0",
		"UUID":       "GPU-1",
		"pci_bus_id": "00000000:00:0A.0",
		"device":     "nvidia0",
		"modelName":  "Tesla T4",
		"Hostname":   "localhost",
	}

	collector.Update(scrape.NodeTotalHourlyCost, node1Info, 0, &start1, nil)
	collector.Update(scrape.NodeTotalHourlyCost, node1Info, 0, &end1, nil)

	collector.Update(scrape.KubecostClusterManagementCost, cluster1Info, 0.1, &start1, nil)
	collector.Update(scrape.KubecostClusterManagementCost, cluster1Info, 0.1, &end1, nil)

	collector.Update(scrape.DCGMFIDEVDECUTIL, gpu1Info, 0, &start1, nil)
	collector.Update(scrape.DCGMFIPROFGRENGINEACTIVE, gpu1Info, 0, &start1, nil)
	collector.Update(scrape.DCGMFIPROFGRENGINEACTIVE, gpu1Info, 1, &end1, nil)

	return &MockCollectorProvider{
		metricsCollector: collector,
	}
}

func TestCollectorMetricsQuerier_QueryNodeActiveMinutes(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := CollectorMetricsQuerier{
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
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("QueryNodeActiveMinutes() = %v, want %v", res, expected)
	}
}

func TestCollectorMetricsQuerier_QueryClusterManagementDuration(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := CollectorMetricsQuerier{
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
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("QueryNodeActiveMinutes() = %v, want %v", res, expected)
	}

}

func TestCollectorMetricsQuerier_QueryGPUsUsageAvg(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := CollectorMetricsQuerier{
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
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("QueryGPUsUsageAvg() = %v, want %v", res, expected)
	}
}

func TestCollectorMetricsQuerier_QueryGPUsUsageMax(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := CollectorMetricsQuerier{
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
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("QueryGPUsUsageMax() = %v, want %v", res, expected)
	}
}

func TestCollectorMetricsQuerier_QueryGPUInfo(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)
	end1, _ := time.Parse(time.RFC3339, End1Str)

	c := CollectorMetricsQuerier{
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
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("QueryGPUInfo() = %v, want %v", res, expected)
	}
}
