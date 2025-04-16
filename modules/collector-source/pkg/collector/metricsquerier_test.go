package collector

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
)

var start1Str = "2025-01-01T00:00:00Z00:00"
var end1Str = "2025-01-01T00:01:00Z00:00"

type MockCollectorProvider struct {
	metricsCollector MetricsCollector
}

func (m *MockCollectorProvider) GetCollector(start, end time.Time) MetricsCollector {
	return m.metricsCollector
}

func GetMockCollectorProvider() CollectorProvider {
	collector := NewOpenCostMetricCollector()

	start1, _ := time.Parse(time.RFC3339, start1Str)
	end1, _ := time.Parse(time.RFC3339, end1Str)

	node1Info := map[string]string{
		"node":        "node1",
		"provider_id": "node1",
	}

	cluster1Info := map[string]string{
		"provisioner_name": "GKE",
	}

	collector.Update(NodeTotalHourlyCost, node1Info, 0, &start1, nil)
	collector.Update(NodeTotalHourlyCost, node1Info, 0, &end1, nil)

	collector.Update(KubecostClusterManagementCost, cluster1Info, 0.1, &start1, nil)
	collector.Update(KubecostClusterManagementCost, cluster1Info, 0.1, &end1, nil)

	return &MockCollectorProvider{
		metricsCollector: collector,
	}
}

func TestCollectorMetricsQuerier_QueryNodeActiveMinutes(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, start1Str)
	end1, _ := time.Parse(time.RFC3339, end1Str)

	c := CollectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resChActiveMins := c.QueryNodeActiveMinutes(time.Now(), time.Now())
	resActiveMins, err := resChActiveMins.Await()
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
	if !reflect.DeepEqual(resActiveMins, expected) {
		t.Errorf("QueryNodeActiveMinutes() = %v, want %v", resActiveMins, expected)
	}
}

func TestCollectorMetricsQuerier_QueryClusterManagementDuration(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, start1Str)
	end1, _ := time.Parse(time.RFC3339, end1Str)

	c := CollectorMetricsQuerier{
		collectorProvider: GetMockCollectorProvider(),
	}
	resChCMDur := c.QueryClusterManagementDuration(start1, end1)
	resCMDur, err := resChCMDur.Await()
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
	if !reflect.DeepEqual(resCMDur, expected) {
		t.Errorf("QueryNodeActiveMinutes() = %v, want %v", resCMDur, expected)
	}

}
