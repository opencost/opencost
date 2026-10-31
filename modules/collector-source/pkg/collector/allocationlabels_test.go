package collector

import (
	"maps"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/synthetic"
)

type allocationStoreUpdater struct {
	store metric.MetricStore
}

func (u *allocationStoreUpdater) Update(set *metric.UpdateSet) {
	for _, update := range set.Updates {
		u.store.Update(update.Name, update.Labels, update.Value, set.Timestamp, update.AdditionalInfo)
	}
}

func TestAllocationCollectors_RequestNodeFallback(t *testing.T) {
	kinds := []struct {
		resource  string
		unit      string
		usageName string
		collector func() *metric.MetricCollector
		wantValue float64
	}{
		{"memory", "byte", metric.ContainerMemoryWorkingSetBytes, NewRAMBytesAllocatedMetricCollector, 6},
		{"cpu", "core", metric.ContainerCPUUsageSecondsTotal, NewCPUCoresAllocatedMetricCollector, 5},
	}
	for _, kind := range kinds {
		for _, nodeLabel := range []string{"missing", "empty", "complete"} {
			for _, usageFirst := range []bool{false, true} {
				name := kind.resource + "/" + nodeLabel + "/request first"
				if usageFirst {
					name = kind.resource + "/" + nodeLabel + "/usage first"
				}
				t.Run(name, func(t *testing.T) {
					store := metric.NewInMemoryMetricStore()
					col := kind.collector()
					if err := store.Register(col); err != nil {
						t.Fatal(err)
					}
					synthesizers := synthetic.NewMetricSynthesizers(
						&allocationStoreUpdater{store: store},
						synthetic.NewContainerCpuAllocationSynthesizer(),
						synthetic.NewContainerMemoryAllocationSynthesizer(),
					)
					labels := map[string]string{
						source.UIDLabel: "pod1", source.ContainerLabel: "app", source.NodeLabel: "node1",
						source.InstanceLabel: "node1", source.PodLabel: "pod", source.NamespaceLabel: "ns",
					}
					start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
					for scrape := range 2 {
						requests := maps.Clone(labels)
						requests[source.ResourceLabel] = kind.resource
						requests[source.UnitLabel] = kind.unit
						usage := maps.Clone(labels)
						switch nodeLabel {
						case "missing":
							delete(usage, source.NodeLabel)
						case "empty":
							usage[source.NodeLabel] = ""
						}
						value := 6.0
						if kind.resource == "cpu" {
							value = float64(scrape * 180)
						}
						requestUpdate := metric.Update{Name: metric.KubePodContainerResourceRequests, Labels: requests, Value: 4}
						usageUpdate := metric.Update{Name: kind.usageName, Labels: usage, Value: value}
						updates := []metric.Update{requestUpdate, usageUpdate}
						if usageFirst {
							updates = []metric.Update{usageUpdate, requestUpdate}
						}
						synthesizers.Update(&metric.UpdateSet{Timestamp: start.Add(time.Duration(scrape) * 30 * time.Second), Updates: updates})
					}
					results := col.Get()
					if len(results) != 1 {
						t.Fatalf("stored allocations = %d, want 1", len(results))
					}
					if !maps.Equal(results[0].MetricLabels, labels) {
						t.Errorf("stored labels = %v, want %v", results[0].MetricLabels, labels)
					}
					if values := results[0].Values; len(values) != 1 || values[0].Value != kind.wantValue {
						t.Errorf("stored values = %v, want one value of %v", values, kind.wantValue)
					}
				})
			}
		}
	}
}
