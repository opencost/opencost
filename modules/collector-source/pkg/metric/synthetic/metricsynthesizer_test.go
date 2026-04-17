package synthetic

import (
	"maps"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

var _ metric.Updater = (*FuncUpdater)(nil)

type FuncUpdater struct {
	f func(*metric.UpdateSet)
}

func NewFuncUpdater(f func(*metric.UpdateSet)) *FuncUpdater {
	return &FuncUpdater{f}
}

func (fu *FuncUpdater) Update(set *metric.UpdateSet) {
	fu.f(set)
}

func toMemoryResource(m map[string]string) map[string]string {
	mm := maps.Clone(m)
	mm[source.ResourceLabel] = "memory"
	mm[source.UnitLabel] = "byte"
	return mm
}

func toCpuResource(m map[string]string) map[string]string {
	mm := maps.Clone(m)
	mm[source.ResourceLabel] = "cpu"
	mm[source.UnitLabel] = "core"
	return mm
}

func findMetric(t *testing.T, set *metric.UpdateSet, name string, container string) *metric.Update {
	t.Helper()

	var metric *metric.Update
	for _, update := range set.Updates {
		if update.Name == name && update.Labels[source.ContainerLabel] == container {
			metric = &update
			break
		}
	}

	return metric
}

func assertMetricValue(t *testing.T, set *metric.UpdateSet, name string, container string, value float64) {
	t.Helper()

	metric := findMetric(t, set, name, container)
	if metric == nil {
		t.Fatalf("Failed to Locate a %s Metric for Container: %s\n", name, container)
		return
	}

	if !util.IsApproximately(metric.Value, value) {
		t.Fatalf("Expected %f for %s [Container: %s], got: %f\n", value, name, container, metric.Value)
		return
	}
}

func assertMetricExists(t *testing.T, set *metric.UpdateSet, name string, container string) {
	t.Helper()

	metric := findMetric(t, set, name, container)
	if metric == nil {
		t.Fatalf("Failed to Locate a %s Metric for Container: %s\n", name, container)
		return
	}
}

func assertNoMetricExists(t *testing.T, set *metric.UpdateSet, name string, container string) {
	t.Helper()

	metric := findMetric(t, set, name, container)
	if metric != nil {
		t.Fatalf("Expected metric to not exist: %s Metric for Container: %s\n", name, container)
		return
	}
}

func TestMetricSynthesizerRAMAllocation(t *testing.T) {
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

	const startingCPUSeconds float64 = 506000.0

	updateSet1 := &metric.UpdateSet{
		Timestamp: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		Updates: []metric.Update{
			// container1 has both requests and usage
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toMemoryResource(container1Info),
				Value:  4.0 * 1024 * 1024 * 1024,
			},
			{
				Name:   metric.ContainerMemoryWorkingSetBytes,
				Labels: maps.Clone(container1Info),
				Value:  5.5 * 1024 * 1024 * 1024,
			},
			// container2 only has usage
			{
				Name:   metric.ContainerMemoryWorkingSetBytes,
				Labels: maps.Clone(container2Info),
				Value:  1.5 * 1024 * 1024 * 1024,
			},
			// add some additional metrics to test filtering
			{
				Name:   metric.KubeNamespaceLabels,
				Labels: maps.Clone(pod1Info),
				Value:  0,
			},
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toCpuResource(container1Info),
				Value:  20,
			},
		},
	}

	updateSet2 := &metric.UpdateSet{
		Timestamp: time.Date(2026, time.January, 1, 0, 0, 30, 0, time.UTC),
		Updates: []metric.Update{
			// container1 has both requests and usage
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toMemoryResource(container1Info),
				Value:  4.0 * 1024 * 1024 * 1024,
			},
			{
				Name:   metric.ContainerMemoryWorkingSetBytes,
				Labels: maps.Clone(container1Info),
				Value:  3.0 * 1024 * 1024 * 1024,
			},
			// container2 only has usage
			{
				Name:   metric.ContainerMemoryWorkingSetBytes,
				Labels: maps.Clone(container2Info),
				Value:  2.5 * 1024 * 1024 * 1024,
			},
			// add some additional metrics to test filtering
			{
				Name:   metric.KubeNamespaceLabels,
				Labels: maps.Clone(pod1Info),
				Value:  0,
			},
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toCpuResource(container1Info),
				Value:  75,
			},
		},
	}

	updateSet3 := &metric.UpdateSet{
		Timestamp: time.Date(2026, time.January, 1, 0, 1, 0, 0, time.UTC),
		Updates: []metric.Update{
			// container1 has both requests and usage
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toMemoryResource(container1Info),
				Value:  4.0 * 1024 * 1024 * 1024,
			},
			{
				Name:   metric.ContainerMemoryWorkingSetBytes,
				Labels: maps.Clone(container1Info),
				Value:  6.0 * 1024 * 1024 * 1024,
			},
			// container2 only has usage
			{
				Name:   metric.ContainerMemoryWorkingSetBytes,
				Labels: maps.Clone(container2Info),
				Value:  1.75 * 1024 * 1024 * 1024,
			},
			// add some additional metrics to test filtering
			{
				Name:   metric.KubeNamespaceLabels,
				Labels: maps.Clone(pod1Info),
				Value:  0,
			},
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toCpuResource(container1Info),
				Value:  135,
			},
		},
	}

	scrape := 0
	updater := NewFuncUpdater(func(us *metric.UpdateSet) {
		// first scrape:
		//  - container1: max(4.0gb, 5.5gb)
		//  - container2: 1.5gb
		if scrape == 0 {
			assertMetricValue(t, us, metric.ContainerMemoryAllocationBytes, "container1", 5.5*1024*1024*1024)
			assertMetricValue(t, us, metric.ContainerMemoryAllocationBytes, "container2", 1.5*1024*1024*1024)
		}

		// second scrape
		//  - container1: max(4.0gb, 3.5gb)
		//  - container2: 2.5gb
		if scrape == 1 {
			assertMetricValue(t, us, metric.ContainerMemoryAllocationBytes, "container1", 4.0*1024*1024*1024)
			assertMetricValue(t, us, metric.ContainerMemoryAllocationBytes, "container2", 2.5*1024*1024*1024)
		}

		// third scrape
		//  - container1: max(4.0gb, 6.0gb)
		//  - container2: 1.75gb
		if scrape == 2 {
			assertMetricValue(t, us, metric.ContainerMemoryAllocationBytes, "container1", 6.0*1024*1024*1024)
			assertMetricValue(t, us, metric.ContainerMemoryAllocationBytes, "container2", 1.75*1024*1024*1024)
		}

		scrape += 1
	})

	metricSynth := NewMetricSynthesizers(updater, NewContainerCpuAllocationSynthesizer(), NewContainerMemoryAllocationSynthesizer())

	metricSynth.Update(updateSet1)
	metricSynth.Update(updateSet2)
	metricSynth.Update(updateSet3)
}

func TestMetricSynthesizerCPUAllocation(t *testing.T) {
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

	const startingCPUSeconds float64 = 506000.0

	updateSet1 := &metric.UpdateSet{
		Timestamp: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		Updates: []metric.Update{
			// container1 has both requests and usage
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toCpuResource(container1Info),
				Value:  0.2,
			},
			{
				Name:   metric.ContainerCPUUsageSecondsTotal,
				Labels: maps.Clone(container1Info),
				Value:  startingCPUSeconds,
			},
			// container2 only has usage
			{
				Name:   metric.ContainerCPUUsageSecondsTotal,
				Labels: maps.Clone(container2Info),
				Value:  startingCPUSeconds,
			},
			// add some additional metrics to test filtering
			{
				Name:   metric.KubeNamespaceLabels,
				Labels: maps.Clone(pod1Info),
				Value:  0,
			},
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toMemoryResource(container1Info),
				Value:  2.5 * 1024.0 * 1024.0 * 1024.0,
			},
		},
	}

	updateSet2 := &metric.UpdateSet{
		Timestamp: time.Date(2026, time.January, 1, 0, 0, 30, 0, time.UTC),
		Updates: []metric.Update{
			// container1 has both requests and usage
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toCpuResource(container1Info),
				Value:  0.2,
			},
			{
				Name:   metric.ContainerCPUUsageSecondsTotal,
				Labels: maps.Clone(container1Info),
				Value:  startingCPUSeconds + 40.0,
			},
			// container2 only has usage
			{
				Name:   metric.ContainerCPUUsageSecondsTotal,
				Labels: maps.Clone(container2Info),
				Value:  startingCPUSeconds + 30.0,
			},
			// add some additional metrics to test filtering
			{
				Name:   metric.KubeNamespaceLabels,
				Labels: maps.Clone(pod1Info),
				Value:  0,
			},
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toMemoryResource(container1Info),
				Value:  2.5 * 1024.0 * 1024.0 * 1024.0,
			},
		},
	}

	updateSet3 := &metric.UpdateSet{
		Timestamp: time.Date(2026, time.January, 1, 0, 1, 0, 0, time.UTC),
		Updates: []metric.Update{
			// container1 has both requests and usage
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toCpuResource(container1Info),
				Value:  0.2,
			},
			{
				Name:   metric.ContainerCPUUsageSecondsTotal,
				Labels: maps.Clone(container1Info),
				Value:  startingCPUSeconds + 40.0 + 5.0,
			},
			// container2 only has usage
			{
				Name:   metric.ContainerCPUUsageSecondsTotal,
				Labels: maps.Clone(container2Info),
				Value:  startingCPUSeconds + 30.0 + 30.0,
			},
			// add some additional metrics to test filtering
			{
				Name:   metric.KubeNamespaceLabels,
				Labels: maps.Clone(pod1Info),
				Value:  0,
			},
			{
				Name:   metric.KubePodContainerResourceRequests,
				Labels: toMemoryResource(container1Info),
				Value:  2.5 * 1024.0 * 1024.0 * 1024.0,
			},
		},
	}

	scrape := 0
	updater := NewFuncUpdater(func(us *metric.UpdateSet) {
		// first scrape:
		//  - container1: alloc = request
		//  - container2: no metric
		if scrape == 0 {
			assertMetricValue(t, us, metric.ContainerCPUAllocation, "container1", 0.2)
			assertNoMetricExists(t, us, metric.ContainerCPUAllocation, "container2")
		}

		// second scrape
		//  - container1: alloc = 40s/30s = 1.33
		//  - container2: alloc = 30s/30s = 1.0
		if scrape == 1 {
			assertMetricValue(t, us, metric.ContainerCPUAllocation, "container1", 1.33333333)
			assertMetricValue(t, us, metric.ContainerCPUAllocation, "container2", 1.0)
		}

		// third scrape
		//  - container1: alloc = 5.0/30.0s = 0.13, so alloc = request again (0.2)
		//  - container2: alloc = 30s/30s = 1.0
		if scrape == 2 {
			assertMetricValue(t, us, metric.ContainerCPUAllocation, "container1", 0.2)
			assertMetricValue(t, us, metric.ContainerCPUAllocation, "container2", 1.0)
		}

		scrape += 1
	})

	metricSynth := NewMetricSynthesizers(updater, NewContainerCpuAllocationSynthesizer(), NewContainerMemoryAllocationSynthesizer())

	metricSynth.Update(updateSet1)
	metricSynth.Update(updateSet2)
	metricSynth.Update(updateSet3)
}
