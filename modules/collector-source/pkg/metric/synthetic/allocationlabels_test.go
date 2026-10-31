package synthetic

import (
	"maps"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

func TestAllocationMetric_Labels(t *testing.T) {
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	kinds := []struct {
		name       string
		resource   string
		unit       string
		metricName string
		synthesize func(*metric.Update, *metric.Update) metric.Update
	}{
		{
			name: "memory", resource: "memory", unit: "byte", metricName: metric.ContainerMemoryAllocationBytes,
			synthesize: func(request, usage *metric.Update) metric.Update {
				return (&ContainerMemoryAllocationMetric{requestMetric: request, usageMetric: usage}).Synthesize()
			},
		},
		{
			name: "cpu", resource: "cpu", unit: "core", metricName: metric.ContainerCPUAllocation,
			synthesize: func(request, usage *metric.Update) metric.Update {
				var cpuUsage *CpuUsageMetric
				if usage != nil {
					cpuUsage = NewCpuUsageMetric(start, &metric.Update{Labels: maps.Clone(usage.Labels), Value: 0})
					cpuUsage.Push(start.Add(time.Second), usage)
				}
				return (&ContainerCpuAllocationMetric{requestMetric: request, usageMetric: cpuUsage}).Synthesize()
			},
		},
	}
	tests := []struct {
		name    string
		request map[string]string
		usage   map[string]string
		want    map[string]string
	}{
		{
			name:    "preserve labels from both inputs",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": "ns1"},
			usage:   map[string]string{"uid": "pod1", "container": "app", "node": "node1", "image": "app:v1"},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": "ns1", "image": "app:v1"},
		},
		{
			name:    "fill missing labels",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "node1", "instance": "node1", "pod": "pod", "namespace": "ns"},
			usage:   map[string]string{"uid": "pod1", "container": "app"},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "node1", "instance": "node1", "pod": "pod", "namespace": "ns"},
		},
		{
			name:    "fill empty labels",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "node1", "instance": "node1", "namespace": "ns"},
			usage:   map[string]string{"uid": "pod1", "container": "app", "node": "", "instance": "", "namespace": ""},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "node1", "instance": "node1", "namespace": "ns"},
		},
		{
			name:    "keep nonempty usage labels on conflict",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "request-node", "instance": "request-node", "namespace": "request-ns", "namespace_uid": "ns1"},
			usage:   map[string]string{"uid": "pod1", "container": "app", "node": "usage-node", "instance": "usage-node", "namespace": "usage-ns"},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "usage-node", "instance": "usage-node", "namespace": "usage-ns", "namespace_uid": "ns1"},
		},
		{
			name:    "empty request label does not erase usage label",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": ""},
			usage:   map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": "ns1"},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": "ns1"},
		},
		{
			name:    "exclude resource and unit from combined labels",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "node1"},
			usage:   map[string]string{"uid": "pod1", "container": "app", "node": "node1", "resource": "unused", "unit": "unused"},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "node1"},
		},
		{
			name:    "request only",
			request: map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": "ns1"},
			want:    map[string]string{"uid": "pod1", "container": "app", "node": "node1", "namespace_uid": "ns1"},
		},
		{
			name:  "usage only",
			usage: map[string]string{"uid": "pod1", "container": "app", "node": "node1", "image": "app:v1"},
			want:  map[string]string{"uid": "pod1", "container": "app", "node": "node1", "image": "app:v1"},
		},
	}
	for _, kind := range kinds {
		t.Run(kind.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					var request, usage *metric.Update
					if tt.request != nil {
						request = &metric.Update{Labels: maps.Clone(tt.request), Value: 4}
						request.Labels[source.ResourceLabel] = kind.resource
						request.Labels[source.UnitLabel] = kind.unit
					}
					if tt.usage != nil {
						usage = &metric.Update{Labels: maps.Clone(tt.usage), Value: 6}
					}
					var requestBefore, usageBefore map[string]string
					if request != nil {
						requestBefore = maps.Clone(request.Labels)
					}
					if usage != nil {
						usageBefore = maps.Clone(usage.Labels)
					}
					got := kind.synthesize(request, usage)
					if !maps.Equal(got.Labels, tt.want) {
						t.Errorf("labels = %v, want %v", got.Labels, tt.want)
					}
					wantValue := 6.0
					if usage == nil {
						wantValue = 4
					}
					if got.Name != kind.metricName || got.Value != wantValue {
						t.Errorf("metric = %s/%v, want %s/%v", got.Name, got.Value, kind.metricName, wantValue)
					}
					// The output must own its labels: mutating it must not affect either input.
					got.Labels[source.NodeLabel] = "changed"
					if request != nil && !maps.Equal(request.Labels, requestBefore) {
						t.Errorf("request labels changed: %v, want %v", request.Labels, requestBefore)
					}
					if usage != nil && !maps.Equal(usage.Labels, usageBefore) {
						t.Errorf("usage labels changed: %v, want %v", usage.Labels, usageBefore)
					}
				})
			}
		})
	}
}

func TestMetricSynthesizerCPUAllocation_LabelCycles(t *testing.T) {
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	base := map[string]string{"uid": "pod1", "container": "app", "node": "request-node", "namespace_uid": "ns1"}
	usageLabels := []map[string]string{
		{"uid": "pod1", "container": "app", "node": "previous-node"},
		{"uid": "pod1", "container": "app"},
		{"uid": "pod1", "container": "app", "node": "current-node"},
	}
	wantLabels := []map[string]string{base, base, usageLabels[2]}
	wantValues := []float64{4, 6, 6}
	scrape := 0
	next := NewFuncUpdater(func(set *metric.UpdateSet) {
		update := findMetric(t, set, metric.ContainerCPUAllocation, "app")
		if update == nil {
			t.Fatal("missing CPU allocation")
		}
		if !maps.Equal(update.Labels, wantLabels[scrape]) || update.Value != wantValues[scrape] {
			t.Errorf("scrape %d: allocation = %v/%v, want %v/%v", scrape, update.Labels, update.Value, wantLabels[scrape], wantValues[scrape])
		}
	})
	synthesizers := NewMetricSynthesizers(next, NewContainerCpuAllocationSynthesizer())
	for scrape = range usageLabels {
		updates := []metric.Update{{Name: metric.ContainerCPUUsageSecondsTotal, Labels: maps.Clone(usageLabels[scrape]), Value: float64(scrape * 180)}}
		if scrape < 2 {
			updates = append(updates, metric.Update{Name: metric.KubePodContainerResourceRequests, Labels: toCpuResource(base), Value: 4})
		}
		synthesizers.Update(&metric.UpdateSet{Timestamp: start.Add(time.Duration(scrape) * 30 * time.Second), Updates: updates})
	}
}
