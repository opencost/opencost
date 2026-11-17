package collector

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
)

// TestNewGPUsRequestedMetricCollectorAcceptsBothResourceLabelForms guards
// against a regression where the GPUs-requested collector only recognized
// the sanitized resource label ("nvidia_com_gpu") and silently dropped
// samples reporting the raw Kubernetes resource name ("nvidia.com/gpu").
func TestNewGPUsRequestedMetricCollectorAcceptsBothResourceLabelForms(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{
			name: "sanitized resource label is accepted",
			labels: map[string]string{
				source.ResourceLabel:  "nvidia_com_gpu",
				source.ContainerLabel: "app",
				source.NodeLabel:      "node-1",
			},
			expected: true,
		},
		{
			name: "raw resource label is accepted",
			labels: map[string]string{
				source.ResourceLabel:  "nvidia.com/gpu",
				source.ContainerLabel: "app",
				source.NodeLabel:      "node-1",
			},
			expected: true,
		},
		{
			name: "unrelated resource is filtered out",
			labels: map[string]string{
				source.ResourceLabel:  "cpu",
				source.ContainerLabel: "app",
				source.NodeLabel:      "node-1",
			},
			expected: false,
		},
		{
			name: "POD container is filtered out",
			labels: map[string]string{
				source.ResourceLabel:  "nvidia.com/gpu",
				source.ContainerLabel: "POD",
				source.NodeLabel:      "node-1",
			},
			expected: false,
		},
		{
			name: "empty container is filtered out",
			labels: map[string]string{
				source.ResourceLabel:  "nvidia.com/gpu",
				source.ContainerLabel: "",
				source.NodeLabel:      "node-1",
			},
			expected: false,
		},
		{
			name: "empty node is filtered out",
			labels: map[string]string{
				source.ResourceLabel:  "nvidia.com/gpu",
				source.ContainerLabel: "app",
				source.NodeLabel:      "",
			},
			expected: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mc := NewGPUsRequestedMetricCollector()
			mc.Update(c.labels, 1, now, nil)

			got := len(mc.Get()) > 0
			if got != c.expected {
				t.Errorf("labels %v: expected recorded=%v, got %v", c.labels, c.expected, got)
			}
		})
	}
}
