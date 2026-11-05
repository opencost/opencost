package scrape

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
)

func Test_isDCGM(t *testing.T) {
	tests := map[string]struct {
		labels map[string]string
		want   bool
	}{
		"nil": {
			labels: nil,
			want:   false,
		},
		"empty": {
			labels: map[string]string{},
			want:   false,
		},
		"app": {
			labels: map[string]string{
				"app": "dcgm-exporter",
			},
			want: true,
		},
		"app.kubernetes.io/name": {
			labels: map[string]string{
				"app.kubernetes.io/name": "dcgm-exporter",
			},
			want: true,
		},
		"app.kubernetes.io/component": {
			labels: map[string]string{
				"app.kubernetes.io/name": "dcgm-exporter",
			},
			want: true,
		},
		"invalid key": {
			labels: map[string]string{
				"invalid-key": "dcgm-exporter",
			},
			want: false,
		},
		"invalid value": {
			labels: map[string]string{
				"app.kubernetes.io/name": "dcgmExporter",
			},
			want: false,
		},
		"case insensitive": {
			labels: map[string]string{
				"app.kubernetes.io/name": "jhlkjhlkDcGm-eXpoRterlkjhlkuh",
			},
			want: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isDCGM(tt.labels); got != tt.want {
				t.Errorf("isDCGM() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_podUIDEnricher(t *testing.T) {
	cache := &clustercache.MockClusterCache{
		Pods: []*clustercache.Pod{
			{UID: "pod-uid-1", Name: "pod1", Namespace: "namespace1"},
			{UID: "pod-uid-2", Name: "pod2", Namespace: "namespace2"},
		},
	}

	tests := map[string]struct {
		labels map[string]string
		want   map[string]string
	}{
		"resolves pod_uid from namespace and pod": {
			labels: map[string]string{
				source.NamespaceLabel: "namespace1",
				source.PodLabel:       "pod1",
			},
			want: map[string]string{
				source.NamespaceLabel: "namespace1",
				source.PodLabel:       "pod1",
				source.PodUIDLabel:    "pod-uid-1",
			},
		},
		"unknown pod is left unset": {
			labels: map[string]string{
				source.NamespaceLabel: "namespace1",
				source.PodLabel:       "unknown-pod",
			},
			want: map[string]string{
				source.NamespaceLabel: "namespace1",
				source.PodLabel:       "unknown-pod",
			},
		},
		"pod name from wrong namespace is left unset": {
			labels: map[string]string{
				source.NamespaceLabel: "namespace2",
				source.PodLabel:       "pod1",
			},
			want: map[string]string{
				source.NamespaceLabel: "namespace2",
				source.PodLabel:       "pod1",
			},
		},
		"missing namespace label is left unset": {
			labels: map[string]string{
				source.PodLabel: "pod1",
			},
			want: map[string]string{
				source.PodLabel: "pod1",
			},
		},
		"missing pod label is left unset": {
			labels: map[string]string{
				source.NamespaceLabel: "namespace1",
			},
			want: map[string]string{
				source.NamespaceLabel: "namespace1",
			},
		},
		"nil labels is left untouched": {
			labels: nil,
			want:   nil,
		},
		"existing non-empty pod_uid is left untouched": {
			labels: map[string]string{
				source.NamespaceLabel: "namespace1",
				source.PodLabel:       "pod1",
				source.PodUIDLabel:    "already-set",
			},
			want: map[string]string{
				source.NamespaceLabel: "namespace1",
				source.PodLabel:       "pod1",
				source.PodUIDLabel:    "already-set",
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			enrich := podUIDEnricher(cache)
			updates := []metric.Update{{Labels: cloneLabels(tt.labels)}}

			enrich(updates)

			assertLabels(t, updates[0].Labels, tt.want)
		})
	}

	// A single call must correctly enrich every update in the batch, not just
	// the first, since updates are processed together rather than one at a time.
	t.Run("enriches every update in a multi-update batch", func(t *testing.T) {
		enrich := podUIDEnricher(cache)
		updates := []metric.Update{
			{Labels: map[string]string{source.NamespaceLabel: "namespace1", source.PodLabel: "pod1"}},
			{Labels: map[string]string{source.NamespaceLabel: "namespace2", source.PodLabel: "pod2"}},
			{Labels: map[string]string{source.NamespaceLabel: "namespace1", source.PodLabel: "unknown-pod"}},
		}

		enrich(updates)

		assertLabels(t, updates[0].Labels, map[string]string{
			source.NamespaceLabel: "namespace1",
			source.PodLabel:       "pod1",
			source.PodUIDLabel:    "pod-uid-1",
		})
		assertLabels(t, updates[1].Labels, map[string]string{
			source.NamespaceLabel: "namespace2",
			source.PodLabel:       "pod2",
			source.PodUIDLabel:    "pod-uid-2",
		})
		assertLabels(t, updates[2].Labels, map[string]string{
			source.NamespaceLabel: "namespace1",
			source.PodLabel:       "unknown-pod",
		})
	})

	t.Run("empty batch does not panic", func(t *testing.T) {
		enrich := podUIDEnricher(cache)
		enrich(nil)
		enrich([]metric.Update{})
	})
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for k, v := range labels {
		clone[k] = v
	}
	return clone
}

func assertLabels(t *testing.T, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got labels %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("label %q = %q, want %q", k, got[k], v)
		}
	}
}
