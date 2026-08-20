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
		},
	}
	enrich := podUIDEnricher(cache)

	tests := map[string]struct {
		update metric.Update
		want   metric.Update
	}{
		"resolves pod_uid from namespace and pod": {
			update: metric.Update{
				Labels: map[string]string{
					source.NamespaceLabel: "namespace1",
					source.PodLabel:       "pod1",
				},
			},
			want: metric.Update{
				Labels: map[string]string{
					source.NamespaceLabel: "namespace1",
					source.PodLabel:       "pod1",
					source.PodUIDLabel:    "pod-uid-1",
				},
			},
		},
		"unknown pod is left unset": {
			update: metric.Update{
				Labels: map[string]string{
					source.NamespaceLabel: "namespace1",
					source.PodLabel:       "unknown-pod",
				},
			},
			want: metric.Update{
				Labels: map[string]string{
					source.NamespaceLabel: "namespace1",
					source.PodLabel:       "unknown-pod",
				},
			},
		},
		"missing namespace or pod label is left unset": {
			update: metric.Update{
				Labels: map[string]string{
					source.PodLabel: "pod1",
				},
			},
			want: metric.Update{
				Labels: map[string]string{
					source.PodLabel: "pod1",
				},
			},
		},
		"existing non-empty pod_uid is left untouched": {
			update: metric.Update{
				Labels: map[string]string{
					source.NamespaceLabel: "namespace1",
					source.PodLabel:       "pod1",
					source.PodUIDLabel:    "already-set",
				},
			},
			want: metric.Update{
				Labels: map[string]string{
					source.NamespaceLabel: "namespace1",
					source.PodLabel:       "pod1",
					source.PodUIDLabel:    "already-set",
				},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := enrich(tt.update)
			if len(got.Labels) != len(tt.want.Labels) {
				t.Fatalf("got labels %v, want %v", got.Labels, tt.want.Labels)
			}
			for k, v := range tt.want.Labels {
				if got.Labels[k] != v {
					t.Errorf("label %q = %q, want %q", k, got.Labels[k], v)
				}
			}
		})
	}
}
