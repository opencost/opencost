package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func TestComputePods(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name      string
		overrides map[string]any
		want      map[string]*kubemodel.Pod
	}{
		{
			name:      "no data returns empty pod map",
			overrides: map[string]any{},
			want:      map[string]*kubemodel.Pod{},
		},
		{
			name: "basic pod info and uptime",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "pod without uptime is not registered",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
			},
			want: map[string]*kubemodel.Pod{},
		},
		{
			name: "pod without namespace uid is not registered",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Pod{},
		},
		{
			name: "pod with node uid",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1", NodeUID: "node-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					NodeUID:      "node-1",
					Start:        start,
					End:          end,
				},
			},
		},
		{
			name: "pod owners are attached",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
				source.QueryPodOwners: []*source.OwnerResult{
					{UID: "pod-1", OwnerUID: "rs-1", OwnerKind: "ReplicaSet", Controller: true},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Owners: []kubemodel.Owner{
						{UID: "rs-1", Kind: kubemodel.OwnerKindReplicaSet, Controller: true},
					},
				},
			},
		},
		{
			name: "pod pvc volumes are attached",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
				source.QueryPodPVCVolumes: []*source.PodPVCVolumeResult{
					{UID: "pod-1", PVCUID: "pvc-1", PodVolumeName: "data"},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					PVCVolumes: []kubemodel.PodPVCVolume{
						{Name: "data", PersistentVolumeClaimUID: "pvc-1"},
					},
				},
			},
		},
		{
			name: "pod labels and annotations are attached",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
				source.QueryPodLabels: []*source.PodLabelsResult{
					{UID: "pod-1", Labels: map[string]string{"app": "web"}},
				},
				source.QueryPodAnnotations: []*source.PodAnnotationsResult{
					{UID: "pod-1", Annotations: map[string]string{"team": "platform"}},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					Labels:       map[string]string{"app": "web"},
					Annotations:  map[string]string{"team": "platform"},
				},
			},
		},
		{
			name: "uptime for unknown pod is ignored",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
					{UID: "unknown-pod", First: start, Last: end},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			seedCluster(ds, start, end)
			for method, result := range tt.overrides {
				ds.Querier.SetOverride(method, result)
			}

			km, err := NewKubeModel(testClusterUID, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			require.NoError(t, err)

			assert.Equal(t, tt.want, kms.Pods)
		})
	}
}