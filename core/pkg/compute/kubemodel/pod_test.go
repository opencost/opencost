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
		{
			name: "network egress: internet, cross-region, cross-zone traffic types",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
				source.QueryPodNetworkEgressBytes: []*source.PodNetworkBytesResult{
					{UID: "pod-1", Internet: true, Service: "svc-a", Value: 100},
					{UID: "pod-1", Internet: false, SameRegion: false, Service: "svc-b", Value: 200},
					{UID: "pod-1", Internet: false, SameRegion: true, SameZone: false, Service: "svc-c", Value: 300},
					{UID: "pod-1", Internet: false, SameRegion: true, SameZone: true, Service: "svc-d", Value: 400}, // no traffic type → skipped
					{UID: "pod-1", Internet: true, Service: "svc-e", Value: 0},                                      // zero bytes → skipped
					{UID: "unknown-pod", Internet: true, Service: "svc-f", Value: 500},                              // unknown pod → skipped
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					NetworkTrafficDetails: []kubemodel.NetworkTrafficDetail{
						{PodUID: "pod-1", TrafficDirection: kubemodel.TrafficDirectionEgress, TrafficType: kubemodel.TrafficTypeInternet, Endpoint: "svc-a", Bytes: 100},
						{PodUID: "pod-1", TrafficDirection: kubemodel.TrafficDirectionEgress, TrafficType: kubemodel.TrafficTypeCrossRegion, Endpoint: "svc-b", Bytes: 200},
						{PodUID: "pod-1", TrafficDirection: kubemodel.TrafficDirectionEgress, TrafficType: kubemodel.TrafficTypeCrossZone, Endpoint: "svc-c", Bytes: 300},
					},
				},
			},
		},
		{
			name: "unknown uid in supplementary data is ignored",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
				// all of these carry an unknown UID — hit the !ok warn paths
				source.QueryPodOwners: []*source.OwnerResult{
					{UID: "unknown-pod", OwnerUID: "rs-1", OwnerKind: "ReplicaSet", Controller: true},
				},
				source.QueryPodPVCVolumes: []*source.PodPVCVolumeResult{
					{UID: "unknown-pod", PVCUID: "pvc-1", PodVolumeName: "data"},
				},
				source.QueryPodLabels: []*source.PodLabelsResult{
					{UID: "unknown-pod", Labels: map[string]string{"app": "web"}},
				},
				source.QueryPodAnnotations: []*source.PodAnnotationsResult{
					{UID: "unknown-pod", Annotations: map[string]string{"team": "platform"}},
				},
				// same-zone+same-region ingress: triggers !ok continue in ingress loop
				source.QueryPodNetworkIngressBytes: []*source.PodNetworkBytesResult{
					{UID: "pod-1", Internet: false, SameRegion: true, SameZone: true, Value: 100},
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
			name: "network ingress traffic is recorded",
			overrides: map[string]any{
				source.QueryPodInfo: []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				},
				source.QueryPodUptime: []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				},
				source.QueryPodNetworkIngressBytes: []*source.PodNetworkBytesResult{
					{UID: "pod-1", Internet: true, NatGateway: true, Service: "svc-a", Value: 512},
					{UID: "pod-1", Internet: false, SameRegion: false, Service: "svc-b", Value: 256},
				},
			},
			want: map[string]*kubemodel.Pod{
				"pod-1": {
					UID:          "pod-1",
					Name:         "my-pod",
					NamespaceUID: "ns-1",
					Start:        start,
					End:          end,
					NetworkTrafficDetails: []kubemodel.NetworkTrafficDetail{
						{PodUID: "pod-1", TrafficDirection: kubemodel.TrafficDirectionIngress, TrafficType: kubemodel.TrafficTypeInternet, IsNatGateway: true, Endpoint: "svc-a", Bytes: 512},
						{PodUID: "pod-1", TrafficDirection: kubemodel.TrafficDirectionIngress, TrafficType: kubemodel.TrafficTypeCrossRegion, Endpoint: "svc-b", Bytes: 256},
					},
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
