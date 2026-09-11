package costmodel

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuildPodMetricMetadataNamespaceIsolation(t *testing.T) {
	running := &clustercache.Pod{
		Namespace: "team-a",
		Name:      "database-0",
		UID:       "running-uid",
		Status:    clustercache.PodStatus{Phase: v1.PodRunning},
	}
	succeeded := &clustercache.Pod{
		Namespace: "team-b",
		Name:      "database-0",
		UID:       "succeeded-uid",
		Status:    clustercache.PodStatus{Phase: v1.PodSucceeded},
	}

	for _, tc := range []struct {
		name string
		pods []*clustercache.Pod
	}{
		{name: "running first", pods: []*clustercache.Pod{running, succeeded}},
		{name: "running last", pods: []*clustercache.Pod{succeeded, running}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := buildPodMetricMetadata(tc.pods)
			for _, want := range []struct {
				namespace string
				uid       string
				phase     v1.PodPhase
			}{
				{namespace: "team-a", uid: "running-uid", phase: v1.PodRunning},
				{namespace: "team-b", uid: "succeeded-uid", phase: v1.PodSucceeded},
			} {
				key := types.NamespacedName{Namespace: want.namespace, Name: "database-0"}
				got, ok := metadata[key]
				if !ok || got.uid != want.uid || got.phase != want.phase {
					t.Errorf("metadata for %s = %+v, present=%t; want UID %q and phase %q", key, got, ok, want.uid, want.phase)
				}
			}

			for _, key := range []types.NamespacedName{
				{Namespace: "team-c", Name: "database-0"},
				{Namespace: "team-a", Name: "missing-0"},
			} {
				if got, ok := metadata[key]; ok || got != (podMetricMetadata{}) {
					t.Errorf("metadata for missing pod %s = %+v, present=%t; want no UID or phase", key, got, ok)
				}
			}
		})
	}
}
