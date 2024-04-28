package metrics

import (
	"testing"

	"github.com/opencost/opencost/pkg/clustercache"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWhitelist(t *testing.T) {
	sampleServices := []*v1.Service{&v1.Service{
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"servicewhitelistlabel": "foo"},
		},
	}}
	replicaSetLabelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{"replicasetwhitelistlabel1": "bar"},
	}
	sampleReplicaSets := []*appsv1.ReplicaSet{{
		Spec: appsv1.ReplicaSetSpec{
			Selector: &replicaSetLabelSelector,
		},
	}}

	sampleStatefulSets := []*appsv1.StatefulSet{}

	kc := NewFakeCache(sampleReplicaSets, sampleStatefulSets, sampleServices)
	wl := map[string]bool{
		"whitelistedlabel": true,
	}
	mc := MetricsConfig{
		DisabledMetrics:    []string{},
		UseLabelsWhitelist: true,
		LabelsWhitelist:    wl,
	}
	kplc := KubePodLabelsCollector{
		KubeClusterCache: kc,
		metricsConfig:    mc,
	}
	kplc.UpdateWhitelist()
	//kplc.labelsWhitelist[]
}

type FakeCache struct {
	clustercache.ClusterCache
	replicasets  []*appsv1.ReplicaSet
	statefulsets []*appsv1.StatefulSet
	services     []*v1.Service
}

func (f FakeCache) GetAllReplicaSets() []*appsv1.ReplicaSet {
	return f.replicasets
}

func (f FakeCache) GetAllStatefulSets() []*appsv1.StatefulSet {
	return f.statefulsets
}

func (f FakeCache) GetAllServices() []*v1.Service {
	return f.services
}

func NewFakeCache(replicasets []*appsv1.ReplicaSet, statefulsets []*appsv1.StatefulSet, services []*v1.Service) FakeCache {
	return FakeCache{
		replicasets:  replicasets,
		statefulsets: statefulsets,
		services:     services,
	}
}
