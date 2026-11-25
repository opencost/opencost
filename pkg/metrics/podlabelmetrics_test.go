package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestUpdateControllerSelectorsCache_NilSelector verifies that nil SpecSelector
// on ReplicaSets and StatefulSets does not panic and is safely skipped.
func TestUpdateControllerSelectorsCache_NilSelector(t *testing.T) {
	nilReplicaSets := []*clustercache.ReplicaSet{
		{SpecSelector: nil}, // must not panic
		{SpecSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "valid"},
		}},
	}
	nilStatefulSets := []*clustercache.StatefulSet{
		{SpecSelector: nil}, // must not panic
		{SpecSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"component": "db"},
		}},
	}

	kc := NewFakeCache(nilReplicaSets, nilStatefulSets, []*clustercache.Service{})
	mc := MetricsConfig{
		DisabledMetrics:    []string{},
		UseLabelsWhitelist: true,
		LabelsWhitelist:    map[string]bool{},
	}
	kplc := KubePodLabelsCollector{
		KubeClusterCache: kc,
		metricsConfig:    mc,
	}

	// Must not panic
	kplc.UpdateWhitelist()

	if !kplc.labelsWhitelist["app"] {
		t.Error("Expected 'app' label from valid ReplicaSet selector to be whitelisted")
	}
	if !kplc.labelsWhitelist["component"] {
		t.Error("Expected 'component' label from valid StatefulSet selector to be whitelisted")
	}
}

func TestWhitelist(t *testing.T) {
	sampleServices := []*clustercache.Service{{
		SpecSelector: map[string]string{"servicewhitelistlabel": "foo"},
	}}
	replicaSetLabelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{"replicasetwhitelistlabel1": "bar"},
	}
	sampleReplicaSets := []*clustercache.ReplicaSet{{
		SpecSelector: &replicaSetLabelSelector,
	}}

	sampleStatefulSets := []*clustercache.StatefulSet{}

	kc := &clustercache.MockClusterCache{ReplicaSets: sampleReplicaSets, StatefulSets: sampleStatefulSets, Services: sampleServices}
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
	if !kplc.labelsWhitelist["servicewhitelistlabel"] {
		t.Errorf("Missing expected label %s", "servicewhitelistlabel")
	}
	if !kplc.labelsWhitelist["replicasetwhitelistlabel1"] {
		t.Errorf("Missing expected label %s", "servicewhitelistlabel1")
	}

}
