package promutil

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/stretchr/testify/assert"
)

func checkSlice(s1, s2 []string) error {
	if len(s1) != len(s2) {
		return fmt.Errorf("len(s1) [%d] != len(s2) [%d]", len(s1), len(s2))
	}

	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return fmt.Errorf("At Index: %d. Different Values %s (s1) != %s (s2)", i, s1[i], s2[i])
		}
	}
	return nil
}

func TestEmptyKubeLabelsToPromLabels(t *testing.T) {
	labels, values := KubeLabelsToLabels(nil)

	if len(labels) != 0 {
		t.Errorf("Labels length is non-zero\n")
	}
	if len(values) != 0 {
		t.Errorf("Values length is non-zero\n")
	}

	labels, values = KubeLabelsToLabels(map[string]string{})

	if len(labels) != 0 {
		t.Errorf("Labels length is non-zero\n")
	}
	if len(values) != 0 {
		t.Errorf("Values length is non-zero\n")
	}
}

func TestKubeLabelsToPromLabels(t *testing.T) {
	var expectedLabels []string = []string{
		"label_app",
		"label_chart",
		"label_control_plane",
		"label_gatekeeper_sh_operation",
		"label_heritage",
		"label_pod_template_hash",
		"label_release",
	}
	var expectedValues []string = []string{
		"gatekeeper",
		"gatekeeper",
		"audit-controller",
		"audit",
		"Helm",
		"5599859cd4",
		"gatekeeper",
	}

	kubeLabels := map[string]string{
		"app":                     "gatekeeper",
		"chart":                   "gatekeeper",
		"control-plane":           "audit-controller",
		"gatekeeper.sh/operation": "audit",
		"heritage":                "Helm",
		"pod-template-hash":       "5599859cd4",
		"release":                 "gatekeeper",
	}

	labels, values := KubePrependQualifierToLabels(kubeLabels, "label_")
	l2, v2 := KubeLabelsToLabels(kubeLabels)

	// Check to make sure we get expected labels and values returned
	err := checkSlice(labels, expectedLabels)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = checkSlice(values, expectedValues)
	if err != nil {
		t.Errorf("%s", err)
	}

	// Check to make sure the helper function returns what the prependqualifier func
	// returns
	err = checkSlice(l2, labels)
	if err != nil {
		t.Errorf("%s", err)
	}

	err = checkSlice(v2, values)
	if err != nil {
		t.Errorf("%s", err)
	}
}

func TestKubePrependQualifierToLabelsDuplicates(t *testing.T) {
	// 7 expected labels/values
	expectedLabels := []string{
		"label_app_",
		"label_chart",
		"label_control_plane",
		"label_gatekeeper_sh_operation",
		"label_heritage",
		"label_pod_template_hash",
		"label_release",
	}
	expectedValues := []string{
		"gatekeeper",
		"gatekeeper",
		"audit-controller",
		"audit",
		"Helm",
		"5599859cd4",
		"gatekeeper",
	}

	// 8 input labels/values, with one duplicate label
	kubeLabels := map[string]string{
		// app- will be sanitized to app_
		"app-":                    "gatekeeper",
		"app_":                    "gatekeeper",
		"chart":                   "gatekeeper",
		"control-plane":           "audit-controller",
		"gatekeeper.sh/operation": "audit",
		"heritage":                "Helm",
		"pod-template-hash":       "5599859cd4",
		"release":                 "gatekeeper",
	}

	labels, values := KubePrependQualifierToLabels(kubeLabels, "label_")

	// Check to make sure we get expected labels and values returned
	err := checkSlice(labels, expectedLabels)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = checkSlice(values, expectedValues)
	if err != nil {
		t.Errorf("%s", err)
	}
}

func TestSanitizeLabels(t *testing.T) {
	type testCase struct {
		in  map[string]string
		exp map[string]string
	}

	tcs := map[string]testCase{
		"empty labels": {
			in:  map[string]string{},
			exp: map[string]string{},
		},
		"no op": {
			in: map[string]string{
				"foo": "bar",
				"baz": "loo",
			},
			exp: map[string]string{
				"foo": "bar",
				"baz": "loo",
			},
		},
		"modification, no collisions": {
			in: map[string]string{
				"foo-foo":   "bar",
				"baz---baz": "loo",
			},
			exp: map[string]string{
				"foo_foo":   "bar",
				"baz___baz": "loo",
			},
		},
		"modification, one collision": {
			in: map[string]string{
				"foo-foo":   "bar",
				"foo+foo":   "bar",
				"baz---baz": "loo",
			},
			exp: map[string]string{
				"foo_foo":   "bar",
				"baz___baz": "loo",
			},
		},
		"modification, all collisions": {
			in: map[string]string{
				"foo-foo": "bar",
				"foo+foo": "bar",
				"foo_foo": "bar",
			},
			exp: map[string]string{
				"foo_foo": "bar",
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			act := SanitizeLabels(tc.in)
			if !reflect.DeepEqual(tc.exp, act) {
				t.Errorf("sanitizing labels failed for case %s: %+v != %+v", name, tc.exp, act)
			}
		})
	}
}

func TestClusterInfoLabels(t *testing.T) {
	expected := map[string]bool{"clusterprofile": true, "errorreporting": true, "id": true, "logcollection": true, "name": true, "productanalytics": true, "provider": true, "provisioner": true, "remotereadenabled": true, "thanosenabled": true, "valuesreporting": true, "version": true}
	clusterInfo := `{"clusterProfile":"production","errorReporting":"true","id":"cluster-one","logCollection":"true","name":"bolt-3","productAnalytics":"true","provider":"GCP","provisioner":"GKE","remoteReadEnabled":"false","thanosEnabled":"false","valuesReporting":"true","version":"1.14+"}`

	var m map[string]any
	err := json.Unmarshal([]byte(clusterInfo), &m)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	labels := MapToLabels(m)
	for k := range expected {
		if _, ok := labels[k]; !ok {
			t.Errorf("Failed to locate key: \"%s\" in labels.", k)
			return
		}
	}
}

func TestPrependQualifierAndMerge(t *testing.T) {
	m := map[string]string{
		"a-a":     "A",
		"b-b.c.d": "B",
		"cfg-c":   "C",
	}

	exLabels := map[string]string{
		"node-type":  "m1.large",
		"cluster.id": "cluster-a",
		"some-value": "524.2",
	}

	expected := map[string]string{
		"label_a_a":        "A",
		"label_b_b_c_d":    "B",
		"label_cfg_c":      "C",
		"label_cluster_id": "cluster-a",
		"label_node_type":  "m1.large",
		"label_some_value": "524.2",
	}

	result := KubePrependQualifierToLabelsAndMerge(m, exLabels, "label_")
	for k, v := range expected {
		val, ok := result[k]
		if !ok {
			t.Fatalf("Expected key: %s in result map, but was not found.", k)
		}
		if val != v {
			t.Fatalf("Expected value: %s for key: %s in result map. Got: %s", v, k, val)
		}
	}
}

func TestPrependQualifierToMap(t *testing.T) {
	m := map[string]string{
		"a-a":     "A",
		"b-b.c.d": "B",
		"cfg-c":   "C",
	}

	expected := map[string]string{
		"label_a_a":     "A",
		"label_b_b_c_d": "B",
		"label_cfg_c":   "C",
	}

	result := KubePrependQualifierToLabelsMap(m, "label_")
	for k, v := range expected {
		val, ok := result[k]
		if !ok {
			t.Fatalf("Expected key: %s in result map, but was not found.", k)
		}
		if val != v {
			t.Fatalf("Expected value: %s for key: %s in result map. Got: %s", v, k, val)
		}
	}
}

func TestPrependQualifierAndMerge_ExternalAddedToBase(t *testing.T) {
	base := map[string]string{"node": "worker-1"}
	external := map[string]string{"region": "us-east-1"}

	got := KubePrependQualifierToLabelsAndMerge(base, external, "label_")

	assert.Equal(t, map[string]string{"label_node": "worker-1", "label_region": "us-east-1"}, got)
}

func TestMerge_BaseWinsOnConflict(t *testing.T) {
	base := map[string]string{"region": "from-node"}
	external := map[string]string{"region": "from-configmap"}

	got := KubePrependQualifierToLabelsAndMerge(external, base, "label_")

	assert.Equal(t, "from-node", got["label_region"])
}

func TestMerge_EmptyExternal(t *testing.T) {
	base := map[string]string{"node": "worker-1"}
	baseWithLabelPrefix := map[string]string{"label_node": "worker-1"}
	got := KubePrependQualifierToLabelsAndMerge(map[string]string{}, base, "label_")

	assert.Equal(t, baseWithLabelPrefix, got)
}

func TestMerge_NilExternal(t *testing.T) {
	base := map[string]string{"node": "worker-1"}
	baseWithLabelPrefix := map[string]string{"label_node": "worker-1"}
	var external map[string]string
	got := KubePrependQualifierToLabelsAndMerge(external, base, "label_")

	assert.Equal(t, baseWithLabelPrefix, got)
}

func TestMerge_EmptyBase(t *testing.T) {
	external := map[string]string{"region": "us-east-1"}
	externalWithLabelPrefix := map[string]string{"label_region": "us-east-1"}
	got := KubePrependQualifierToLabelsAndMerge(external, map[string]string{}, "label_")

	assert.Equal(t, externalWithLabelPrefix, got)
}

func TestMerge_BothEmpty(t *testing.T) {
	got := KubePrependQualifierToLabelsAndMerge(map[string]string{}, map[string]string{}, "label_")

	assert.Empty(t, got)
}

// TestKubePrependQualifierToLabelsAndMerge proves the original base map is not modified.
// A naive implementation using `out := base` copies the map header only,
// so writes to out also mutate the caller's map.
func TestMerge_DoesNotMutateBase(t *testing.T) {
	base := map[string]string{"node": "worker-1"}
	external := map[string]string{"region": "us-east-1"}

	_ = KubePrependQualifierToLabelsAndMerge(external, base, "label_")

	assert.Equal(t, map[string]string{"node": "worker-1"}, base, "Merge must not mutate the base map")
}
