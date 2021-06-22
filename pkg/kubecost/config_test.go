package kubecost

import "testing"

func TestLabelConfig_Map(t *testing.T) {
	var m map[string]string
	var lc *LabelConfig

	m = lc.Map()
	if len(m) != 18 {
		t.Fatalf("Map: expected length %d; got length %d", 18, len(m))
	}
	if val, ok := m["deployment_external_label"]; !ok || val != "kubernetes_deployment" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_deployment", val)
	}
	if val, ok := m["namespace_external_label"]; !ok || val != "kubernetes_namespace" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_namespace", val)
	}

	lc = &LabelConfig{
		DaemonsetExternalLabel: "kubernetes_ds",
	}
	m = lc.Map()
	if len(m) != 18 {
		t.Fatalf("Map: expected length %d; got length %d", 18, len(m))
	}
	if val, ok := m["daemonset_external_label"]; !ok || val != "kubernetes_ds" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_ds", val)
	}
	if val, ok := m["namespace_external_label"]; !ok || val != "kubernetes_namespace" {
		t.Fatalf("Map: expected %s; got %s", "kubernetes_namespace", val)
	}
}

func TestLabelConfig_ExternalQueryLabels(t *testing.T) {
	var qls map[string]string
	var lc *LabelConfig

	qls = lc.ExternalQueryLabels()
	if len(qls) != 13 {
		t.Fatalf("ExternalQueryLabels: expected length %d; got length %d", 13, len(qls))
	}
	if val, ok := qls["kubernetes_deployment"]; !ok || val != "deployment_external_label" {
		t.Fatalf("ExternalQueryLabels: expected %s; got %s", "deployment_external_label", val)
	}
	if val, ok := qls["kubernetes_namespace"]; !ok || val != "namespace_external_label" {
		t.Fatalf("ExternalQueryLabels: expected %s; got %s", "namespace_external_label", val)
	}

	lc = &LabelConfig{
		DaemonsetExternalLabel: "kubernetes_ds",
	}
	qls = lc.ExternalQueryLabels()
	if len(qls) != 13 {
		t.Fatalf("ExternalQueryLabels: expected length %d; got length %d", 13, len(qls))
	}
	if val, ok := qls["kubernetes_ds"]; !ok || val != "daemonset_external_label" {
		t.Fatalf("ExternalQueryLabels: expected %s; got %s", "daemonset_external_label", val)
	}
	if val, ok := qls["kubernetes_namespace"]; !ok || val != "namespace_external_label" {
		t.Fatalf("ExternalQueryLabels: expected %s; got %s", "namespace_external_label", val)
	}
}

func TestLabelConfig_AllocationPropertyLabels(t *testing.T) {
	var labels map[string]string
	var lc *LabelConfig

	labels = lc.AllocationPropertyLabels()
	if len(labels) != 18 {
		t.Fatalf("AllocationPropertyLabels: expected length %d; got length %d", 18, len(labels))
	}
	if val, ok := labels["namespace"]; !ok || val != "kubernetes_namespace" {
		t.Fatalf("AllocationPropertyLabels: expected %s; got %s", "kubernetes_namespace", val)
	}
	if val, ok := labels["label:env"]; !ok || val != "env" {
		t.Fatalf("AllocationPropertyLabels: expected %s; got %s", "env", val)
	}

	lc = &LabelConfig{
		NamespaceExternalLabel: "kubens",
		EnvironmentLabel:       "kubeenv",
	}
	labels = lc.AllocationPropertyLabels()
	if len(labels) != 18 {
		t.Fatalf("AllocationPropertyLabels: expected length %d; got length %d", 18, len(labels))
	}
	if val, ok := labels["namespace"]; !ok || val != "kubens" {
		t.Fatalf("AllocationPropertyLabels: expected %s; got %s", "kubens", val)
	}
	if val, ok := labels["label:kubeenv"]; !ok || val != "kubeenv" {
		t.Fatalf("AllocationPropertyLabels: expected %s; got %s", "kubeenv", val)
	}
}

func TestLabelConfig_GetExternalAllocationName(t *testing.T) {
	labels := map[string]string{
		"kubens":                      "kubecost-staging",
		"env":                         "env1",
		"app":                         "app1",
		"kubernetes_cluster":          "cluster-one",
		"kubernetes_namespace":        "kubecost",
		"kubernetes_controller":       "kubecost-controller",
		"kubernetes_daemonset":        "kubecost-daemonset",
		"kubernetes_deployment":       "kubecost-deployment",
		"kubernetes_statefulset":      "kubecost-statefulset",
		"kubernetes_service":          "kubecost-service",
		"kubernetes_pod":              "kubecost-cost-analyzer-abc123",
		"kubernetes_label_department": "kubecost-department",
		"kubernetes_label_env":        "kubecost-env",
		"kubernetes_label_owner":      "kubecost-owner",
		"kubernetes_label_app":        "kubecost-app",
		"kubernetes_label_team":       "kubecost-team",
	}

	testCases := []struct {
		aggBy    string
		expected string
	}{
		{"label:env", "env=env1"},
		{"label:app", "app=app1"},
		{"cluster", "cluster-one"},
		{"namespace", "kubecost"},
		{"controller", "kubecost-controller"},
		{"daemonset", "kubecost-daemonset"},
		{"deployment", "kubecost-deployment"},
		{"statefulset", "kubecost-statefulset"},
		{"service", "kubecost-service"},
		{"pod", "kubecost-cost-analyzer-abc123"},
		{"pod", "kubecost-cost-analyzer-abc123"},
		{"notathing", ""},
		{"", ""},
	}

	var lc *LabelConfig

	// If lc is nil, everything should still work off of defaults.
	for _, tc := range testCases {
		actual := lc.GetExternalAllocationName(labels, tc.aggBy)
		if actual != tc.expected {
			t.Fatalf("GetExternalAllocationName failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}

	// If lc is default, everything should work, just like the nil.
	lc = NewLabelConfig()
	for _, tc := range testCases {
		actual := lc.GetExternalAllocationName(labels, tc.aggBy)
		if actual != tc.expected {
			t.Fatalf("GetExternalAllocationName failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}

	// Change the external label for namespace and confirm it still works
	lc.NamespaceExternalLabel = "kubens"

	testCases = []struct {
		aggBy    string
		expected string
	}{
		{"namespace", "kubecost-staging"},
	}
	for _, tc := range testCases {
		actual := lc.GetExternalAllocationName(labels, tc.aggBy)
		if actual != tc.expected {
			t.Fatalf("GetExternalAllocationName failed; expected '%s'; got '%s'", tc.expected, actual)
		}
	}
}
