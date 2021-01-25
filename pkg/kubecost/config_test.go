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

func TestTestLabelConfig_AllocationPropertyLabels(t *testing.T) {
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
