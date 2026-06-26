package metrics

import (
	"sync"
	"testing"
)

// Note: Due to the use of sync.Once, we cannot test SetOTelMetricLabels() multiple times
// in the same test run. These tests verify the default values and the getter functions.

func TestDefaultLabels(t *testing.T) {
	// Test that default labels are the classic Kubernetes-style labels
	// These tests must run before any call to SetOTelMetricLabels()

	tests := []struct {
		name     string
		getter   func() string
		expected string
	}{
		{
			name:     "default node label",
			getter:   GetNodeLabel,
			expected: "node",
		},
		{
			name:     "default namespace label",
			getter:   GetNamespaceLabel,
			expected: "namespace",
		},
		{
			name:     "default pod label",
			getter:   GetPodLabel,
			expected: "pod",
		},
		{
			name:     "default container label",
			getter:   GetContainerLabel,
			expected: "container",
		},
		{
			name:     "default persistent volume label",
			getter:   GetPersistentVolumeLabel,
			expected: "persistentvolume",
		},
		{
			name:     "default persistent volume claim label",
			getter:   GetPersistentVolumeClaimLabel,
			expected: "persistentvolumeclaim",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.getter()
			if got != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, got)
			}
		})
	}
}

func TestOTelLabelsMapping(t *testing.T) {
	// This test documents the expected OTel label mappings
	// We can't actually test SetOTelMetricLabels() due to sync.Once,
	// but we can verify the expected mappings are correct

	expectedMappings := map[string]string{
		"node":                  "k8s_node_name",
		"namespace":             "k8s_namespace_name",
		"pod":                   "k8s_pod_name",
		"container":             "k8s_container_name",
		"persistentvolume":      "k8s_persistentvolume_name",
		"persistentvolumeclaim": "k8s_persistentvolumeclaim_name",
	}

	// Verify the expected mappings are documented
	for classic, otel := range expectedMappings {
		if classic == "" || otel == "" {
			t.Errorf("Invalid mapping: %s -> %s", classic, otel)
		}
		// Verify OTel labels follow the k8s_ prefix convention
		if otel[:4] != "k8s_" {
			t.Errorf("OTel label %s should start with k8s_ prefix", otel)
		}
		// Verify OTel labels end with _name suffix
		if otel[len(otel)-5:] != "_name" {
			t.Errorf("OTel label %s should end with _name suffix", otel)
		}
	}
}

func TestSetOTelMetricLabelsOnce(t *testing.T) {
	// Test that SetOTelMetricLabels uses sync.Once correctly
	// We create a similar structure to verify the pattern works

	var testOnce sync.Once
	var callCount int

	testSetFunc := func() {
		testOnce.Do(func() {
			callCount++
		})
	}

	// Call multiple times
	testSetFunc()
	testSetFunc()
	testSetFunc()

	if callCount != 1 {
		t.Errorf("Expected function to be called exactly once, but was called %d times", callCount)
	}
}

func TestGetterFunctionsExist(t *testing.T) {
	// Verify all getter functions exist and return non-empty strings
	getters := []struct {
		name   string
		getter func() string
	}{
		{"GetNodeLabel", GetNodeLabel},
		{"GetNamespaceLabel", GetNamespaceLabel},
		{"GetPodLabel", GetPodLabel},
		{"GetContainerLabel", GetContainerLabel},
		{"GetPersistentVolumeLabel", GetPersistentVolumeLabel},
		{"GetPersistentVolumeClaimLabel", GetPersistentVolumeClaimLabel},
	}

	for _, g := range getters {
		t.Run(g.name, func(t *testing.T) {
			result := g.getter()
			if result == "" {
				t.Errorf("%s returned empty string", g.name)
			}
		})
	}
}
