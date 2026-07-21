package models

import (
	"reflect"
	"testing"
)

func TestNewNetworkKey(t *testing.T) {
	t.Run("extracts topology labels correctly", func(t *testing.T) {
		labels := map[string]string{
			"topology.kubernetes.io/region": "us-west-2",
			"topology.kubernetes.io/zone":   "us-west-2a",
		}
		key := NewNetworkKey(labels, "cluster-prod")

		if key.GetRegion() != "us-west-2" {
			t.Errorf("expected region 'us-west-2', got '%s'", key.GetRegion())
		}
		if key.GetZone() != "us-west-2a" {
			t.Errorf("expected zone 'us-west-2a', got '%s'", key.GetZone())
		}
		if key.GetClusterID() != "cluster-prod" {
			t.Errorf("expected clusterID 'cluster-prod', got '%s'", key.GetClusterID())
		}
	})

	t.Run("handles nil labels gracefully", func(t *testing.T) {
		key := NewNetworkKey(nil, "cluster-1")

		if key.GetRegion() != "" {
			t.Errorf("expected empty region, got '%s'", key.GetRegion())
		}
		if key.GetZone() != "" {
			t.Errorf("expected empty zone, got '%s'", key.GetZone())
		}
		if key.GetClusterID() != "cluster-1" {
			t.Errorf("expected clusterID 'cluster-1', got '%s'", key.GetClusterID())
		}
		if key.GetLabels() == nil {
			t.Errorf("expected non-nil empty map from GetLabels()")
		}
	})

	t.Run("clones input labels map defensively", func(t *testing.T) {
		labels := map[string]string{
			"topology.kubernetes.io/region": "us-east-1",
			"topology.kubernetes.io/zone":   "us-east-1b",
		}
		key := NewNetworkKey(labels, "cluster-dev")

		// Mutate original map after creation
		labels["topology.kubernetes.io/region"] = "mutated-region"
		labels["extra"] = "value"

		if key.GetRegion() != "us-east-1" {
			t.Errorf("expected region to remain 'us-east-1', got '%s'", key.GetRegion())
		}

		keyLabels := key.GetLabels()
		if keyLabels["extra"] == "value" {
			t.Errorf("key labels map was affected by original map mutation")
		}

		// Mutate map returned by GetLabels()
		keyLabels["mutated"] = "true"
		if key.GetLabels()["mutated"] == "true" {
			t.Errorf("GetLabels() did not return a defensive copy")
		}
	})
}

func TestDefaultNetworkKey_ID(t *testing.T) {
	tests := []struct {
		name     string
		key      *DefaultNetworkKey
		expected string
	}{
		{
			name:     "nil receiver",
			key:      nil,
			expected: "",
		},
		{
			name: "zone present preferred over region",
			key: &DefaultNetworkKey{
				Zone:   "us-east-1a",
				Region: "us-east-1",
			},
			expected: "us-east-1a",
		},
		{
			name: "zone only",
			key: &DefaultNetworkKey{
				Zone: "us-east-1a",
			},
			expected: "us-east-1a",
		},
		{
			name: "region only",
			key: &DefaultNetworkKey{
				Region: "us-east-1",
			},
			expected: "us-east-1",
		},
		{
			name:     "both empty",
			key:      &DefaultNetworkKey{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.key.ID(); got != tt.expected {
				t.Errorf("ID() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDefaultNetworkKey_Features(t *testing.T) {
	tests := []struct {
		name     string
		key      *DefaultNetworkKey
		expected string
	}{
		{
			name:     "nil receiver",
			key:      nil,
			expected: "",
		},
		{
			name: "both region and zone present",
			key: &DefaultNetworkKey{
				Region: "eu-west-1",
				Zone:   "eu-west-1a",
			},
			expected: "eu-west-1,eu-west-1a",
		},
		{
			name: "zone only present",
			key: &DefaultNetworkKey{
				Zone: "eu-west-1a",
			},
			expected: "eu-west-1a",
		},
		{
			name: "region only present",
			key: &DefaultNetworkKey{
				Region: "eu-west-1",
			},
			expected: "eu-west-1",
		},
		{
			name:     "neither region nor zone present",
			key:      &DefaultNetworkKey{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.key.Features(); got != tt.expected {
				t.Errorf("Features() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDefaultNetworkKey_AccessorsAndNilSafety(t *testing.T) {
	t.Run("nil receiver handles all accessors safely", func(t *testing.T) {
		var nilKey *DefaultNetworkKey

		if got := nilKey.GetZone(); got != "" {
			t.Errorf("GetZone() on nil = %v, want empty string", got)
		}
		if got := nilKey.GetRegion(); got != "" {
			t.Errorf("GetRegion() on nil = %v, want empty string", got)
		}
		if got := nilKey.GetClusterID(); got != "" {
			t.Errorf("GetClusterID() on nil = %v, want empty string", got)
		}
		if got := nilKey.GetLabels(); got != nil {
			t.Errorf("GetLabels() on nil = %v, want nil", got)
		}
	})

	t.Run("valid receiver returns correct values", func(t *testing.T) {
		labels := map[string]string{"env": "prod"}
		key := &DefaultNetworkKey{
			Zone:      "us-central1-a",
			Region:    "us-central1",
			ClusterID: "gcp-cluster",
			Labels:    labels,
		}

		if key.GetZone() != "us-central1-a" {
			t.Errorf("GetZone() = %v, want us-central1-a", key.GetZone())
		}
		if key.GetRegion() != "us-central1" {
			t.Errorf("GetRegion() = %v, want us-central1", key.GetRegion())
		}
		if key.GetClusterID() != "gcp-cluster" {
			t.Errorf("GetClusterID() = %v, want gcp-cluster", key.GetClusterID())
		}
		if !reflect.DeepEqual(key.GetLabels(), labels) {
			t.Errorf("GetLabels() = %v, want %v", key.GetLabels(), labels)
		}
	})
}
