package gcp

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/stretchr/testify/assert"
)

func TestIsK8s(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		expect bool
	}{
		{
			name: "GKE volume label",
			labels: map[string]string{
				"goog-gke-volume": "true",
			},
			expect: true,
		},
		{
			name: "GKE node label",
			labels: map[string]string{
				"goog-gke-node": "true",
			},
			expect: true,
		},
		{
			name: "GKE cluster name label",
			labels: map[string]string{
				"goog-k8s-cluster-name": "my-cluster",
			},
			expect: true,
		},
		{
			name: "Multiple GKE labels",
			labels: map[string]string{
				"goog-gke-volume":       "true",
				"goog-gke-node":         "true",
				"goog-k8s-cluster-name": "my-cluster",
			},
			expect: true,
		},
		{
			name: "No GKE labels",
			labels: map[string]string{
				"other-label": "value",
			},
			expect: false,
		},
		{
			name:   "Empty labels",
			labels: map[string]string{},
			expect: false,
		},
		{
			name:   "Nil labels",
			labels: nil,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsK8s(tt.labels)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestParseProviderID(t *testing.T) {
	tests := []struct {
		name       string
		providerID string
		expected   string
	}{
		{
			name:       "Standard GCE provider ID",
			providerID: "projects/123456789/instances/gke-cluster-3-default-pool-xxxx-yy",
			expected:   "gke-cluster-3-default-pool-xxxx-yy",
		},
		{
			name:       "Provider ID with trailing slash",
			providerID: "projects/123456789/instances/gke-cluster-3-default-pool-xxxx-yy/",
			expected:   "", // The function doesn't handle trailing slashes, so expect empty string
		},
		{
			name:       "Provider ID without project prefix",
			providerID: "gke-cluster-3-default-pool-xxxx-yy",
			expected:   "gke-cluster-3-default-pool-xxxx-yy",
		},
		{
			name:       "Empty provider ID",
			providerID: "",
			expected:   "",
		},
		{
			name:       "Provider ID with no match",
			providerID: "invalid-format",
			expected:   "invalid-format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseProviderID(tt.providerID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSelectCategory(t *testing.T) {
	tests := []struct {
		name        string
		service     string
		description string
		expected    string
	}{
		// Network category tests
		{
			name:        "Network download",
			service:     "Compute Engine",
			description: "Network download",
			expected:    opencost.NetworkCategory,
		},
		{
			name:        "Network ingress",
			service:     "Compute Engine",
			description: "Network ingress",
			expected:    opencost.NetworkCategory,
		},
		{
			name:        "Network egress",
			service:     "Compute Engine",
			description: "Network egress",
			expected:    opencost.NetworkCategory,
		},
		{
			name:        "Static IP",
			service:     "Compute Engine",
			description: "Static IP",
			expected:    opencost.NetworkCategory,
		},
		{
			name:        "External IP",
			service:     "Compute Engine",
			description: "External IP",
			expected:    opencost.NetworkCategory,
		},
		{
			name:        "Load balanced",
			service:     "Compute Engine",
			description: "Load balanced",
			expected:    opencost.NetworkCategory,
		},
		{
			name:        "Pub/Sub service",
			service:     "pub/sub",
			description: "Some description",
			expected:    opencost.NetworkCategory,
		},

		// Storage category tests
		{
			name:        "Storage service",
			service:     "storage",
			description: "Some description",
			expected:    opencost.StorageCategory,
		},
		{
			name:        "PD capacity",
			service:     "Compute Engine",
			description: "PD capacity",
			expected:    opencost.StorageCategory,
		},
		{
			name:        "PD IOPS",
			service:     "Compute Engine",
			description: "PD IOPS",
			expected:    opencost.StorageCategory,
		},
		{
			name:        "PD snapshot",
			service:     "Compute Engine",
			description: "PD snapshot",
			expected:    opencost.StorageCategory,
		},
		{
			name:        "SQL service",
			service:     "sql",
			description: "Some description",
			expected:    opencost.StorageCategory,
		},
		{
			name:        "BigQuery service",
			service:     "bigquery",
			description: "Some description",
			expected:    opencost.StorageCategory,
		},

		// Compute category tests
		{
			name:        "Compute service",
			service:     "compute",
			description: "Some description",
			expected:    opencost.ComputeCategory,
		},

		// Management category tests
		{
			name:        "Kubernetes service",
			service:     "kubernetes",
			description: "Some description",
			expected:    opencost.ManagementCategory,
		},

		// Other category tests
		{
			name:        "Licensing fee",
			service:     "Compute Engine",
			description: "Licensing fee",
			expected:    opencost.OtherCategory,
		},
		{
			name:        "Unknown service",
			service:     "unknown-service",
			description: "Some description",
			expected:    opencost.OtherCategory,
		},
		{
			name:        "Empty service and description",
			service:     "",
			description: "",
			expected:    opencost.OtherCategory,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectCategory(tt.service, tt.description)
			assert.Equal(t, tt.expected, result)
		})
	}
}
