package kubemodel

import (
	"testing"
)

func TestParseServiceType(t *testing.T) {
	tests := []struct {
		input    string
		expected ServiceType
	}{
		// Exact canonical values
		{"ClusterIP", ServiceTypeClusterIP},
		{"NodePort", ServiceTypeNodePort},
		{"LoadBalancer", ServiceTypeLoadBalancer},
		{"ExternalName", ServiceTypeExternalName},
		// Case-insensitive
		{"clusterip", ServiceTypeClusterIP},
		{"nodeport", ServiceTypeNodePort},
		{"loadbalancer", ServiceTypeLoadBalancer},
		{"externalname", ServiceTypeExternalName},
		{"CLUSTERIP", ServiceTypeClusterIP},
		{"LOADBALANCER", ServiceTypeLoadBalancer},
		// "lb" alias
		{"lb", ServiceTypeLoadBalancer},
		{"LB", ServiceTypeLoadBalancer},
		// Unknown input defaults to ClusterIP
		{"", ServiceTypeClusterIP},
		{"unknown", ServiceTypeClusterIP},
		{"ingress", ServiceTypeClusterIP},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseServiceType(tt.input)
			if got != tt.expected {
				t.Errorf("ParseServiceType(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
