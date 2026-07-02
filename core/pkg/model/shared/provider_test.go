package shared

import (
	"testing"
)

func TestParseProvider(t *testing.T) {
	tests := []struct {
		input    string
		expected Provider
	}{
		// Canonical values
		{"AWS", ProviderAWS},
		{"GCP", ProviderGCP},
		{"Azure", ProviderAzure},
		{"Alibaba", ProviderAlibaba},
		{"DigitalOcean", ProviderDigitalOcean},
		{"Oracle", ProviderOracle},
		// Case-insensitive
		{"aws", ProviderAWS},
		{"gcp", ProviderGCP},
		{"azure", ProviderAzure},
		{"alibaba", ProviderAlibaba},
		{"digitalocean", ProviderDigitalOcean},
		{"oracle", ProviderOracle},
		{"AWS", ProviderAWS},
		{"AZURE", ProviderAzure},
		// Aliases
		{"amazon", ProviderAWS},
		{"Amazon", ProviderAWS},
		{"gce", ProviderGCP},
		{"GCE", ProviderGCP},
		{"google", ProviderGCP},
		{"Google", ProviderGCP},
		{"microsoft", ProviderAzure},
		{"Microsoft", ProviderAzure},
		{"do", ProviderDigitalOcean},
		{"DO", ProviderDigitalOcean},
		{"oci", ProviderOracle},
		{"OCI", ProviderOracle},
		// Unknown input returns empty
		{"", ProviderEmpty},
		{"unknown", ProviderEmpty},
		{"scaleway", ProviderEmpty},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseProvider(tt.input)
			if got != tt.expected {
				t.Errorf("ParseProvider(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
