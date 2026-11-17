package cloud

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
		{"Scaleway", ProviderScaleway},
		{"OTC", ProviderOTC},
		{"OVH", ProviderOVH},
		{"STACKIT", ProviderSTACKIT},
		// Case-insensitive
		{"aws", ProviderAWS},
		{"gcp", ProviderGCP},
		{"azure", ProviderAzure},
		{"alibaba", ProviderAlibaba},
		{"digitalocean", ProviderDigitalOcean},
		{"oracle", ProviderOracle},
		{"scaleway", ProviderScaleway},
		{"otc", ProviderOTC},
		{"ovh", ProviderOVH},
		{"stackit", ProviderSTACKIT},
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
		{"scw", ProviderScaleway},
		{"kapsule", ProviderScaleway},
		{"ovhcloud", ProviderOVH},
		{"ovh-mks", ProviderOVH},
		{"ske", ProviderSTACKIT},
		// Unknown input returns Custom
		{"", ProviderCustom},
		{"unknown", ProviderCustom},
		{"ibm", ProviderCustom},
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
