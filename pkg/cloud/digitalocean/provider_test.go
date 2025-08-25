package digitalocean

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
)

func newTestProviderWithFile(t *testing.T, filename string) (*DOKS, func() int) {
	t.Helper()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var count int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))

	t.Cleanup(server.Close)

	provider := NewDOKSProvider(server.URL)
	return provider, func() int { return count }
}

func newTestProviderWith404(t *testing.T) *DOKS {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	t.Cleanup(server.Close)

	provider := NewDOKSProvider(server.URL)
	return provider
}

func TestNodePricing_APIMatches(t *testing.T) {
	provider, callCount := newTestProviderWithFile(t, "testdata/do_pricing.json")

	key := &doksKey{
		Labels: map[string]string{
			"node.kubernetes.io/instance-type": "s-1vcpu-2gb",
			"kubernetes.io/arch":               "amd64",
		},
	}

	node, meta, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if node == nil {
		t.Fatal("expected node pricing, got nil")
	}

	assertEqual := func(name, got, want string) {
		if got != want {
			t.Errorf("%s: got %s, want %s", name, got, want)
		}
	}

	assertEqual("Cost", node.Cost, "0.01199")
	assertEqual("VCPUCost", node.VCPUCost, "0.00400") // 1/3
	assertEqual("RAMCost", node.RAMCost, "0.00799")   // 2/3
	assertEqual("VCPU", node.VCPU, "1")
	assertEqual("RAM", node.RAM, "2GiB")
	assertEqual("ArchType", node.ArchType, "amd64")
	assertEqual("PricingType", string(node.PricingType), string(models.DefaultPrices))

	if meta.Source != "digitalocean" {
		t.Errorf("expected metadata source to be digitalocean, got: %s", meta.Source)
	}

	if c := callCount(); c != 1 {
		t.Errorf("expected 1 API call, got %d", c)
	}
}

func TestNodePricing_Fallback(t *testing.T) {
	provider, callCount := newTestProviderWithFile(t, "testdata/do_pricing.json")

	key := &doksKey{
		Labels: map[string]string{
			"node.kubernetes.io/instance-type": "s-2vcpu-4gb",
			"kubernetes.io/arch":               "amd64",
		},
	}

	node, meta, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if node == nil {
		t.Fatal("expected node pricing, got nil")
	}

	assertEqual := func(name, got, want string) {
		if got != want {
			t.Errorf("%s: got %s, want %s", name, got, want)
		}
	}

	assertEqual("Cost", node.Cost, "0.03571")
	assertEqual("VCPUCost", node.VCPUCost, "0.00595")
	assertEqual("RAMCost", node.RAMCost, "0.00595")
	assertEqual("VCPU", node.VCPU, "2")
	assertEqual("RAM", node.RAM, "4GiB")
	assertEqual("ArchType", node.ArchType, "amd64")
	assertEqual("PricingType", string(node.PricingType), string(models.DefaultPrices))

	if meta.Source != "static-fallback" {
		t.Errorf("expected metadata source to be static-fallback, got: %s", meta.Source)
	}

	if c := callCount(); c != 1 {
		t.Errorf("expected 1 API call, got %d", c)
	}
}

func TestNodePricing_Estimation_C8Intel(t *testing.T) {
	provider := newTestProviderWith404(t)

	key := &doksKey{
		Labels: map[string]string{
			"node.kubernetes.io/instance-type": "c-8-intel",
			"kubernetes.io/arch":               "amd64",
		},
	}

	node, meta, err := provider.NodePricing(key)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expectedCost := "0.32440"
	expectedVCPUCost := "0.01352"
	expectedRAMCost := "0.01352"

	if node.Cost != expectedCost {
		t.Errorf("Cost: got %s, want %s", node.Cost, expectedCost)
	}
	if node.VCPUCost != expectedVCPUCost {
		t.Errorf("VCPUCost: got %s, want %s", node.VCPUCost, expectedVCPUCost)
	}
	if node.RAMCost != expectedRAMCost {
		t.Errorf("RAMCost: got %s, want %s", node.RAMCost, expectedRAMCost)
	}
	if node.VCPU != "8" {
		t.Errorf("VCPU: got %s, want 8", node.VCPU)
	}
	if node.RAM != "16GiB" {
		t.Errorf("RAM: got %s, want 16GiB", node.RAM)
	}
	if meta.Source != "static-fallback" {
		t.Errorf("expected metadata source to be estimated, got: %s", meta.Source)
	}
}

func TestNodePricing_EstimationFromSlug(t *testing.T) {
	tests := []struct {
		name            string
		slug            string
		expectedVCPU    string
		expectedRAM     string
		expectedCost    string
		expectedCPU     string
		expectedRAMCost string
	}{
		{
			name:            "s-4vcpu-8gb",
			slug:            "s-4vcpu-8gb",
			expectedVCPU:    "4",
			expectedRAM:     "8GiB",
			expectedCost:    "0.07143",
			expectedCPU:     "0.00595",
			expectedRAMCost: "0.00595",
		},
		{
			name:            "m-8vcpu-64gb",
			slug:            "m-8vcpu-64gb",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.50000",
			expectedCPU:     "0.00694",
			expectedRAMCost: "0.00694",
		},
		{
			name:            "g-4vcpu-16gb-intel",
			slug:            "g-4vcpu-16gb-intel",
			expectedVCPU:    "4",
			expectedRAM:     "16GiB",
			expectedCost:    "0.22470",
			expectedCPU:     "0.01124",
			expectedRAMCost: "0.01124",
		},
	}

	provider := newTestProviderWith404(t) // Force fallback/estimate

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := &doksKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": tc.slug,
					"kubernetes.io/arch":               "amd64",
				},
			}

			node, meta, err := provider.NodePricing(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if node == nil {
				t.Fatal("expected node to be non-nil")
			}

			assertEqual := func(field, got, want string) {
				if got != want {
					t.Errorf("%s: got %s, want %s", field, got, want)
				}
			}

			assertEqual("Cost", node.Cost, tc.expectedCost)
			assertEqual("VCPUCost", node.VCPUCost, tc.expectedCPU)
			assertEqual("RAMCost", node.RAMCost, tc.expectedRAMCost)
			assertEqual("VCPU", node.VCPU, tc.expectedVCPU)
			assertEqual("RAM", node.RAM, tc.expectedRAM)
			assertEqual("ArchType", node.ArchType, "amd64")

			if meta.Source != "static-fallback" {
				t.Errorf("expected metadata source to be 'estimated', got: %s", meta.Source)
			}
		})
	}
}

func TestNodePricing_Estimation_BaseSlugs(t *testing.T) {
	tests := []struct {
		name            string
		slug            string
		expectedVCPU    string
		expectedRAM     string
		expectedCost    string
		expectedCPU     string
		expectedRAMCost string
	}{
		{
			name:            "c-8-intel",
			slug:            "c-8-intel",
			expectedVCPU:    "8",
			expectedRAM:     "16GiB",
			expectedCost:    "0.32440",
			expectedCPU:     "0.01352",
			expectedRAMCost: "0.01352",
		},
		{
			name:            "s-2vcpu-4gb",
			slug:            "s-2vcpu-4gb",
			expectedVCPU:    "2",
			expectedRAM:     "4GiB",
			expectedCost:    "0.03571",
			expectedCPU:     "0.00595",
			expectedRAMCost: "0.00595",
		},
		{
			name:            "m-4vcpu-32gb",
			slug:            "m-4vcpu-32gb",
			expectedVCPU:    "4",
			expectedRAM:     "32GiB",
			expectedCost:    "0.25000",
			expectedCPU:     "0.00694",
			expectedRAMCost: "0.00694",
		},
		{
			name:            "g-16vcpu-64gb-intel",
			slug:            "g-16vcpu-64gb-intel",
			expectedVCPU:    "16",
			expectedRAM:     "64GiB",
			expectedCost:    "0.89880",
			expectedCPU:     "0.01124",
			expectedRAMCost: "0.01124",
		},
	}

	provider := newTestProviderWith404(t) // ensures fallback path is tested

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := &doksKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": tc.slug,
					"kubernetes.io/arch":               "amd64",
				},
			}

			node, meta, err := provider.NodePricing(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if node == nil {
				t.Fatal("expected node to be non-nil")
			}

			assertEqual := func(field, got, want string) {
				if got != want {
					t.Errorf("%s: got %s, want %s", field, got, want)
				}
			}

			assertEqual("Cost", node.Cost, tc.expectedCost)
			assertEqual("VCPUCost", node.VCPUCost, tc.expectedCPU)
			assertEqual("RAMCost", node.RAMCost, tc.expectedRAMCost)
			assertEqual("VCPU", node.VCPU, tc.expectedVCPU)
			assertEqual("RAM", node.RAM, tc.expectedRAM)
			assertEqual("ArchType", node.ArchType, "amd64")

			if meta.Source != "static-fallback" {
				t.Errorf("expected metadata source to be 'static-fallback', got: %s", meta.Source)
			}
		})
	}
}

func TestNodePricing_Estimation_FamilySeeds(t *testing.T) {
	tests := []struct {
		name            string
		slug            string
		expectedVCPU    string
		expectedRAM     string
		expectedCost    string
		expectedCPU     string
		expectedRAMCost string
	}{
		{
			name:            "c-16",
			slug:            "c-16",
			expectedVCPU:    "16",
			expectedRAM:     "32GiB",
			expectedCost:    "0.50000",
			expectedCPU:     "0.01042",
			expectedRAMCost: "0.01042",
		},
		{
			name:            "c-16-intel",
			slug:            "c-16-intel",
			expectedVCPU:    "16",
			expectedRAM:     "32GiB",
			expectedCost:    "0.64880",
			expectedCPU:     "0.01352",
			expectedRAMCost: "0.01352",
		},

		{
			name:            "c2-8vcpu-16gb",
			slug:            "c2-8vcpu-16gb",
			expectedVCPU:    "8",
			expectedRAM:     "16GiB",
			expectedCost:    "0.27976",
			expectedCPU:     "0.01166",
			expectedRAMCost: "0.01166",
		},
		{
			name:            "c2-8vcpu-16gb-intel",
			slug:            "c2-8vcpu-16gb-intel",
			expectedVCPU:    "8",
			expectedRAM:     "16GiB",
			expectedCost:    "0.36310",
			expectedCPU:     "0.01513",
			expectedRAMCost: "0.01513",
		},
		{
			name:            "g-8vcpu-32gb",
			slug:            "g-8vcpu-32gb",
			expectedVCPU:    "8",
			expectedRAM:     "32GiB",
			expectedCost:    "0.37500",
			expectedCPU:     "0.00937",
			expectedRAMCost: "0.00937",
		},
		{
			name:            "g-8vcpu-32gb-intel",
			slug:            "g-8vcpu-32gb-intel",
			expectedVCPU:    "8",
			expectedRAM:     "32GiB",
			expectedCost:    "0.44940",
			expectedCPU:     "0.01124",
			expectedRAMCost: "0.01124",
		},
		{
			name:            "gd-40vcpu-160gb",
			slug:            "gd-40vcpu-160gb",
			expectedVCPU:    "40",
			expectedRAM:     "160GiB",
			expectedCost:    "2.02380",
			expectedCPU:     "0.01012",
			expectedRAMCost: "0.01012",
		},
		{
			name:            "gd-16vcpu-64gb-intel",
			slug:            "gd-16vcpu-64gb-intel",
			expectedVCPU:    "16",
			expectedRAM:     "64GiB",
			expectedCost:    "0.94048",
			expectedCPU:     "0.01176",
			expectedRAMCost: "0.01176",
		},
		{
			name:            "m-16vcpu-128gb",
			slug:            "m-16vcpu-128gb",
			expectedVCPU:    "16",
			expectedRAM:     "128GiB",
			expectedCost:    "1.00000",
			expectedCPU:     "0.00694",
			expectedRAMCost: "0.00694",
		},
		{
			name:            "m-16vcpu-128gb-intel",
			slug:            "m-16vcpu-128gb-intel",
			expectedVCPU:    "16",
			expectedRAM:     "128GiB",
			expectedCost:    "1.17858",
			expectedCPU:     "0.00818",
			expectedRAMCost: "0.00818",
		},

		// m3
		{
			name:            "m3-8vcpu-64gb",
			slug:            "m3-8vcpu-64gb",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.61905",
			expectedCPU:     "0.00860",
			expectedRAMCost: "0.00860",
		},
		{
			name:            "m3-32vcpu-256gb-intel",
			slug:            "m3-32vcpu-256gb-intel",
			expectedVCPU:    "32",
			expectedRAM:     "256GiB",
			expectedCost:    "2.61904",
			expectedCPU:     "0.00909",
			expectedRAMCost: "0.00909",
		},
		{
			name:            "m6-8vcpu-64gb",
			slug:            "m6-8vcpu-64gb",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.77976",
			expectedCPU:     "0.01083",
			expectedRAMCost: "0.01083",
		},
		{
			name:            "m6-24vcpu-192gb",
			slug:            "m6-24vcpu-192gb",
			expectedVCPU:    "24",
			expectedRAM:     "192GiB",
			expectedCost:    "2.33928",
			expectedCPU:     "0.01083",
			expectedRAMCost: "0.01083",
		},
		{
			name:            "s-1vcpu-2gb",
			slug:            "s-1vcpu-2gb",
			expectedVCPU:    "1",
			expectedRAM:     "2GiB",
			expectedCost:    "0.01786",
			expectedCPU:     "0.00595",
			expectedRAMCost: "0.00595",
		},
		{
			name:            "s-8vcpu-16gb-intel",
			slug:            "s-8vcpu-16gb-intel",
			expectedVCPU:    "8",
			expectedRAM:     "16GiB",
			expectedCost:    "0.16666",
			expectedCPU:     "0.00694",
			expectedRAMCost: "0.00694",
		},
		{
			name:            "so-8vcpu-64gb",
			slug:            "so-8vcpu-64gb",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.77976",
			expectedCPU:     "0.01083",
			expectedRAMCost: "0.01083",
		},
		{
			name:            "so-8vcpu-64gb-intel",
			slug:            "so-8vcpu-64gb-intel",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.77976",
			expectedCPU:     "0.01083",
			expectedRAMCost: "0.01083",
		},
		{
			name:            "so1_5-8vcpu-64gb",
			slug:            "so1_5-8vcpu-64gb",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.97024",
			expectedCPU:     "0.01348",
			expectedRAMCost: "0.01348",
		},
		{
			name:            "so1_5-8vcpu-64gb-intel",
			slug:            "so1_5-8vcpu-64gb-intel",
			expectedVCPU:    "8",
			expectedRAM:     "64GiB",
			expectedCost:    "0.82738",
			expectedCPU:     "0.01149",
			expectedRAMCost: "0.01149",
		},
	}

	provider := newTestProviderWith404(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key := &doksKey{
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": tc.slug,
					"kubernetes.io/arch":               "amd64",
				},
			}

			node, meta, err := provider.NodePricing(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if node == nil {
				t.Fatal("expected node to be non-nil")
			}

			assertEqual := func(field, got, want string) {
				if got != want {
					t.Errorf("%s: got %s, want %s", field, got, want)
				}
			}

			assertEqual("Cost", node.Cost, tc.expectedCost)
			assertEqual("VCPUCost", node.VCPUCost, tc.expectedCPU)
			assertEqual("RAMCost", node.RAMCost, tc.expectedRAMCost)
			assertEqual("VCPU", node.VCPU, tc.expectedVCPU)
			assertEqual("RAM", node.RAM, tc.expectedRAM)
			assertEqual("ArchType", node.ArchType, "amd64")

			if meta.Source != "static-fallback" {
				t.Errorf("expected metadata source to be 'static-fallback', got: %s", meta.Source)
			}
		})
	}
}
