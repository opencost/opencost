package digitalocean

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/digitalocean/godo"
	"github.com/opencost/opencost/pkg/cloud/models"
)

func newTestProviderWithFile(t *testing.T, filename string) (*DOKS, func() int) {
	t.Helper()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Parse the JSON data to get sizes
	var response DOResponse
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Set a fake token for testing
	t.Setenv("DIGITALOCEAN_ACCESS_TOKEN", "test_token_dop_v1_fake")

	// Convert DOSize to godo.Size for mock
	var godoSizes []godo.Size
	for _, doSize := range response.Sizes {
		godoSize := godo.Size{
			Slug:         doSize.Slug,
			Memory:       doSize.Memory,
			Vcpus:        doSize.VCPUs,
			Disk:         doSize.Disk,
			Transfer:     doSize.Transfer,
			PriceMonthly: doSize.PriceMonthly,
			PriceHourly:  doSize.PriceHourly,
			Regions:      doSize.Regions,
			Available:    doSize.Available,
			Description:  doSize.Description,
		}
		// Convert GPU info if present
		if doSize.GPUInfo.Count > 0 {
			godoSize.GPUInfo = &godo.GPUInfo{
				Count: doSize.GPUInfo.Count,
				Model: doSize.GPUInfo.Model,
				VRAM: &godo.VRAM{
					Amount: doSize.GPUInfo.VRAM.Amount,
					Unit:   doSize.GPUInfo.VRAM.Unit,
				},
			}
		}
		godoSizes = append(godoSizes, godoSize)
	}

	// Create a mock godo client with all sizes on a single page
	var callCount int
	mockService := &testMockSizesService{
		sizes:     godoSizes,
		callCount: &callCount,
	}

	provider := &DOKS{
		PricingURL: "https://api.digitalocean.com/v2/sizes",
		Cache:      &PricingCache{},
		Sizes:      make(map[string]*DOSize),
		client:     &godo.Client{Sizes: mockService},
	}

	return provider, func() int { return callCount }
}

// testMockSizesService is a simple mock that returns all sizes on a single page
type testMockSizesService struct {
	sizes     []godo.Size
	callCount *int
}

func (m *testMockSizesService) List(ctx context.Context, opt *godo.ListOptions) ([]godo.Size, *godo.Response, error) {
	*m.callCount++
	// Return all sizes on page 1 (no pagination for simple tests)
	return m.sizes, &godo.Response{
		Links: &godo.Links{
			Pages: &godo.Pages{},
		},
	}, nil
}

func (m *testMockSizesService) GetStorage(ctx context.Context, slug string) (*godo.Size, *godo.Response, error) {
	return nil, nil, nil
}

func newTestProviderWith404(t *testing.T) *DOKS {
	t.Helper()

	// Set a fake token for testing
	t.Setenv("DIGITALOCEAN_ACCESS_TOKEN", "test_token_dop_v1_fake")

	// Create a mock service that returns an error
	errorService := &testErrorSizesService{}

	provider := &DOKS{
		PricingURL: "https://api.digitalocean.com/v2/sizes",
		Cache:      &PricingCache{},
		Sizes:      make(map[string]*DOSize),
		client:     &godo.Client{Sizes: errorService},
	}

	return provider
}

// testErrorSizesService returns an error for testing error handling
type testErrorSizesService struct{}

func (m *testErrorSizesService) List(ctx context.Context, opt *godo.ListOptions) ([]godo.Size, *godo.Response, error) {
	return nil, nil, fmt.Errorf("API error: 404 Not Found")
}

func (m *testErrorSizesService) GetStorage(ctx context.Context, slug string) (*godo.Size, *godo.Response, error) {
	return nil, nil, nil
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

	assertEqual("Cost", node.Cost, "0.01786")
	assertEqual("VCPUCost", node.VCPUCost, "0.00595") // 1/3
	assertEqual("RAMCost", node.RAMCost, "0.01191")   // 2/3
	assertEqual("VCPU", node.VCPU, "1")
	assertEqual("RAM", node.RAM, "2GiB")
	assertEqual("ArchType", node.ArchType, "amd64")
	assertEqual("PricingType", string(node.PricingType), string(models.DefaultPrices))

	if meta.Source != "digitalocean-sizes-api" {
		t.Errorf("expected metadata source to be digitalocean-sizes-api, got: %s", meta.Source)
	}

	if c := callCount(); c != 1 {
		t.Errorf("expected 1 API call, got %d", c)
	}
}

func TestNodePricing_S2(t *testing.T) {
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
	assertEqual("VCPUCost", node.VCPUCost, "0.01190")
	assertEqual("RAMCost", node.RAMCost, "0.02381")
	assertEqual("VCPU", node.VCPU, "2")
	assertEqual("RAM", node.RAM, "4GiB")
	assertEqual("ArchType", node.ArchType, "amd64")
	assertEqual("PricingType", string(node.PricingType), string(models.DefaultPrices))

	if meta.Source != "digitalocean-sizes-api" {
		t.Errorf("expected metadata source to be digitalocean-sizes-api, got: %s", meta.Source)
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

func TestDOKSRefreshCustomPricing(t *testing.T) {
	do := &DOKS{}
	if err := do.RefreshCustomPricing(); err != nil {
		t.Fatalf("DOKS.RefreshCustomPricing() unexpected error: %v", err)
	}
}

func TestNodePricing_GPU(t *testing.T) {
	provider, callCount := newTestProviderWithFile(t, "testdata/do_pricing.json")

	key := &doksKey{
		Labels: map[string]string{
			"node.kubernetes.io/instance-type": "gpu-h100x1-80gb",
			"kubernetes.io/arch":               "amd64",
		},
	}

	// Verify key methods - might return defaults but shouldn't panic
	if count := key.GPUCount(); count != 1 {
		t.Errorf("expected GPUCount 1, got %d", count)
	}
	if gpuType := key.GPUType(); gpuType != "h100" {
		t.Errorf("expected GPUType h100, got %s", gpuType)
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

	assertEqual("Cost", node.Cost, "3.39000")
	assertEqual("VCPUCost", node.VCPUCost, "0.26077") // 3.39 * 20 / 260 = 0.260769...
	assertEqual("RAMCost", node.RAMCost, "3.12923")   // 3.39 * 240 / 260 = 3.129230...
	assertEqual("VCPU", node.VCPU, "20")
	assertEqual("RAM", node.RAM, "240GiB")
	assertEqual("GPU", node.GPU, "1")
	assertEqual("GPUName", node.GPUName, "nvidia_h100")

	if meta.Source != "digitalocean-sizes-api" {
		t.Errorf("expected metadata source to be digitalocean-sizes-api, got: %s", meta.Source)
	}

	if c := callCount(); c != 1 {
		t.Errorf("expected 1 API call, got %d", c)
	}
}

// mockSizesService implements the godo.SizesService interface for testing pagination
type mockSizesService struct {
	pages [][]godo.Size
}

func (m *mockSizesService) List(ctx context.Context, opt *godo.ListOptions) ([]godo.Size, *godo.Response, error) {
	if opt == nil {
		opt = &godo.ListOptions{}
	}

	// Pages are 1-indexed in godo
	page := opt.Page
	if page == 0 {
		page = 1
	}

	// Check if page is within range
	if page > len(m.pages) {
		// Return last page indicator
		return []godo.Size{}, &godo.Response{
			Links: &godo.Links{
				Pages: &godo.Pages{}, // No Next link = last page
			},
		}, nil
	}

	sizes := m.pages[page-1]

	// Create response with pagination links
	// godo.Pages has: First, Last, Next, Prev
	resp := &godo.Response{
		Links: &godo.Links{
			Pages: &godo.Pages{
				// Set First link (required for CurrentPage to parse)
				First: "https://api.digitalocean.com/v2/sizes?page=1&per_page=20",
			},
		},
	}

	// Set Last link - always set for godo to work
	resp.Links.Pages.Last = fmt.Sprintf("https://api.digitalocean.com/v2/sizes?page=%d&per_page=20", len(m.pages))

	// Set Next link if not on the last page
	if page < len(m.pages) {
		resp.Links.Pages.Next = fmt.Sprintf("https://api.digitalocean.com/v2/sizes?page=%d&per_page=20", page+1)
	}

	// Set Prev link if not on the first page
	if page > 1 {
		resp.Links.Pages.Prev = fmt.Sprintf("https://api.digitalocean.com/v2/sizes?page=%d&per_page=20", page-1)
	}

	return sizes, resp, nil
}

func (m *mockSizesService) GetStorage(ctx context.Context, slug string) (*godo.Size, *godo.Response, error) {
	return nil, nil, nil
}

// createMockGodoClient creates a godo client with a mock SizesService for pagination testing
func createMockGodoClient(t *testing.T) *godo.Client {
	t.Helper()

	page1 := []godo.Size{
		{
			Slug:        "s-1vcpu-2gb",
			Memory:      2048,
			Vcpus:       1,
			Disk:        50,
			PriceHourly: 0.01786,
			Available:   true,
		},
		{
			Slug:        "s-2vcpu-4gb",
			Memory:      4096,
			Vcpus:       2,
			Disk:        80,
			PriceHourly: 0.03571,
			Available:   true,
		},
	}

	page2 := []godo.Size{
		{
			Slug:        "m-4vcpu-32gb",
			Memory:      32768,
			Vcpus:       4,
			Disk:        160,
			PriceHourly: 0.25000,
			Available:   true,
		},
		{
			Slug:        "m-8vcpu-64gb",
			Memory:      65536,
			Vcpus:       8,
			Disk:        320,
			PriceHourly: 0.50000,
			Available:   true,
		},
	}

	page3 := []godo.Size{
		{
			Slug:        "c-8-intel",
			Memory:      16384,
			Vcpus:       8,
			Disk:        160,
			PriceHourly: 0.32440,
			Available:   true,
		},
		{
			Slug:        "c-16-intel",
			Memory:      32768,
			Vcpus:       16,
			Disk:        320,
			PriceHourly: 0.64880,
			Available:   true,
		},
	}

	// Create a client (we'll replace its Sizes service)
	client := godo.NewFromToken("test_token")

	// Replace the Sizes service with our mock
	client.Sizes = &mockSizesService{
		pages: [][]godo.Size{page1, page2, page3},
	}

	return client
}

// TestFetchPricingData_Pagination verifies that pagination is correctly handled when fetching sizes
func TestFetchPricingData_Pagination(t *testing.T) {
	t.Setenv("DIGITALOCEAN_ACCESS_TOKEN", "test_token_dop_v1_fake")

	// Create a provider with a mock client that simulates pagination
	provider := &DOKS{
		PricingURL: "https://api.digitalocean.com/v2/sizes",
		Cache:      &PricingCache{},
		Sizes:      make(map[string]*DOSize),
		client:     createMockGodoClient(t),
	}

	// Fetch pricing data which triggers pagination
	response, err := provider.fetchPricingData()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if response == nil {
		t.Fatal("expected non-nil response")
	}

	// Verify that all sizes from all pages were collected
	expectedSizes := map[string]bool{
		"s-1vcpu-2gb":  true,
		"s-2vcpu-4gb":  true,
		"m-4vcpu-32gb": true,
		"m-8vcpu-64gb": true,
		"c-8-intel":    true,
		"c-16-intel":   true,
	}

	// Check that all expected sizes are in the provider's sizes map
	for slug := range expectedSizes {
		if _, exists := provider.Sizes[slug]; !exists {
			t.Errorf("expected size %q to be indexed, but it was not", slug)
		}
	}

	// Verify the total count
	if len(provider.Sizes) != len(expectedSizes) {
		t.Errorf("expected %d sizes, got %d", len(expectedSizes), len(provider.Sizes))
	}

	// Verify specific size details
	testCases := []struct {
		slug           string
		expectedVCPUs  int
		expectedMemory int
	}{
		{"s-1vcpu-2gb", 1, 2048},
		{"s-2vcpu-4gb", 2, 4096},
		{"m-4vcpu-32gb", 4, 32768},
		{"m-8vcpu-64gb", 8, 65536},
		{"c-8-intel", 8, 16384},
		{"c-16-intel", 16, 32768},
	}

	for _, tc := range testCases {
		size, exists := provider.Sizes[tc.slug]
		if !exists {
			t.Fatalf("expected size %q to exist", tc.slug)
		}

		if size.VCPUs != tc.expectedVCPUs {
			t.Errorf("size %q: expected %d vCPUs, got %d", tc.slug, tc.expectedVCPUs, size.VCPUs)
		}

		if size.Memory != tc.expectedMemory {
			t.Errorf("size %q: expected %d MB memory, got %d", tc.slug, tc.expectedMemory, size.Memory)
		}
	}

	// Verify caching works (second fetch should use cached data)
	response2, err := provider.fetchPricingData()
	if err != nil {
		t.Fatalf("expected no error on second fetch, got: %v", err)
	}

	if response2 == nil {
		t.Fatal("expected non-nil response on second fetch")
	}

	// Verify cache timestamp was updated
	if provider.Cache.lastUpdate.IsZero() {
		t.Error("expected cache to have a non-zero timestamp")
	}
}
