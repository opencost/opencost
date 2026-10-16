package provider

import (
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestParseLocalDiskID(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"empty string": {
			input: "",
			want:  "",
		},
		"generic string": {
			input: "test",
			want:  "test",
		},
		"AWS node provider id": {
			input: "aws:///us-east-2a/i-0fea4fd46592d050b",
			want:  "i-0fea4fd46592d050b",
		},
		"GCP node provider id": {
			input: "gce://guestbook-11111/us-central1-a/gke-niko-n1-standard-2-wlkla-8d48e58a-hfy7",
			want:  "gke-niko-n1-standard-2-wlkla-8d48e58a-hfy7",
		},
		"Azure vmss provider id": {
			input: "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourceGroups/mc_test_eastus2/providers/Microsoft.Compute/virtualMachineScaleSets/aks-userpool-12345678-vmss/virtualMachines/11",
			want:  "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourcegroups/mc_test_eastus2/providers/microsoft.compute/disks/aks-userpool-12345678-vmss00000b_osdisk",
		},
		"Azure vm provider id": {
			input: "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourceGroups/mc_test_eastus2/providers/Microsoft.Compute/virtualMachines/master-0",
			want:  "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourcegroups/mc_test_eastus2/providers/microsoft.compute/disks/master-0_osdisk",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ParseLocalDiskID(tt.input); got != tt.want {
				t.Errorf("ParseLocalDiskID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProviderConfigUpdateFromMapPreservesHourlyPrices(t *testing.T) {
	confMan := config.NewConfigFileManager(storage.NewMemoryStorage())
	providerConfig := NewProviderConfig(confMan, "default.json")

	updated, err := providerConfig.UpdateFromMap(map[string]string{
		"CPU":     "0.031611",
		"spotCPU": "0.006655",
		"RAM":     "0.004237",
		"spotRAM": "0.000892",
		"GPU":     "0.95",
		"spotGPU": "0.308",
		"storage": "0.00005479452",
	})
	if err != nil {
		t.Fatalf("UpdateFromMap returned error: %v", err)
	}

	if updated.CPU != "0.031611" {
		t.Errorf("CPU = %q, want hourly value %q", updated.CPU, "0.031611")
	}
	if updated.SpotCPU != "0.006655" {
		t.Errorf("SpotCPU = %q, want hourly value %q", updated.SpotCPU, "0.006655")
	}
	if updated.RAM != "0.004237" {
		t.Errorf("RAM = %q, want hourly value %q", updated.RAM, "0.004237")
	}
	if updated.SpotRAM != "0.000892" {
		t.Errorf("SpotRAM = %q, want hourly value %q", updated.SpotRAM, "0.000892")
	}
	if updated.GPU != "0.95" {
		t.Errorf("GPU = %q, want hourly value %q", updated.GPU, "0.95")
	}
	if updated.SpotGPU != "0.308" {
		t.Errorf("SpotGPU = %q, want hourly value %q", updated.SpotGPU, "0.308")
	}
	if updated.Storage != "0.00005479452" {
		t.Errorf("Storage = %q, want hourly value %q", updated.Storage, "0.00005479452")
	}
}

func TestCustomProviderGetKeyDetectsGPUCapacity(t *testing.T) {
	cases := []struct {
		name         string
		provider     *CustomProvider
		labels       map[string]string
		capacity     v1.ResourceList
		wantGPUType  string
		wantGPUCount int
	}{
		{
			name: "nvidia GPU capacity",
			capacity: v1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("2"),
			},
			wantGPUType:  "nvidia.com/gpu",
			wantGPUCount: 2,
		},
		{
			name: "virtual GPU capacity",
			capacity: v1.ResourceList{
				"k8s.amazonaws.com/vgpu": resource.MustParse("3"),
			},
			wantGPUType:  "k8s.amazonaws.com/vgpu",
			wantGPUCount: 3,
		},
		{
			name: "configured GPU label takes precedence over capacity type",
			provider: &CustomProvider{
				GPULabel: "gpu.example/type",
			},
			labels: map[string]string{
				"gpu.example/type": "a100",
			},
			capacity: v1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("4"),
			},
			wantGPUType:  "a100",
			wantGPUCount: 4,
		},
		{
			name:         "no GPU capacity",
			capacity:     v1.ResourceList{},
			wantGPUType:  "",
			wantGPUCount: 0,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			customProvider := tt.provider
			if customProvider == nil {
				customProvider = &CustomProvider{}
			}
			labels := tt.labels
			if labels == nil {
				labels = map[string]string{}
			}

			key := customProvider.GetKey(labels, &clustercache.Node{
				Labels: labels,
				Status: v1.NodeStatus{
					Capacity: tt.capacity,
				},
			})

			if got := key.GPUType(); got != tt.wantGPUType {
				t.Errorf("GPUType() = %q, want %q", got, tt.wantGPUType)
			}
			if got := key.GPUCount(); got != tt.wantGPUCount {
				t.Errorf("GPUCount() = %d, want %d", got, tt.wantGPUCount)
			}
		})
	}
}

func TestCustomProviderNodePricingUsesDetectedGPUCount(t *testing.T) {
	customProvider := &CustomProvider{
		Pricing: map[string]*NodePrice{
			"default": {
				CPU: "0.031611",
				RAM: "0.004237",
			},
			"default,gpu": {
				CPU: "0.031611",
				RAM: "0.004237",
				GPU: "0.95",
			},
		},
	}

	key := customProvider.GetKey(map[string]string{}, &clustercache.Node{
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("2"),
			},
		},
	})

	node, _, err := customProvider.NodePricing(key)
	if err != nil {
		t.Fatalf("NodePricing returned error: %v", err)
	}

	if node.VCPUCost != "0.031611" {
		t.Errorf("VCPUCost = %q, want %q", node.VCPUCost, "0.031611")
	}
	if node.RAMCost != "0.004237" {
		t.Errorf("RAMCost = %q, want %q", node.RAMCost, "0.004237")
	}
	if node.GPUCost != "0.95" {
		t.Errorf("GPUCost = %q, want %q", node.GPUCost, "0.95")
	}
	if node.GPU != "2" {
		t.Errorf("GPU = %q, want %q", node.GPU, "2")
	}
}

func TestCustomProviderClusterInfoUsesStaticDefaultName(t *testing.T) {
	t.Setenv(coreenv.ClusterIDEnvVar, "")

	customProvider := newTestCustomProvider(t, nil)

	info, err := customProvider.ClusterInfo()
	if err != nil {
		t.Fatalf("ClusterInfo returned error: %v", err)
	}

	if info["name"] != "Custom Cluster" {
		t.Errorf("name = %q, want %q", info["name"], "Custom Cluster")
	}
	if info["id"] != "default-cluster" {
		t.Errorf("id = %q, want %q", info["id"], "default-cluster")
	}
}

func TestCustomProviderLoadBalancerPricingEmptyConfig(t *testing.T) {
	customProvider := newTestCustomProvider(t, nil)

	lb, err := customProvider.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("LoadBalancerPricing returned error: %v", err)
	}
	if lb.Cost != 0 {
		t.Errorf("Cost = %f, want 0", lb.Cost)
	}
}

func TestCustomProviderLoadBalancerPricingUsesDefaultLBPriceFallback(t *testing.T) {
	customProvider := newTestCustomProvider(t, map[string]string{
		"defaultLBPrice": "0.025",
	})

	lb, err := customProvider.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("LoadBalancerPricing returned error: %v", err)
	}
	if lb.Cost != 0.025 {
		t.Errorf("Cost = %f, want 0.025", lb.Cost)
	}
}

func TestCustomProviderLoadBalancerPricingUsesForwardingRulePrice(t *testing.T) {
	customProvider := newTestCustomProvider(t, map[string]string{
		"firstFiveForwardingRulesCost": "0.02",
		"defaultLBPrice":               "0.025",
	})

	lb, err := customProvider.LoadBalancerPricing()
	if err != nil {
		t.Fatalf("LoadBalancerPricing returned error: %v", err)
	}
	if lb.Cost != 0.02 {
		t.Errorf("Cost = %f, want 0.02", lb.Cost)
	}
}

func TestCustomProviderLoadBalancerPricingInvalidValue(t *testing.T) {
	customProvider := newTestCustomProvider(t, map[string]string{
		"firstFiveForwardingRulesCost": "not-a-price",
	})

	_, err := customProvider.LoadBalancerPricing()
	if err == nil {
		t.Fatal("LoadBalancerPricing returned nil error, want invalid pricing error")
	}
}

func newTestCustomProvider(t *testing.T, pricing map[string]string) *CustomProvider {
	t.Helper()

	confMan := config.NewConfigFileManager(storage.NewMemoryStorage())
	providerConfig := NewProviderConfig(confMan, "default.json")
	if pricing != nil {
		if _, err := providerConfig.UpdateFromMap(pricing); err != nil {
			t.Fatalf("UpdateFromMap returned error: %v", err)
		}
	}

	return &CustomProvider{
		Config: providerConfig,
	}
}

// unknownCustomPricingKey stands in for a node class that the custom provider
// does not recognize. It mirrors the key reported in opencost/opencost#4020,
// which triggers the CPU/RAM custom-pricing fallback path.
type unknownCustomPricingKey struct{}

func (unknownCustomPricingKey) ID() string       { return "unknown" }
func (unknownCustomPricingKey) Features() string { return "unknown" }
func (unknownCustomPricingKey) GPUType() string  { return "" }
func (unknownCustomPricingKey) GPUCount() int    { return 0 }

// TestCustomProviderPricingStableAcrossUpdates reproduces the custom-pricing
// instability reported in opencost/opencost#4020. The provider must return the
// configured CPU/RAM values every time, even after repeated GetConfig calls or
// config updates.
func TestCustomProviderPricingStableAcrossUpdates(t *testing.T) {
	const (
		wantCPU = "0.006407"
		wantRAM = "0.000859"
	)

	cp := newTestCustomProvider(t, map[string]string{
		"CPU": wantCPU,
		"RAM": wantRAM,
	})

	if err := cp.DownloadPricingData(); err != nil {
		t.Fatalf("DownloadPricingData(): %v", err)
	}

	for i := 0; i < 3; i++ {
		cfg, err := cp.GetConfig()
		if err != nil {
			t.Fatalf("GetConfig() (iteration %d): %v", i, err)
		}
		if cfg.CPU != wantCPU {
			t.Fatalf("GetConfig().CPU = %q, want %q on iteration %d", cfg.CPU, wantCPU, i)
		}
		if cfg.RAM != wantRAM {
			t.Fatalf("GetConfig().RAM = %q, want %q on iteration %d", cfg.RAM, wantRAM, i)
		}

		nodePrice, err := cp.NodePricing(unknownCustomPricingKey{})
		if err != nil {
			t.Fatalf("NodePricing(unknownCustomPricingKey{}) (iteration %d): %v", i, err)
		}
		if nodePrice.VCPUCost != wantCPU {
			t.Fatalf("NodePricing.VCPUCost = %q, want %q on iteration %d", nodePrice.VCPUCost, wantCPU, i)
		}
		if nodePrice.RAMCost != wantRAM {
			t.Fatalf("NodePricing.RAMCost = %q, want %q on iteration %d", nodePrice.RAMCost, wantRAM, i)
		}
	}

	// Simulate a config-watch update that does not touch CPU/RAM. The previously
	// configured prices must still be returned.
	if _, err := cp.Config.UpdateFromMap(map[string]string{
		"ProjectID": "stable-project",
	}); err != nil {
		t.Fatalf("UpdateFromMap(ProjectID): %v", err)
	}

	cfg, err := cp.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig() after unrelated update: %v", err)
	}
	if cfg.CPU != wantCPU || cfg.RAM != wantRAM {
		t.Fatalf("after unrelated update CPU=%q RAM=%q, want CPU=%q RAM=%q", cfg.CPU, cfg.RAM, wantCPU, wantRAM)
	}
}

// TestGetCustomPricingDataReturnsDefensiveCopy guards the fix for
// opencost/opencost#4020: callers of GetConfig/GetCustomPricingData must not
// be able to corrupt the provider's cached configuration.
func TestGetCustomPricingDataReturnsDefensiveCopy(t *testing.T) {
	cp := newTestCustomProvider(t, map[string]string{
		"CPU": "0.006407",
		"RAM": "0.000859",
	})

	cfg, err := cp.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig(): %v", err)
	}

	cfg.CPU = "0.000009"
	cfg.RAM = "0.000009"

	cfg2, err := cp.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig() after mutation: %v", err)
	}
	if cfg2.CPU != "0.006407" || cfg2.RAM != "0.000859" {
		t.Fatalf("cached config mutated: CPU=%q RAM=%q", cfg2.CPU, cfg2.RAM)
	}
}

// TestGetCustomPricingDataConcurrentUpdatesAndReadsStable exercises the
// read/write lock around the cached pricing config. This is a regression guard
// for the race that could corrupt custom pricing during config updates.
func TestGetCustomPricingDataConcurrentUpdatesAndReadsStable(t *testing.T) {
	confMan := config.NewConfigFileManager(storage.NewMemoryStorage())
	pc := NewProviderConfig(confMan, "default.json")
	if _, err := pc.UpdateFromMap(map[string]string{
		"CPU": "0.006407",
		"RAM": "0.000859",
	}); err != nil {
		t.Fatalf("UpdateFromMap: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = pc.UpdateFromMap(map[string]string{
					"ProjectID": "concurrent-project",
				})
			}
		}()
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				cfg, err := pc.GetCustomPricingData()
				if err != nil {
					t.Errorf("GetCustomPricingData: %v", err)
					return
				}
				if cfg.CPU != "0.006407" || cfg.RAM != "0.000859" {
					t.Errorf("concurrent read saw CPU=%q RAM=%q", cfg.CPU, cfg.RAM)
					return
				}
			}
		}()
	}

	wg.Wait()
}
