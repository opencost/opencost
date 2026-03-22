package provider

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
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

// testProviderConfig implements models.ProviderConfig for unit tests.
type testProviderConfig struct {
	customPricing *models.CustomPricing
}

func (m *testProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	return m.customPricing, nil
}

func (m *testProviderConfig) Update(_ func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *testProviderConfig) UpdateFromMap(_ map[string]string) (*models.CustomPricing, error) {
	return nil, nil
}

func (m *testProviderConfig) ConfigFileManager() *config.ConfigFileManager {
	return nil
}

func TestCustomProviderRefreshCustomPricing(t *testing.T) {
	cp := &models.CustomPricing{
		CPU:            "0.02",
		RAM:            "0.003",
		GPU:            "0.5",
		SpotCPU:        "0.01",
		SpotRAM:        "0.002",
		SpotGPU:        "0.25",
		SpotLabel:      "node.kubernetes.io/instance-type",
		SpotLabelValue: "spot",
		GpuLabel:       "gamma.co/gpu",
		GpuLabelValue:  "true",
	}
	cp2 := &CustomProvider{Config: &testProviderConfig{customPricing: cp}}
	if err := cp2.RefreshCustomPricing(); err != nil {
		t.Fatalf("CustomProvider.RefreshCustomPricing() unexpected error: %v", err)
	}
	if cp2.SpotLabel != cp.SpotLabel {
		t.Errorf("SpotLabel = %q, want %q", cp2.SpotLabel, cp.SpotLabel)
	}
	if cp2.GPULabel != cp.GpuLabel {
		t.Errorf("GPULabel = %q, want %q", cp2.GPULabel, cp.GpuLabel)
	}
	if cp2.Pricing["default"] == nil {
		t.Fatal("Pricing[\"default\"] should not be nil")
	}
	if cp2.Pricing["default"].CPU != cp.CPU {
		t.Errorf("Pricing[\"default\"].CPU = %q, want %q", cp2.Pricing["default"].CPU, cp.CPU)
	}
}

func TestCSVProviderRefreshCustomPricing(t *testing.T) {
	c := &CSVProvider{}
	if err := c.RefreshCustomPricing(); err != nil {
		t.Fatalf("CSVProvider.RefreshCustomPricing() unexpected error: %v", err)
	}
}
