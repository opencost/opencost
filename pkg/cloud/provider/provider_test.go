package provider

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/config"
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
