package azure

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
)

func TestMapAzureDiskType(t *testing.T) {
	tests := []struct {
		name     string
		skuName  string
		expected pricing.VolumeType
	}{
		{
			name:     "Premium SSD V2",
			skuName:  "Premium SSD v2 Managed Disk",
			expected: pricing.VolumeTypePremiumV2LRS,
		},
		{
			name:     "PremiumV2 variant",
			skuName:  "PremiumV2 LRS Disk",
			expected: pricing.VolumeTypePremiumV2LRS,
		},
		{
			name:     "Premium SSD",
			skuName:  "Premium SSD Managed Disk",
			expected: pricing.VolumeTypePremiumLRS,
		},
		{
			name:     "Standard SSD",
			skuName:  "Standard SSD Managed Disk",
			expected: pricing.VolumeTypeStandardSSDLRS,
		},
		{
			name:     "StandardSSD variant",
			skuName:  "StandardSSD LRS",
			expected: pricing.VolumeTypeStandardSSDLRS,
		},
		{
			name:     "Standard HDD",
			skuName:  "Standard HDD Managed Disk",
			expected: pricing.VolumeTypeStandardHDDLRS,
		},
		{
			name:     "Ultra SSD",
			skuName:  "Ultra SSD Managed Disk",
			expected: pricing.VolumeTypeUltraSSDLRS,
		},
		{
			name:     "Unknown type",
			skuName:  "Some Unknown Disk Type",
			expected: pricing.VolumeTypeNil,
		},
		{
			name:     "Case insensitive Premium",
			skuName:  "PREMIUM SSD MANAGED DISK",
			expected: pricing.VolumeTypePremiumLRS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapAzureDiskType(tt.skuName)
			if result != tt.expected {
				t.Errorf("mapAzureDiskType(%q) = %v, want %v", tt.skuName, result, tt.expected)
			}
		})
	}
}

func TestIncludeItem(t *testing.T) {
	source := &AzurePricingSource{
		config: AzurePricingSourceConfig{
			CurrencyCode: "USD",
		},
	}

	tests := []struct {
		name     string
		item     AzurePricingAttributes
		expected bool
	}{
		{
			name: "valid Linux VM",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v3",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv3 Series",
				SkuName:       "D2s v3",
			},
			expected: true,
		},
		{
			name: "Windows VM - excluded",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v3",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv3 Series Windows",
				SkuName:       "D2s v3",
			},
			expected: false,
		},
		{
			name: "Low priority - excluded",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v3",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv3 Series",
				SkuName:       "D2s v3 Low Priority",
			},
			expected: false,
		},
		{
			name: "Cloud Services - excluded",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v3",
				ArmRegionName: "eastus",
				ProductName:   "Cloud Services Dsv3 Series",
				SkuName:       "D2s v3",
			},
			expected: false,
		},
		{
			name: "CloudServices variant - excluded",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v3",
				ArmRegionName: "eastus",
				ProductName:   "CloudServices Dsv3 Series",
				SkuName:       "D2s v3",
			},
			expected: false,
		},
		{
			name: "Missing ArmSkuName - excluded",
			item: AzurePricingAttributes{
				ArmSkuName:    "",
				ArmRegionName: "eastus",
				ProductName:   "Virtual Machines Dsv3 Series",
				SkuName:       "D2s v3",
			},
			expected: false,
		},
		{
			name: "Missing ArmRegionName - excluded",
			item: AzurePricingAttributes{
				ArmSkuName:    "Standard_D2s_v3",
				ArmRegionName: "",
				ProductName:   "Virtual Machines Dsv3 Series",
				SkuName:       "D2s v3",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := source.includeItem(tt.item)
			if result != tt.expected {
				t.Errorf("includeItem() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIncludeDiskItem(t *testing.T) {
	source := &AzurePricingSource{
		config: AzurePricingSourceConfig{
			CurrencyCode: "USD",
		},
	}

	tests := []struct {
		name     string
		item     AzurePricingAttributes
		expected bool
	}{
		{
			name: "Managed disk - included",
			item: AzurePricingAttributes{
				ArmRegionName: "eastus",
				ProductName:   "Premium SSD Managed Disk",
				SkuName:       "P10 LRS",
			},
			expected: true,
		},
		{
			name: "Managed Disk uppercase - included",
			item: AzurePricingAttributes{
				ArmRegionName: "eastus",
				ProductName:   "PREMIUM SSD MANAGED DISK",
				SkuName:       "P10 LRS",
			},
			expected: true,
		},
		{
			name: "Unmanaged disk - excluded",
			item: AzurePricingAttributes{
				ArmRegionName: "eastus",
				ProductName:   "Premium SSD Unmanaged Disk",
				SkuName:       "P10",
			},
			expected: false,
		},
		{
			name: "Missing region - excluded",
			item: AzurePricingAttributes{
				ArmRegionName: "",
				ProductName:   "Premium SSD Managed Disk",
				SkuName:       "P10 LRS",
			},
			expected: false,
		},
		{
			name: "Storage account - excluded",
			item: AzurePricingAttributes{
				ArmRegionName: "eastus",
				ProductName:   "Storage Account",
				SkuName:       "Standard LRS",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := source.includeDiskItem(tt.item)
			if result != tt.expected {
				t.Errorf("includeDiskItem() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBuildVMURL(t *testing.T) {
	tests := []struct {
		name         string
		currencyCode string
		wantContains []string
	}{
		{
			name:         "USD currency",
			currencyCode: "USD",
			wantContains: []string{
				"prices.azure.com",
				"serviceName+eq+%27Virtual+Machines%27",
				"priceType+eq+%27Consumption%27",
				"currencyCode=USD",
			},
		},
		{
			name:         "EUR currency",
			currencyCode: "EUR",
			wantContains: []string{
				"prices.azure.com",
				"currencyCode=EUR",
			},
		},
		{
			name:         "Empty currency",
			currencyCode: "",
			wantContains: []string{
				"prices.azure.com",
				"serviceName+eq+%27Virtual+Machines%27",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &AzurePricingSource{
				config: AzurePricingSourceConfig{
					CurrencyCode: tt.currencyCode,
				},
			}
			url := source.buildVMURL()
			for _, want := range tt.wantContains {
				if !contains(url, want) {
					t.Errorf("buildVMURL() = %v, want to contain %v", url, want)
				}
			}
		})
	}
}

func TestBuildDiskURL(t *testing.T) {
	tests := []struct {
		name         string
		currencyCode string
		wantContains []string
	}{
		{
			name:         "USD currency",
			currencyCode: "USD",
			wantContains: []string{
				"prices.azure.com",
				"serviceName+eq+%27Storage%27",
				"priceType+eq+%27Consumption%27",
				"currencyCode=USD",
			},
		},
		{
			name:         "EUR currency",
			currencyCode: "EUR",
			wantContains: []string{
				"prices.azure.com",
				"currencyCode=EUR",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &AzurePricingSource{
				config: AzurePricingSourceConfig{
					CurrencyCode: tt.currencyCode,
				},
			}
			url := source.buildDiskURL()
			for _, want := range tt.wantContains {
				if !contains(url, want) {
					t.Errorf("buildDiskURL() = %v, want to contain %v", url, want)
				}
			}
		})
	}
}

func TestNewAzurePricingSource(t *testing.T) {
	config := AzurePricingSourceConfig{
		CurrencyCode: "USD",
	}

	source := NewAzurePricingSource(config)

	if source == nil {
		t.Fatal("NewAzurePricingSource() returned nil")
	}

	if source.config.CurrencyCode != "USD" {
		t.Errorf("CurrencyCode = %v, want USD", source.config.CurrencyCode)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || 
		(len(s) > 0 && (s[0:len(substr)] == substr || contains(s[1:], substr))))
}

// Made with Bob
