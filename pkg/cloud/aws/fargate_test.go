package aws

import (
	"encoding/json"
	"os"
	"testing"
)

var testRegionPricing = FargateRegionPricing{
	usageTypeFargateLinuxX86CPU:    0.0404800000,
	usageTypeFargateLinuxX86RAM:    0.0044450000,
	usageTypeFargateLinuxArmCPU:    0.0323800000,
	usageTypeFargateLinuxArmRAM:    0.0035600000,
	usageTypeFargateWindowsCPU:     0.0465520000,
	usageTypeFargateWindowsLicense: 0.0460000000,
	usageTypeFargateWindowsRAM:     0.0051117500,
}

func TestFargatePricing_populatePricing(t *testing.T) {
	// Load test data
	testDataPath := "testdata/ecs-pricing-us-east-1.json"
	data, err := os.ReadFile(testDataPath)
	if err != nil {
		t.Fatalf("Failed to read test data: %v", err)
	}

	var pricing AWSPricing
	err = json.Unmarshal(data, &pricing)
	if err != nil {
		t.Fatalf("Failed to unmarshal test data: %v", err)
	}

	tests := []struct {
		name    string
		pricing *AWSPricing
		wantErr bool
	}{
		{
			name:    "valid pricing data",
			pricing: &pricing,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFargatePricing()

			err := f.populatePricing(tt.pricing)

			if tt.wantErr {
				if err == nil {
					t.Errorf("populatePricing() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("populatePricing() unexpected error: %v", err)
				return
			}

			// Verify that regions were populated
			if len(f.regions) == 0 {
				t.Error("populatePricing() did not populate any regions")
				return
			}

			// Check that us-east-1 pricing was populated (from test data)
			usEast1, ok := f.regions["us-east-1"]
			if !ok {
				t.Error("populatePricing() did not populate us-east-1 region")
				return
			}

			// Verify all required usage types are present
			for _, usageType := range fargateUsageTypes {
				if price, ok := usEast1[usageType]; !ok {
					t.Errorf("populatePricing() missing usage type %s", usageType)
				} else if price <= 0 {
					t.Errorf("populatePricing() invalid price %f for usage type %s", price, usageType)
				}
			}

			// Test specific pricing values from test data
			for usageType, expectedPrice := range testRegionPricing {
				if actualPrice, ok := usEast1[usageType]; ok {
					if actualPrice != expectedPrice {
						t.Errorf("populatePricing() price mismatch for %s: expected %f, got %f", usageType, expectedPrice, actualPrice)
					}
				}
			}
		})
	}
}

func TestFargatePricing_GetHourlyPricing(t *testing.T) {
	// Create a Fargate pricing instance with test data
	f := NewFargatePricing()

	// Populate test pricing data for us-east-1
	f.regions["us-east-1"] = testRegionPricing

	tests := []struct {
		name        string
		region      string
		os          string
		arch        string
		expectedCPU float64
		expectedRAM float64
		expectedErr bool
	}{
		{
			name:        "linux amd64",
			region:      "us-east-1",
			os:          "linux",
			arch:        "amd64",
			expectedCPU: 0.0404800000,
			expectedRAM: 0.0044450000,
			expectedErr: false,
		},
		{
			name:        "linux arm64",
			region:      "us-east-1",
			os:          "linux",
			arch:        "arm64",
			expectedCPU: 0.0323800000,
			expectedRAM: 0.0035600000,
			expectedErr: false,
		},
		{
			name:        "windows (any arch)",
			region:      "us-east-1",
			os:          "windows",
			arch:        "amd64",
			expectedCPU: 0.0925520000, // CPU + License: 0.0465520000 + 0.0460000000
			expectedRAM: 0.0051117500,
			expectedErr: false,
		},
		{
			name:        "unknown region",
			region:      "unknown-region",
			os:          "linux",
			arch:        "amd64",
			expectedCPU: 0,
			expectedRAM: 0,
			expectedErr: true,
		},
		{
			name:        "unknown os",
			region:      "us-east-1",
			os:          "macos",
			arch:        "amd64",
			expectedCPU: 0,
			expectedRAM: 0,
			expectedErr: true,
		},
		{
			name:        "unknown arch for linux",
			region:      "us-east-1",
			os:          "linux",
			arch:        "unknown",
			expectedCPU: 0,
			expectedRAM: 0,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpu, memory, err := f.GetHourlyPricing(tt.region, tt.os, tt.arch)

			if tt.expectedErr {
				if err == nil {
					t.Errorf("GetHourlyPricing() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("GetHourlyPricing() unexpected error: %v", err)
				return
			}

			if cpu != tt.expectedCPU {
				t.Errorf("GetHourlyPricing() CPU price mismatch: expected %f, got %f", tt.expectedCPU, cpu)
			}

			if memory != tt.expectedRAM {
				t.Errorf("GetHourlyPricing() RAM price mismatch: expected %f, got %f", tt.expectedRAM, memory)
			}
		})
	}
}

func TestFargateRegionPricing_Validate(t *testing.T) {
	tests := []struct {
		name    string
		pricing FargateRegionPricing
		wantErr bool
	}{
		{
			name: "valid complete pricing",
			pricing: FargateRegionPricing{
				usageTypeFargateLinuxX86CPU:    0.04048,
				usageTypeFargateLinuxX86RAM:    0.004445,
				usageTypeFargateLinuxArmCPU:    0.03238,
				usageTypeFargateLinuxArmRAM:    0.00356,
				usageTypeFargateWindowsCPU:     0.046552,
				usageTypeFargateWindowsLicense: 0.046,
				usageTypeFargateWindowsRAM:     0.00511175,
			},
			wantErr: false,
		},
		{
			name: "missing linux x86 CPU",
			pricing: FargateRegionPricing{
				usageTypeFargateLinuxX86RAM:    0.004445,
				usageTypeFargateLinuxArmCPU:    0.03238,
				usageTypeFargateLinuxArmRAM:    0.00356,
				usageTypeFargateWindowsCPU:     0.046552,
				usageTypeFargateWindowsLicense: 0.046,
				usageTypeFargateWindowsRAM:     0.00511175,
			},
			wantErr: true,
		},
		{
			name: "missing linux x86 RAM",
			pricing: FargateRegionPricing{
				usageTypeFargateLinuxX86CPU:    0.04048,
				usageTypeFargateLinuxArmCPU:    0.03238,
				usageTypeFargateLinuxArmRAM:    0.00356,
				usageTypeFargateWindowsCPU:     0.046552,
				usageTypeFargateWindowsLicense: 0.046,
				usageTypeFargateWindowsRAM:     0.00511175,
			},
			wantErr: true,
		},
		{
			name: "missing windows license",
			pricing: FargateRegionPricing{
				usageTypeFargateLinuxX86CPU: 0.04048,
				usageTypeFargateLinuxX86RAM: 0.004445,
				usageTypeFargateLinuxArmCPU: 0.03238,
				usageTypeFargateLinuxArmRAM: 0.00356,
				usageTypeFargateWindowsCPU:  0.046552,
				usageTypeFargateWindowsRAM:  0.00511175,
			},
			wantErr: true,
		},
		{
			name:    "empty pricing",
			pricing: FargateRegionPricing{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pricing.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("Validate() expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}
