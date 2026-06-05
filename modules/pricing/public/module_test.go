package public

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

func TestNewPricingModule(t *testing.T) {
	// Create a temporary directory for test data
	tmpDir := t.TempDir()
	awsDir := filepath.Join(tmpDir, "aws")
	if err := os.MkdirAll(awsDir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	// Create test pricing data
	testPricingSet := &pricing.PricingSet{
		Nodes: []*pricing.NodePricing{
			{
				Properties: pricing.NodePricingProperties{
					Provider:     pricing.AWSProvider,
					InstanceType: "t3.medium",
					Region:       "us-east-1",
					Provisioning: pricing.ProvisioningOnDemand,
				},
				Prices: pricing.Prices{
					unit.USD: []pricing.Price{
						{Currency: unit.USD, Unit: unit.Hour, Price: 0.0416},
					},
				},
			},
			{
				Properties: pricing.NodePricingProperties{
					Provider:     pricing.AWSProvider,
					InstanceType: "t3.large",
					Region:       "us-west-2",
					Provisioning: pricing.ProvisioningOnDemand,
				},
				Prices: pricing.Prices{
					unit.USD: []pricing.Price{
						{Currency: unit.USD, Unit: unit.Hour, Price: 0.0832},
					},
				},
			},
		},
		Volumes: []*pricing.VolumePricing{
			{
				Properties: pricing.VolumePricingProperties{
					Provider:   pricing.AWSProvider,
					VolumeType: pricing.VolumeTypeGP3,
					Region:     "us-east-1",
				},
				Prices: pricing.Prices{
					unit.USD: []pricing.Price{
						{Currency: unit.USD, Unit: unit.Hour, Price: 0.00011},
					},
				},
			},
		},
	}

	// Write test data to file
	data, err := json.Marshal(testPricingSet)
	if err != nil {
		t.Fatalf("failed to marshal test data: %v", err)
	}

	testFile := filepath.Join(awsDir, "aws-usd.json")
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Test creating pricing module
	config := PricingModuleConfig{
		BaseDir:  tmpDir,
		Provider: pricing.AWSProvider,
		Currency: unit.USD,
	}

	pm, err := NewPricingModule(config)
	if err != nil {
		t.Fatalf("NewPricingModule() error = %v", err)
	}

	if pm == nil {
		t.Fatal("NewPricingModule() returned nil")
	}

	if pm.Providers == nil {
		t.Fatal("Providers not initialized")
	}

	// Verify indexing worked
	if (*pm.Providers)[pricing.AWSProvider] == nil {
		t.Error("AWS provider not indexed")
	}
}

func TestGetNodePricing(t *testing.T) {
	// Create test pricing module
	tmpDir := t.TempDir()
	awsDir := filepath.Join(tmpDir, "aws")
	if err := os.MkdirAll(awsDir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	testPricingSet := &pricing.PricingSet{
		Nodes: []*pricing.NodePricing{
			{
				Properties: pricing.NodePricingProperties{
					Provider:     pricing.AWSProvider,
					InstanceType: "t3.medium",
					Region:       "us-east-1",
					Provisioning: pricing.ProvisioningOnDemand,
				},
				Prices: pricing.Prices{
					unit.USD: []pricing.Price{
						{Currency: unit.USD, Unit: unit.Hour, Price: 0.0416},
					},
				},
			},
		},
		Volumes: []*pricing.VolumePricing{},
	}

	data, _ := json.Marshal(testPricingSet)
	testFile := filepath.Join(awsDir, "aws-usd.json")
	os.WriteFile(testFile, data, 0644)

	config := PricingModuleConfig{
		BaseDir:  tmpDir,
		Provider: pricing.AWSProvider,
		Currency: unit.USD,
	}

	pm, err := NewPricingModule(config)
	if err != nil {
		t.Fatalf("NewPricingModule() error = %v", err)
	}

	tests := []struct {
		name         string
		provider     pricing.Provider
		instanceType string
		region       string
		wantErr      bool
		wantPrice    float64
	}{
		{
			name:         "valid lookup",
			provider:     pricing.AWSProvider,
			instanceType: "t3.medium",
			region:       "us-east-1",
			wantErr:      false,
			wantPrice:    0.0416,
		},
		{
			name:         "invalid provider",
			provider:     pricing.GCPProvider,
			instanceType: "t3.medium",
			region:       "us-east-1",
			wantErr:      true,
		},
		{
			name:         "invalid instance type",
			provider:     pricing.AWSProvider,
			instanceType: "nonexistent",
			region:       "us-east-1",
			wantErr:      true,
		},
		{
			name:         "invalid region",
			provider:     pricing.AWSProvider,
			instanceType: "t3.medium",
			region:       "nonexistent",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodePricing, err := pm.GetNodePricing(tt.provider, tt.instanceType, tt.region)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetNodePricing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if nodePricing == nil {
					t.Error("GetNodePricing() returned nil")
					return
				}
				if nodePricing.Properties.InstanceType != tt.instanceType {
					t.Errorf("InstanceType = %v, want %v", nodePricing.Properties.InstanceType, tt.instanceType)
				}
				if nodePricing.Properties.Region != tt.region {
					t.Errorf("Region = %v, want %v", nodePricing.Properties.Region, tt.region)
				}
				prices := nodePricing.Prices[unit.USD]
				if len(prices) > 0 && prices[0].Price != tt.wantPrice {
					t.Errorf("Price = %v, want %v", prices[0].Price, tt.wantPrice)
				}
			}
		})
	}
}

func TestGetVolumePricing(t *testing.T) {
	tmpDir := t.TempDir()
	awsDir := filepath.Join(tmpDir, "aws")
	if err := os.MkdirAll(awsDir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	testPricingSet := &pricing.PricingSet{
		Nodes: []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{
			{
				Properties: pricing.VolumePricingProperties{
					Provider:   pricing.AWSProvider,
					VolumeType: pricing.VolumeTypeGP3,
					Region:     "us-east-1",
				},
				Prices: pricing.Prices{
					unit.USD: []pricing.Price{
						{Currency: unit.USD, Unit: unit.Hour, Price: 0.00011},
					},
				},
			},
		},
	}

	data, _ := json.Marshal(testPricingSet)
	testFile := filepath.Join(awsDir, "aws-usd.json")
	os.WriteFile(testFile, data, 0644)

	config := PricingModuleConfig{
		BaseDir:  tmpDir,
		Provider: pricing.AWSProvider,
		Currency: unit.USD,
	}

	pm, err := NewPricingModule(config)
	if err != nil {
		t.Fatalf("NewPricingModule() error = %v", err)
	}

	tests := []struct {
		name       string
		provider   pricing.Provider
		volumeType string
		region     string
		wantErr    bool
		wantPrice  float64
	}{
		{
			name:       "valid lookup",
			provider:   pricing.AWSProvider,
			volumeType: string(pricing.VolumeTypeGP3),
			region:     "us-east-1",
			wantErr:    false,
			wantPrice:  0.00011,
		},
		{
			name:       "invalid provider",
			provider:   pricing.GCPProvider,
			volumeType: string(pricing.VolumeTypeGP3),
			region:     "us-east-1",
			wantErr:    true,
		},
		{
			name:       "invalid volume type",
			provider:   pricing.AWSProvider,
			volumeType: "nonexistent",
			region:     "us-east-1",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volumePricing, err := pm.GetVolumePricing(tt.provider, tt.volumeType, tt.region)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetVolumePricing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if volumePricing == nil {
					t.Error("GetVolumePricing() returned nil")
					return
				}
				if string(volumePricing.Properties.VolumeType) != tt.volumeType {
					t.Errorf("VolumeType = %v, want %v", volumePricing.Properties.VolumeType, tt.volumeType)
				}
				prices := volumePricing.Prices[unit.USD]
				if len(prices) > 0 && prices[0].Price != tt.wantPrice {
					t.Errorf("Price = %v, want %v", prices[0].Price, tt.wantPrice)
				}
			}
		})
	}
}

func TestNewNodePricingReader(t *testing.T) {
	tmpDir := t.TempDir()
	awsDir := filepath.Join(tmpDir, "aws")
	if err := os.MkdirAll(awsDir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	testPricingSet := &pricing.PricingSet{
		Nodes: []*pricing.NodePricing{
			{
				Properties: pricing.NodePricingProperties{
					Provider:     pricing.AWSProvider,
					InstanceType: "t3.medium",
					Region:       "us-east-1",
				},
				Prices: pricing.Prices{},
			},
		},
		Volumes: []*pricing.VolumePricing{},
	}

	data, _ := json.Marshal(testPricingSet)
	testFile := filepath.Join(awsDir, "aws-usd.json")
	os.WriteFile(testFile, data, 0644)

	config := PricingModuleConfig{
		BaseDir:  tmpDir,
		Provider: pricing.AWSProvider,
		Currency: unit.USD,
	}

	pm, err := NewPricingModule(config)
	if err != nil {
		t.Fatalf("NewPricingModule() error = %v", err)
	}

	ctx := context.Background()
	reader, err := pm.NewNodePricingReader(ctx)
	if err != nil {
		t.Fatalf("NewNodePricingReader() error = %v", err)
	}

	if reader == nil {
		t.Fatal("NewNodePricingReader() returned nil")
	}
}

func TestNewVolumePricingReader(t *testing.T) {
	tmpDir := t.TempDir()
	awsDir := filepath.Join(tmpDir, "aws")
	if err := os.MkdirAll(awsDir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	testPricingSet := &pricing.PricingSet{
		Nodes: []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{
			{
				Properties: pricing.VolumePricingProperties{
					Provider:   pricing.AWSProvider,
					VolumeType: pricing.VolumeTypeGP3,
					Region:     "us-east-1",
				},
				Prices: pricing.Prices{},
			},
		},
	}

	data, _ := json.Marshal(testPricingSet)
	testFile := filepath.Join(awsDir, "aws-usd.json")
	os.WriteFile(testFile, data, 0644)

	config := PricingModuleConfig{
		BaseDir:  tmpDir,
		Provider: pricing.AWSProvider,
		Currency: unit.USD,
	}

	pm, err := NewPricingModule(config)
	if err != nil {
		t.Fatalf("NewPricingModule() error = %v", err)
	}

	ctx := context.Background()
	reader, err := pm.NewVolumePricingReader(ctx)
	if err != nil {
		t.Fatalf("NewVolumePricingReader() error = %v", err)
	}

	if reader == nil {
		t.Fatal("NewVolumePricingReader() returned nil")
	}
}

// Made with Bob
