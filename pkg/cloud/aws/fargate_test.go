package aws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
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

func TestFargatePricing_Initialize(t *testing.T) {
	// Load test data
	testDataPath := "testdata/ecs-pricing-us-east-1.json"
	data, err := os.ReadFile(testDataPath)
	if err != nil {
		t.Fatalf("Failed to read test data: %v", err)
	}

	// Create a test HTTP server that serves the pricing data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer server.Close()

	// Set up test environment variable to use our test server
	t.Setenv("AWS_ECS_PRICING_URL", server.URL)

	tests := []struct {
		name     string
		nodeList []*clustercache.Node
		wantErr  bool
	}{
		{
			name: "successful initialization",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-east-1",
					},
				},
			},
			wantErr: false,
		},
		{
			name:     "empty node list",
			nodeList: []*clustercache.Node{},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFargatePricing()
			err := f.Initialize(tt.nodeList)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Initialize() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Initialize() unexpected error: %v", err)
				return
			}

			// Verify that regions were populated
			if len(f.regions) == 0 {
				t.Error("Initialize() did not populate any regions")
				return
			}

			// Check that us-east-1 pricing was populated (from test data)
			usEast1, ok := f.regions["us-east-1"]
			if !ok {
				t.Error("Initialize() did not populate us-east-1 region")
				return
			}

			// Verify all required usage types are present
			for _, usageType := range fargateUsageTypes {
				if price, ok := usEast1[usageType]; !ok {
					t.Errorf("Initialize() missing usage type %s", usageType)
				} else if price <= 0 {
					t.Errorf("Initialize() invalid price %f for usage type %s", price, usageType)
				}
			}
		})
	}
}

func TestFargatePricing_Initialize_HTTPError(t *testing.T) {
	// Create a test HTTP server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Set up test environment variable to use our test server
	t.Setenv("AWS_ECS_PRICING_URL", server.URL)

	f := NewFargatePricing()
	nodeList := []*clustercache.Node{
		{
			Name: "test-node",
			Labels: map[string]string{
				"topology.kubernetes.io/region": "us-east-1",
			},
		},
	}

	err := f.Initialize(nodeList)
	if err == nil {
		t.Error("Initialize() expected error for HTTP 500, got nil")
	}
}

func TestFargatePricing_Initialize_InvalidJSON(t *testing.T) {
	// Create a test HTTP server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	// Set up test environment variable to use our test server
	t.Setenv("AWS_ECS_PRICING_URL", server.URL)

	f := NewFargatePricing()
	nodeList := []*clustercache.Node{
		{
			Name: "test-node",
			Labels: map[string]string{
				"topology.kubernetes.io/region": "us-east-1",
			},
		},
	}

	err := f.Initialize(nodeList)
	if err == nil {
		t.Error("Initialize() expected error for invalid JSON, got nil")
	}
}

func TestFargatePricing_getPricingURL(t *testing.T) {
	tests := []struct {
		name     string
		nodeList []*clustercache.Node
		envVar   string
		expected string
	}{
		{
			name: "with environment variable override",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-east-1",
					},
				},
			},
			envVar:   "https://custom-pricing-url.com",
			expected: "https://custom-pricing-url.com",
		},
		{
			name: "without environment variable - single region",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "us-west-2",
					},
				},
			},
			envVar:   "",
			expected: "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonECS/current/us-west-2/index.json",
		},
		{
			name: "without environment variable - Chinese region",
			nodeList: []*clustercache.Node{
				{
					Name: "test-node",
					Labels: map[string]string{
						"topology.kubernetes.io/region": "cn-north-1",
					},
				},
			},
			envVar:   "",
			expected: "https://pricing.cn-north-1.amazonaws.com.cn/offers/v1.0/cn/AmazonECS/current/cn-north-1/index.json",
		},
		{
			name:     "without environment variable - empty node list",
			nodeList: []*clustercache.Node{},
			envVar:   "",
			expected: "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonECS/current/index.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envVar != "" {
				t.Setenv("AWS_ECS_PRICING_URL", tt.envVar)
			} else {
				t.Setenv("AWS_ECS_PRICING_URL", "")
			}

			f := NewFargatePricing()
			result := f.getPricingURL(tt.nodeList)

			if result != tt.expected {
				t.Errorf("getPricingURL() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestFargatePricing_ValidateAWSPricingFormat validates that the actual AWS pricing API
// returns data in the expected format. This test is skipped by default and only runs
// when INTEGRATION=true to avoid hitting AWS APIs in regular CI runs.
func TestFargatePricing_ValidateAWSPricingFormat(t *testing.T) {
	if os.Getenv("INTEGRATION") != "true" {
		t.Skip("Skipping integration test. Set INTEGRATION=true to run.")
	}

	nodes := []*clustercache.Node{
		{
			Labels: map[string]string{
				"topology.kubernetes.io/region": "us-east-1",
			},
		},
	}

	url := getPricingListURL("AmazonECS", nodes)
	t.Logf("Testing AWS pricing URL: %s", url)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("Failed to fetch pricing data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Unexpected status code: %d", resp.StatusCode)
	}

	var pricing AWSPricing
	if err := json.NewDecoder(resp.Body).Decode(&pricing); err != nil {
		t.Fatalf("Failed to decode pricing data - AWS format may have changed: %v", err)
	}

	if len(pricing.Products) == 0 {
		t.Fatal("Expected products in pricing data, got none - AWS format may have changed")
	}

	if len(pricing.Terms.OnDemand) == 0 {
		t.Fatal("Expected OnDemand terms in pricing data, got none - AWS format may have changed")
	}

	t.Logf("✓ AWS pricing format validated: %d products, %d OnDemand terms",
		len(pricing.Products), len(pricing.Terms.OnDemand))
}
