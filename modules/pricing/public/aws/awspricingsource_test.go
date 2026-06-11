package aws

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/pricing"
)

func TestNewAWSPricingSource(t *testing.T) {
	config := AWSPricingSourceConfig{
		CurrencyCode: "USD",
	}

	source := NewAWSPricingSource(config)

	if source == nil {
		t.Fatal("NewAWSPricingSource() returned nil")
	}

	if source.config.CurrencyCode != "USD" {
		t.Errorf("CurrencyCode = %v, want USD", source.config.CurrencyCode)
	}
}

func TestUsageTypeRegex(t *testing.T) {
	tests := []struct {
		name      string
		usageType string
		wantMatch bool
		wantGroup string
	}{
		{
			name:      "Standard EBS usage",
			usageType: "USE1-EBS:VolumeUsage.gp3",
			wantMatch: true,
			wantGroup: "EBS:VolumeUsage.gp3",
		},
		{
			name:      "GP2 volume",
			usageType: "USW2-EBS:VolumeUsage.gp2",
			wantMatch: true,
			wantGroup: "EBS:VolumeUsage.gp2",
		},
		{
			name:      "Standard volume",
			usageType: "USE1-EBS:VolumeUsage",
			wantMatch: true,
			wantGroup: "EBS:VolumeUsage",
		},
		{
			name:      "IO1 volume",
			usageType: "USE1-EBS:VolumeUsage.piops",
			wantMatch: true,
			wantGroup: "EBS:VolumeUsage.piops",
		},
		{
			name:      "No region prefix",
			usageType: "EBS:VolumeUsage.gp3",
			wantMatch: true,
			wantGroup: "EBS:VolumeUsage.gp3",
		},
		{
			name:      "Non-EBS usage",
			usageType: "BoxUsage:t3.medium",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := usageTypeRegex.FindStringSubmatch(tt.usageType)
			if tt.wantMatch {
				if len(matches) == 0 {
					t.Errorf("usageTypeRegex did not match %q", tt.usageType)
					return
				}
				// The last group should contain the EBS usage type
				actualGroup := matches[len(matches)-1]
				if actualGroup != tt.wantGroup {
					t.Errorf("usageTypeRegex matched %q, want %q", actualGroup, tt.wantGroup)
				}
			} else {
				if len(matches) > 0 {
					t.Errorf("usageTypeRegex unexpectedly matched %q", tt.usageType)
				}
			}
		})
	}
}

func TestAWSVolumeTypes(t *testing.T) {
	tests := []struct {
		usageType    string
		expectedType pricing.VolumeType
		shouldExist  bool
	}{
		{
			usageType:    "EBS:VolumeUsage.gp2",
			expectedType: pricing.VolumeTypeGP2,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeUsage.gp3",
			expectedType: pricing.VolumeTypeGP3,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeUsage",
			expectedType: pricing.VolumeTypeStandard,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeUsage.sc1",
			expectedType: pricing.VolumeTypeSC1,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeP-IOPS.piops",
			expectedType: pricing.VolumeTypeIO1,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeUsage.st1",
			expectedType: pricing.VolumeTypeST1,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeUsage.piops",
			expectedType: pricing.VolumeTypeIO1,
			shouldExist:  true,
		},
		{
			usageType:    "EBS:VolumeUsage.io2",
			expectedType: pricing.VolumeTypeIO2,
			shouldExist:  true,
		},
		{
			usageType:   "EBS:VolumeUsage.unknown",
			shouldExist: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.usageType, func(t *testing.T) {
			volumeType, exists := awsVolumeTypes[tt.usageType]
			if exists != tt.shouldExist {
				t.Errorf("awsVolumeTypes[%q] exists = %v, want %v", tt.usageType, exists, tt.shouldExist)
				return
			}
			if tt.shouldExist && volumeType != tt.expectedType {
				t.Errorf("awsVolumeTypes[%q] = %v, want %v", tt.usageType, volumeType, tt.expectedType)
			}
		})
	}
}

func TestOnDemandRateCodes(t *testing.T) {
	// Test that expected rate codes exist
	expectedCodes := []string{"JRTCKXETXF"}
	for _, code := range expectedCodes {
		if _, exists := OnDemandRateCodes[code]; !exists {
			t.Errorf("OnDemandRateCodes missing expected code: %s", code)
		}
	}

	// Test that we have at least one code
	if len(OnDemandRateCodes) == 0 {
		t.Error("OnDemandRateCodes is empty")
	}
}

func TestOnDemandRateCodesCn(t *testing.T) {
	// Test that expected China rate codes exist
	expectedCodes := []string{"99YE2YK9UR", "5Y9WH78GDR", "KW44MY7SZN"}
	for _, code := range expectedCodes {
		if _, exists := OnDemandRateCodesCn[code]; !exists {
			t.Errorf("OnDemandRateCodesCn missing expected code: %s", code)
		}
	}

	// Test that we have at least one code
	if len(OnDemandRateCodesCn) == 0 {
		t.Error("OnDemandRateCodesCn is empty")
	}
}

func TestHourlyRateCodes(t *testing.T) {
	if HourlyRateCode == "" {
		t.Error("HourlyRateCode is empty")
	}
	if HourlyRateCodeCn == "" {
		t.Error("HourlyRateCodeCn is empty")
	}
	if HourlyRateCode == HourlyRateCodeCn {
		t.Error("HourlyRateCode and HourlyRateCodeCn should be different")
	}
}

func TestPriceListEC2PricePerUnit_ForCurrency(t *testing.T) {
	tests := []struct {
		name     string
		price    PriceListEC2PricePerUnit
		currency string
		expected string
	}{
		{
			name: "USD currency",
			price: PriceListEC2PricePerUnit{
				USD: "0.0416",
				CNY: "0.2800",
			},
			currency: "USD",
			expected: "0.0416",
		},
		{
			name: "CNY currency",
			price: PriceListEC2PricePerUnit{
				USD: "0.0416",
				CNY: "0.2800",
			},
			currency: "CNY",
			expected: "0.2800",
		},
		{
			name: "CNY lowercase",
			price: PriceListEC2PricePerUnit{
				USD: "0.0416",
				CNY: "0.2800",
			},
			currency: "cny",
			expected: "0.2800",
		},
		{
			name: "Unknown currency defaults to USD",
			price: PriceListEC2PricePerUnit{
				USD: "0.0416",
				CNY: "0.2800",
			},
			currency: "EUR",
			expected: "0.0416",
		},
		{
			name: "CNY empty falls back to USD",
			price: PriceListEC2PricePerUnit{
				USD: "0.0416",
				CNY: "",
			},
			currency: "CNY",
			expected: "0.0416",
		},
		{
			name: "Empty currency defaults to USD",
			price: PriceListEC2PricePerUnit{
				USD: "0.0416",
				CNY: "0.2800",
			},
			currency: "",
			expected: "0.0416",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.price.ForCurrency(tt.currency)
			if result != tt.expected {
				t.Errorf("ForCurrency(%q) = %v, want %v", tt.currency, result, tt.expected)
			}
		})
	}
}

func TestPriceListEC2Term_String(t *testing.T) {
	term := &PriceListEC2Term{
		Sku:           "TEST123",
		OfferTermCode: "JRTCKXETXF",
		PriceDimensions: map[string]*PriceListEC2PriceDimension{
			"TEST123.JRTCKXETXF.6YS6EN2CT7": {
				Unit: "Hrs",
				PricePerUnit: PriceListEC2PricePerUnit{
					USD: "0.0416",
				},
			},
		},
	}

	result := term.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
	// Should contain the SKU
	if !containsSubstring(result, "TEST123") {
		t.Errorf("String() = %v, should contain SKU 'TEST123'", result)
	}
}

func TestPriceListEC2PriceDimension_String(t *testing.T) {
	pd := &PriceListEC2PriceDimension{
		Unit: "Hrs",
		PricePerUnit: PriceListEC2PricePerUnit{
			USD: "0.0416",
		},
	}

	result := pd.String()
	if result == "" {
		t.Error("String() returned empty string")
	}
	// Should contain unit
	if !containsSubstring(result, "Hrs") {
		t.Errorf("String() = %v, should contain unit 'Hrs'", result)
	}
}

// Helper function to check if string contains substring
func containsSubstring(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Made with Bob
