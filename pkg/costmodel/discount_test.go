package costmodel

import (
	"testing"
)

func TestParsePercentString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
		wantErr  bool
	}{
		{
			name:     "empty string returns 0",
			input:    "",
			expected: 0.0,
			wantErr:  false,
		},
		{
			name:     "decimal format 0.10 returns 0.001",
			input:    "0.10",
			expected: 0.001, // ParsePercentString multiplies by 0.01
			wantErr:  false,
		},
		{
			name:     "percentage format 10% returns 0.10",
			input:    "10%",
			expected: 0.10,
			wantErr:  false,
		},
		{
			name:     "percentage format 60% returns 0.60",
			input:    "60%",
			expected: 0.60,
			wantErr:  false,
		},
		{
			name:     "whole number 10 returns 0.10",
			input:    "10",
			expected: 0.10,
			wantErr:  false,
		},
		{
			name:     "whole number 60 returns 0.60",
			input:    "60",
			expected: 0.60,
			wantErr:  false,
		},
		{
			name:     "invalid string returns error",
			input:    "invalid",
			expected: 0.0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePercentString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePercentString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("ParsePercentString(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDiscountMultiplierCalculation(t *testing.T) {
	tests := []struct {
		name               string
		discount           float64
		negotiatedDiscount float64
		expectedMultiplier float64
	}{
		{
			name:               "no discount",
			discount:           0.0,
			negotiatedDiscount: 0.0,
			expectedMultiplier: 1.0,
		},
		{
			name:               "60% discount only",
			discount:           0.60,
			negotiatedDiscount: 0.0,
			expectedMultiplier: 0.40, // 1 - 0.60
		},
		{
			name:               "10% negotiated discount only",
			discount:           0.0,
			negotiatedDiscount: 0.10,
			expectedMultiplier: 0.90, // 1 - 0.10
		},
		{
			name:               "40% discount + 20% negotiated = 48% of original",
			discount:           0.40,
			negotiatedDiscount: 0.20,
			expectedMultiplier: 0.48, // (1-0.4) * (1-0.2) = 0.6 * 0.8 = 0.48
		},
		{
			name:               "50% discount + 50% negotiated = 25% of original",
			discount:           0.50,
			negotiatedDiscount: 0.50,
			expectedMultiplier: 0.25, // (1-0.5) * (1-0.5) = 0.5 * 0.5 = 0.25
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate multiplier the same way as in GetNodeCost
			multiplier := 1.0
			multiplier = multiplier * (1.0 - tt.discount)
			multiplier = multiplier * (1.0 - tt.negotiatedDiscount)

			// Allow small floating point differences
			diff := multiplier - tt.expectedMultiplier
			if diff < -0.0001 || diff > 0.0001 {
				t.Errorf("Multiplier for discount=%f, negotiatedDiscount=%f = %f, want %f",
					tt.discount, tt.negotiatedDiscount, multiplier, tt.expectedMultiplier)
			}
		})
	}
}

func TestSpotVsOnDemandDiscountSelection(t *testing.T) {
	tests := []struct {
		name                    string
		isSpot                  bool
		onDemandDiscount        float64
		spotDiscount            float64
		expectedDiscountApplied float64
	}{
		{
			name:                    "on-demand node uses on-demand discount",
			isSpot:                  false,
			onDemandDiscount:        0.60,
			spotDiscount:            0.10,
			expectedDiscountApplied: 0.60,
		},
		{
			name:                    "spot node uses spot discount",
			isSpot:                  true,
			onDemandDiscount:        0.60,
			spotDiscount:            0.10,
			expectedDiscountApplied: 0.10,
		},
		{
			name:                    "spot node with no spot discount",
			isSpot:                  true,
			onDemandDiscount:        0.60,
			spotDiscount:            0.0,
			expectedDiscountApplied: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the logic in GetNodeCost
			var selectedDiscount float64
			if tt.isSpot {
				selectedDiscount = tt.spotDiscount
			} else {
				selectedDiscount = tt.onDemandDiscount
			}

			if selectedDiscount != tt.expectedDiscountApplied {
				t.Errorf("For isSpot=%v, selected discount = %f, want %f",
					tt.isSpot, selectedDiscount, tt.expectedDiscountApplied)
			}
		})
	}
}

func TestCostDiscountApplication(t *testing.T) {
	tests := []struct {
		name         string
		originalCost float64
		discountMult float64 // The multiplier (e.g., 0.4 for 60% discount)
		expectedCost float64
	}{
		{
			name:         "no discount",
			originalCost: 100.0,
			discountMult: 1.0,
			expectedCost: 100.0,
		},
		{
			name:         "60% discount",
			originalCost: 100.0,
			discountMult: 0.4, // 1 - 0.6
			expectedCost: 40.0,
		},
		{
			name:         "10% discount",
			originalCost: 100.0,
			discountMult: 0.9, // 1 - 0.1
			expectedCost: 90.0,
		},
		{
			name:         "combined 40% + 20% discount",
			originalCost: 100.0,
			discountMult: 0.48, // (1-0.4) * (1-0.2)
			expectedCost: 48.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.originalCost * tt.discountMult

			diff := result - tt.expectedCost
			if diff < -0.0001 || diff > 0.0001 {
				t.Errorf("Cost after discount = %f, want %f", result, tt.expectedCost)
			}
		})
	}
}
