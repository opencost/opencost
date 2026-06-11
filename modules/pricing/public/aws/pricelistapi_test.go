package aws

import (
	"testing"

	"github.com/opencost/opencost/pkg/env"
)

func TestGetListPriceURL(t *testing.T) {
	t.Run("uses override when configured", func(t *testing.T) {
		t.Setenv(env.AWSPricingURL, "https://example.com/custom.json")

		got := getListPriceURL("AmazonEC2", "us-east-1")

		if got != "https://example.com/custom.json" {
			t.Fatalf("expected override URL, got %q", got)
		}
	})

	t.Run("builds standard regional URL", func(t *testing.T) {
		t.Setenv(env.AWSPricingURL, "")

		got := getListPriceURL("AmazonEC2", "us-west-2")
		want := "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-west-2/index.json"

		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})

	t.Run("builds standard global URL when region empty", func(t *testing.T) {
		t.Setenv(env.AWSPricingURL, "")

		got := getListPriceURL("AmazonEC2", "")
		want := "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"

		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})

	t.Run("uses china endpoint for china regions", func(t *testing.T) {
		t.Setenv(env.AWSPricingURL, "")

		got := getListPriceURL("AmazonEC2", "cn-north-1")
		want := "https://pricing.cn-north-1.amazonaws.com.cn/offers/v1.0/cn/AmazonEC2/current/cn-north-1/index.json"

		if got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	})
}

func TestPriceListEC2PricePerUnitForCurrency(t *testing.T) {
	tests := []struct {
		name string
		unit PriceListEC2PricePerUnit
		code string
		want string
	}{
		{
			name: "returns CNY when requested and present",
			unit: PriceListEC2PricePerUnit{USD: "1.23", CNY: "8.88"},
			code: "CNY",
			want: "8.88",
		},
		{
			name: "falls back to USD when CNY missing",
			unit: PriceListEC2PricePerUnit{USD: "1.23"},
			code: "CNY",
			want: "1.23",
		},
		{
			name: "handles lowercase currency code",
			unit: PriceListEC2PricePerUnit{USD: "1.23", CNY: "8.88"},
			code: "cny",
			want: "8.88",
		},
		{
			name: "falls back to USD for unknown currency",
			unit: PriceListEC2PricePerUnit{USD: "1.23", CNY: "8.88"},
			code: "EUR",
			want: "1.23",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.unit.ForCurrency(tt.code)
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

// Made with Bob
