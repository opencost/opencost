package azure

import (
	"testing"
)

func TestParseAzureSubscriptionID(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "azure:///subscriptions/0badafdf-1234-abcd-wxyz-123456789/...",
			expected: "0badafdf-1234-abcd-wxyz-123456789",
		},
		{
			input:    "azure:/subscriptions/0badafdf-1234-abcd-wxyz-123456789/...",
			expected: "",
		},
		{
			input:    "azure:///subscriptions//",
			expected: "",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, test := range cases {
		result := ParseAzureSubscriptionID(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}
