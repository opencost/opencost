package cloud

import (
	"testing"
)

func TestParseGCPInstanceTypeLabel(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "n1-standard-2",
			expected: "n1standard",
		},
		{
			input:    "e2-medium",
			expected: "e2medium",
		},
		{
			input:    "k3s",
			expected: "unknown",
		},
		{
			input:    "custom-n1-standard-2",
			expected: "custom",
		},
	}

	for _, test := range cases {
		result := parseGCPInstanceTypeLabel(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}
