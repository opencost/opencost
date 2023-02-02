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

func TestParseGCPProjectID(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "gce://guestbook-12345/...",
			expected: "guestbook-12345",
		},
		{
			input:    "gce:/guestbook-12345/...",
			expected: "",
		},
		{
			input:    "asdfa",
			expected: "",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, test := range cases {
		result := parseGCPProjectID(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestGetUsageType(t *testing.T) {
	cases := []struct {
		input    map[string]string
		expected string
	}{
		{
			input: map[string]string{
				GKEPreemptibleLabel: "true",
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				GKESpotLabel: "true",
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				KarpenterCapacityTypeLabel: KarpenterCapacitySpotTypeValue,
			},
			expected: "preemptible",
		},
		{
			input: map[string]string{
				"someotherlabel": "true",
			},
			expected: "ondemand",
		},
		{
			input:    map[string]string{},
			expected: "ondemand",
		},
	}

	for _, test := range cases {
		result := getUsageType(test.input)
		if result != test.expected {
			t.Errorf("Input: %v, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}
