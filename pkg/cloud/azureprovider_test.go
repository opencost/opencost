package cloud

import (
	"fmt"
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
		result := parseAzureSubscriptionID(test.input)
		if result != test.expected {
			t.Errorf("Input: %s, Expected: %s, Actual: %s", test.input, test.expected, result)
		}
	}
}

func TestGetDisks(t *testing.T) {

	azure := &Azure{
		Config: &ProviderConfig{
			customPricing: &CustomPricing{
				AzureSubscriptionID: "0bd50fdf-c923-4e1e-850c-196dd3dcc5d3", //!Dont publish this
			},
		},
	}

	// cases := map[string]struct {
	// 	x string
	// }{
	// 	"test": {
	// 		x: "hi",
	// 	},
	// }

	result, err := azure.GetDisks()
	if err != nil {
		fmt.Printf("%s", err)
	}
	fmt.Printf("%v", result)

}
