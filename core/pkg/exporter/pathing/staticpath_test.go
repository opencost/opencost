package pathing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticFilePathFormatter(t *testing.T) {
	type testCase struct {
		name     string
		rootDir  string
		pipeline string
		prefix   string
		in       string
		fileExt  string
		expected string
	}

	testCases := []testCase{
		{
			name:     "no prefix no ext",
			rootDir:  "cloud-agent",
			pipeline: "pricingmodel",
			prefix:   "",
			in:       "aws_list_pricing_api",
			fileExt:  "",
			expected: "cloud-agent/pricingmodel/aws_list_pricing_api",
		},
		{
			name:     "no prefix with ext",
			rootDir:  "cloud-agent",
			pipeline: "pricingmodel",
			prefix:   "",
			in:       "aws_list_pricing_api",
			fileExt:  "bin",
			expected: "cloud-agent/pricingmodel/aws_list_pricing_api.bin",
		},
		{
			name:     "with prefix with ext",
			rootDir:  "cloud-agent",
			pipeline: "pricingmodel",
			prefix:   "v1",
			in:       "aws_list_pricing_api",
			fileExt:  "bin",
			expected: "cloud-agent/pricingmodel/v1.aws_list_pricing_api.bin",
		},
		{
			name:     "with prefix no ext",
			rootDir:  "cloud-agent",
			pipeline: "pricingmodel",
			prefix:   "v1",
			in:       "aws_list_pricing_api",
			fileExt:  "",
			expected: "cloud-agent/pricingmodel/v1.aws_list_pricing_api",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathing, err := NewStaticFileStoragePathFormatter(tc.rootDir, tc.pipeline)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			result := pathing.ToFullPath(tc.prefix, tc.in, tc.fileExt)
			require.Equal(t, tc.expected, result)
		})
	}
}
