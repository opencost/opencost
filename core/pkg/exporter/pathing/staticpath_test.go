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
			rootDir:  "federated",
			pipeline: "pricing",
			prefix:   "",
			in:       "aws/public_api_ec2_pricing",
			fileExt:  "",
			expected: "federated/pricing/aws/public_api_ec2_pricing",
		},
		{
			name:     "no prefix with ext",
			rootDir:  "federated",
			pipeline: "pricing",
			prefix:   "",
			in:       "aws/public_api_ec2_pricing",
			fileExt:  "bin",
			expected: "federated/pricing/aws/public_api_ec2_pricing.bin",
		},
		{
			name:     "with prefix with ext",
			rootDir:  "federated",
			pipeline: "pricing",
			prefix:   "v1",
			in:       "aws/public_api_ec2_pricing",
			fileExt:  "bin",
			expected: "federated/pricing/aws/v1.public_api_ec2_pricing.bin",
		},
		{
			name:     "with prefix no ext",
			rootDir:  "federated",
			pipeline: "pricing",
			prefix:   "v1",
			in:       "aws/public_api_ec2_pricing",
			fileExt:  "",
			expected: "federated/pricing/aws/v1.public_api_ec2_pricing",
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
