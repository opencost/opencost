package pathing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewStaticFileStoragePathFormatter(t *testing.T) {
	tests := []struct {
		name     string
		rootDir  string
		pipeline string
		wantErr  bool
	}{
		{"empty rootDir returns error", "", "pricingmodel", true},
		{"empty pipeline returns error", "cloud-agent", "", true},
		{"valid args returns formatter", "cloud-agent", "pricingmodel", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewStaticFileStoragePathFormatter(tt.rootDir, tt.pipeline)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, f)
			} else {
				require.NoError(t, err)
				require.NotNil(t, f)
			}
		})
	}
}

func TestStaticFileStoragePathFormatter_Dir(t *testing.T) {
	tests := []struct {
		rootDir  string
		pipeline string
		expected string
	}{
		{"cloud-agent", "pricingmodel", "cloud-agent/pricingmodel"},
		{"/var/data", "kubemodel", "/var/data/kubemodel"},
		{"root", "a/b/c", "root/a/b/c"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			f, err := NewStaticFileStoragePathFormatter(tt.rootDir, tt.pipeline)
			require.NoError(t, err)
			require.Equal(t, tt.expected, f.Dir())
		})
	}
}

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
		{
			name:     "in with subdir no prefix no ext",
			rootDir:  "cloud-agent",
			pipeline: "pricingmodel",
			prefix:   "",
			in:       "us-east-1/aws_list_pricing_api",
			fileExt:  "",
			expected: "cloud-agent/pricingmodel/us-east-1/aws_list_pricing_api",
		},
		{
			name:     "in with subdir with prefix and ext",
			rootDir:  "cloud-agent",
			pipeline: "pricingmodel",
			prefix:   "v1",
			in:       "us-east-1/aws_list_pricing_api",
			fileExt:  "bin",
			expected: "cloud-agent/pricingmodel/us-east-1/v1.aws_list_pricing_api.bin",
		},
		{
			name:     "in with nested subdir with ext",
			rootDir:  "/var/data",
			pipeline: "kubemodel",
			prefix:   "",
			in:       "2024/01/nodes",
			fileExt:  "bin",
			expected: "/var/data/kubemodel/2024/01/nodes.bin",
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
