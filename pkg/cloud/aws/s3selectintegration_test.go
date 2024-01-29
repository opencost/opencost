package aws

import (
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestS3Integration_GetCloudCost(t *testing.T) {
	s3ConfigPath := os.Getenv("S3_CONFIGURATION")
	if s3ConfigPath == "" {
		t.Skip("skipping integration test, set environment variable S3_CONFIGURATION")
	}
	s3ConfigBin, err := os.ReadFile(s3ConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var s3Config S3Configuration
	err = json.Unmarshal(s3ConfigBin, &s3Config)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}
	testCases := map[string]struct {
		integration *S3SelectIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{
		// No CUR data is expected within 2 days of now
		"too_recent_window": {
			integration: &S3SelectIntegration{
				S3SelectQuerier: S3SelectQuerier{
					S3Connection: S3Connection{
						S3Configuration: s3Config,
					},
				},
			},
			end:      time.Now(),
			start:    time.Now().Add(-timeutil.Day),
			expected: true,
		},
		// CUR data should be available
		"last week window": {
			integration: &S3SelectIntegration{
				S3SelectQuerier: S3SelectQuerier{
					S3Connection: S3Connection{
						S3Configuration: s3Config,
					},
				},
			},
			end:      time.Now().Add(-7 * timeutil.Day),
			start:    time.Now().Add(-8 * timeutil.Day),
			expected: false,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := testCase.integration.GetCloudCost(testCase.start, testCase.end)
			if err != nil {
				t.Errorf("Other error during testing %s", err)
			} else if actual.IsEmpty() != testCase.expected {
				t.Errorf("Incorrect result, actual emptiness: %t, expected: %t", actual.IsEmpty(), testCase.expected)
			}
		})
	}
}
