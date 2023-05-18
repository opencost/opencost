package aws

import (
	"os"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/util/json"
	"github.com/opencost/opencost/pkg/util/timeutil"
)

func GetCloudCost_Test(t *testing.T) {
	athenaConfigPath := os.Getenv("ATHENA_CONFIGURATION")
	if athenaConfigPath == "" {
		t.Skip("skipping integration test, set environment variable ATHENA_CONFIGURATION")
	}
	athenaConfigBin, err := os.ReadFile(athenaConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var athenaConfig AthenaConfiguration
	err = json.Unmarshal(athenaConfigBin, &athenaConfig)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}
	testCases := map[string]struct {
		integration *AthenaIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{
		// No CUR data is expected within 2 days of now
		"too_recent_window": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: athenaConfig,
				},
			},
			end:      time.Now(),
			start:    time.Now().Add(-timeutil.Day),
			expected: true,
		},
		// CUR data should be available
		"last week window": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: athenaConfig,
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
