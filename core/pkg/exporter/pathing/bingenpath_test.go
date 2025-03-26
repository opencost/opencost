package pathing

import (
	"testing"
	"time"
)

func TestBingenPathFormatter(t *testing.T) {
	type testCase struct {
		name       string
		rootPath   string
		clusterID  string
		pipeline   string
		resolution *time.Duration
		prefix     string
		expected   string
	}

	testCases := []testCase{
		{
			name:       "no resolution",
			rootPath:   "",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1704110400-1704114000",
		},
		{
			name:       "with resolution",
			rootPath:   "",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1h/1704110400-1704114000",
		},
		{
			name:       "no resolution with prefix",
			rootPath:   "",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "test",
			expected:   "federated/cluster-a/etl/bingen/allocation/test.1704110400-1704114000",
		},
		{
			name:       "with resolution with prefix",
			rootPath:   "",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "test",
			expected:   "federated/cluster-a/etl/bingen/allocation/1h/test.1704110400-1704114000",
		},
		{
			name:       "daily resolution",
			rootPath:   "",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{24 * time.Hour}[0],
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1d/1704110400-1704196800",
		},
		{
			name:       "weekly resolution",
			rootPath:   "",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{7 * 24 * time.Hour}[0],
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1w/1704110400-1704715200",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathing, err := NewBingenStoragePathFormatter(tc.rootPath, tc.clusterID, tc.pipeline, tc.resolution)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
			end := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
			if tc.resolution != nil {
				end = start.Add(*tc.resolution)
			}

			result := pathing.ToFullPath(tc.prefix, start, end)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}
