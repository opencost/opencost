package pathing

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
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
			rootPath:   "federated",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1704110400-1704114000",
		},
		{
			name:       "with resolution",
			rootPath:   "federated",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1h/1704110400-1704114000",
		},
		{
			name:       "no resolution with prefix",
			rootPath:   "federated",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "test",
			expected:   "federated/cluster-a/etl/bingen/allocation/test.1704110400-1704114000",
		},
		{
			name:       "with resolution with prefix",
			rootPath:   "federated",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "test",
			expected:   "federated/cluster-a/etl/bingen/allocation/1h/test.1704110400-1704114000",
		},
		{
			name:       "daily resolution",
			rootPath:   "federated",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{24 * time.Hour}[0],
			prefix:     "",
			expected:   "federated/cluster-a/etl/bingen/allocation/1d/1704110400-1704196800",
		},
		{
			name:       "weekly resolution",
			rootPath:   "federated",
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

			result := pathing.ToFullPath(tc.prefix, opencost.NewClosedWindow(start, end), "")
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

func TestEventPathFormatter(t *testing.T) {
	type testCase struct {
		name      string
		rootPath  string
		clusterID string
		event     string
		subPaths  []string
		prefix    string
		fileExt   string
		expected  string
	}

	testCases := []testCase{
		{
			name:      "with root path with file extension",
			rootPath:  "/tmp/federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "",
			fileExt:   "json",
			expected:  "/tmp/federated/cluster-a/heartbeat/20240101124000.json",
		},
		{
			name:      "with file extension",
			rootPath:  "federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "",
			fileExt:   "json",
			expected:  "federated/cluster-a/heartbeat/20240101124000.json",
		},
		{
			name:      "with root path with file extension with sub-paths",
			rootPath:  "/tmp/federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{"foo", "bar"},
			prefix:    "",
			fileExt:   "json",
			expected:  "/tmp/federated/cluster-a/heartbeat/foo/bar/20240101124000.json",
		},
		{
			name:      "without file extension",
			rootPath:  "federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "",
			fileExt:   "",
			expected:  "federated/cluster-a/heartbeat/20240101124000",
		},
		{
			name:      "with prefix with file extension",
			rootPath:  "federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "test",
			fileExt:   "json",
			expected:  "federated/cluster-a/heartbeat/test.20240101124000.json",
		},
		{
			name:      "with prefix with file extension with sub-paths",
			rootPath:  "federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{"foo", "bar", "baz"},
			prefix:    "test",
			fileExt:   "json",
			expected:  "federated/cluster-a/heartbeat/foo/bar/baz/test.20240101124000.json",
		},
		{
			name:      "with prefix without file extension",
			rootPath:  "federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "test",
			fileExt:   "",
			expected:  "federated/cluster-a/heartbeat/test.20240101124000",
		},
		{
			name:      "with prefix without file extension with sub-paths",
			rootPath:  "federated",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{"foo"},
			prefix:    "test",
			fileExt:   "",
			expected:  "federated/cluster-a/heartbeat/foo/test.20240101124000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathing, err := NewEventStoragePathFormatter(tc.rootPath, tc.clusterID, tc.event, tc.subPaths...)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			timestamp := time.Date(2024, 1, 1, 12, 40, 0, 0, time.UTC)

			result := pathing.ToFullPath(tc.prefix, timestamp, tc.fileExt)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}
