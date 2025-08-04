package pathing

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestBingenPathFormatter(t *testing.T) {
	type testCase struct {
		name       string
		clusterID  string
		pipeline   string
		resolution *time.Duration
		prefix     string
		expected   string
	}

	testCases := []testCase{
		{
			name:       "no resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1704110400-1704114000", defaultRootDir, baseStorageDir),
		},
		{
			name:       "with resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1h/1704110400-1704114000", defaultRootDir, baseStorageDir),
		},
		{
			name:       "no resolution with prefix",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "test",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/test.1704110400-1704114000", defaultRootDir, baseStorageDir),
		},
		{
			name:       "with resolution with prefix",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "test",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1h/test.1704110400-1704114000", defaultRootDir, baseStorageDir),
		},
		{
			name:       "daily resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{24 * time.Hour}[0],
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1d/1704110400-1704196800", defaultRootDir, baseStorageDir),
		},
		{
			name:       "weekly resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{7 * 24 * time.Hour}[0],
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1w/1704110400-1704715200", defaultRootDir, baseStorageDir),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathing, err := NewDefaultStoragePathFormatter(tc.clusterID, tc.pipeline, tc.resolution)
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
			rootPath:  "/tmp/root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "",
			fileExt:   "json",
			expected:  "/tmp/root/cluster-a/heartbeat/20240101124000.json",
		},
		{
			name:      "with file extension",
			rootPath:  "root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "",
			fileExt:   "json",
			expected:  "root/cluster-a/heartbeat/20240101124000.json",
		},
		{
			name:      "with root path with file extension with sub-paths",
			rootPath:  "/tmp/root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{"foo", "bar"},
			prefix:    "",
			fileExt:   "json",
			expected:  "/tmp/root/cluster-a/heartbeat/foo/bar/20240101124000.json",
		},
		{
			name:      "without file extension",
			rootPath:  "root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "",
			fileExt:   "",
			expected:  "root/cluster-a/heartbeat/20240101124000",
		},
		{
			name:      "with prefix with file extension",
			rootPath:  "root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "test",
			fileExt:   "json",
			expected:  "root/cluster-a/heartbeat/test.20240101124000.json",
		},
		{
			name:      "with prefix with file extension with sub-paths",
			rootPath:  "root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{"foo", "bar", "baz"},
			prefix:    "test",
			fileExt:   "json",
			expected:  "root/cluster-a/heartbeat/foo/bar/baz/test.20240101124000.json",
		},
		{
			name:      "with prefix without file extension",
			rootPath:  "root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{},
			prefix:    "test",
			fileExt:   "",
			expected:  "root/cluster-a/heartbeat/test.20240101124000",
		},
		{
			name:      "with prefix without file extension with sub-paths",
			rootPath:  "root",
			clusterID: "cluster-a",
			event:     "heartbeat",
			subPaths:  []string{"foo"},
			prefix:    "test",
			fileExt:   "",
			expected:  "root/cluster-a/heartbeat/foo/test.20240101124000",
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
