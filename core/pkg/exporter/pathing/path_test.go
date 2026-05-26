package pathing

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/stretchr/testify/require"
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
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1704110400-1704114000", DefaultRootDir, BaseStorageDir),
		},
		{
			name:       "with resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1h/1704110400-1704114000", DefaultRootDir, BaseStorageDir),
		},
		{
			name:       "no resolution with prefix",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: nil,
			prefix:     "test",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/test.1704110400-1704114000", DefaultRootDir, BaseStorageDir),
		},
		{
			name:       "with resolution with prefix",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{1 * time.Hour}[0],
			prefix:     "test",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1h/test.1704110400-1704114000", DefaultRootDir, BaseStorageDir),
		},
		{
			name:       "daily resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{24 * time.Hour}[0],
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1d/1704110400-1704196800", DefaultRootDir, BaseStorageDir),
		},
		{
			name:       "weekly resolution",
			clusterID:  "cluster-a",
			pipeline:   "allocation",
			resolution: &[]time.Duration{7 * 24 * time.Hour}[0],
			prefix:     "",
			expected:   fmt.Sprintf("%s/cluster-a/%s/allocation/1w/1704110400-1704715200", DefaultRootDir, BaseStorageDir),
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

func TestKubeModelPathFormatter(t *testing.T) {
	type testCase struct {
		name       string
		start      time.Time
		rootDir    string
		clusterID  string
		resolution string
		prefix     string
		exp        string
	}

	rootDir := "/path/to/root"

	testCases := []testCase{
		{
			name:       "10m no prefix",
			start:      time.Date(2025, time.December, 15, 12, 0, 0, 0, time.UTC),
			rootDir:    rootDir,
			clusterID:  "96d1c1d0-2183-416c-b8f7-754f42fd461a",
			resolution: "10m",
			prefix:     "",
			exp:        fmt.Sprintf("%s/96d1c1d0-2183-416c-b8f7-754f42fd461a/kubemodel/%s/%s/%s", rootDir, "10m", "2025/12/15", "20251215120000"),
		},
		{
			name:       "1h no prefix",
			start:      time.Date(2025, time.December, 15, 12, 0, 0, 0, time.UTC),
			rootDir:    rootDir,
			clusterID:  "96d1c1d0-2183-416c-b8f7-754f42fd461a",
			resolution: "1h",
			prefix:     "",
			exp:        fmt.Sprintf("%s/96d1c1d0-2183-416c-b8f7-754f42fd461a/kubemodel/%s/%s/%s", rootDir, "1h", "2025/12/15", "20251215120000"),
		},
		{
			name:       "1d no prefix",
			start:      time.Date(2025, time.December, 15, 12, 0, 0, 0, time.UTC),
			rootDir:    rootDir,
			clusterID:  "96d1c1d0-2183-416c-b8f7-754f42fd461a",
			resolution: "1d",
			prefix:     "",
			exp:        fmt.Sprintf("%s/96d1c1d0-2183-416c-b8f7-754f42fd461a/kubemodel/%s/%s/%s", rootDir, "1d", "2025/12/15", "20251215120000"),
		},
		{
			name:       "1d prefix",
			start:      time.Date(2025, time.December, 15, 12, 0, 0, 0, time.UTC),
			rootDir:    rootDir,
			clusterID:  "96d1c1d0-2183-416c-b8f7-754f42fd461a",
			resolution: "1d",
			prefix:     "pre",
			exp:        fmt.Sprintf("%s/96d1c1d0-2183-416c-b8f7-754f42fd461a/kubemodel/%s/%s/%s", rootDir, "1d", "2025/12/15", "pre.20251215120000"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pathing, err := NewKubeModelStoragePathFormatter(tc.rootDir, tc.clusterID, tc.resolution)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var dur time.Duration
			switch tc.resolution {
			case "10m":
				dur = 10 * time.Minute
			case "1h":
				dur = time.Hour
			case "1d":
				dur = 24 * time.Hour
			default:
				t.Errorf("unexpected resolution: %s", tc.resolution)
			}
			end := tc.start.Add(dur)

			// dir := pathing.Dir()

			act := pathing.ToFullPath(tc.prefix, opencost.NewClosedWindow(tc.start, end), "")
			require.Equal(t, tc.exp, act)
		})
	}
}
