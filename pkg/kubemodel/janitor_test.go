package kubemodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// storeFile writes a single zero-byte placeholder at the path the janitor would
// find for a given resolution and window start time.
func storeFile(t *testing.T, store storage.Storage, appName, clusterID string, res time.Duration, start time.Time) string {
	t.Helper()
	resStr := timeutil.FormatStoreResolution(res)
	f, err := pathing.NewKubeModelStoragePathFormatter(appName, clusterID, resStr)
	require.NoError(t, err)

	w := opencost.NewClosedWindow(start, start.Add(res))
	p := f.ToFullPath("", w, "bingen")
	require.NoError(t, store.Write(p, []byte("data")))
	return p
}

// fileExists reports whether path p is present in store.
func fileExists(t *testing.T, store storage.Storage, p string) bool {
	t.Helper()
	ok, err := store.Exists(p)
	require.NoError(t, err)
	return ok
}

// --- parseKubeModelFileTimestamp ---

func TestParseKubeModelFileTimestamp(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "bare timestamp with extension",
			input: "20240115103000.bingen",
			want:  time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:  "timestamp without extension",
			input: "20240115103000",
			want:  time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:    "completely invalid name",
			input:   "notadate.bingen",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseKubeModelFileTimestamp(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// --- retentionFor ---

func TestRetentionFor(t *testing.T) {
	cases := []struct {
		name string
		res  time.Duration
		want time.Duration
	}{
		{"1d resolution uses day retention", timeutil.Day, time.Duration(janitorDefault1dRetention) * timeutil.Day},
		{"1h resolution uses hour retention", time.Hour, time.Duration(janitorDefault1hRetention) * time.Hour},
		{"10m resolution uses 10m retention", 10 * time.Minute, time.Duration(janitorDefault10mRetention) * 10 * time.Minute},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := retentionFor(tc.res)
			require.Equal(t, tc.want, got)
		})
	}
}

// --- pruneResolution / cleanDay ---

func TestJanitor_PrunesExpiredFiles(t *testing.T) {
	store := storage.NewMemoryStorage()
	j := NewJanitor(store, testAppName, testClusterID, []time.Duration{time.Hour})

	now := time.Now().UTC().Truncate(time.Hour)
	cutoff := now.Add(-3 * time.Hour)

	// Two files before the cutoff — should be removed.
	expiredPath1 := storeFile(t, store, testAppName, testClusterID, time.Hour, cutoff.Add(-2*time.Hour))
	expiredPath2 := storeFile(t, store, testAppName, testClusterID, time.Hour, cutoff.Add(-time.Hour))

	// One file after the cutoff — should be kept.
	keptPath := storeFile(t, store, testAppName, testClusterID, time.Hour, cutoff.Add(time.Hour))

	resStr := timeutil.FormatStoreResolution(time.Hour)
	baseDir := fmt.Sprintf("%s/%s/kubemodel/%s", testAppName, testClusterID, resStr)
	j.pruneResolution(baseDir, cutoff)

	require.False(t, fileExists(t, store, expiredPath1), "expired file 1 should be removed")
	require.False(t, fileExists(t, store, expiredPath2), "expired file 2 should be removed")
	require.True(t, fileExists(t, store, keptPath), "recent file should be kept")
}

func TestJanitor_KeepsFilesExactlyAtCutoff(t *testing.T) {
	store := storage.NewMemoryStorage()
	j := NewJanitor(store, testAppName, testClusterID, []time.Duration{time.Hour})

	now := time.Now().UTC().Truncate(time.Hour)
	cutoff := now.Add(-3 * time.Hour)

	// File whose timestamp equals the cutoff exactly — should be kept (not Before).
	atCutoffPath := storeFile(t, store, testAppName, testClusterID, time.Hour, cutoff)

	resStr := timeutil.FormatStoreResolution(time.Hour)
	baseDir := fmt.Sprintf("%s/%s/kubemodel/%s", testAppName, testClusterID, resStr)
	j.pruneResolution(baseDir, cutoff)

	require.True(t, fileExists(t, store, atCutoffPath), "file at cutoff boundary should not be removed")
}

func TestJanitor_EmptyStorageIsNoop(t *testing.T) {
	store := storage.NewMemoryStorage()
	j := NewJanitor(store, testAppName, testClusterID, []time.Duration{time.Hour})

	resStr := timeutil.FormatStoreResolution(time.Hour)
	baseDir := fmt.Sprintf("%s/%s/kubemodel/%s", testAppName, testClusterID, resStr)

	// Should not panic or error on an empty store.
	require.NotPanics(t, func() {
		j.pruneResolution(baseDir, time.Now().UTC())
	})
}

func TestJanitor_FilesWithUnparsableNamesAreSkipped(t *testing.T) {
	store := storage.NewMemoryStorage()
	j := NewJanitor(store, testAppName, testClusterID, []time.Duration{time.Hour})

	now := time.Now().UTC().Truncate(time.Hour)
	cutoff := now.Add(-time.Hour)

	// Write a file at a valid day-dir path but with an unparsable name — janitor
	// should skip it rather than panic or remove it.
	resStr := timeutil.FormatStoreResolution(time.Hour)
	badPath := fmt.Sprintf("%s/%s/kubemodel/%s/%s/garbage.bingen",
		testAppName, testClusterID, resStr,
		cutoff.Add(-24*time.Hour).Format("2006/01/02"),
	)
	require.NoError(t, store.Write(badPath, []byte("data")))

	baseDir := fmt.Sprintf("%s/%s/kubemodel/%s", testAppName, testClusterID, resStr)
	require.NotPanics(t, func() {
		j.pruneResolution(baseDir, cutoff)
	})

	// File with bad name must not have been deleted.
	require.True(t, fileExists(t, store, badPath))
}

// --- Start / Stop idempotency ---

func TestJanitor_StartIsIdempotent(t *testing.T) {
	store := storage.NewMemoryStorage()
	j := NewJanitor(store, testAppName, testClusterID, []time.Duration{time.Hour})

	j.Start(time.Hour)
	j.Start(time.Hour) // second call must not panic or spawn a second goroutine
	j.Stop()
}

func TestJanitor_StopIsIdempotent(t *testing.T) {
	store := storage.NewMemoryStorage()
	j := NewJanitor(store, testAppName, testClusterID, []time.Duration{time.Hour})

	j.Start(time.Hour)
	j.Stop()
	j.Stop() // second stop must not panic (closing a closed channel would panic)
}
