package kubemodel

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	coremodel "github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const (
	testAppName   = "test-app"
	testClusterID = "test-cluster"
)

// storeKMS marshals kms and writes it to store at the path the querier expects for the given window.
func storeKMS(t *testing.T, store storage.Storage, appName, clusterID string, res time.Duration, w opencost.Window) {
	t.Helper()
	resStr := timeutil.FormatStoreResolution(res)
	f, err := pathing.NewKubeModelStoragePathFormatter(appName, clusterID, resStr)
	require.NoError(t, err)

	p := f.ToFullPath("", w, exporter.BingenExt)
	kms := coremodel.NewMockKubeModelSet(*w.Start(), *w.End())
	data, err := kms.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, store.Write(p, data))
}

// --- snapResolution ---

func TestSnapResolution(t *testing.T) {
	cases := []struct {
		name     string
		window   opencost.Window
		expected time.Duration
	}{
		{
			name:     "1h window snaps to 1h",
			window:   opencost.NewClosedWindow(time.Time{}, time.Time{}.Add(time.Hour)),
			expected: time.Hour,
		},
		{
			name:     "3h window snaps to 1h",
			window:   opencost.NewClosedWindow(time.Time{}, time.Time{}.Add(3*time.Hour)),
			expected: time.Hour,
		},
		{
			name:     "12h window snaps to 1h",
			window:   opencost.NewClosedWindow(time.Time{}, time.Time{}.Add(12*time.Hour)),
			expected: time.Hour,
		},
		{
			name:     "24h window snaps to 1d",
			window:   opencost.NewClosedWindow(time.Time{}, time.Time{}.Add(timeutil.Day)),
			expected: timeutil.Day,
		},
		{
			name:     "48h window snaps to 1d",
			window:   opencost.NewClosedWindow(time.Time{}, time.Time{}.Add(2*timeutil.Day)),
			expected: timeutil.Day,
		},
		{
			name:     "90m window (not evenly divisible) falls back to 1h",
			window:   opencost.NewClosedWindow(time.Time{}, time.Time{}.Add(90*time.Minute)),
			expected: time.Hour,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := snapResolution(tc.window)
			if got != tc.expected {
				t.Errorf("snapResolution(%v) = %v, want %v", tc.window.Duration(), got, tc.expected)
			}
		})
	}
}

// --- Querier.Query ---

func TestQuerier_Query_RejectsOpenWindow(t *testing.T) {
	q := NewQuerier(testAppName, testClusterID, storage.NewMemoryStorage())

	end := time.Now().UTC()
	_, err := q.Query(opencost.NewWindow(nil, &end))
	require.Error(t, err, "nil start should be rejected")

	start := time.Now().UTC()
	_, err = q.Query(opencost.NewWindow(&start, nil))
	require.Error(t, err, "nil end should be rejected")
}

func TestQuerier_Query_EmptyStorageReturnsNoResults(t *testing.T) {
	q := NewQuerier(testAppName, testClusterID, storage.NewMemoryStorage())

	start := time.Now().UTC().Truncate(time.Hour)
	results, err := q.Query(opencost.NewClosedWindow(start, start.Add(3*time.Hour)))
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestQuerier_Query_SingleHourlyWindow(t *testing.T) {
	store := storage.NewMemoryStorage()
	q := NewQuerier(testAppName, testClusterID, store)

	start := time.Now().UTC().Truncate(time.Hour)
	window := opencost.NewClosedWindow(start, start.Add(time.Hour))
	storeKMS(t, store, testAppName, testClusterID, time.Hour, window)

	results, err := q.Query(window)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestQuerier_Query_MultipleHourlySubWindows(t *testing.T) {
	store := storage.NewMemoryStorage()
	q := NewQuerier(testAppName, testClusterID, store)

	start := time.Now().UTC().Truncate(time.Hour)
	for i := range 3 {
		w := opencost.NewClosedWindow(start.Add(time.Duration(i)*time.Hour), start.Add(time.Duration(i+1)*time.Hour))
		storeKMS(t, store, testAppName, testClusterID, time.Hour, w)
	}

	results, err := q.Query(opencost.NewClosedWindow(start, start.Add(3*time.Hour)))
	require.NoError(t, err)
	require.Len(t, results, 3)
}

func TestQuerier_Query_SkipsMissingSubWindows(t *testing.T) {
	store := storage.NewMemoryStorage()
	q := NewQuerier(testAppName, testClusterID, store)

	start := time.Now().UTC().Truncate(time.Hour)
	// Write first and third sub-windows; leave the middle missing.
	storeKMS(t, store, testAppName, testClusterID, time.Hour, opencost.NewClosedWindow(start, start.Add(time.Hour)))
	storeKMS(t, store, testAppName, testClusterID, time.Hour, opencost.NewClosedWindow(start.Add(2*time.Hour), start.Add(3*time.Hour)))

	results, err := q.Query(opencost.NewClosedWindow(start, start.Add(3*time.Hour)))
	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestQuerier_Query_DailyResolutionForFullDayWindow(t *testing.T) {
	store := storage.NewMemoryStorage()
	q := NewQuerier(testAppName, testClusterID, store)

	start := time.Now().UTC().Truncate(timeutil.Day)
	window := opencost.NewClosedWindow(start, start.Add(timeutil.Day))
	storeKMS(t, store, testAppName, testClusterID, timeutil.Day, window)

	results, err := q.Query(window)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestQuerier_Query_TruncatesWindowToResolution(t *testing.T) {
	store := storage.NewMemoryStorage()
	q := NewQuerier(testAppName, testClusterID, store)

	// Aligned hour boundary for the one sub-window we expect to be queried.
	alignedStart := time.Now().UTC().Truncate(time.Hour)
	alignedWindow := opencost.NewClosedWindow(alignedStart, alignedStart.Add(time.Hour))
	storeKMS(t, store, testAppName, testClusterID, time.Hour, alignedWindow)

	// Query with start/end that are 15 minutes into the hour — both truncate to
	// the same aligned boundary, so we still get the one stored sub-window.
	unalignedStart := alignedStart.Add(15 * time.Minute)
	unalignedEnd := alignedStart.Add(time.Hour + 15*time.Minute)
	results, err := q.Query(opencost.NewClosedWindow(unalignedStart, unalignedEnd))
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestQuerier_Query_CorruptDataReturnsError(t *testing.T) {
	store := storage.NewMemoryStorage()
	q := NewQuerier(testAppName, testClusterID, store)

	start := time.Now().UTC().Truncate(time.Hour)
	window := opencost.NewClosedWindow(start, start.Add(time.Hour))

	resStr := timeutil.FormatStoreResolution(time.Hour)
	f, err := pathing.NewKubeModelStoragePathFormatter(testAppName, testClusterID, resStr)
	require.NoError(t, err)
	p := f.ToFullPath("", window, exporter.BingenExt)
	require.NoError(t, store.Write(p, []byte("not valid binary data")))

	_, err = q.Query(window)
	require.Error(t, err)
}
