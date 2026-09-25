package metric

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

// flakyStorage wraps a MemoryStorage and fails writes, reads or lists while the corresponding flag is set.
type flakyStorage struct {
	*storage.MemoryStorage

	mu        sync.Mutex
	failWrite bool
	failRead  bool
	failList  bool
}

var errBucketUnavailable = errors.New(`Put "https://bucket.s3.amazonaws.com/wal?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=deadbeef": 503 Service Unavailable`)

func (fs *flakyStorage) set(write, read, list bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.failWrite, fs.failRead, fs.failList = write, read, list
}

func (fs *flakyStorage) Write(path string, data []byte) error {
	fs.mu.Lock()
	fail := fs.failWrite
	fs.mu.Unlock()
	if fail {
		return errBucketUnavailable
	}
	return fs.MemoryStorage.Write(path, data)
}

func (fs *flakyStorage) Read(path string) ([]byte, error) {
	fs.mu.Lock()
	fail := fs.failRead
	fs.mu.Unlock()
	if fail {
		return nil, errBucketUnavailable
	}
	return fs.MemoryStorage.Read(path)
}

func (fs *flakyStorage) List(path string) ([]*storage.StorageInfo, error) {
	fs.mu.Lock()
	fail := fs.failList
	fs.mu.Unlock()
	if fail {
		return nil, errBucketUnavailable
	}
	return fs.MemoryStorage.List(path)
}

func newTestWalinator(t *testing.T, store storage.Storage) *Walinator {
	t.Helper()
	res1d, err := util.NewResolution(util.ResolutionConfiguration{Interval: "1d", Retention: 3})
	if err != nil {
		t.Fatalf("failed to create resolution: %s", err)
	}
	resolutions := []*util.Resolution{res1d}
	wal, err := NewWalinator("test", "test", store, resolutions, NewMetricRepository(resolutions, testMetricCollector))
	if err != nil {
		t.Fatalf("failed to create walinator: %s", err)
	}
	return wal
}

func testUpdateSet(ts time.Time) *UpdateSet {
	return &UpdateSet{
		Timestamp: ts,
		Updates: []Update{{
			Name:   TestMetric,
			Labels: map[string]string{"test": "test"},
			Value:  1,
		}},
	}
}

// TestWalinator_ExportOutageIsObservable reproduces F-34: WAL writes fail for a period, then the
// process restarts. Both the write failures and the resulting hole in restored history must be
// visible through Status(), not only in logs.
func TestWalinator_ExportOutageIsObservable(t *testing.T) {
	store := &flakyStorage{MemoryStorage: storage.NewMemoryStorage()}
	wal := newTestWalinator(t, store)

	base := time.Now().UTC().Truncate(timeutil.Day).Add(-12 * time.Hour)
	scrape := 30 * time.Second

	// 10 good scrapes, 20 failed scrapes (a 10 minute outage), 10 good scrapes
	ts := base
	for i := 0; i < 10; i++ {
		wal.Update(testUpdateSet(ts))
		ts = ts.Add(scrape)
	}
	outageStart := ts.Add(-scrape)

	store.set(true, false, false)
	for i := 0; i < 20; i++ {
		wal.Update(testUpdateSet(ts))
		ts = ts.Add(scrape)
	}

	status := wal.Status()
	if !status.Enabled {
		t.Errorf("expected wal status to be enabled")
	}
	if status.ConsecutiveExportFailures != 20 || status.ExportFailuresTotal != 20 {
		t.Errorf("expected 20 consecutive and total export failures, got %d and %d", status.ConsecutiveExportFailures, status.ExportFailuresTotal)
	}
	if status.LastExportErrorAt.IsZero() || status.LastExportError == "" {
		t.Errorf("expected last export error to be recorded, got %+v", status)
	}
	if strings.Contains(status.LastExportError, "AKIAEXAMPLE") || strings.Contains(status.LastExportError, "X-Amz-Signature") {
		t.Errorf("last export error leaks credentials: %q", status.LastExportError)
	}

	store.set(false, false, false)
	for i := 0; i < 10; i++ {
		wal.Update(testUpdateSet(ts))
		ts = ts.Add(scrape)
	}

	status = wal.Status()
	if status.ConsecutiveExportFailures != 0 {
		t.Errorf("expected consecutive failures to reset after recovery, got %d", status.ConsecutiveExportFailures)
	}
	if status.ExportFailuresTotal != 20 {
		t.Errorf("expected total failures to be retained after recovery, got %d", status.ExportFailuresTotal)
	}
	if status.LastExportSuccess.Before(status.LastExportErrorAt) {
		t.Errorf("expected last success after last error once recovered")
	}

	// restart: a new walinator over the same storage
	restarted := newTestWalinator(t, store)
	restarted.restore()

	rs := restarted.Status()
	if !rs.RestoreCompleted {
		t.Fatalf("expected restore to be completed")
	}
	if rs.RestoreObjectsSeen != 20 || rs.RestoreObjectsApplied != 20 || rs.RestoreErrors != 0 {
		t.Errorf("expected 20 objects seen and applied with no errors, got seen=%d applied=%d errors=%d",
			rs.RestoreObjectsSeen, rs.RestoreObjectsApplied, rs.RestoreErrors)
	}
	wantGap := 21 * scrape
	if rs.RestoreLargestGap != wantGap {
		t.Errorf("expected largest restored gap %s, got %s", wantGap, rs.RestoreLargestGap)
	}
	if !rs.RestoreLargestGapStart.Equal(outageStart) {
		t.Errorf("expected largest gap to start at %s, got %s", outageStart, rs.RestoreLargestGapStart)
	}
	if !rs.RestoreOldest.Equal(base) || !rs.RestoreNewest.Equal(ts.Add(-scrape)) {
		t.Errorf("unexpected restored range %s - %s", rs.RestoreOldest, rs.RestoreNewest)
	}
}

// TestWalinator_RestoreFailuresAreObservable covers restore errors that were previously log-only:
// unreadable objects and an unlistable bucket.
func TestWalinator_RestoreFailuresAreObservable(t *testing.T) {
	store := &flakyStorage{MemoryStorage: storage.NewMemoryStorage()}
	wal := newTestWalinator(t, store)

	base := time.Now().UTC().Truncate(timeutil.Day).Add(-12 * time.Hour)
	for i := 0; i < 5; i++ {
		wal.Update(testUpdateSet(base.Add(time.Duration(i) * time.Minute)))
	}

	t.Run("unreadable objects", func(t *testing.T) {
		store.set(false, true, false)
		defer store.set(false, false, false)

		restarted := newTestWalinator(t, store)
		restarted.restore()

		rs := restarted.Status()
		if !rs.RestoreCompleted || rs.RestoreObjectsSeen != 5 || rs.RestoreObjectsApplied != 0 || rs.RestoreErrors != 5 {
			t.Errorf("expected 5 seen, 0 applied, 5 errors; got %+v", rs)
		}
	})

	t.Run("unlistable bucket", func(t *testing.T) {
		store.set(false, false, true)
		defer store.set(false, false, false)

		restarted := newTestWalinator(t, store)
		restarted.restore()

		rs := restarted.Status()
		if !rs.RestoreCompleted || rs.RestoreListError == "" {
			t.Errorf("expected list error to be reported, got %+v", rs)
		}
		if strings.Contains(rs.RestoreListError, "AKIAEXAMPLE") {
			t.Errorf("restore list error leaks credentials: %q", rs.RestoreListError)
		}
	})
}
