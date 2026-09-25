package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeWALStatusProvider struct {
	status source.WALStatus
}

func (f *fakeWALStatusProvider) WALStatus() source.WALStatus { return f.status }

func TestWALStatusCollector(t *testing.T) {
	provider := &fakeWALStatusProvider{status: source.WALStatus{
		Enabled:                   true,
		LastExportSuccess:         time.Unix(1700000000, 0),
		ExportFailuresTotal:       7,
		ConsecutiveExportFailures: 2,
		RestoreCompleted:          true,
		RestoreObjectsApplied:     100,
		RestoreErrors:             3,
		RestoreDuration:           1500 * time.Millisecond,
		RestoreLargestGap:         10 * time.Minute,
	}}
	collector := WALStatusCollector{
		provider:      provider,
		metricsConfig: MetricsConfig{DisabledMetrics: []string{"opencost_wal_restore_duration_seconds"}},
	}

	expected := `
# HELP opencost_wal_consecutive_export_failures Collector WAL writes that have failed since the last successful write.
# TYPE opencost_wal_consecutive_export_failures gauge
opencost_wal_consecutive_export_failures 2
# HELP opencost_wal_export_failures_total Total failed collector WAL writes since start.
# TYPE opencost_wal_export_failures_total counter
opencost_wal_export_failures_total 7
# HELP opencost_wal_last_export_success_timestamp_seconds Unix time of the most recent successful collector WAL write, 0 if none.
# TYPE opencost_wal_last_export_success_timestamp_seconds gauge
opencost_wal_last_export_success_timestamp_seconds 1.7e+09
# HELP opencost_wal_restore_errors_total Collector WAL objects that could not be read or decoded during the startup restore.
# TYPE opencost_wal_restore_errors_total counter
opencost_wal_restore_errors_total 3
# HELP opencost_wal_restore_largest_gap_seconds Largest interval between consecutive restored collector WAL objects. Values well above the scrape interval indicate history that was never persisted or could not be restored.
# TYPE opencost_wal_restore_largest_gap_seconds gauge
opencost_wal_restore_largest_gap_seconds 600
# HELP opencost_wal_restore_list_failed 1 if the collector WAL startup restore could not list stored objects, meaning nothing was restored.
# TYPE opencost_wal_restore_list_failed gauge
opencost_wal_restore_list_failed 0
# HELP opencost_wal_restore_objects_applied Collector WAL objects applied during the startup restore.
# TYPE opencost_wal_restore_objects_applied gauge
opencost_wal_restore_objects_applied 100
# HELP opencost_wal_restore_completed 1 once the collector WAL startup restore has finished, 0 before.
# TYPE opencost_wal_restore_completed gauge
opencost_wal_restore_completed 1
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(expected)); err != nil {
		t.Errorf("unexpected metrics: %s", err)
	}

	provider.status = source.WALStatus{}
	if n := testutil.CollectAndCount(collector); n != 0 {
		t.Errorf("expected no metrics without a wal, got %d", n)
	}
}

type fakeWALDataSource struct {
	source.OpenCostDataSource
	fakeWALStatusProvider
}

// An embedding application that already registered the WAL collector on the default registry must
// not cause InitWALMetrics to panic.
func TestInitWALMetrics_AlreadyRegistered(t *testing.T) {
	existing := WALStatusCollector{provider: &fakeWALStatusProvider{}}
	if err := prometheus.Register(existing); err != nil {
		t.Fatalf("failed to pre-register collector: %s", err)
	}
	t.Cleanup(func() { prometheus.Unregister(existing) })

	ds := &fakeWALDataSource{fakeWALStatusProvider: fakeWALStatusProvider{status: source.WALStatus{Enabled: true}}}
	InitWALMetrics(ds, &MetricsConfig{})
	InitWALMetrics(ds, &MetricsConfig{})
}
