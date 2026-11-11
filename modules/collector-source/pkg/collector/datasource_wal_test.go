package collector

import (
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
)

var _ source.WALStatusProvider = (*collectorDataSource)(nil)

func TestWALDiagnosticDetails(t *testing.T) {
	healthy := source.WALStatus{Enabled: true, LastExportSuccess: time.Now(), RestoreCompleted: true, RestoreObjectsSeen: 3, RestoreObjectsApplied: 3}
	details, err := walDiagnosticDetails(healthy)
	if err != nil {
		t.Fatalf("expected healthy wal to pass, got %s", err)
	}
	if details["restoreObjectsApplied"] != 3 {
		t.Errorf("unexpected details: %v", details)
	}

	failing := healthy
	failing.ConsecutiveExportFailures = 4
	failing.LastExportError = "503"
	failing.RestoreErrors = 1
	failing.RestoreListError = "denied"
	_, err = walDiagnosticDetails(failing)
	if err == nil {
		t.Fatalf("expected failing wal to report an error")
	}
	for _, want := range []string{"4 consecutive write failures", "could not list", "failed to read 1 of 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected %q in %q", want, err)
		}
	}
}

func TestCollectorDataSource_WALStatusWithoutWAL(t *testing.T) {
	c := &collectorDataSource{}
	if c.WALStatus().Enabled {
		t.Errorf("expected disabled status without a wal")
	}
}
