package cloudcost

import (
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/pkg/cloud"
)

type fakeIntegration struct{}

func (fakeIntegration) GetCloudCost(time.Time, time.Time) (*opencost.CloudCostSetRange, error) {
	return nil, nil
}
func (fakeIntegration) GetStatus() cloud.ConnectionStatus     { return cloud.InitialStatus }
func (fakeIntegration) RefreshStatus() cloud.ConnectionStatus { return cloud.InitialStatus }

// TestIngestor_Status_ConcurrentWithCoverageWrite guards against a data race on
// the coverage window: Status() read it without holding coverageLock while the
// build loop reassigns it under the lock. Run with -race to detect it.
func TestIngestor_Status_ConcurrentWithCoverageWrite(t *testing.T) {
	now := time.Now().UTC()
	ing := &ingestor{
		integration: fakeIntegration{},
		coverage:    opencost.NewClosedWindow(now, now.Add(time.Hour)),
	}

	window := opencost.NewClosedWindow(now, now.Add(2*time.Hour))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			ing.expandCoverage(window)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			_ = ing.Status()
		}
	}()
	wg.Wait()
}

func TestIngestor_runStart(t *testing.T) {
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

	tests := map[string]struct {
		lastRun     time.Time
		retention   int
		monthToDate bool
		want        time.Time
	}{
		"regular run starts RunWindow back": {
			lastRun: day(2026, 9, 30).Add(16 * time.Hour), retention: 15, want: day(2026, 9, 27),
		},
		"month-to-date within retention starts on the 1st": {
			lastRun: day(2026, 9, 20).Add(16 * time.Hour), retention: 30, monthToDate: true, want: day(2026, 9, 1),
		},
		"month-to-date at month end is clamped to retention": {
			lastRun: day(2026, 9, 30).Add(16 * time.Hour), retention: 15, monthToDate: true, want: day(2026, 9, 15),
		},
		"month-to-date early in the month is clamped instead of rebuilding last month": {
			lastRun: day(2026, 10, 2).Add(16 * time.Hour), retention: 30, monthToDate: true, want: day(2026, 9, 2),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ing := &ingestor{
				lastRun: tt.lastRun,
				config: IngestorConfig{
					Resolution: timeutil.Day,
					RunWindow:  3 * timeutil.Day,
					Duration:   time.Duration(tt.retention) * timeutil.Day,
				},
			}
			limit := opencost.RoundBack(tt.lastRun, timeutil.Day).Add(-ing.config.Duration)
			if got := ing.runStart(limit, tt.monthToDate); !got.Equal(tt.want) {
				t.Errorf("runStart() = %s, want %s", got, tt.want)
			}
		})
	}
}
