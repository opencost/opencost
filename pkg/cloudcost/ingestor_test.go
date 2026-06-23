package cloudcost

import (
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
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
