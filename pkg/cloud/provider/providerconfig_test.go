package provider

import (
	"fmt"
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
)

func newTestProviderConfig(t *testing.T) *ProviderConfig {
	t.Helper()
	// Memory storage: file storage cannot write the config on Windows
	// (WriteLocked is not implemented there), and these tests are about the
	// copy-on-write publication, not the file.
	return NewProviderConfig(config.NewConfigFileManager(storage.NewMemoryStorage()), "pricing.json")
}

// TestProviderConfig_UpdateIsCopyOnWrite checks that Update publishes a new
// value instead of writing the one GetCustomPricingData already handed out,
// which readers use without holding the config's lock.
func TestProviderConfig_UpdateIsCopyOnWrite(t *testing.T) {
	pc := newTestProviderConfig(t)

	before, err := pc.GetCustomPricingData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cpu := before.CPU

	if _, err := pc.Update(func(c *models.CustomPricing) error {
		c.CPU = "9.99"
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if before.CPU != cpu {
		t.Fatalf("Update wrote the value readers hold: CPU %q, want %q", before.CPU, cpu)
	}
	after, _ := pc.GetCustomPricingData()
	if after.CPU != "9.99" {
		t.Fatalf("expected the update to be published, got CPU %q", after.CPU)
	}

	// A failed update leaves the published value as it was.
	if _, err := pc.Update(func(c *models.CustomPricing) error {
		c.CPU = "1.23"
		return fmt.Errorf("rejected")
	}); err == nil {
		t.Fatalf("expected the update error to propagate")
	}
	if got, _ := pc.GetCustomPricingData(); got.CPU != "9.99" {
		t.Fatalf("a failed update changed the config: CPU %q", got.CPU)
	}
}

// TestProviderConfig_ConcurrentReadsAndUpdates mirrors providers reading
// their pricing while the API or a ConfigMap updates it. Run with -race,
// in-place updates fail it.
func TestProviderConfig_ConcurrentReadsAndUpdates(t *testing.T) {
	pc := newTestProviderConfig(t)
	if _, err := pc.GetCustomPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				c, _ := pc.GetCustomPricingData()
				_ = c.CPU + c.RAM + c.Storage
			}
		}()
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pc.UpdateFromMap(map[string]string{"CPU": fmt.Sprintf("0.0%d%d", i, j)})
			}
		}(i)
	}
	wg.Wait()
}
