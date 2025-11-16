package promutil

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSanitizeLabels_ConcurrentAccess reproduces the concurrent map access issue
// reported in https://github.com/opencost/opencost/issues/3388
func TestSanitizeLabels_ConcurrentAccess(t *testing.T) {
	// Create a shared map that will be accessed concurrently
	sharedLabels := make(map[string]string)
	for i := 0; i < 100; i++ {
		sharedLabels[fmt.Sprintf("label-%d", i)] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup
	iterations := 100
	goroutines := 10

	// This test should panic with "concurrent map iteration and map write"
	// when run with -race flag or in high concurrency scenarios
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Goroutine 1-5: Read/iterate the map via SanitizeLabels
				if id < 5 {
					_ = SanitizeLabels(sharedLabels)
				} else {
					// Goroutine 6-10: Write to the map
					sharedLabels[fmt.Sprintf("label-new-%d-%d", id, i)] = fmt.Sprintf("value-%d-%d", id, i)
					delete(sharedLabels, fmt.Sprintf("label-%d", i%100))
				}
				time.Sleep(time.Microsecond) // Small delay to increase chance of collision
			}
		}(g)
	}

	wg.Wait()
}

// TestKubePrependQualifierToLabels_ConcurrentAccess tests concurrent access to KubePrependQualifierToLabels
func TestKubePrependQualifierToLabels_ConcurrentAccess(t *testing.T) {
	sharedLabels := make(map[string]string)
	for i := 0; i < 50; i++ {
		sharedLabels[fmt.Sprintf("annotation.%d/name", i)] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup
	iterations := 100
	goroutines := 10

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if id < 5 {
					// Read operations
					_, _ = KubePrependQualifierToLabels(sharedLabels, "label_")
				} else {
					// Write operations
					sharedLabels[fmt.Sprintf("annotation.new-%d-%d/name", id, i)] = fmt.Sprintf("value-%d-%d", id, i)
					delete(sharedLabels, fmt.Sprintf("annotation.%d/name", i%50))
				}
				time.Sleep(time.Microsecond)
			}
		}(g)
	}

	wg.Wait()
}
