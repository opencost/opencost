// +build ignore

// This program reproduces the concurrent map access issue
// reported in https://github.com/opencost/opencost/issues/3388
//
// Run with: go run test_race_condition.go
// Or with race detector: go run -race test_race_condition.go
//
// This simulates the SanitizeLabels function being called concurrently
// while another goroutine modifies the same map.

package main

import (
	"fmt"
	"regexp"
	"sync"
	"time"
)

var invalidLabelCharRE = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// SanitizeLabelName replaces all illegal prometheus label characters with _
func SanitizeLabelName(s string) string {
	return invalidLabelCharRE.ReplaceAllString(s, "_")
}

// SanitizeLabels sanitizes all label names in the given map
// This is the vulnerable function from core/pkg/util/promutil/promutil.go:118-126
func SanitizeLabels(labels map[string]string) map[string]string {
	response := make(map[string]string, len(labels))

	for k, v := range labels { // RACE CONDITION: iterating over potentially shared map
		response[SanitizeLabelName(k)] = v
	}

	return response
}

func main() {
	fmt.Println("Testing concurrent map access issue (Issue #3388)...")
	fmt.Println("This test simulates the race condition in SanitizeLabels")
	fmt.Println()

	// Create a shared map that will be accessed concurrently
	sharedLabels := make(map[string]string)
	for i := 0; i < 100; i++ {
		sharedLabels[fmt.Sprintf("label.%d/name", i)] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup
	iterations := 1000
	goroutines := 10
	panicChan := make(chan interface{}, goroutines)

	fmt.Printf("Starting %d goroutines, each running %d iterations...\n", goroutines, iterations)
	fmt.Println("Half will read (via SanitizeLabels), half will write to the map")
	fmt.Println()

	startTime := time.Now()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicChan <- r
				}
			}()

			for i := 0; i < iterations; i++ {
				if id < 5 {
					// Goroutine 0-4: Read/iterate the map via SanitizeLabels
					_ = SanitizeLabels(sharedLabels)
				} else {
					// Goroutine 5-9: Write to the map
					sharedLabels[fmt.Sprintf("label.new-%d-%d/name", id, i)] = fmt.Sprintf("value-%d-%d", id, i)
					if i > 0 {
						delete(sharedLabels, fmt.Sprintf("label.%d/name", i%100))
					}
				}
				// Small delay to increase chance of collision
				if i%100 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(g)
	}

	wg.Wait()
	close(panicChan)

	duration := time.Since(startTime)

	// Check if any goroutine panicked
	panicCount := 0
	var lastPanic interface{}
	for p := range panicChan {
		panicCount++
		lastPanic = p
	}

	fmt.Printf("Test completed in %v\n\n", duration)

	if panicCount > 0 {
		fmt.Printf("✓ ISSUE REPRODUCED: %d panic(s) occurred\n", panicCount)
		fmt.Printf("Last panic: %v\n\n", lastPanic)
		fmt.Println("This confirms the concurrent map access issue reported in:")
		fmt.Println("- Issue #3388: concurrent map iteration and map write in SanitizeLabels")
		fmt.Println("- Issue #2910: concurrent map read and map write in label matching")
	} else {
		fmt.Println("⚠ No panic occurred in this run")
		fmt.Println("The race condition is timing-dependent and may not always trigger.")
		fmt.Println("Try running with: go run -race test_race_condition.go")
		fmt.Println("Or run multiple times to increase chances of reproduction.")
	}
}
