package promutil

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSanitizeLabels_Basic tests basic functionality
func TestSanitizeLabels_Basic(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name:     "nil map",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty map",
			input:    map[string]string{},
			expected: map[string]string{},
		},
		{
			name: "valid labels",
			input: map[string]string{
				"app":     "myapp",
				"version": "1.0",
			},
			expected: map[string]string{
				"app":     "myapp",
				"version": "1.0",
			},
		},
		{
			name: "labels with special characters",
			input: map[string]string{
				"app.kubernetes.io/name":    "myapp",
				"app.kubernetes.io/version": "1.0",
				"label-with-dash":           "value",
				"label/with/slash":          "value2",
			},
			expected: map[string]string{
				"app_kubernetes_io_name":    "myapp",
				"app_kubernetes_io_version": "1.0",
				"label_with_dash":           "value",
				"label_with_slash":          "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeLabels(tt.input)

			if tt.expected == nil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d labels, got %d", len(tt.expected), len(result))
			}

			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("expected result[%s] = %s, got %s", k, v, result[k])
				}
			}
		})
	}
}

// TestSanitizeLabels_DoesNotModifyInput ensures the function doesn't modify the input map
func TestSanitizeLabels_DoesNotModifyInput(t *testing.T) {
	input := map[string]string{
		"app.kubernetes.io/name": "myapp",
		"version":                "1.0",
	}

	// Create a copy to compare later
	original := make(map[string]string)
	for k, v := range input {
		original[k] = v
	}

	_ = SanitizeLabels(input)

	// Verify input wasn't modified
	if len(input) != len(original) {
		t.Errorf("input map length changed: expected %d, got %d", len(original), len(input))
	}

	for k, v := range original {
		if input[k] != v {
			t.Errorf("input map modified: key %s changed from %s to %s", k, v, input[k])
		}
	}
}

// TestSanitizeLabels_ConcurrentSafety tests that SanitizeLabels is safe to call concurrently
// even when the source map is being modified by other goroutines.
// This test addresses issue #3388: concurrent map iteration and map write
func TestSanitizeLabels_ConcurrentSafety(t *testing.T) {
	// Create a map that will be modified by some goroutines
	sourceMap := make(map[string]string)
	for i := 0; i < 50; i++ {
		sourceMap[fmt.Sprintf("label-%d", i)] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup
	iterations := 100
	readers := 5
	writers := 5
	errorChan := make(chan error, readers+writers)

	// Reader goroutines - call SanitizeLabels
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errorChan <- fmt.Errorf("reader %d panicked: %v", id, r)
				}
			}()

			for j := 0; j < iterations; j++ {
				result := SanitizeLabels(sourceMap)
				if result == nil {
					errorChan <- fmt.Errorf("reader %d got nil result", id)
					return
				}
			}
		}(i)
	}

	// Writer goroutines - modify the source map
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sourceMap[fmt.Sprintf("new-label-%d-%d", id, j)] = fmt.Sprintf("value-%d-%d", id, j)
				if j > 0 {
					delete(sourceMap, fmt.Sprintf("label-%d", j%50))
				}
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// Check for any errors
	for err := range errorChan {
		t.Error(err)
	}
}

// TestKubePrependQualifierToLabels_ConcurrentSafety tests concurrent access safety
func TestKubePrependQualifierToLabels_ConcurrentSafety(t *testing.T) {
	sourceMap := make(map[string]string)
	for i := 0; i < 50; i++ {
		sourceMap[fmt.Sprintf("annotation.%d/name", i)] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup
	iterations := 100
	readers := 5
	writers := 5
	errorChan := make(chan error, readers+writers)

	// Reader goroutines
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errorChan <- fmt.Errorf("reader %d panicked: %v", id, r)
				}
			}()

			for j := 0; j < iterations; j++ {
				keys, values := KubePrependQualifierToLabels(sourceMap, "label_")
				if len(keys) != len(values) {
					errorChan <- fmt.Errorf("reader %d got mismatched keys/values", id)
					return
				}
			}
		}(i)
	}

	// Writer goroutines
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sourceMap[fmt.Sprintf("annotation.new-%d-%d/name", id, j)] = fmt.Sprintf("value-%d-%d", id, j)
				if j > 0 {
					delete(sourceMap, fmt.Sprintf("annotation.%d/name", j%50))
				}
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	for err := range errorChan {
		t.Error(err)
	}
}

// TestCopyStringMap tests the CopyStringMap utility function
func TestCopyStringMap(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]string
	}{
		{
			name:  "nil map",
			input: nil,
		},
		{
			name:  "empty map",
			input: map[string]string{},
		},
		{
			name: "map with values",
			input: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CopyStringMap(tt.input)

			if tt.input == nil {
				if result != nil {
					t.Errorf("expected nil result for nil input, got %v", result)
				}
				return
			}

			// Verify the copy has the same contents
			if len(result) != len(tt.input) {
				t.Errorf("expected length %d, got %d", len(tt.input), len(result))
			}

			for k, v := range tt.input {
				if result[k] != v {
					t.Errorf("expected result[%s] = %s, got %s", k, v, result[k])
				}
			}

			// Verify it's actually a copy (different map instance)
			if tt.input != nil && len(tt.input) > 0 {
				// Modify the copy
				result["new-key"] = "new-value"

				// Verify original is unchanged
				if _, exists := tt.input["new-key"]; exists {
					t.Error("modifying copy affected the original map")
				}
			}
		})
	}
}

// TestCopyStringMap_ConcurrentSafety tests that CopyStringMap creates independent copies
func TestCopyStringMap_ConcurrentSafety(t *testing.T) {
	sourceMap := make(map[string]string)
	for i := 0; i < 50; i++ {
		sourceMap[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("value-%d", i)
	}

	var wg sync.WaitGroup
	iterations := 100
	copiers := 5
	writers := 5
	errorChan := make(chan error, copiers+writers)

	// Copier goroutines - create copies and verify them
	for i := 0; i < copiers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errorChan <- fmt.Errorf("copier %d panicked: %v", id, r)
				}
			}()

			for j := 0; j < iterations; j++ {
				copied := CopyStringMap(sourceMap)
				if copied == nil {
					errorChan <- fmt.Errorf("copier %d got nil result", id)
					return
				}
				// Verify we can iterate over the copy safely
				for k := range copied {
					_ = k
				}
			}
		}(i)
	}

	// Writer goroutines - modify the source map
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				sourceMap[fmt.Sprintf("new-key-%d-%d", id, j)] = fmt.Sprintf("value-%d-%d", id, j)
				if j > 0 {
					delete(sourceMap, fmt.Sprintf("key-%d", j%50))
				}
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	for err := range errorChan {
		t.Error(err)
	}
}
