package costmodel

import (
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// createTestAllocationSets creates N AllocationSets for benchmarking
func createTestAllocationSets(n int, allocsPerSet int) []*opencost.AllocationSet {
	sets := make([]*opencost.AllocationSet, n)
	baseTime := time.Now().Truncate(time.Hour)

	for i := 0; i < n; i++ {
		start := baseTime.Add(time.Duration(i) * time.Hour)
		end := start.Add(time.Hour)
		as := opencost.NewAllocationSet(start, end)

		// Create multiple allocations within each set
		for j := 0; j < allocsPerSet; j++ {
			podName := fmt.Sprintf("pod-%d", j)
			alloc := &opencost.Allocation{
				Name: podName,
				Properties: &opencost.AllocationProperties{
					Cluster:   "cluster-1",
					Namespace: "namespace-1",
					Pod:       podName,
				},
				Start:  start,
				End:    end,
				Window: opencost.NewClosedWindow(start, end),
			}

			// Set some cost values
			hours := end.Sub(start).Hours()
			cpuCost := float64(j+1) * 0.05 * hours
			ramCost := float64(j+1) * 0.03 * hours
			alloc.CPUCost = cpuCost
			alloc.RAMCost = ramCost
			alloc.CPUCoreHours = float64(j+1) * 0.5 * hours
			alloc.RAMByteHours = float64(j+1) * 1024 * 1024 * 100 * hours

			// Set raw allocation data for max usage tracking
			alloc.RawAllocationOnly = &opencost.RawAllocationOnlyData{
				CPUCoreUsageMax:  float64(j+1) * 0.5,
				RAMBytesUsageMax: float64(j+1) * 1024 * 1024 * 100, // 100MB base
			}

			as.Insert(alloc)
		}

		sets[i] = as
	}

	return sets
}

// accumulateIteratively simulates the new approach: accumulate as we go
func accumulateIteratively(sets []*opencost.AllocationSet) (*opencost.AllocationSet, error) {
	var result *opencost.AllocationSet

	for _, as := range sets {
		if as == nil {
			continue
		}

		if result == nil {
			result = as
		} else {
			acc, err := result.Accumulate(as)
			if err != nil {
				return nil, err
			}
			result = acc
		}
	}

	return result, nil
}

// accumulateViaRange simulates the old approach: collect in range, then accumulate
func accumulateViaRange(sets []*opencost.AllocationSet) (*opencost.AllocationSet, error) {
	asr := opencost.NewAllocationSetRange(sets...)

	resultASR, err := asr.Accumulate(opencost.AccumulateOptionAll)
	if err != nil {
		return nil, err
	}

	if resultASR == nil || len(resultASR.Allocations) == 0 {
		return nil, nil
	}

	return resultASR.Allocations[0], nil
}

// BenchmarkAccumulateIteratively_10Sets benchmarks the new iterative approach with 10 sets
func BenchmarkAccumulateIteratively_10Sets(b *testing.B) {
	sets := createTestAllocationSets(10, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateIteratively(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateViaRange_10Sets benchmarks the old range-based approach with 10 sets
func BenchmarkAccumulateViaRange_10Sets(b *testing.B) {
	sets := createTestAllocationSets(10, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateViaRange(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateIteratively_50Sets benchmarks the new iterative approach with 50 sets
func BenchmarkAccumulateIteratively_50Sets(b *testing.B) {
	sets := createTestAllocationSets(50, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateIteratively(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateViaRange_50Sets benchmarks the old range-based approach with 50 sets
func BenchmarkAccumulateViaRange_50Sets(b *testing.B) {
	sets := createTestAllocationSets(50, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateViaRange(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateIteratively_100Sets benchmarks the new iterative approach with 100 sets
func BenchmarkAccumulateIteratively_100Sets(b *testing.B) {
	sets := createTestAllocationSets(100, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateIteratively(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateViaRange_100Sets benchmarks the old range-based approach with 100 sets
func BenchmarkAccumulateViaRange_100Sets(b *testing.B) {
	sets := createTestAllocationSets(100, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateViaRange(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateIteratively_500Sets benchmarks the new iterative approach with 500 sets (large time range)
func BenchmarkAccumulateIteratively_500Sets(b *testing.B) {
	sets := createTestAllocationSets(500, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateIteratively(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccumulateViaRange_500Sets benchmarks the old range-based approach with 500 sets (large time range)
func BenchmarkAccumulateViaRange_500Sets(b *testing.B) {
	sets := createTestAllocationSets(500, 50)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := accumulateViaRange(sets)
		if err != nil {
			b.Fatal(err)
		}
	}
}
