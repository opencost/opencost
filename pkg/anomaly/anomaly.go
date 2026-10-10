package anomaly

import (
	"math"
	"sort"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// AnomalyDetail contains detailed information about a single detected anomaly.
type AnomalyDetail struct {
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Cost      float64   `json:"cost"`
	Baseline  float64   `json:"baseline"`  // Mean or Median depending on algorithm
	Deviation float64   `json:"deviation"` // StdDev or MAD depending on algorithm
	Score     float64   `json:"score"`
	Threshold float64   `json:"threshold"`
}

// AnomalyReport lists the anomalies detected for a specific aggregation key (e.g. namespace).
type AnomalyReport struct {
	Key       string          `json:"key"`
	Anomalies []AnomalyDetail `json:"anomalies"`
}

// stats calculates the mean and sample standard deviation of a slice.
func stats(slice []float64) (float64, float64) {
	n := len(slice)
	if n == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range slice {
		sum += v
	}
	mean := sum / float64(n)
	var varianceSum float64
	for _, v := range slice {
		varianceSum += (v - mean) * (v - mean)
	}
	var stdDev float64
	if n > 1 {
		stdDev = math.Sqrt(varianceSum / float64(n-1))
	} else {
		stdDev = 0
	}
	return mean, stdDev
}

// madStats calculates the median and median absolute deviation (MAD) of a slice.
func madStats(slice []float64) (float64, float64) {
	n := len(slice)
	if n == 0 {
		return 0, 0
	}
	sorted := make([]float64, n)
	copy(sorted, slice)
	sort.Float64s(sorted)

	var median float64
	if n%2 == 1 {
		median = sorted[n/2]
	} else {
		median = (sorted[n/2-1] + sorted[n/2]) / 2.0
	}

	devs := make([]float64, n)
	for i, v := range slice {
		devs[i] = math.Abs(v - median)
	}
	sort.Float64s(devs)

	var mad float64
	if n%2 == 1 {
		mad = devs[n/2]
	} else {
		mad = (devs[n/2-1] + devs[n/2]) / 2.0
	}

	return median, mad
}

// Detect processes the AllocationSetRange and identifies cost anomalies.
func Detect(asr *opencost.AllocationSetRange, step, lookback time.Duration, algorithm string, threshold, minCost float64) ([]AnomalyReport, error) {
	if asr == nil || len(asr.Allocations) == 0 {
		return nil, nil
	}

	// Determine the lookback count in terms of data points.
	// Minimum lookback is 2 points (to be able to calculate standard deviation/MAD).
	lookbackPoints := int(lookback / step)
	if lookbackPoints < 2 {
		lookbackPoints = 2
	}

	// 1. Gather all unique keys across the entire time series range
	allKeys := make(map[string]bool)
	for _, set := range asr.Allocations {
		if set != nil {
			for key := range set.Allocations {
				allKeys[key] = true
			}
		}
	}

	numSets := len(asr.Allocations)
	reports := []AnomalyReport{}

	// 2. Run detection for each key
	for key := range allKeys {
		// Construct the time series of costs
		costs := make([]float64, numSets)
		for i, set := range asr.Allocations {
			if set != nil {
				if alloc, ok := set.Allocations[key]; ok {
					costs[i] = alloc.TotalCost()
				} else {
					costs[i] = 0.0
				}
			}
		}

		anomalies := []AnomalyDetail{}

		// Scan through the time series (starting from 1, since we need at least 1 historical point)
		for i := 0; i < numSets; i++ {
			// Find start index of historical lookback window (excluding current point)
			startIdx := i - lookbackPoints
			if startIdx < 0 {
				startIdx = 0
			}

			// We need at least 2 historical data points to compute baseline
			historyLen := i - startIdx
			if historyLen < 2 {
				continue
			}

			history := costs[startIdx:i]
			currentVal := costs[i]

			// Only evaluate spikes that exceed our minimum cost threshold
			if currentVal < minCost {
				continue
			}

			var isAnomaly bool
			var baseline, deviation, score float64

			if algorithm == "zscore" {
				mean, stdDev := stats(history)
				baseline = mean
				deviation = stdDev
				if stdDev > 0 {
					score = (currentVal - mean) / stdDev
					if score > threshold {
						isAnomaly = true
					}
				} else if currentVal > mean {
					// stdDev is 0 (all history is equal), so any increase is an anomaly
					score = math.MaxFloat64
					isAnomaly = true
				}
			} else { // default to mad
				median, mad := madStats(history)
				baseline = median
				deviation = mad
				if mad > 0 {
					// Modified Z-score using 0.6745 multiplier
					score = 0.6745 * (currentVal - median) / mad
					if score > threshold {
						isAnomaly = true
					}
				} else if currentVal > median {
					// mad is 0, any increase is an anomaly
					score = math.MaxFloat64
					isAnomaly = true
				}
			}

			if isAnomaly {
				var startVal, endVal time.Time
				if asr.Allocations[i].Window.Start() != nil {
					startVal = *asr.Allocations[i].Window.Start()
				}
				if asr.Allocations[i].Window.End() != nil {
					endVal = *asr.Allocations[i].Window.End()
				}

				anomalies = append(anomalies, AnomalyDetail{
					Start:     startVal,
					End:       endVal,
					Cost:      currentVal,
					Baseline:  baseline,
					Deviation: deviation,
					Score:     score,
					Threshold: threshold,
				})
			}
		}

		if len(anomalies) > 0 {
			reports = append(reports, AnomalyReport{
				Key:       key,
				Anomalies: anomalies,
			})
		}
	}

	return reports, nil
}
