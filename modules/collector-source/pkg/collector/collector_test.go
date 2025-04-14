package collector

import "testing"

func TestBasicCollectorFunctionality(t *testing.T) {
	// avg of 55 (sum of [1,10]) / data points (10) = 5.5
	const expected = 55.0 / 10.0

	labelsA := map[string]string{
		"container": "container-a",
		"uid":       "uid-a",
		"pod":       "pod-a",
		"namespace": "namespace-a",
		"instance":  "instance-a",
		"node":      "node-a",
		"cluster":   "cluster-a",
	}

	labelsB := map[string]string{
		"container": "container-b",
		"uid":       "uid-b",
		"pod":       "pod-b",
		"namespace": "namespace-b",
		"instance":  "instance-b",
		"node":      "node-b",
		"cluster":   "cluster-a",
	}

	collector := NewOpenCostMetricCollector()

	for i := 1; i <= 10; i++ {
		collector.Update(ContainerMemoryWorkingSetBytes, labelsA, float64(i), nil, nil)
		collector.Update(ContainerMemoryWorkingSetBytes, labelsB, float64(i), nil, nil)
	}

	results, err := collector.Query(RAMUsageAverageID)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	for _, result := range results {
		if result.Values[0].Value != expected {
			t.Fatalf("expected %f, got %f", expected, result.Values[0].Value)
		}

		t.Logf("+-- Result -------------------------------")
		t.Logf("| Labels: %v", result.MetricLabels)
		t.Logf("| Value: %v", result.Values[0].Value)
		t.Logf("+----------------------------------------")
	}
}
