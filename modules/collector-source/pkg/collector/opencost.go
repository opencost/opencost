package collector

func NewOpenCostMetricCollector() MetricsCollector {
	memCollector := NewInMemoryMetricsCollector()

	// Register all the metrics
	memCollector.Register(NewRAMUsageAverageMetricInstance())
	// etc...

	// Use ./modules/prometheus-source/pkg/prom/metricsquerier.go as a good
	// reference for the Queries we require (and therefore, the metrics we need to register).

	return memCollector
}

// There are a couple ways we can make "Reporting" of the metrics a bit cleaner:
// -- we can write thin API friendly wrappers that can be used to funnel value updates into
//    collector.Update(...) calls [similar to prom]. This is purely convenience and there isn't
//    really an architecture bearing weight on this decisions. Whatever is easier to use.
