package kubemodel

// Resolution represents the time granularity for data aggregation
type Resolution string

const (
	Resolution10M Resolution = "10m" // 10 minutes
	Resolution1H  Resolution = "1h"  // 1 hour
	Resolution1D  Resolution = "1d"  // 1 day
)