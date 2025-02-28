package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

// NOTE (bolt): This is currently not being used directly in the prometheus data source, but may be useful in the future
// NOTE (bolt): when it comes to pricing local storage options per provider. Recommendation is to abstract this into some
// NOTE (bolt): type of storage queury registry.
var providerStorageQueries = map[string]func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string{
	"aws": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"gcp": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		// TODO Set to the price for the appropriate storage class. It's not trivial to determine the local storage disk type
		// See https://cloud.google.com/compute/disks-image-pricing#persistentdisk
		localStorageCost := 0.04

		baseMetric := "container_fs_limit_bytes"
		if used {
			baseMetric = "container_fs_usage_bytes"
		}

		fmtCumulativeQuery := `sum(
			sum_over_time(%s{device!="tmpfs", id="/", %s}[%s:1m])
		) by (%s) / 60 / 730 / 1024 / 1024 / 1024 * %f`

		fmtMonthlyQuery := `sum(
			avg_over_time(%s{device!="tmpfs", id="/", %s}[%s:1m])
		) by (%s) / 1024 / 1024 / 1024 * %f`

		fmtQuery := fmtCumulativeQuery
		if rate {
			fmtQuery = fmtMonthlyQuery
		}
		fmtWindow := timeutil.DurationString(end.Sub(start))

		return fmt.Sprintf(fmtQuery, baseMetric, config.ClusterFilter, fmtWindow, config.ClusterLabel, localStorageCost)
	},
	"azure": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"alibaba": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"scaleway": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"otc": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"oracle": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"csv": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
	"custom": func(config *OpenCostPrometheusConfig, start, end time.Time, rate bool, used bool) string {
		return ""
	},
}

var _ = providerStorageQueries
