package prom

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	promsource "github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
)

// QueryDataCoverage returns the time range for which data is available
func (pds *PrometheusMetricsQuerier) QueryDataCoverage(limitDays int) (time.Time, time.Time, error) {
	cfg := pds.promConfig

	// Query for the earliest timestamp with data
	// OTel kubeletstats receiver uses container_memory_working_set (not k8s_container_memory_working_set)
	startQuery := fmt.Sprintf(`min(min_over_time(container_memory_working_set{k8s_container_name!="POD",k8s_container_name!="",%s}[%dd]))`, cfg.ClusterFilter, limitDays)

	// Query for the latest timestamp with data
	endQuery := fmt.Sprintf(`max(max_over_time(container_memory_working_set{k8s_container_name!="POD",k8s_container_name!="",%s}[%dd]))`, cfg.ClusterFilter, limitDays)

	log.Debugf(PrometheusMetricsQueryLogFormat, "QueryDataCoverage", time.Now().Unix(), startQuery)

	// Execute queries
	startResults, _, err := pds.NewNamedContext(promsource.AllocationContextName).QuerySync(startQuery)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	endResults, _, err := pds.NewNamedContext(promsource.AllocationContextName).QuerySync(endQuery)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	var start, end time.Time

	// Parse start time
	if len(startResults) > 0 && len(startResults[0].Values) > 0 {
		start = time.Unix(int64(startResults[0].Values[0].Timestamp), 0)
	}

	// Parse end time
	if len(endResults) > 0 && len(endResults[0].Values) > 0 {
		end = time.Unix(int64(endResults[0].Values[0].Timestamp), 0)
	}

	return start, end, nil
}
