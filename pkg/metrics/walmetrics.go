package metrics

import (
	"errors"
	"sync"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/prometheus/client_golang/prometheus"
)

var walMetricsInit sync.Once

// walMetric describes a single WAL status metric and how to derive its value from a WALStatus.
type walMetric struct {
	name      string
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	value     func(source.WALStatus) float64
}

func newWALMetric(name, help string, valueType prometheus.ValueType, value func(source.WALStatus) float64) walMetric {
	return walMetric{
		name:      name,
		desc:      prometheus.NewDesc(name, help, nil, nil),
		valueType: valueType,
		value:     value,
	}
}

var walMetrics = []walMetric{
	newWALMetric("opencost_wal_last_export_success_timestamp_seconds",
		"Unix time of the most recent successful collector WAL write, 0 if none.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 {
			return unixOrZero(s.LastExportSuccess.Unix(), s.LastExportSuccess.IsZero())
		}),
	newWALMetric("opencost_wal_export_failures_total",
		"Total failed collector WAL writes since start.", prometheus.CounterValue,
		func(s source.WALStatus) float64 { return float64(s.ExportFailuresTotal) }),
	newWALMetric("opencost_wal_consecutive_export_failures",
		"Collector WAL writes that have failed since the last successful write.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return float64(s.ConsecutiveExportFailures) }),
	newWALMetric("opencost_wal_restore_completed",
		"1 once the collector WAL startup restore has finished, 0 before.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return boolToFloat(s.RestoreCompleted) }),
	newWALMetric("opencost_wal_restore_list_failed",
		"1 if the collector WAL startup restore could not list stored objects, meaning nothing was restored.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return boolToFloat(s.RestoreListError != "") }),
	newWALMetric("opencost_wal_restore_objects_applied",
		"Collector WAL objects applied during the startup restore.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return float64(s.RestoreObjectsApplied) }),
	newWALMetric("opencost_wal_restore_errors",
		"Collector WAL objects that could not be read or decoded during the startup restore.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return float64(s.RestoreErrors) }),
	newWALMetric("opencost_wal_restore_duration_seconds",
		"Duration of the collector WAL startup restore.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return s.RestoreDuration.Seconds() }),
	newWALMetric("opencost_wal_restore_newest_timestamp_seconds",
		"Unix time of the newest collector WAL object applied during the startup restore, 0 if none.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return unixOrZero(s.RestoreNewest.Unix(), s.RestoreNewest.IsZero()) }),
	newWALMetric("opencost_wal_restore_tail_gap_seconds",
		"Interval between the newest restored collector WAL object and the start of the restore: history not persisted before the restart. Includes the downtime of a normal restart; covers the whole retention window when nothing was restored (e.g. a first install).", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return s.RestoreTailGap.Seconds() }),
	newWALMetric("opencost_wal_restore_largest_gap_seconds",
		"Largest interval between consecutive restored collector WAL objects. Values well above the scrape interval indicate history that was never persisted or could not be restored.", prometheus.GaugeValue,
		func(s source.WALStatus) float64 { return s.RestoreLargestGap.Seconds() }),
}

// WALStatusCollector is a prometheus collector that reports the status of a data source's write-ahead log.
type WALStatusCollector struct {
	provider      source.WALStatusProvider
	metricsConfig MetricsConfig
}

// Describe sends the descriptors of all enabled WAL metrics.
func (wc WALStatusCollector) Describe(ch chan<- *prometheus.Desc) {
	disabled := wc.metricsConfig.GetDisabledMetricsMap()
	for _, m := range walMetrics {
		if _, ok := disabled[m.name]; !ok {
			ch <- m.desc
		}
	}
}

// Collect reads the current WAL status and emits all enabled WAL metrics. Nothing is emitted when the
// data source has no WAL configured.
func (wc WALStatusCollector) Collect(ch chan<- prometheus.Metric) {
	status := wc.provider.WALStatus()
	if !status.Enabled {
		return
	}

	disabled := wc.metricsConfig.GetDisabledMetricsMap()
	for _, m := range walMetrics {
		if _, ok := disabled[m.name]; ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.desc, m.valueType, m.value(status))
	}
}

// InitWALMetrics registers WAL status metrics for the data source if it implements
// source.WALStatusProvider. Registration happens at most once per process, and an existing
// registration of the same metrics (for example by an embedding application) is left in place.
func InitWALMetrics(dataSource source.OpenCostDataSource, metricsConfig *MetricsConfig) {
	provider, ok := dataSource.(source.WALStatusProvider)
	if !ok || metricsConfig == nil {
		return
	}

	walMetricsInit.Do(func() {
		err := prometheus.Register(WALStatusCollector{
			provider:      provider,
			metricsConfig: *metricsConfig,
		})
		if err != nil {
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) {
				log.Debugf("WAL status metrics already registered")
				return
			}
			log.Warnf("Failed to register WAL status metrics: %s", err)
		}
	})
}

func unixOrZero(unix int64, zero bool) float64 {
	if zero {
		return 0
	}
	return float64(unix)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
