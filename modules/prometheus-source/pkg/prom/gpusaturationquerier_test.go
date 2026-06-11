package prom

import (
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
)

func TestBuildGPUThrottleViolationQuery(t *testing.T) {
	query := buildGPUThrottleViolationQuery(`cluster_id="c1"`, "1h", "cluster_id", 3600)

	branches := strings.Split(query, " or ")
	if len(branches) != 4 {
		t.Fatalf("expected 4 violation branches, got %d: %s", len(branches), query)
	}

	// one hour is 3.6e9 microseconds: each branch must normalize by it
	wantBranch := `label_replace(avg(increase(DCGM_FI_DEV_POWER_VIOLATION{container!="",cluster_id="c1"}[1h])) by (container, pod, namespace, device, modelName, UUID, GPU_I_PROFILE, GPU_I_ID, uid, cluster_id) / 3.6e+09, "reason", "power", "", "")`
	if branches[0] != wantBranch {
		t.Errorf("violation branch mismatch:\n got %s\nwant %s", branches[0], wantBranch)
	}

	for metric, reason := range map[string]string{
		"DCGM_FI_DEV_POWER_VIOLATION":       "power",
		"DCGM_FI_DEV_THERMAL_VIOLATION":     "thermal",
		"DCGM_FI_DEV_SYNC_BOOST_VIOLATION":  "sync_boost",
		"DCGM_FI_DEV_BOARD_LIMIT_VIOLATION": "board_limit",
	} {
		if !strings.Contains(query, metric) {
			t.Errorf("expected query to reference %s", metric)
		}
		if !strings.Contains(query, `"reason", "`+reason+`"`) {
			t.Errorf("expected query to tag reason %q", reason)
		}
	}
}

func TestBuildGPUThrottleReasonQuery(t *testing.T) {
	query := buildGPUThrottleReasonQuery(`cluster_id="c1"`, "1h", "cluster_id", 5)

	// one label_replace-wrapped branch per saturation-relevant reason bit
	if got := strings.Count(query, "label_replace"); got != 6 {
		t.Fatalf("expected 6 reason branches, got %d: %s", got, query)
	}

	// the first branch tests the sw_power_cap bit (0x4 == 4) per sample at
	// the subquery resolution, then averages the 0/1 results over the window
	wantBranch := `label_replace(avg(avg_over_time(((floor((DCGM_FI_DEV_CLOCK_THROTTLE_REASONS{container!="",cluster_id="c1"} or DCGM_FI_DEV_CLOCKS_EVENT_REASONS{container!="",cluster_id="c1"}) / 4)) % 2)[1h:5m])) by (container, pod, namespace, device, modelName, UUID, GPU_I_PROFILE, GPU_I_ID, uid, cluster_id), "reason", "sw_power_cap", "", "")`
	if !strings.HasPrefix(query, wantBranch+" or ") {
		t.Errorf("reason query does not start with expected sw_power_cap branch:\n got %s\nwant prefix %s", query, wantBranch)
	}

	for reason, bit := range map[string]string{
		"sw_power_cap":   "/ 4",
		"hw_slowdown":    "/ 8",
		"sync_boost":     "/ 16",
		"sw_thermal":     "/ 32",
		"hw_thermal":     "/ 64",
		"hw_power_brake": "/ 128",
	} {
		if !strings.Contains(query, `"reason", "`+reason+`"`) {
			t.Errorf("expected query to tag reason %q", reason)
		}
		if !strings.Contains(query, bit) {
			t.Errorf("expected query to test bit via %q for reason %q", bit, reason)
		}
	}
}

// TestGPUSaturationQueries runs every saturation query against the no-op
// client and asserts the logged query references the expected DCGM source
// metric, carries the cluster filter, and groups by the saturation label
// set.
func TestGPUSaturationQueries(t *testing.T) {
	initLogging(t, "debug", false)

	logWriter := new(SingleLogWriter)
	zerologger.Logger = zerologger.Output(zerolog.ConsoleWriter{
		Out:        logWriter,
		TimeFormat: "",
		NoColor:    true,
		PartsExclude: []string{
			zerolog.TimestampFieldName,
			zerolog.LevelFieldName,
			zerolog.CallerFieldName,
		},
	})
	defer initLogging(t, "debug", false)

	t.Setenv("PROMETHEUS_SERVER_ENDPOINT", "nowhere")
	t.Setenv("CURRENT_CLUSTER_ID_FILTER_ENABLED", "true")
	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv("GPU_MEMORY_SATURATION_THRESHOLD", "0.8")

	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err != nil {
		t.Fatalf("Failed to create OpenCost Prometheus config: %v", err)
	}

	mock := new(NoOpPromClient)
	contextFactory := NewContextFactory(mock, config)
	querier := newPrometheusMetricsQuerier(config, mock, contextFactory)

	queryEnd := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	queryStart := queryEnd.Add(-24 * time.Hour)

	tests := map[string]struct {
		query      func(time.Time, time.Time)
		wantMetric string
		wantExtra  string
	}{
		"QueryGPUThrottleViolationRatio": {
			query:      func(s, e time.Time) { querier.QueryGPUThrottleViolationRatio(s, e) },
			wantMetric: "DCGM_FI_DEV_POWER_VIOLATION",
		},
		"QueryGPUThrottleReasonRatio": {
			query:      func(s, e time.Time) { querier.QueryGPUThrottleReasonRatio(s, e) },
			wantMetric: "DCGM_FI_DEV_CLOCK_THROTTLE_REASONS",
			wantExtra:  "DCGM_FI_DEV_CLOCKS_EVENT_REASONS",
		},
		"QueryGPUMemoryUsedRatioAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUMemoryUsedRatioAvg(s, e) },
			wantMetric: "DCGM_FI_DEV_FB_USED",
			wantExtra:  "DCGM_FI_DEV_FB_FREE",
		},
		"QueryGPUMemoryUsedRatioMax": {
			query:      func(s, e time.Time) { querier.QueryGPUMemoryUsedRatioMax(s, e) },
			wantMetric: "DCGM_FI_DEV_FB_USED",
			wantExtra:  "max_over_time",
		},
		"QueryGPUMemoryPressureRatio": {
			query:      func(s, e time.Time) { querier.QueryGPUMemoryPressureRatio(s, e) },
			wantMetric: "DCGM_FI_DEV_FB_USED",
			wantExtra:  ">= bool 0.8",
		},
		"QueryGPUXIDErrorCount": {
			query:      func(s, e time.Time) { querier.QueryGPUXIDErrorCount(s, e) },
			wantMetric: "DCGM_FI_DEV_XID_ERRORS",
			wantExtra:  "changes(",
		},
		"QueryGPUDRAMActiveAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUDRAMActiveAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_DRAM_ACTIVE",
			wantExtra:  "avg_over_time",
		},
		"QueryGPUDRAMActiveMax": {
			query:      func(s, e time.Time) { querier.QueryGPUDRAMActiveMax(s, e) },
			wantMetric: "DCGM_FI_PROF_DRAM_ACTIVE",
			wantExtra:  "max_over_time",
		},
		"QueryGPUSMActiveAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUSMActiveAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_SM_ACTIVE",
		},
		"QueryGPUSMOccupancyAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUSMOccupancyAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_SM_OCCUPANCY",
		},
		"QueryGPUPCIeTxBytesAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUPCIeTxBytesAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_PCIE_TX_BYTES",
			wantExtra:  "rate(",
		},
		"QueryGPUPCIeRxBytesAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUPCIeRxBytesAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_PCIE_RX_BYTES",
		},
		"QueryGPUNVLinkTxBytesAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUNVLinkTxBytesAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_NVLINK_TX_BYTES",
		},
		"QueryGPUNVLinkRxBytesAvg": {
			query:      func(s, e time.Time) { querier.QueryGPUNVLinkRxBytesAvg(s, e) },
			wantMetric: "DCGM_FI_PROF_NVLINK_RX_BYTES",
		},
	}

	const wantFilter = `cluster_id="test-cluster"`

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			tc.query(queryStart, queryEnd)
			logged := logWriter.Log

			if !strings.Contains(logged, testName) {
				t.Errorf("expected log to contain query name %q, got: %s", testName, logged)
			}
			if !strings.Contains(logged, tc.wantMetric) {
				t.Errorf("expected query to reference %q, got: %s", tc.wantMetric, logged)
			}
			if tc.wantExtra != "" && !strings.Contains(logged, tc.wantExtra) {
				t.Errorf("expected query to contain %q, got: %s", tc.wantExtra, logged)
			}
			if !strings.Contains(logged, wantFilter) {
				t.Errorf("expected query to contain cluster filter %q, got: %s", wantFilter, logged)
			}
			if !strings.Contains(logged, gpuSaturationByLabels) {
				t.Errorf("expected query to group by %q, got: %s", gpuSaturationByLabels, logged)
			}
		})
	}
}
