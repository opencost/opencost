package prom

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
)

func initLogging(t *testing.T, logLevel string, colorEnabled bool) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerologger.Logger = zerologger.Output(zerolog.ConsoleWriter{
		Out:        zerolog.NewTestWriter(t),
		TimeFormat: time.RFC3339Nano,
		NoColor:    !colorEnabled,
	})

	logLevelParsed, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		logLevelParsed = zerolog.DebugLevel
	}

	zerolog.SetGlobalLevel(logLevelParsed)
}

type SingleLogWriter struct {
	Log string
}

// Write to testing.TB.
func (slw *SingleLogWriter) Write(p []byte) (n int, err error) {
	err = nil
	n = len(p)

	slw.Log = string(p)
	return
}

type NoOpPromClient struct {
}

func (mpc *NoOpPromClient) URL(ep string, args map[string]string) *url.URL {
	return &url.URL{}
}
func (mpc *NoOpPromClient) Do(c context.Context, req *http.Request) (*http.Response, []byte, error) {
	return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte{})),
		},
		[]byte{},
		nil

}

func TestQueryLogs(t *testing.T) {
	// init logging
	initLogging(t, "debug", false)

	// use a single log writer so we can examine the logs after each query
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

	// reinitialize logging when tests are complete
	defer initLogging(t, "debug", false)

	t.Setenv("PROMETHEUS_SERVER_ENDPOINT", "nowhere")

	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err != nil {
		t.Fatalf("Failed to create OpenCost Prometheus config: %v", err)
		return
	}

	mock := new(NoOpPromClient)
	contextFactory := NewContextFactory(mock, config)

	querier := newPrometheusMetricsQuerier(config, mock, contextFactory)

	queryEnd := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	queryStart := queryEnd.Add(-24 * time.Hour)

	tests := map[string]func(time.Time, time.Time){
		"QueryPVActiveMinutes":                          func(s, e time.Time) { querier.QueryPVActiveMinutes(s, e) },
		"QueryPVUsedAverage":                            func(s, e time.Time) { querier.QueryPVUsedAverage(s, e) },
		"QueryPVUsedMax":                                func(s, e time.Time) { querier.QueryPVUsedMax(s, e) },
		"QueryLocalStorageActiveMinutes":                func(s, e time.Time) { querier.QueryLocalStorageActiveMinutes(s, e) },
		"QueryLocalStorageCost":                         func(s, e time.Time) { querier.QueryLocalStorageCost(s, e) },
		"QueryLocalStorageUsedCost":                     func(s, e time.Time) { querier.QueryLocalStorageUsedCost(s, e) },
		"QueryLocalStorageUsedAvg":                      func(s, e time.Time) { querier.QueryLocalStorageUsedAvg(s, e) },
		"QueryLocalStorageUsedMax":                      func(s, e time.Time) { querier.QueryLocalStorageUsedMax(s, e) },
		"QueryLocalStorageBytes":                        func(s, e time.Time) { querier.QueryLocalStorageBytes(s, e) },
		"QueryNodeActiveMinutes":                        func(s, e time.Time) { querier.QueryNodeActiveMinutes(s, e) },
		"QueryNodeCPUCoresCapacity":                     func(s, e time.Time) { querier.QueryNodeCPUCoresCapacity(s, e) },
		"QueryNodeCPUCoresAllocatable":                  func(s, e time.Time) { querier.QueryNodeCPUCoresAllocatable(s, e) },
		"QueryNodeRAMBytesCapacity":                     func(s, e time.Time) { querier.QueryNodeRAMBytesCapacity(s, e) },
		"QueryNodeRAMBytesAllocatable":                  func(s, e time.Time) { querier.QueryNodeRAMBytesAllocatable(s, e) },
		"QueryNodeGPUCount":                             func(s, e time.Time) { querier.QueryNodeGPUCount(s, e) },
		"QueryNodeCPUModeTotal":                         func(s, e time.Time) { querier.QueryNodeCPUModeTotal(s, e) },
		"QueryNodeIsSpot":                               func(s, e time.Time) { querier.QueryNodeIsSpot(s, e) },
		"QueryNodeRAMSystemPercent":                     func(s, e time.Time) { querier.QueryNodeRAMSystemPercent(s, e) },
		"QueryNodeRAMUserPercent":                       func(s, e time.Time) { querier.QueryNodeRAMUserPercent(s, e) },
		"QueryLBActiveMinutes":                          func(s, e time.Time) { querier.QueryLBActiveMinutes(s, e) },
		"QueryLBPricePerHr":                             func(s, e time.Time) { querier.QueryLBPricePerHr(s, e) },
		"QueryClusterManagementDuration":                func(s, e time.Time) { querier.QueryClusterManagementDuration(s, e) },
		"QueryClusterManagementPricePerHr":              func(s, e time.Time) { querier.QueryClusterManagementPricePerHr(s, e) },
		"QueryPods":                                     func(s, e time.Time) { querier.QueryPods(s, e) },
		"QueryPodsUID":                                  func(s, e time.Time) { querier.QueryPodsUID(s, e) },
		"QueryRAMBytesAllocated":                        func(s, e time.Time) { querier.QueryRAMBytesAllocated(s, e) },
		"QueryRAMRequests":                              func(s, e time.Time) { querier.QueryRAMRequests(s, e) },
		"QueryRAMLimits":                                func(s, e time.Time) { querier.QueryRAMLimits(s, e) },
		"QueryRAMUsageAvg":                              func(s, e time.Time) { querier.QueryRAMUsageAvg(s, e) },
		"QueryRAMUsageMax":                              func(s, e time.Time) { querier.QueryRAMUsageMax(s, e) },
		"QueryNodeRAMPricePerGiBHr":                     func(s, e time.Time) { querier.QueryNodeRAMPricePerGiBHr(s, e) },
		"QueryCPUCoresAllocated":                        func(s, e time.Time) { querier.QueryCPUCoresAllocated(s, e) },
		"QueryCPURequests":                              func(s, e time.Time) { querier.QueryCPURequests(s, e) },
		"QueryCPULimits":                                func(s, e time.Time) { querier.QueryCPULimits(s, e) },
		"QueryCPUUsageAvg":                              func(s, e time.Time) { querier.QueryCPUUsageAvg(s, e) },
		"QueryCPUUsageMax":                              func(s, e time.Time) { querier.QueryCPUUsageMax(s, e) },
		"QueryNodeCPUPricePerHr":                        func(s, e time.Time) { querier.QueryNodeCPUPricePerHr(s, e) },
		"QueryGPUsAllocated":                            func(s, e time.Time) { querier.QueryGPUsAllocated(s, e) },
		"QueryGPUsRequested":                            func(s, e time.Time) { querier.QueryGPUsRequested(s, e) },
		"QueryGPUsUsageAvg":                             func(s, e time.Time) { querier.QueryGPUsUsageAvg(s, e) },
		"QueryGPUsUsageMax":                             func(s, e time.Time) { querier.QueryGPUsUsageMax(s, e) },
		"QueryNodeGPUPricePerHr":                        func(s, e time.Time) { querier.QueryNodeGPUPricePerHr(s, e) },
		"QueryGPUInfo":                                  func(s, e time.Time) { querier.QueryGPUInfo(s, e) },
		"QueryIsGPUShared":                              func(s, e time.Time) { querier.QueryIsGPUShared(s, e) },
		"QueryPodPVCAllocation":                         func(s, e time.Time) { querier.QueryPodPVCAllocation(s, e) },
		"QueryPVCBytesRequested":                        func(s, e time.Time) { querier.QueryPVCBytesRequested(s, e) },
		"QueryPVCInfo":                                  func(s, e time.Time) { querier.QueryPVCInfo(s, e) },
		"QueryPVBytes":                                  func(s, e time.Time) { querier.QueryPVBytes(s, e) },
		"QueryPVPricePerGiBHour":                        func(s, e time.Time) { querier.QueryPVPricePerGiBHour(s, e) },
		"QueryPVInfo":                                   func(s, e time.Time) { querier.QueryPVInfo(s, e) },
		"QueryNetZoneGiB":                               func(s, e time.Time) { querier.QueryNetZoneGiB(s, e) },
		"QueryNetZonePricePerGiB":                       func(s, e time.Time) { querier.QueryNetZonePricePerGiB(s, e) },
		"QueryNetRegionGiB":                             func(s, e time.Time) { querier.QueryNetRegionGiB(s, e) },
		"QueryNetRegionPricePerGiB":                     func(s, e time.Time) { querier.QueryNetRegionPricePerGiB(s, e) },
		"QueryNetInternetGiB":                           func(s, e time.Time) { querier.QueryNetInternetGiB(s, e) },
		"QueryNetInternetPricePerGiB":                   func(s, e time.Time) { querier.QueryNetInternetPricePerGiB(s, e) },
		"QueryNetInternetServiceGiB":                    func(s, e time.Time) { querier.QueryNetInternetServiceGiB(s, e) },
		"QueryNetTransferBytes":                         func(s, e time.Time) { querier.QueryNetTransferBytes(s, e) },
		"QueryNetZoneIngressGiB":                        func(s, e time.Time) { querier.QueryNetZoneIngressGiB(s, e) },
		"QueryNetRegionIngressGiB":                      func(s, e time.Time) { querier.QueryNetRegionIngressGiB(s, e) },
		"QueryNetInternetIngressGiB":                    func(s, e time.Time) { querier.QueryNetInternetIngressGiB(s, e) },
		"QueryNetInternetServiceIngressGiB":             func(s, e time.Time) { querier.QueryNetInternetServiceIngressGiB(s, e) },
		"QueryNetReceiveBytes":                          func(s, e time.Time) { querier.QueryNetReceiveBytes(s, e) },
		"QueryNamespaceAnnotations":                     func(s, e time.Time) { querier.QueryNamespaceAnnotations(s, e) },
		"QueryPodAnnotations":                           func(s, e time.Time) { querier.QueryPodAnnotations(s, e) },
		"QueryNodeLabels":                               func(s, e time.Time) { querier.QueryNodeLabels(s, e) },
		"QueryNamespaceLabels":                          func(s, e time.Time) { querier.QueryNamespaceLabels(s, e) },
		"QueryPodLabels":                                func(s, e time.Time) { querier.QueryPodLabels(s, e) },
		"QueryServiceLabels":                            func(s, e time.Time) { querier.QueryServiceLabels(s, e) },
		"QueryDeploymentLabels":                         func(s, e time.Time) { querier.QueryDeploymentLabels(s, e) },
		"QueryStatefulSetLabels":                        func(s, e time.Time) { querier.QueryStatefulSetLabels(s, e) },
		"QueryDaemonSetLabels":                          func(s, e time.Time) { querier.QueryDaemonSetLabels(s, e) },
		"QueryJobLabels":                                func(s, e time.Time) { querier.QueryJobLabels(s, e) },
		"QueryPodsWithReplicaSetOwner":                  func(s, e time.Time) { querier.QueryPodsWithReplicaSetOwner(s, e) },
		"QueryReplicaSetsWithoutOwners":                 func(s, e time.Time) { querier.QueryReplicaSetsWithoutOwners(s, e) },
		"QueryReplicaSetsWithRollout":                   func(s, e time.Time) { querier.QueryReplicaSetsWithRollout(s, e) },
		"QueryResourceQuotaSpecCPURequestAverage":       func(s, e time.Time) { querier.QueryResourceQuotaSpecCPURequestAverage(s, e) },
		"QueryResourceQuotaSpecCPURequestMax":           func(s, e time.Time) { querier.QueryResourceQuotaSpecCPURequestMax(s, e) },
		"QueryResourceQuotaSpecRAMRequestAverage":       func(s, e time.Time) { querier.QueryResourceQuotaSpecRAMRequestAverage(s, e) },
		"QueryResourceQuotaSpecRAMRequestMax":           func(s, e time.Time) { querier.QueryResourceQuotaSpecRAMRequestMax(s, e) },
		"QueryResourceQuotaSpecCPULimitAverage":         func(s, e time.Time) { querier.QueryResourceQuotaSpecCPULimitAverage(s, e) },
		"QueryResourceQuotaSpecCPULimitMax":             func(s, e time.Time) { querier.QueryResourceQuotaSpecCPULimitMax(s, e) },
		"QueryResourceQuotaSpecRAMLimitAverage":         func(s, e time.Time) { querier.QueryResourceQuotaSpecRAMLimitAverage(s, e) },
		"QueryResourceQuotaSpecRAMLimitMax":             func(s, e time.Time) { querier.QueryResourceQuotaSpecRAMLimitMax(s, e) },
		"QueryResourceQuotaStatusUsedCPURequestAverage": func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedCPURequestAverage(s, e) },
		"QueryResourceQuotaStatusUsedCPURequestMax":     func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedCPURequestMax(s, e) },
		"QueryResourceQuotaStatusUsedRAMRequestAverage": func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedRAMRequestAverage(s, e) },
		"QueryResourceQuotaStatusUsedRAMRequestMax":     func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedRAMRequestMax(s, e) },
		"QueryResourceQuotaStatusUsedCPULimitAverage":   func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedCPULimitAverage(s, e) },
		"QueryResourceQuotaStatusUsedCPULimitMax":       func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedCPULimitMax(s, e) },
		"QueryResourceQuotaStatusUsedRAMLimitAverage":   func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedRAMLimitAverage(s, e) },
		"QueryResourceQuotaStatusUsedRAMLimitMax":       func(s, e time.Time) { querier.QueryResourceQuotaStatusUsedRAMLimitMax(s, e) },
	}

	for testName, queryFunc := range tests {
		t.Run(fmt.Sprintf("TestQueryLog_%s", testName), func(t *testing.T) {
			checkQueryLog(t, logWriter, queryFunc, testName, queryStart, queryEnd)
		})
	}
}

func checkQueryLog(t *testing.T, logWriter *SingleLogWriter, query func(time.Time, time.Time), queryName string, start time.Time, end time.Time) {
	t.Helper()

	// remove the query formatting for the log
	var headerFormat = PrometheusMetricsQueryLogFormat[:len(PrometheusMetricsQueryLogFormat)-3]

	query(start, end)

	// get log output from executing query
	output := logWriter.Log

	expectedHeader := fmt.Sprintf(headerFormat, queryName, end.Unix())
	headerLen := len(expectedHeader)

	if len(output) < headerLen {
		t.Errorf("Expected log header length %d, but got %d", headerLen, len(output))
		return
	}

	actual := output[:headerLen]
	if actual != expectedHeader {
		t.Errorf("Expected log header '%s', but got '%s'", expectedHeader, actual)
		return
	}
}
