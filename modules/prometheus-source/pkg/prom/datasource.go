package prom

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/env"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"

	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
)

const (
	apiPrefix         = "/api/v1"
	epAlertManagers   = apiPrefix + "/alertmanagers"
	epLabelValues     = apiPrefix + "/label/:name/values"
	epSeries          = apiPrefix + "/series"
	epTargets         = apiPrefix + "/targets"
	epSnapshot        = apiPrefix + "/admin/tsdb/snapshot"
	epDeleteSeries    = apiPrefix + "/admin/tsdb/delete_series"
	epCleanTombstones = apiPrefix + "/admin/tsdb/clean_tombstones"
	epConfig          = apiPrefix + "/status/config"
	epFlags           = apiPrefix + "/status/flags"
	epRules           = apiPrefix + "/rules"
)

// helper for query range proxy requests
func toStartEndStep(qp httputil.QueryParams) (start, end time.Time, step time.Duration, err error) {
	var e error

	ss := qp.Get("start", "")
	es := qp.Get("end", "")
	ds := qp.Get("duration", "")
	layout := "2006-01-02T15:04:05.000Z"

	start, e = time.Parse(layout, ss)
	if e != nil {
		err = fmt.Errorf("Error parsing time %s. Error: %s", ss, err)
		return
	}
	end, e = time.Parse(layout, es)
	if e != nil {
		err = fmt.Errorf("Error parsing time %s. Error: %s", es, err)
		return
	}
	step, e = time.ParseDuration(ds)
	if e != nil {
		err = fmt.Errorf("Error parsing duration %s. Error: %s", ds, err)
		return
	}
	err = nil

	return
}

// creates a new help error which indicates the caller can retry and is non-fatal.
func newHelpRetryError(format string, args ...any) error {
	formatWithHelp := format + "\nTroubleshooting help available at: %s"
	args = append(args, PrometheusTroubleshootingURL)

	cause := fmt.Errorf(formatWithHelp, args...)
	return source.NewHelpRetryError(cause)
}

// PrometheusDataSource is the OpenCost data source implementation leveraging Prometheus. Prometheus provides longer retention periods and
// more detailed metrics than the OpenCost Collector, which is useful for historical analysis and cost forecasting.
type PrometheusDataSource struct {
	promConfig   *OpenCostPrometheusConfig
	promClient   prometheus.Client
	promContexts *ContextFactory

	thanosConfig   *OpenCostThanosConfig
	thanosClient   prometheus.Client
	thanosContexts *ContextFactory

	metricsQuerier *PrometheusMetricsQuerier
	clusterMap     clusters.ClusterMap
	clusterInfo    clusters.ClusterInfoProvider
}

// NewDefaultPrometheusDataSource creates and initializes a new `PrometheusDataSource` with configuration
// parsed from environment variables. This function will block until a connection to prometheus is established,
// or fails. It is recommended to run this function in a goroutine on a retry cycle.
func NewDefaultPrometheusDataSource(clusterInfoProvider clusters.ClusterInfoProvider) (*PrometheusDataSource, error) {
	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus config from env: %w", err)
	}

	var thanosConfig *OpenCostThanosConfig
	if env.IsThanosEnabled() {
		// thanos initialization is not fatal, so we log the error and continue
		thanosConfig, err = NewOpenCostThanosConfigFromEnv()
		if err != nil {
			log.Warnf("Thanos was enabled, but failed to create thanos config from env: %s. Continuing...", err.Error())
		}
	}

	return NewPrometheusDataSource(clusterInfoProvider, config, thanosConfig)
}

// NewPrometheusDataSource initializes clients for Prometheus and Thanos, and returns a new PrometheusDataSource.
func NewPrometheusDataSource(infoProvider clusters.ClusterInfoProvider, promConfig *OpenCostPrometheusConfig, thanosConfig *OpenCostThanosConfig) (*PrometheusDataSource, error) {
	promClient, err := NewPrometheusClient(promConfig.ServerEndpoint, promConfig.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build prometheus client: %w", err)
	}

	// validation of the prometheus client

	m, err := Validate(promClient, promConfig)
	if err != nil || !m.Running {
		if err != nil {
			return nil, newHelpRetryError("failed to query prometheus at %s: %w", promConfig.ServerEndpoint, err)
		} else if !m.Running {
			return nil, newHelpRetryError("prometheus at %s is not running", promConfig.ServerEndpoint)
		}
	} else {
		log.Infof("Success: retrieved the 'up' query against prometheus at: %s", promConfig.ServerEndpoint)
	}

	// we don't consider this a fatal error, but we log for visibility
	api := prometheusAPI.NewAPI(promClient)
	bi, err := api.Buildinfo(context.Background())

	if err != nil {
		log.Infof("No valid prometheus config file at %s. Error: %s.\nTroubleshooting help available at: %s.\n**Ignore if using cortex/mimir/thanos here**", promConfig.ServerEndpoint, err.Error(), PrometheusTroubleshootingURL)
	} else {
		log.Infof("Retrieved a prometheus config file from: %s", promConfig.ServerEndpoint)
		promConfig.Version = bi.Version

		// for versions of prometheus >= 3.0.0, we need to offset the resolution for range queries
		// due to a breaking change in prometheus lookback and range query alignment
		v, err := semver.NewVersion(promConfig.Version)
		if err != nil {
			log.Warnf("Failed to parse prometheus version %s. Error: %s", promConfig.Version, err.Error())
		} else {
			promConfig.IsOffsetResolution = v.Major() >= 3
		}
	}

	// Fix scrape interval if zero by attempting to lookup the interval for the configured job
	if promConfig.ScrapeInterval == 0 {
		promConfig.ScrapeInterval = time.Minute

		// Lookup scrape interval for kubecost job, update if found
		si, err := ScrapeIntervalFor(promClient, promConfig.JobName)
		if err == nil {
			promConfig.ScrapeInterval = si
		}
	}

	log.Infof("Using scrape interval of %f", promConfig.ScrapeInterval.Seconds())

	promContexts := NewContextFactory(promClient, promConfig)

	var thanosClient prometheus.Client
	var thanosContexts *ContextFactory

	// if the thanos configuration is non-nil, we assume intent to use thanos. However, failure to
	// initialize the thanos client is not fatal, and we will log the error and continue.
	if thanosConfig != nil {
		thanosHost := thanosConfig.ServerEndpoint
		if thanosHost != "" {
			thanosCli, _ := NewThanosClient(thanosHost, thanosConfig)

			_, err = Validate(thanosCli, thanosConfig.OpenCostPrometheusConfig)
			if err != nil {
				log.Warnf("Failed to query Thanos at %s. Error: %s.", thanosHost, err.Error())
				thanosClient = thanosCli
			} else {
				log.Infof("Success: retrieved the 'up' query against Thanos at: %s", thanosHost)

				thanosClient = thanosCli
			}

			thanosContexts = NewContextFactory(thanosClient, thanosConfig.OpenCostPrometheusConfig)
		} else {
			log.Infof("Error resolving environment variable: $%s", env.ThanosQueryUrlEnvVar)
		}
	}

	// metadata creation for cluster info
	thanosEnabled := thanosClient != nil
	metadata := map[string]string{
		clusters.ClusterInfoThanosEnabledKey: fmt.Sprintf("%t", thanosEnabled),
	}
	if thanosEnabled {
		metadata[clusters.ClusterInfoThanosOffsetKey] = thanosConfig.Offset
	}

	// cluster info provider
	clusterInfoProvider := clusters.NewClusterInfoDecorator(infoProvider, metadata)

	var clusterMap clusters.ClusterMap
	if thanosEnabled {
		clusterMap = newPrometheusClusterMap(thanosContexts, clusterInfoProvider, 10*time.Minute)
	} else {
		clusterMap = newPrometheusClusterMap(promContexts, clusterInfoProvider, 5*time.Minute)
	}

	// create metrics querier implementation for prometheus and thanos
	metricsQuerier := newPrometheusMetricsQuerier(
		promConfig,
		promClient,
		promContexts,
		thanosConfig,
		thanosClient,
		thanosContexts,
	)

	return &PrometheusDataSource{
		promConfig:     promConfig,
		promClient:     promClient,
		promContexts:   promContexts,
		thanosConfig:   thanosConfig,
		thanosClient:   thanosClient,
		thanosContexts: thanosContexts,
		metricsQuerier: metricsQuerier,
		clusterMap:     clusterMap,
		clusterInfo:    clusterInfoProvider,
	}, nil
}

var proto = protocol.HTTP()

// prometheusMetadata returns the metadata for the prometheus server
func (pds *PrometheusDataSource) prometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	resp := proto.ToResponse(Validate(pds.promClient, pds.promConfig))
	proto.WriteResponse(w, resp)
}

// prometheusRecordingRules is a proxy for /rules against prometheus
func (pds *PrometheusDataSource) prometheusRecordingRules(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := pds.promClient.URL(epRules, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "error creating Prometheus rule request: %s", err)
		return
	}

	_, body, err := pds.promClient.Do(r.Context(), req)
	if err != nil {
		fmt.Fprintf(w, "error making Prometheus rule request: %s", err)
		return
	}

	w.Write(body)
}

// prometheusConfig returns the current configuration of the prometheus server
func (pds *PrometheusDataSource) prometheusConfig(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	pConfig := map[string]string{
		"address": pds.promConfig.ServerEndpoint,
	}

	body, err := json.Marshal(pConfig)
	if err != nil {
		fmt.Fprintf(w, "Error marshalling prometheus config")
	} else {
		w.Write(body)
	}
}

// prometheusTargets is a proxy for /targets against prometheus
func (pds *PrometheusDataSource) prometheusTargets(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	u := pds.promClient.URL(epTargets, nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		fmt.Fprintf(w, "error creating Prometheus rule request: %s", err)
		return
	}

	_, body, err := pds.promClient.Do(r.Context(), req)
	if err != nil {
		fmt.Fprintf(w, "error making Prometheus rule request: %s", err)
		return
	}

	w.Write(body)
}

// status returns the status of the prometheus client
func (pds *PrometheusDataSource) status(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promServer := pds.promConfig.ServerEndpoint

	api := prometheusAPI.NewAPI(pds.promClient)
	result, err := api.Buildinfo(r.Context())
	if err != nil {
		fmt.Fprintf(w, "Using Prometheus at %s, Error: %s", promServer, err)
	} else {
		fmt.Fprintf(w, "Using Prometheus at %s, version: %s", promServer, result.Version)
	}
}

// prometheusQuery is a proxy for /query against prometheus
func (pds *PrometheusDataSource) prometheusQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Query Parameter 'query' is unset'")))
		return
	}

	// Attempt to parse time as either a unix timestamp or as an RFC3339 value
	var timeVal time.Time
	timeStr := qp.Get("time", "")
	if len(timeStr) > 0 {
		if t, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			timeVal = time.Unix(t, 0)
		} else if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			timeVal = t
		}

		// If time is given, but not parse-able, return an error
		if timeVal.IsZero() {
			http.Error(w, fmt.Sprintf("time must be a unix timestamp or RFC3339 value; illegal value given: %s", timeStr), http.StatusBadRequest)
		}
	}

	ctx := pds.promContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQuery(query, timeVal)
	if err != nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Error running query %s. Error: %s", query, err)))
		return
	}

	w.Write(body) // prometheusQueryRange is a proxy for /query_range against prometheus
}

func (pds *PrometheusDataSource) prometheusQueryRange(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		fmt.Fprintf(w, "Error parsing query from request parameters.")
		return
	}

	start, end, duration, err := toStartEndStep(qp)
	if err != nil {
		fmt.Fprintf(w, "error: %s", err)
		return
	}

	ctx := pds.promContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQueryRange(query, start, end, duration)
	if err != nil {
		fmt.Fprintf(w, "Error running query %s. Error: %s", query, err)
		return
	}

	w.Write(body)
}

// thanosQuery is a proxy for /query against thanos
func (pds *PrometheusDataSource) thanosQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if pds.thanosClient == nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("ThanosDisabled")))
		return
	}

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Query Parameter 'query' is unset'")))
		return
	}

	// Attempt to parse time as either a unix timestamp or as an RFC3339 value
	var timeVal time.Time
	timeStr := qp.Get("time", "")
	if len(timeStr) > 0 {
		if t, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			timeVal = time.Unix(t, 0)
		} else if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			timeVal = t
		}

		// If time is given, but not parse-able, return an error
		if timeVal.IsZero() {
			http.Error(w, fmt.Sprintf("time must be a unix timestamp or RFC3339 value; illegal value given: %s", timeStr), http.StatusBadRequest)
		}
	}

	ctx := pds.thanosContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQuery(query, timeVal)
	if err != nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("Error running query %s. Error: %s", query, err)))
		return
	}

	w.Write(body)
}

// thanosQueryRange is a proxy for /query_range against thanos
func (pds *PrometheusDataSource) thanosQueryRange(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if pds.thanosClient == nil {
		proto.WriteResponse(w, proto.ToResponse(nil, fmt.Errorf("ThanosDisabled")))
		return
	}

	qp := httputil.NewQueryParams(r.URL.Query())
	query := qp.Get("query", "")
	if query == "" {
		fmt.Fprintf(w, "Error parsing query from request parameters.")
		return
	}

	start, end, duration, err := toStartEndStep(qp)
	if err != nil {
		fmt.Fprintf(w, "error: %s", err)
		return
	}

	ctx := pds.thanosContexts.NewNamedContext(FrontendContextName)
	body, err := ctx.RawQueryRange(query, start, end, duration)
	if err != nil {
		fmt.Fprintf(w, "Error running query %s. Error: %s", query, err)
		return
	}

	w.Write(body)
}

// promtheusQueueState returns the current state of the prometheus and thanos request queues
func (pds *PrometheusDataSource) prometheusQueueState(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promQueueState, err := GetPrometheusQueueState(pds.promClient, pds.promConfig)
	if err != nil {
		proto.WriteResponse(w, proto.ToResponse(nil, err))
		return
	}

	result := map[string]*PrometheusQueueState{
		"prometheus": promQueueState,
	}

	if pds.thanosClient != nil {
		thanosQueueState, err := GetPrometheusQueueState(pds.thanosClient, pds.thanosConfig.OpenCostPrometheusConfig)
		if err != nil {
			log.Warnf("Error getting Thanos queue state: %s", err)
		} else {
			result["thanos"] = thanosQueueState
		}
	}

	proto.WriteResponse(w, proto.ToResponse(result, nil))
}

// prometheusMetrics retrieves availability of Prometheus and Thanos metrics
func (pds *PrometheusDataSource) prometheusMetrics(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	promMetrics := GetPrometheusMetrics(pds.promClient, pds.promConfig, "")

	result := map[string][]*PrometheusDiagnostic{
		"prometheus": promMetrics,
	}

	if pds.thanosClient != nil {
		thanosMetrics := GetPrometheusMetrics(pds.thanosClient, pds.thanosConfig.OpenCostPrometheusConfig, pds.thanosConfig.Offset)
		result["thanos"] = thanosMetrics
	}

	proto.WriteResponse(w, proto.ToResponse(result, nil))
}

func (pds *PrometheusDataSource) PrometheusClient() prometheus.Client {
	return pds.promClient
}

func (pds *PrometheusDataSource) PrometheusConfig() *OpenCostPrometheusConfig {
	return pds.promConfig
}

func (pds *PrometheusDataSource) PrometheusContexts() *ContextFactory {
	return pds.promContexts
}

func (pds *PrometheusDataSource) ThanosClient() prometheus.Client {
	return pds.thanosClient
}

func (pds *PrometheusDataSource) ThanosConfig() *OpenCostThanosConfig {
	return pds.thanosConfig
}

func (pds *PrometheusDataSource) ThanosContexts() *ContextFactory {
	return pds.thanosContexts
}

func (pds *PrometheusDataSource) RegisterEndPoints(router *httprouter.Router) {
	// endpoints migrated from server
	router.GET("/validatePrometheus", pds.prometheusMetadata)
	router.GET("/prometheusRecordingRules", pds.prometheusRecordingRules)
	router.GET("/prometheusConfig", pds.prometheusConfig)
	router.GET("/prometheusTargets", pds.prometheusTargets)
	router.GET("/status", pds.status)

	// prom query proxies
	router.GET("/prometheusQuery", pds.prometheusQuery)
	router.GET("/prometheusQueryRange", pds.prometheusQueryRange)
	router.GET("/thanosQuery", pds.thanosQuery)
	router.GET("/thanosQueryRange", pds.thanosQueryRange)

	// diagnostics
	router.GET("/diagnostics/requestQueue", pds.prometheusQueueState)
	router.GET("/diagnostics/prometheusMetrics", pds.prometheusMetrics)
}

// RegisterDiagnostics registers any custom data source diagnostics with the `DiagnosticService` that can
// be used to report externally.
func (pds *PrometheusDataSource) RegisterDiagnostics(diagService diagnostics.DiagnosticService) {
	const PrometheusDiagnosticCategory = "prometheus"

	for _, dd := range diagnosticDefinitions {
		err := diagService.Register(dd.ID, dd.Description, PrometheusDiagnosticCategory, func(ctx context.Context) (map[string]any, error) {
			promDiag := dd.NewDiagnostic(pds.promConfig.ClusterFilter, "")

			promContext := pds.promContexts.NewNamedContext(DiagnosticContextName)
			e := promDiag.executePrometheusDiagnosticQuery(promContext)
			if e != nil {
				return nil, fmt.Errorf("failed to execute prometheus diagnostic: %s - %w", dd.ID, e)
			}

			return promDiag.AsMap(), nil
		})

		if err != nil {
			log.Warnf("Failed to register prometheus diagnostic %s: %s", dd.ID, err.Error())
		}
	}
}

func (pds *PrometheusDataSource) RefreshInterval() time.Duration {
	return pds.promConfig.ScrapeInterval
}

func (pds *PrometheusDataSource) Metrics() source.MetricsQuerier {
	return pds.metricsQuerier
}

func (pds *PrometheusDataSource) ClusterMap() clusters.ClusterMap {
	return pds.clusterMap
}

// ClusterInfo returns the ClusterInfoProvider for the local cluster.
func (pds *PrometheusDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	return pds.clusterInfo
}

func (pds *PrometheusDataSource) BatchDuration() time.Duration {
	return pds.promConfig.MaxQueryDuration
}

func (pds *PrometheusDataSource) Resolution() time.Duration {
	return pds.promConfig.DataResolution
}
