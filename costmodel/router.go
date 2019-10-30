package costmodel

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/julienschmidt/httprouter"
	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	"github.com/patrickmn/go-cache"
	prometheusClient "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	prometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"
	prometheusTroubleshootingEp    = "http://docs.kubecost.com/custom-prom#troubleshoot"
	RFC3339Milli                   = "2006-01-02T15:04:05.000Z"
)

var (
	// gitCommit is set by the build system
	gitCommit string
)

var Router = httprouter.New()
var A Accesses

type Accesses struct {
	PrometheusClient              prometheusClient.Client
	ThanosClient                  prometheusClient.Client
	KubeClientSet                 kubernetes.Interface
	Cloud                         costAnalyzerCloud.Provider
	CPUPriceRecorder              *prometheus.GaugeVec
	RAMPriceRecorder              *prometheus.GaugeVec
	PersistentVolumePriceRecorder *prometheus.GaugeVec
	GPUPriceRecorder              *prometheus.GaugeVec
	NodeTotalPriceRecorder        *prometheus.GaugeVec
	RAMAllocationRecorder         *prometheus.GaugeVec
	CPUAllocationRecorder         *prometheus.GaugeVec
	GPUAllocationRecorder         *prometheus.GaugeVec
	PVAllocationRecorder          *prometheus.GaugeVec
	ContainerUptimeRecorder       *prometheus.GaugeVec
	NetworkZoneEgressRecorder     prometheus.Gauge
	NetworkRegionEgressRecorder   prometheus.Gauge
	NetworkInternetEgressRecorder prometheus.Gauge
	ServiceSelectorRecorder       *prometheus.GaugeVec
	DeploymentSelectorRecorder    *prometheus.GaugeVec
	Model                         *CostModel
	AggregateCache                *cache.Cache
	CostDataCache                 *cache.Cache
	OutOfClusterCache             *cache.Cache
}

type DataEnvelope struct {
	Code    int         `json:"code"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
}

// filterCostData allows through only CostData that matches the given filters for namespace and clusterId
func filterCostData(data map[string]*CostData, namespace, clusterId string) map[string]*CostData {
	result := make(map[string]*CostData)
	for key, datum := range data {
		if costDataPassesFilters(datum, namespace, clusterId) {
			result[key] = datum
		}
	}
	return result
}

func filterFields(fields string, data map[string]*CostData) map[string]CostData {
	fs := strings.Split(fields, ",")
	fmap := make(map[string]bool)
	for _, f := range fs {
		fieldNameLower := strings.ToLower(f) // convert to go struct name by uppercasing first letter
		klog.V(1).Infof("to delete: %s", fieldNameLower)
		fmap[fieldNameLower] = true
	}
	filteredData := make(map[string]CostData)
	for cname, costdata := range data {
		s := reflect.TypeOf(*costdata)
		val := reflect.ValueOf(*costdata)
		costdata2 := CostData{}
		cd2 := reflect.New(reflect.Indirect(reflect.ValueOf(costdata2)).Type()).Elem()
		n := s.NumField()
		for i := 0; i < n; i++ {
			field := s.Field(i)
			value := val.Field(i)
			value2 := cd2.Field(i)
			if _, ok := fmap[strings.ToLower(field.Name)]; !ok {
				value2.Set(reflect.Value(value))
			}
		}
		filteredData[cname] = cd2.Interface().(CostData)
	}
	return filteredData
}

func normalizeTimeParam(param string) (string, error) {
	// convert days to hours
	if param[len(param)-1:] == "d" {
		count := param[:len(param)-1]
		val, err := strconv.ParseInt(count, 10, 64)
		if err != nil {
			return "", err
		}
		val = val * 24
		param = fmt.Sprintf("%dh", val)
	}

	return param, nil
}

// parsePercentString takes a string of expected format "N%" and returns a floating point 0.0N.
// If the "%" symbol is missing, it just returns 0.0N. Empty string is interpreted as "0%" and
// return 0.0.
func parsePercentString(percentStr string) (float64, error) {
	if len(percentStr) == 0 {
		return 0.0, nil
	}
	if percentStr[len(percentStr)-1:] == "%" {
		percentStr = percentStr[:len(percentStr)-1]
	}
	discount, err := strconv.ParseFloat(percentStr, 64)
	if err != nil {
		return 0.0, err
	}
	discount *= 0.01

	return discount, nil
}

// parseDuration converts a Prometheus-style duration string into a Duration
func parseDuration(duration string) (*time.Duration, error) {
	unitStr := duration[len(duration)-1:]
	var unit time.Duration
	switch unitStr {
	case "s":
		unit = time.Second
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24.0 * time.Hour
	default:
		return nil, fmt.Errorf("error parsing duration: %s did not match expected format [0-9+](s|m|d|h)", duration)
	}

	amountStr := duration[:len(duration)-1]
	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing duration: %s did not match expected format [0-9+](s|m|d|h)", duration)
	}

	dur := time.Duration(amount) * unit
	return &dur, nil
}

// parseTimeRange returns a start and end time, respectively, which are converted from
// a duration and offset, defined as strings with Prometheus-style syntax.
func parseTimeRange(duration, offset string) (*time.Time, *time.Time, error) {
	// endTime defaults to the current time, unless an offset is explicity declared,
	// in which case it shifts endTime back by given duration
	endTime := time.Now()
	if offset != "" {
		o, err := time.ParseDuration(offset)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)
		}
		endTime = endTime.Add(-1 * o)
	}

	// if duration is defined in terms of days, convert to hours
	// e.g. convert "2d" to "48h"
	durationNorm, err := normalizeTimeParam(duration)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing duration (%s): %s", duration, err)
	}

	// convert time duration into start and end times, formatted
	// as ISO datetime strings
	dur, err := time.ParseDuration(durationNorm)
	if err != nil {
		return nil, nil, fmt.Errorf("errorf parsing duration (%s): %s", durationNorm, err)
	}
	startTime := endTime.Add(-1 * dur)

	return &startTime, &endTime, nil
}

func wrapDataWithMessage(data interface{}, err error, message string) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Message: message,
		})

	}

	return resp
}

func wrapData(data interface{}, err error) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})

	}

	return resp
}

// RefreshPricingData needs to be called when a new node joins the fleet, since we cache the relevant subsets of pricing data to avoid storing the whole thing.
func (a *Accesses) RefreshPricingData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := a.Cloud.DownloadPricingData()

	w.Write(wrapData(nil, err))
}

func (a *Accesses) CostDataModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("timeWindow")
	offset := r.URL.Query().Get("offset")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")
	aggregationField := r.URL.Query().Get("aggregation")
	subfields := strings.Split(r.URL.Query().Get("aggregationSubfield"), ",")

	if offset != "" {
		offset = "offset " + offset
	}

	data, err := a.Model.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, window, offset, namespace)
	if aggregationField != "" {
		c, err := a.Cloud.GetConfig()
		if err != nil {
			w.Write(wrapData(nil, err))
			return
		}

		discount, err := parsePercentString(c.Discount)
		if err != nil {
			w.Write(wrapData(nil, err))
			return
		}

		dur, err := time.ParseDuration(window)
		if err != nil {
			w.Write(wrapData(nil, err))
			return
		}
		// dataCount is the number of time series data expected for the given interval,
		// which we compute because Prometheus time series vectors omit zero values.
		// This assumes hourly data, incremented by one to capture the 0th data point.
		dataCount := int(dur.Hours())

		opts := &AggregationOptions{
			DataCount:        dataCount,
			Discount:         discount,
			IdleCoefficients: make(map[string]float64),
		}
		agg := AggregateCostData(data, aggregationField, subfields, a.Cloud, opts)
		w.Write(wrapData(agg, nil))
	} else {
		if fields != "" {
			filteredData := filterFields(fields, data)
			w.Write(wrapData(filteredData, err))
		} else {
			w.Write(wrapData(data, err))
		}
	}
}

func (a *Accesses) ClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	data, err := ClusterCosts(a.PrometheusClient, a.Cloud, window, offset)
	w.Write(wrapData(data, err))
}

func (a *Accesses) ClusterCostsOverTime(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	data, err := ClusterCostsOverTime(a.PrometheusClient, a.Cloud, start, end, window, offset)
	w.Write(wrapData(data, err))
}

// AggregateCostModel handles HTTP requests to the aggregated cost model API, which can be parametrized
// by time period using window and offset, aggregation field and subfield (e.g. grouping by label.app
// using aggregation=label, aggregationSubfield=app), and filtered by namespace and cluster.
func (a *Accesses) AggregateCostModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	duration := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	field := r.URL.Query().Get("aggregation")
	subfieldStr := r.URL.Query().Get("aggregationSubfield")
	rate := r.URL.Query().Get("rate")
	allocateIdle := r.URL.Query().Get("allocateIdle") == "true"
	sharedNamespaces := r.URL.Query().Get("sharedNamespaces")
	sharedLabelNames := r.URL.Query().Get("sharedLabelNames")
	sharedLabelValues := r.URL.Query().Get("sharedLabelValues")
	remote := r.URL.Query().Get("remote") != "false"

	subfields := []string{}
	if len(subfieldStr) > 0 {
		subfields = strings.Split(r.URL.Query().Get("aggregationSubfield"), ",")
	}

	// timeSeries == true maintains the time series dimension of the data,
	// which by default gets summed over the entire interval
	includeTimeSeries := r.URL.Query().Get("timeSeries") == "true"

	// efficiency == true aggregates and returns usage and efficiency data
	includeEfficiency := r.URL.Query().Get("efficiency") == "true"

	// disableCache, if set to "true", tells this function to recompute and
	// cache the requested data
	disableCache := r.URL.Query().Get("disableCache") == "true"

	// clearCache, if set to "true", tells this function to flush the cache,
	// then recompute and cache the requested data
	clearCache := r.URL.Query().Get("clearCache") == "true"

	// aggregation field is required
	if field == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(wrapData(nil, fmt.Errorf("Missing aggregation field parameter")))
		return
	}

	// aggregation subfield is required when aggregation field is "label"
	if field == "label" && len(subfields) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(wrapData(nil, fmt.Errorf("Missing aggregation subfield parameter for aggregation by label")))
		return
	}

	// enforce one of four available rate options
	if rate != "" && rate != "hourly" && rate != "daily" && rate != "monthly" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(wrapData(nil, fmt.Errorf("If set, rate parameter must be one of: 'hourly', 'daily', 'monthly'")))
		return
	}

	// clear cache prior to checking the cache so that a clearCache=true
	// request always returns a freshly computed value
	if clearCache {
		a.AggregateCache.Flush()
		a.CostDataCache.Flush()
	}

	// parametrize cache key by all request parameters
	aggKey := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%t:%t:%t",
		duration, offset, namespace, cluster, field, strings.Join(subfields, ","), rate,
		allocateIdle, includeTimeSeries, includeEfficiency)

	// check the cache for aggregated response; if cache is hit and not disabled, return response
	if result, found := a.AggregateCache.Get(aggKey); found && !disableCache {
		w.Write(wrapDataWithMessage(result, nil, fmt.Sprintf("aggregate cache hit: %s", aggKey)))
		return
	}

	// enable remote if it is available and not disabled
	remoteAvailable := os.Getenv(remoteEnabled) == "true"
	remoteEnabled := remote && remoteAvailable

	// Use Thanos Client if it exists (enabled) and remote flag set
	var pClient prometheusClient.Client
	if remote && a.ThanosClient != nil {
		pClient = a.ThanosClient
	} else {
		pClient = a.PrometheusClient
	}

	// convert duration and offset to start and end times
	startTime, endTime, err := parseTimeRange(duration, offset)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(wrapData(nil, fmt.Errorf("Error parsing duration (%s) and offset (%s)", duration, offset)))
		return
	}
	durationHours := endTime.Sub(*startTime).Hours()

	threeHoursAgo := time.Now().Add(-3 * time.Hour)
	if a.ThanosClient != nil && endTime.After(threeHoursAgo) {
		klog.Infof("Setting end time backwards to first present data")
		*endTime = time.Now().Add(-3 * time.Hour)
	}

	// determine resolution by size of duration
	resolution := "1h"
	if durationHours >= 2160 {
		// 90 days
		resolution = "72h"
	} else if durationHours >= 720 {
		// 30 days
		resolution = "24h"
	} else if durationHours >= 168 {
		// 7 days
		resolution = "6h"
	} else if durationHours >= 48 {
		// 2 days
		resolution = "2h"
	}
	resolutionDuration, err := parseDuration(resolution)
	resolutionHours := resolutionDuration.Hours()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(wrapData(nil, fmt.Errorf("Error parsing resolution (%s)", resolution)))
		return
	}

	// exclude the last window of the time frame to match Prometheus definitions of range, offset, and resolution
	//   e.g. requesting duration=2d, offset=1d, resolution=1h on Jan 4 12:00:00 should provide data for Jan 1 12:00 - Jan 3 12:00
	//        which has the equivalent start and end times of Jan 1 1:00 and Jan 3 12:00, respectively.
	*startTime = startTime.Add(1 * *resolutionDuration)

	// attempt to retrieve cost data from cache
	var costData map[string]*CostData
	key := fmt.Sprintf(`%s:%s:%s:%t`, duration, offset, resolution, remoteEnabled)
	cacheData, found := a.CostDataCache.Get(key)
	if found && !disableCache {
		ok := false
		costData, ok = cacheData.(map[string]*CostData)
		if !ok {
			klog.Errorf("caching error: failed to cast cost data to struct: %s", key)
		}
	} else {
		start := startTime.Format(RFC3339Milli)
		end := endTime.Format(RFC3339Milli)
		costData, err = a.Model.ComputeCostDataRange(pClient, a.KubeClientSet, a.Cloud, start, end, resolution, "", "", remoteEnabled)
		if err != nil {
			w.Write(wrapData(nil, err))
			return
		}

		a.CostDataCache.Set(key, costData, cache.DefaultExpiration)
	}

	// filter cost data by namespace and cluster after caching for maximal cache hits
	costData = filterCostData(costData, namespace, cluster)

	c, err := a.Cloud.GetConfig()
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}
	discount, err := parsePercentString(c.Discount)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	idleCoefficients := make(map[string]float64)
	if allocateIdle {
		windowStr := fmt.Sprintf("%dh", int(durationHours))
		if a.ThanosClient != nil {
			klog.Infof("Setting offset to 3h")
			offset = "3h"
		}
		idleCoefficients, err = ComputeIdleCoefficient(costData, pClient, a.Cloud, discount, windowStr, offset, resolution)
		if err != nil {
			klog.Errorf("error computing idle coefficient: windowString=%s, offset=%s, err=%s", windowStr, offset, err)
			w.Write(wrapData(nil, err))
			return
		}
	}

	sn := []string{}
	sln := []string{}
	slv := []string{}
	if sharedNamespaces != "" {
		sn = strings.Split(sharedNamespaces, ",")
	}
	if sharedLabelNames != "" {
		sln = strings.Split(sharedLabelNames, ",")
		slv = strings.Split(sharedLabelValues, ",")
		if len(sln) != len(slv) || slv[0] == "" {
			w.Write(wrapData(nil, fmt.Errorf("Supply exacly one label value per label name")))
			return
		}
	}
	var sr *SharedResourceInfo
	if len(sn) > 0 || len(sln) > 0 {
		sr = NewSharedResourceInfo(true, sn, sln, slv)
	}

	for cid, idleCoefficient := range idleCoefficients {
		klog.Infof("Idle Coeff: %s: %f", cid, idleCoefficient)
	}

	dataCount := int(durationHours / resolutionHours)
	klog.V(1).Infof("data count = %d for duration (%fh) resolution (%fh)", dataCount, durationHours, resolutionHours)

	// aggregate cost model data by given fields and cache the result for the default expiration
	opts := &AggregationOptions{
		DataCount:             dataCount,
		Discount:              discount,
		IdleCoefficients:      idleCoefficients,
		IncludeEfficiency:     includeEfficiency,
		IncludeTimeSeries:     includeTimeSeries,
		Rate:                  rate,
		ResolutionCoefficient: resolutionHours,
		SharedResourceInfo:    sr,
	}
	result := AggregateCostData(costData, field, subfields, a.Cloud, opts)
	a.AggregateCache.Set(aggKey, result, cache.DefaultExpiration)

	w.Write(wrapDataWithMessage(result, nil, fmt.Sprintf("aggregate cache miss: %s", aggKey)))
}

func (a *Accesses) CostDataModelRange(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	aggregationField := r.URL.Query().Get("aggregation")
	subfields := strings.Split(r.URL.Query().Get("aggregationSubfield"), ",")
	remote := r.URL.Query().Get("remote")

	remoteAvailable := os.Getenv(remoteEnabled)
	remoteEnabled := false
	if remoteAvailable == "true" && remote != "false" {
		remoteEnabled = true
	}

	// Use Thanos Client if it exists (enabled) and remote flag set
	var pClient prometheusClient.Client
	if remote != "false" && a.ThanosClient != nil {
		pClient = a.ThanosClient
	} else {
		pClient = a.PrometheusClient
	}

	data, err := a.Model.ComputeCostDataRange(pClient, a.KubeClientSet, a.Cloud, start, end, window, namespace, cluster, remoteEnabled)
	if err != nil {
		w.Write(wrapData(nil, err))
	}
	if aggregationField != "" {
		c, err := a.Cloud.GetConfig()
		if err != nil {
			w.Write(wrapData(nil, err))
			return
		}

		discount, err := parsePercentString(c.Discount)
		if err != nil {
			w.Write(wrapData(nil, err))
			return
		}

		opts := &AggregationOptions{
			Discount:         discount,
			IdleCoefficients: make(map[string]float64),
		}
		agg := AggregateCostData(data, aggregationField, subfields, a.Cloud, opts)
		w.Write(wrapData(agg, nil))
	} else {
		if fields != "" {
			filteredData := filterFields(fields, data)
			w.Write(wrapData(filteredData, err))
		} else {
			w.Write(wrapData(data, err))
		}
	}
}

// CostDataModelRangeLarge is experimental multi-cluster and long-term data storage in SQL support.
func (a *Accesses) CostDataModelRangeLarge(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	startString := r.URL.Query().Get("start")
	endString := r.URL.Query().Get("end")
	windowString := r.URL.Query().Get("window")

	var start time.Time
	var end time.Time
	var err error

	if windowString == "" {
		windowString = "1h"
	}
	if startString != "" {
		start, err = time.Parse(RFC3339Milli, startString)
		if err != nil {
			klog.V(1).Infof("Error parsing time " + startString + ". Error: " + err.Error())
			w.Write(wrapData(nil, err))
		}
	} else {
		window, err := time.ParseDuration(windowString)
		if err != nil {
			w.Write(wrapData(nil, fmt.Errorf("Invalid duration '%s'", windowString)))

		}
		start = time.Now().Add(-2 * window)
	}
	if endString != "" {
		end, err = time.Parse(RFC3339Milli, endString)
		if err != nil {
			klog.V(1).Infof("Error parsing time " + endString + ". Error: " + err.Error())
			w.Write(wrapData(nil, err))
		}
	} else {
		end = time.Now()
	}

	remoteLayout := "2006-01-02T15:04:05Z"
	remoteStartStr := start.Format(remoteLayout)
	remoteEndStr := end.Format(remoteLayout)
	klog.V(1).Infof("Using remote database for query from %s to %s with window %s", startString, endString, windowString)

	data, err := CostDataRangeFromSQL("", "", windowString, remoteStartStr, remoteEndStr)
	w.Write(wrapData(data, err))
}

func (a *Accesses) OutofClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	aggregator := r.URL.Query().Get("aggregator")
	customAggregation := r.URL.Query().Get("customAggregation")
	var data []*costAnalyzerCloud.OutOfClusterAllocation
	var err error
	if customAggregation != "" {
		data, err = a.Cloud.ExternalAllocations(start, end, customAggregation)
	} else {
		data, err = a.Cloud.ExternalAllocations(start, end, "kubernetes_"+aggregator)
	}
	w.Write(wrapData(data, err))
}

func (a *Accesses) OutOfClusterCostsWithCache(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// start date for which to query costs, inclusive; format YYYY-MM-DD
	start := r.URL.Query().Get("start")
	// end date for which to query costs, inclusive; format YYYY-MM-DD
	end := r.URL.Query().Get("end")
	// aggregator sets the field by which to aggregate; default, prepended by "kubernetes_"
	kubernetesAggregation := r.URL.Query().Get("aggregator")
	// customAggregation allows full customization of aggregator w/o prepending
	customAggregation := r.URL.Query().Get("customAggregation")
	// disableCache, if set to "true", tells this function to recompute and
	// cache the requested data
	disableCache := r.URL.Query().Get("disableCache") == "true"
	// clearCache, if set to "true", tells this function to flush the cache,
	// then recompute and cache the requested data
	clearCache := r.URL.Query().Get("clearCache") == "true"

	aggregation := "kubernetes_" + kubernetesAggregation
	if customAggregation != "" {
		aggregation = customAggregation
	}

	// clear cache prior to checking the cache so that a clearCache=true
	// request always returns a freshly computed value
	if clearCache {
		a.OutOfClusterCache.Flush()
	}

	// attempt to retrieve cost data from cache
	key := fmt.Sprintf(`%s:%s:%s`, start, end, aggregation)
	if value, found := a.OutOfClusterCache.Get(key); found && !disableCache {
		if data, ok := value.([]*costAnalyzerCloud.OutOfClusterAllocation); ok {
			w.Write(wrapDataWithMessage(data, nil, fmt.Sprintf("out of cluser cache hit: %s", key)))
			return
		}
		klog.Errorf("caching error: failed to type cast data: %s", key)
	}

	data, err := a.Cloud.ExternalAllocations(start, end, aggregation)
	if err == nil {
		a.OutOfClusterCache.Set(key, data, cache.DefaultExpiration)
	}

	w.Write(wrapDataWithMessage(data, err, fmt.Sprintf("out of cluser cache miss: %s", key)))
}

func (p *Accesses) GetAllNodePricing(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.AllNodePricing()
	w.Write(wrapData(data, err))
}

func (p *Accesses) GetConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.GetConfig()
	w.Write(wrapData(data, err))
}

func (p *Accesses) UpdateSpotInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.SpotInfoUpdateType)
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	err = p.Cloud.DownloadPricingData()
	if err != nil {
		klog.V(1).Infof("Error redownloading data on config update: %s", err.Error())
	}
	return
}

func (p *Accesses) UpdateAthenaInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.AthenaInfoUpdateType)
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) UpdateBigQueryInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.BigqueryUpdateType)
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) UpdateConfigByKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, "")
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) ManagementPlatform(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.GetManagementPlatform()
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) ClusterInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.ClusterInfo()
	w.Write(wrapData(data, err))

}

func Healthz(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

func (p *Accesses) GetPrometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(wrapData(ValidatePrometheus(p.PrometheusClient, false)))
}

func (p *Accesses) ContainerUptimes(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	res, err := ComputeUptimes(p.PrometheusClient)
	w.Write(wrapData(res, err))
}

func (a *Accesses) recordPrices() {
	go func() {
		containerSeen := make(map[string]bool)
		nodeSeen := make(map[string]bool)
		pvSeen := make(map[string]bool)
		pvcSeen := make(map[string]bool)

		getKeyFromLabelStrings := func(labels ...string) string {
			return strings.Join(labels, ",")
		}
		getLabelStringsFromKey := func(key string) []string {
			return strings.Split(key, ",")
		}

		for {
			klog.V(4).Info("Recording prices...")
			podlist := a.Model.Cache.GetAllPods()
			podStatus := make(map[string]v1.PodPhase)
			for _, pod := range podlist {
				podStatus[pod.Name] = pod.Status.Phase
			}

			// Record network pricing at global scope
			networkCosts, err := a.Cloud.NetworkPricing()
			if err != nil {
				klog.V(4).Infof("Failed to retrieve network costs: %s", err.Error())
			} else {
				a.NetworkZoneEgressRecorder.Set(networkCosts.ZoneNetworkEgressCost)
				a.NetworkRegionEgressRecorder.Set(networkCosts.RegionNetworkEgressCost)
				a.NetworkInternetEgressRecorder.Set(networkCosts.InternetNetworkEgressCost)
			}

			data, err := a.Model.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, "2m", "", "")
			if err != nil {
				klog.V(1).Info("Error in price recording: " + err.Error())
				// zero the for loop so the time.Sleep will still work
				data = map[string]*CostData{}
			}

			for _, costs := range data {
				nodeName := costs.NodeName
				node := costs.NodeData
				if node == nil {
					klog.V(4).Infof("Skipping Node \"%s\" due to missing Node Data costs", nodeName)
					continue
				}
				cpuCost, _ := strconv.ParseFloat(node.VCPUCost, 64)
				cpu, _ := strconv.ParseFloat(node.VCPU, 64)
				ramCost, _ := strconv.ParseFloat(node.RAMCost, 64)
				ram, _ := strconv.ParseFloat(node.RAMBytes, 64)
				gpu, _ := strconv.ParseFloat(node.GPU, 64)
				gpuCost, _ := strconv.ParseFloat(node.GPUCost, 64)

				totalCost := cpu*cpuCost + ramCost*(ram/1024/1024/1024) + gpu*gpuCost

				namespace := costs.Namespace
				podName := costs.PodName
				containerName := costs.Name

				if costs.PVCData != nil {
					for _, pvc := range costs.PVCData {
						if pvc.Volume != nil {
							a.PVAllocationRecorder.WithLabelValues(namespace, podName, pvc.Claim, pvc.VolumeName).Set(pvc.Values[0].Value)
							labelKey := getKeyFromLabelStrings(namespace, podName, pvc.Claim, pvc.VolumeName)
							pvcSeen[labelKey] = true
						}
					}
				}

				a.CPUPriceRecorder.WithLabelValues(nodeName, nodeName).Set(cpuCost)
				a.RAMPriceRecorder.WithLabelValues(nodeName, nodeName).Set(ramCost)
				a.GPUPriceRecorder.WithLabelValues(nodeName, nodeName).Set(gpuCost)
				a.NodeTotalPriceRecorder.WithLabelValues(nodeName, nodeName).Set(totalCost)
				labelKey := getKeyFromLabelStrings(nodeName, nodeName)
				nodeSeen[labelKey] = true

				if len(costs.RAMAllocation) > 0 {
					a.RAMAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.RAMAllocation[0].Value)
				}
				if len(costs.CPUAllocation) > 0 {
					a.CPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.CPUAllocation[0].Value)
				}
				if len(costs.GPUReq) > 0 {
					// allocation here is set to the request because shared GPU usage not yet supported.
					a.GPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.GPUReq[0].Value)
				}
				labelKey = getKeyFromLabelStrings(namespace, podName, containerName, nodeName, nodeName)
				if podStatus[podName] == v1.PodRunning { // Only report data for current pods
					containerSeen[labelKey] = true
				} else {
					containerSeen[labelKey] = false
				}

				storageClasses := a.Model.Cache.GetAllStorageClasses()
				storageClassMap := make(map[string]map[string]string)
				for _, storageClass := range storageClasses {
					params := storageClass.Parameters
					storageClassMap[storageClass.ObjectMeta.Name] = params
					if storageClass.GetAnnotations()["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.GetAnnotations()["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
						storageClassMap["default"] = params
						storageClassMap[""] = params
					}
				}

				pvs := a.Model.Cache.GetAllPersistentVolumes()
				for _, pv := range pvs {
					parameters, ok := storageClassMap[pv.Spec.StorageClassName]
					if !ok {
						klog.V(4).Infof("Unable to find parameters for storage class \"%s\". Does pv \"%s\" have a storageClassName?", pv.Spec.StorageClassName, pv.Name)
					}
					cacPv := &costAnalyzerCloud.PV{
						Class:      pv.Spec.StorageClassName,
						Region:     pv.Labels[v1.LabelZoneRegion],
						Parameters: parameters,
					}
					GetPVCost(cacPv, pv, a.Cloud)
					c, _ := strconv.ParseFloat(cacPv.Cost, 64)
					a.PersistentVolumePriceRecorder.WithLabelValues(pv.Name, pv.Name).Set(c)
					labelKey := getKeyFromLabelStrings(pv.Name, pv.Name)
					pvSeen[labelKey] = true
				}
				containerUptime, _ := ComputeUptimes(a.PrometheusClient)
				for key, uptime := range containerUptime {
					container, _ := NewContainerMetricFromKey(key)
					a.ContainerUptimeRecorder.WithLabelValues(container.Namespace, container.PodName, container.ContainerName).Set(uptime)
				}
			}
			for labelString, seen := range nodeSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.NodeTotalPriceRecorder.DeleteLabelValues(labels...)
					a.CPUPriceRecorder.DeleteLabelValues(labels...)
					a.GPUPriceRecorder.DeleteLabelValues(labels...)
					a.RAMPriceRecorder.DeleteLabelValues(labels...)
					delete(nodeSeen, labelString)
				}
				nodeSeen[labelString] = false
			}
			for labelString, seen := range containerSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.RAMAllocationRecorder.DeleteLabelValues(labels...)
					a.CPUAllocationRecorder.DeleteLabelValues(labels...)
					a.GPUAllocationRecorder.DeleteLabelValues(labels...)
					a.ContainerUptimeRecorder.DeleteLabelValues(labels...)
					delete(containerSeen, labelString)
				}
				containerSeen[labelString] = false
			}
			for labelString, seen := range pvSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.PersistentVolumePriceRecorder.DeleteLabelValues(labels...)
					delete(pvSeen, labelString)
				}
				pvSeen[labelString] = false
			}
			for labelString, seen := range pvcSeen {
				if !seen {
					labels := getLabelStringsFromKey(labelString)
					a.PVAllocationRecorder.DeleteLabelValues(labels...)
					delete(pvcSeen, labelString)
				}
				pvcSeen[labelString] = false
			}
			time.Sleep(time.Minute)
		}
	}()
}

func init() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	klog.V(1).Infof("Starting cost-model (git commit \"%s\")", gitCommit)

	address := os.Getenv(prometheusServerEndpointEnvVar)
	if address == "" {
		klog.Fatalf("No address for prometheus set in $%s. Aborting.", prometheusServerEndpointEnvVar)
	}

	var LongTimeoutRoundTripper http.RoundTripper = &http.Transport{ // may be necessary for long prometheus queries. TODO: make this configurable
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   120 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	pc := prometheusClient.Config{
		Address:      address,
		RoundTripper: LongTimeoutRoundTripper,
	}
	promCli, _ := prometheusClient.NewClient(pc)

	api := prometheusAPI.NewAPI(promCli)
	_, err := api.Config(context.Background())
	if err != nil {
		klog.Fatalf("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prometheusTroubleshootingEp)
	}
	klog.V(1).Info("Success: retrieved a prometheus config file from: " + address)

	_, err = ValidatePrometheus(promCli, false)
	if err != nil {
		klog.Fatalf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prometheusTroubleshootingEp)
	}
	klog.V(1).Info("Success: retrieved the 'up' query against prometheus at: " + address)

	// Kubernetes API setup
	kc, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		panic(err.Error())
	}

	cloudProviderKey := os.Getenv("CLOUD_PROVIDER_API_KEY")
	cloudProvider, err := costAnalyzerCloud.NewProvider(kubeClientset, cloudProviderKey)
	if err != nil {
		panic(err.Error())
	}

	cpuGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_cpu_hourly_cost",
		Help: "node_cpu_hourly_cost hourly cost for each cpu on this node",
	}, []string{"instance", "node"})

	ramGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_ram_hourly_cost",
		Help: "node_ram_hourly_cost hourly cost for each gb of ram on this node",
	}, []string{"instance", "node"})

	gpuGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_gpu_hourly_cost",
		Help: "node_gpu_hourly_cost hourly cost for each gpu on this node",
	}, []string{"instance", "node"})

	totalGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_total_hourly_cost",
		Help: "node_total_hourly_cost Total node cost per hour",
	}, []string{"instance", "node"})

	pvGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pv_hourly_cost",
		Help: "pv_hourly_cost Cost per GB per hour on a persistent disk",
	}, []string{"volumename", "persistentvolume"})

	RAMAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_memory_allocation_bytes",
		Help: "container_memory_allocation_bytes Bytes of RAM used",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	CPUAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_cpu_allocation",
		Help: "container_cpu_allocation Percent of a single CPU used in a minute",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	GPUAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_gpu_allocation",
		Help: "container_gpu_allocation GPU used",
	}, []string{"namespace", "pod", "container", "instance", "node"})
	PVAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pod_pvc_allocation",
		Help: "pod_pvc_allocation Bytes used by a PVC attached to a pod",
	}, []string{"namespace", "pod", "persistentvolumeclaim", "persistentvolume"})

	ContainerUptimeRecorder := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_uptime_seconds",
		Help: "container_uptime_seconds Seconds a container has been running",
	}, []string{"namespace", "pod", "container"})

	NetworkZoneEgressRecorder := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubecost_network_zone_egress_cost",
		Help: "kubecost_network_zone_egress_cost Total cost per GB egress across zones",
	})
	NetworkRegionEgressRecorder := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubecost_network_region_egress_cost",
		Help: "kubecost_network_region_egress_cost Total cost per GB egress across regions",
	})
	NetworkInternetEgressRecorder := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubecost_network_internet_egress_cost",
		Help: "kubecost_network_internet_egress_cost Total cost per GB of internet egress.",
	})

	prometheus.MustRegister(cpuGv)
	prometheus.MustRegister(ramGv)
	prometheus.MustRegister(gpuGv)
	prometheus.MustRegister(totalGv)
	prometheus.MustRegister(pvGv)
	prometheus.MustRegister(RAMAllocation)
	prometheus.MustRegister(CPUAllocation)
	prometheus.MustRegister(ContainerUptimeRecorder)
	prometheus.MustRegister(PVAllocation)
	prometheus.MustRegister(NetworkZoneEgressRecorder, NetworkRegionEgressRecorder, NetworkInternetEgressRecorder)
	prometheus.MustRegister(ServiceCollector{
		KubeClientSet: kubeClientset,
	})
	prometheus.MustRegister(DeploymentCollector{
		KubeClientSet: kubeClientset,
	})

	// cache responses from model for a default of 5 minutes; clear expired responses every 10 minutes
	aggregateCache := cache.New(time.Minute*5, time.Minute*10)
	costDataCache := cache.New(time.Minute*5, time.Minute*10)
	outOfClusterCache := cache.New(time.Minute*5, time.Minute*10)

	A = Accesses{
		PrometheusClient:              promCli,
		KubeClientSet:                 kubeClientset,
		Cloud:                         cloudProvider,
		CPUPriceRecorder:              cpuGv,
		RAMPriceRecorder:              ramGv,
		GPUPriceRecorder:              gpuGv,
		NodeTotalPriceRecorder:        totalGv,
		RAMAllocationRecorder:         RAMAllocation,
		CPUAllocationRecorder:         CPUAllocation,
		GPUAllocationRecorder:         GPUAllocation,
		PVAllocationRecorder:          PVAllocation,
		ContainerUptimeRecorder:       ContainerUptimeRecorder,
		NetworkZoneEgressRecorder:     NetworkZoneEgressRecorder,
		NetworkRegionEgressRecorder:   NetworkRegionEgressRecorder,
		NetworkInternetEgressRecorder: NetworkInternetEgressRecorder,
		PersistentVolumePriceRecorder: pvGv,
		Model:                         NewCostModel(kubeClientset),
		AggregateCache:                aggregateCache,
		CostDataCache:                 costDataCache,
		OutOfClusterCache:             outOfClusterCache,
	}

	remoteEnabled := os.Getenv(remoteEnabled)
	if remoteEnabled == "true" {
		info, err := cloudProvider.ClusterInfo()
		klog.Infof("Saving cluster  with id:'%s', and name:'%s' to durable storage", info["id"], info["name"])
		if err != nil {
			klog.Infof("Error saving cluster id %s", err.Error())
		}
		_, _, err = costAnalyzerCloud.GetOrCreateClusterMeta(info["id"], info["name"])
		if err != nil {
			klog.Infof("Unable to set cluster id '%s' for cluster '%s', %s", info["id"], info["name"], err.Error())
		}
	}

	// Thanos Client
	if os.Getenv(thanosEnabled) == "true" {
		thanosUrl := os.Getenv(thanosQueryUrl)
		if thanosUrl != "" {
			var thanosRT http.RoundTripper = &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   120 * time.Second,
					KeepAlive: 120 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 10 * time.Second,
			}

			thanosConfig := prometheusClient.Config{
				Address:      thanosUrl,
				RoundTripper: thanosRT,
			}
			thanosCli, _ := prometheusClient.NewClient(thanosConfig)

			_, err = ValidatePrometheus(thanosCli, true)
			if err != nil {
				klog.Fatalf("Failed to query Thanos at %s. Error: %s.", thanosUrl, err.Error())
			} else {
				klog.V(1).Info("Success: retrieved the 'up' query against Thanos at: " + thanosUrl)

				A.ThanosClient = thanosCli
			}

		} else {
			klog.Infof("Error resolving environment variable: $%s", thanosQueryUrl)
		}
	}

	err = A.Cloud.DownloadPricingData()
	if err != nil {
		klog.V(1).Info("Failed to download pricing data: " + err.Error())
	}

	A.recordPrices()

	Router.GET("/costDataModel", A.CostDataModel)
	Router.GET("/costDataModelRange", A.CostDataModelRange)
	Router.GET("/costDataModelRangeLarge", A.CostDataModelRangeLarge)
	Router.GET("/outOfClusterCosts", A.OutOfClusterCostsWithCache)
	Router.GET("/allNodePricing", A.GetAllNodePricing)
	Router.GET("/healthz", Healthz)
	Router.GET("/getConfigs", A.GetConfigs)
	Router.POST("/refreshPricing", A.RefreshPricingData)
	Router.POST("/updateSpotInfoConfigs", A.UpdateSpotInfoConfigs)
	Router.POST("/updateAthenaInfoConfigs", A.UpdateAthenaInfoConfigs)
	Router.POST("/updateBigQueryInfoConfigs", A.UpdateBigQueryInfoConfigs)
	Router.POST("/updateConfigByKey", A.UpdateConfigByKey)
	Router.GET("/clusterCostsOverTime", A.ClusterCostsOverTime)
	Router.GET("/clusterCosts", A.ClusterCosts)
	Router.GET("/validatePrometheus", A.GetPrometheusMetadata)
	Router.GET("/managementPlatform", A.ManagementPlatform)
	Router.GET("/clusterInfo", A.ClusterInfo)
	Router.GET("/containerUptimes", A.ContainerUptimes)
	Router.GET("/aggregatedCostModel", A.AggregateCostModel)
}
