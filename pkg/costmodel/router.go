package costmodel

import (
	"context"
	"flag"
	"fmt"
	"github.com/kubecost/cost-model/pkg/util/timeutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog"

	"github.com/julienschmidt/httprouter"

	sentry "github.com/getsentry/sentry-go"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/clustercache"
	cm "github.com/kubecost/cost-model/pkg/clustermanager"
	"github.com/kubecost/cost-model/pkg/costmodel/clusters"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/errors"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"
	"github.com/kubecost/cost-model/pkg/util/json"
	prometheus "github.com/prometheus/client_golang/api"
	prometheusClient "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrickmn/go-cache"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	prometheusTroubleshootingEp = "http://docs.kubecost.com/custom-prom#troubleshoot"
	RFC3339Milli                = "2006-01-02T15:04:05.000Z"
	maxCacheMinutes1d           = 11
	maxCacheMinutes2d           = 17
	maxCacheMinutes7d           = 37
	maxCacheMinutes30d          = 137
	CustomPricingSetting        = "CustomPricing"
	DiscountSetting             = "Discount"
)

var (
	// gitCommit is set by the build system
	gitCommit string
)

// Accesses defines a singleton application instance, providing access to
// Prometheus, Kubernetes, the cloud provider, and caches.
type Accesses struct {
	Router            *httprouter.Router
	PrometheusClient  prometheusClient.Client
	ThanosClient      prometheusClient.Client
	KubeClientSet     kubernetes.Interface
	ClusterManager    *cm.ClusterManager
	ClusterMap        clusters.ClusterMap
	CloudProvider     cloud.Provider
	Model             *CostModel
	MetricsEmitter    *CostModelMetricsEmitter
	OutOfClusterCache *cache.Cache
	AggregateCache    *cache.Cache
	CostDataCache     *cache.Cache
	ClusterCostsCache *cache.Cache
	CacheExpiration   map[time.Duration]time.Duration
	AggAPI            Aggregator
	// SettingsCache stores current state of app settings
	SettingsCache *cache.Cache
	// settingsSubscribers tracks channels through which changes to different
	// settings will be published in a pub/sub model
	settingsSubscribers map[string][]chan string
	settingsMutex       sync.Mutex
}

// GetPrometheusClient decides whether the default Prometheus client or the Thanos client
// should be used.
func (a *Accesses) GetPrometheusClient(remote bool) prometheusClient.Client {
	// Use Thanos Client if it exists (enabled) and remote flag set
	var pc prometheusClient.Client

	if remote && a.ThanosClient != nil {
		pc = a.ThanosClient
	} else {
		pc = a.PrometheusClient
	}

	return pc
}

// GetCacheExpiration looks up and returns custom cache expiration for the given duration.
// If one does not exists, it returns the default cache expiration, which is defined by
// the particular cache.
func (a *Accesses) GetCacheExpiration(dur time.Duration) time.Duration {
	if expiration, ok := a.CacheExpiration[dur]; ok {
		return expiration
	}
	return cache.DefaultExpiration
}

// GetCacheRefresh determines how long to wait before refreshing the cache for the given duration,
// which is done 1 minute before we expect the cache to expire, or 1 minute if expiration is
// not found or is less than 2 minutes.
func (a *Accesses) GetCacheRefresh(dur time.Duration) time.Duration {
	expiry := a.GetCacheExpiration(dur).Minutes()
	if expiry <= 2.0 {
		return time.Minute
	}
	mins := time.Duration(expiry/2.0) * time.Minute
	return mins
}

func (a *Accesses) ClusterCostsFromCacheHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	duration := 24 * time.Hour
	offset := time.Minute
	durationHrs := "24h"
	fmtOffset := "1m"
	pClient := a.GetPrometheusClient(true)

	key := fmt.Sprintf("%s:%s", durationHrs, fmtOffset)
	if data, valid := a.ClusterCostsCache.Get(key); valid {
		clusterCosts := data.(map[string]*ClusterCosts)
		w.Write(WrapDataWithMessage(clusterCosts, nil, "clusterCosts cache hit"))
	} else {
		data, err := a.ComputeClusterCosts(pClient, a.CloudProvider, duration, offset, true)
		w.Write(WrapDataWithMessage(data, err, fmt.Sprintf("clusterCosts cache miss: %s", key)))
	}
}

type Response struct {
	Code    int         `json:"code"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
	Warning string      `json:"warning,omitempty"`
}

// FilterFunc is a filter that returns true iff the given CostData should be filtered out, and the environment that was used as the filter criteria, if it was an aggregate
type FilterFunc func(*CostData) (bool, string)

// FilterCostData allows through only CostData that matches all the given filter functions
func FilterCostData(data map[string]*CostData, retains []FilterFunc, filters []FilterFunc) (map[string]*CostData, int, map[string]int) {
	result := make(map[string]*CostData)
	filteredEnvironments := make(map[string]int)
	filteredContainers := 0
DataLoop:
	for key, datum := range data {
		for _, rf := range retains {
			if ok, _ := rf(datum); ok {
				result[key] = datum
				// if any retain function passes, the data is retained and move on
				continue DataLoop
			}
		}
		for _, ff := range filters {
			if ok, environment := ff(datum); !ok {
				if environment != "" {
					filteredEnvironments[environment]++
				}
				filteredContainers++
				// if any filter function check fails, move on to the next datum
				continue DataLoop
			}
		}
		result[key] = datum
	}

	return result, filteredContainers, filteredEnvironments
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
	if param == "" {
		return "", fmt.Errorf("invalid time param")
	}
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

// ParsePercentString takes a string of expected format "N%" and returns a floating point 0.0N.
// If the "%" symbol is missing, it just returns 0.0N. Empty string is interpreted as "0%" and
// return 0.0.
func ParsePercentString(percentStr string) (float64, error) {
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

func WrapData(data interface{}, err error) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})
	}

	return resp
}

func WrapDataWithMessage(data interface{}, err error, message string) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Message: message,
		})
	}

	return resp
}

func WrapDataWithWarning(data interface{}, err error, warning string) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Warning: warning,
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Warning: warning,
		})
	}

	return resp
}

func WrapDataWithMessageAndWarning(data interface{}, err error, message, warning string) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusInternalServerError,
			Status:  "error",
			Message: err.Error(),
			Warning: warning,
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&Response{
			Code:    http.StatusOK,
			Status:  "success",
			Data:    data,
			Message: message,
			Warning: warning,
		})
	}

	return resp
}

// RefreshPricingData needs to be called when a new node joins the fleet, since we cache the relevant subsets of pricing data to avoid storing the whole thing.
func (a *Accesses) RefreshPricingData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := a.CloudProvider.DownloadPricingData()

	w.Write(WrapData(nil, err))
}

func (a *Accesses) CostDataModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("timeWindow")
	offset := r.URL.Query().Get("offset")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")

	if offset != "" {
		offset = "offset " + offset
	}

	data, err := a.Model.ComputeCostData(a.PrometheusClient, a.CloudProvider, window, offset, namespace)

	if fields != "" {
		filteredData := filterFields(fields, data)
		w.Write(WrapData(filteredData, err))
	} else {
		w.Write(WrapData(data, err))
	}

}

func (a *Accesses) ClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	if window == "" {
		w.Write(WrapData(nil, fmt.Errorf("missing window arguement")))
		return
	}
	windowDur, err := timeutil.ParseDuration(window)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("error parsing window (%s): %s", window, err)))
		return
	}

	// offset is not a required parameter
	var offsetDur time.Duration
	if offset != "" {
		offsetDur, err = timeutil.ParseDuration(offset)
		if err != nil {
			w.Write(WrapData(nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)))
			return
		}
	}


	useThanos, _ := strconv.ParseBool(r.URL.Query().Get("multi"))

	if useThanos && !thanos.IsEnabled() {
		w.Write(WrapData(nil, fmt.Errorf("Multi=true while Thanos is not enabled.")))
		return
	}

	var client prometheusClient.Client
	if useThanos {
		client = a.ThanosClient
		offsetDur = thanos.OffsetDuration()

	} else {
		client = a.PrometheusClient
	}

	data, err := a.ComputeClusterCosts(client, a.CloudProvider, windowDur, offsetDur, true)
	w.Write(WrapData(data, err))
}

func (a *Accesses) ClusterCostsOverTime(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	if window == "" {
		w.Write(WrapData(nil, fmt.Errorf("missing window arguement")))
		return
	}
	windowDur, err := timeutil.ParseDuration(window)
	if err != nil {
		w.Write(WrapData(nil, fmt.Errorf("error parsing window (%s): %s", window, err)))
		return
	}

	// offset is not a required parameter
	var offsetDur time.Duration
	if offset != "" {
		offsetDur, err = timeutil.ParseDuration(offset)
		if err != nil {
			w.Write(WrapData(nil, fmt.Errorf("error parsing offset (%s): %s", offset, err)))
			return
		}
	}

	data, err := ClusterCostsOverTime(a.PrometheusClient, a.CloudProvider, start, end, windowDur, offsetDur)
	w.Write(WrapData(data, err))
}

func (a *Accesses) CostDataModelRange(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	windowStr := r.URL.Query().Get("window")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	remote := r.URL.Query().Get("remote")
	remoteEnabled := env.IsRemoteEnabled() && remote != "false"

	layout := "2006-01-02T15:04:05.000Z"
	start, err := time.Parse(layout, startStr)
	if err != nil {
		w.Write(WrapDataWithMessage(nil, fmt.Errorf("invalid start date: %s", startStr), fmt.Sprintf("invalid start date: %s", startStr)))
		return
	}
	end, err := time.Parse(layout, endStr)
	if err != nil {
		w.Write(WrapDataWithMessage(nil, fmt.Errorf("invalid end date: %s", endStr), fmt.Sprintf("invalid end date: %s", endStr)))
		return
	}

	window := kubecost.NewWindow(&start, &end)
	if window.IsOpen() || window.IsEmpty() || window.IsNegative() {
		w.Write(WrapDataWithMessage(nil, fmt.Errorf("invalid date range: %s", window), fmt.Sprintf("invalid date range: %s", window)))
		return
	}

	resolution := time.Hour
	if resDur, err := time.ParseDuration(windowStr); err == nil {
		resolution = resDur
	}

	// Use Thanos Client if it exists (enabled) and remote flag set
	var pClient prometheusClient.Client
	if remote != "false" && a.ThanosClient != nil {
		pClient = a.ThanosClient
	} else {
		pClient = a.PrometheusClient
	}

	data, err := a.Model.ComputeCostDataRange(pClient, a.CloudProvider, window, resolution, namespace, cluster, remoteEnabled)
	if err != nil {
		w.Write(WrapData(nil, err))
	}
	if fields != "" {
		filteredData := filterFields(fields, data)
		w.Write(WrapData(filteredData, err))
	} else {
		w.Write(WrapData(data, err))
	}
}

func parseAggregations(customAggregation, aggregator, filterType string) (string, []string, string) {
	var key string
	var filter string
	var val []string
	if customAggregation != "" {
		key = customAggregation
		filter = filterType
		val = strings.Split(customAggregation, ",")
	} else {
		aggregations := strings.Split(aggregator, ",")
		for i, agg := range aggregations {
			aggregations[i] = "kubernetes_" + agg
		}
		key = strings.Join(aggregations, ",")
		filter = "kubernetes_" + filterType
		val = aggregations
	}
	return key, val, filter
}

func (a *Accesses) OutofClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	aggregator := r.URL.Query().Get("aggregator")
	customAggregation := r.URL.Query().Get("customAggregation")
	filterType := r.URL.Query().Get("filterType")
	filterValue := r.URL.Query().Get("filterValue")
	var data []*cloud.OutOfClusterAllocation
	var err error
	_, aggregations, filter := parseAggregations(customAggregation, aggregator, filterType)
	data, err = a.CloudProvider.ExternalAllocations(start, end, aggregations, filter, filterValue, false)
	w.Write(WrapData(data, err))
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

	filterType := r.URL.Query().Get("filterType")
	filterValue := r.URL.Query().Get("filterValue")

	aggregationkey, aggregation, filter := parseAggregations(customAggregation, kubernetesAggregation, filterType)

	// clear cache prior to checking the cache so that a clearCache=true
	// request always returns a freshly computed value
	if clearCache {
		a.OutOfClusterCache.Flush()
	}

	// attempt to retrieve cost data from cache
	key := fmt.Sprintf(`%s:%s:%s:%s:%s`, start, end, aggregationkey, filter, filterValue)
	if value, found := a.OutOfClusterCache.Get(key); found && !disableCache {
		if data, ok := value.([]*cloud.OutOfClusterAllocation); ok {
			w.Write(WrapDataWithMessage(data, nil, fmt.Sprintf("out of cluster cache hit: %s", key)))
			return
		}
		klog.Errorf("caching error: failed to type cast data: %s", key)
	}

	data, err := a.CloudProvider.ExternalAllocations(start, end, aggregation, filter, filterValue, false)
	if err == nil {
		a.OutOfClusterCache.Set(key, data, cache.DefaultExpiration)
	}

	w.Write(WrapDataWithMessage(data, err, fmt.Sprintf("out of cluser cache miss: %s", key)))
}

func (a *Accesses) GetAllNodePricing(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.CloudProvider.AllNodePricing()
	w.Write(WrapData(data, err))
}

func (a *Accesses) GetConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.GetConfig()
	w.Write(WrapData(data, err))
}

func (a *Accesses) UpdateSpotInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, cloud.SpotInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		klog.V(1).Infof("Error redownloading data on config update: %s", err.Error())
	}
	return
}

func (a *Accesses) UpdateAthenaInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, cloud.AthenaInfoUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) UpdateBigQueryInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, cloud.BigqueryUpdateType)
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) UpdateConfigByKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := a.CloudProvider.UpdateConfig(r.Body, "")
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) ManagementPlatform(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.CloudProvider.GetManagementPlatform()
	if err != nil {
		w.Write(WrapData(data, err))
		return
	}
	w.Write(WrapData(data, err))
	return
}

func (a *Accesses) ClusterInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := GetClusterInfo(a.KubeClientSet, a.CloudProvider)

	w.Write(WrapData(data, nil))
}

func (a *Accesses) GetClusterInfoMap(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.ClusterMap.AsMap()

	w.Write(WrapData(data, nil))
}

func (a *Accesses) GetServiceAccountStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(a.CloudProvider.ServiceAccountStatus(), nil))
}

func (a *Accesses) GetPricingSourceStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(a.CloudProvider.PricingSourceStatus(), nil))
}

func (a *Accesses) GetPricingSourceCounts(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(a.Model.GetPricingSourceCounts()))
}

func (a *Accesses) GetPrometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	w.Write(WrapData(prom.Validate(a.PrometheusClient)))
}

// Creates a new ClusterManager instance using a boltdb storage. If that fails,
// then we fall back to a memory-only storage.
func newClusterManager() *cm.ClusterManager {
	clustersConfigFile := "/var/configs/clusters/default-clusters.yaml"

	// Return a memory-backed cluster manager populated by configmap
	return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)

	// NOTE: The following should be used with a persistent disk store. Since the
	// NOTE: configmap approach is currently the "persistent" source (entries are read-only
	// NOTE: on the backend), we don't currently need to store on disk.
	/*
		path := env.GetConfigPath()
		db, err := bolt.Open(path+"costmodel.db", 0600, nil)
		if err != nil {
			klog.V(1).Infof("[Error] Failed to create costmodel.db: %s", err.Error())
			return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)
		}

		store, err := cm.NewBoltDBClusterStorage("clusters", db)
		if err != nil {
			klog.V(1).Infof("[Error] Failed to Create Cluster Storage: %s", err.Error())
			return cm.NewConfiguredClusterManager(cm.NewMapDBClusterStorage(), clustersConfigFile)
		}

		return cm.NewConfiguredClusterManager(store, clustersConfigFile)
	*/
}

type ConfigWatchers struct {
	ConfigmapName string
	WatchFunc     func(string, map[string]string) error
}

// captures the panic event in sentry
func capturePanicEvent(err string, stack string) {
	msg := fmt.Sprintf("Panic: %s\nStackTrace: %s\n", err, stack)
	klog.V(1).Infoln(msg)
	sentry.CurrentHub().CaptureEvent(&sentry.Event{
		Level:   sentry.LevelError,
		Message: msg,
	})
	sentry.Flush(5 * time.Second)
}

// handle any panics reported by the errors package
func handlePanic(p errors.Panic) bool {
	err := p.Error

	if err != nil {
		if err, ok := err.(error); ok {
			capturePanicEvent(err.Error(), p.Stack)
		}

		if err, ok := err.(string); ok {
			capturePanicEvent(err, p.Stack)
		}
	}

	// Return true to recover iff the type is http, otherwise allow kubernetes
	// to recover.
	return p.Type == errors.PanicTypeHTTP
}

func Initialize(additionalConfigWatchers ...ConfigWatchers) *Accesses {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	klog.V(1).Infof("Starting cost-model (git commit \"%s\")", env.GetAppVersion())

	var err error
	if errorReportingEnabled {
		err = sentry.Init(sentry.ClientOptions{Release: env.GetAppVersion()})
		if err != nil {
			klog.Infof("Failed to initialize sentry for error reporting")
		} else {
			err = errors.SetPanicHandler(handlePanic)
			if err != nil {
				klog.Infof("Failed to set panic handler: %s", err)
			}
		}
	}

	address := env.GetPrometheusServerEndpoint()
	if address == "" {
		klog.Fatalf("No address for prometheus set in $%s. Aborting.", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	klog.Infof("Prometheus/Thanos Client Max Concurrency set to %d", queryConcurrency)

	timeout := 120 * time.Second
	keepAlive := 120 * time.Second
	scrapeInterval, _ := time.ParseDuration("1m")

	promCli, err := prom.NewPrometheusClient(address, timeout, keepAlive, queryConcurrency, "")
	if err != nil {
		klog.Fatalf("Failed to create prometheus client, Error: %v", err)
	}

	api := prometheusAPI.NewAPI(promCli)
	pcfg, err := api.Config(context.Background())
	if err != nil {
		klog.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/thanos here.", address, err.Error(), prometheusTroubleshootingEp)
	} else {
		klog.V(1).Info("Retrieved a prometheus config file from: " + address)
		sc, err := GetPrometheusConfig(pcfg.YAML)
		if err != nil {
			klog.Infof("Fix YAML error %s", err)
		}
		for _, scrapeconfig := range sc.ScrapeConfigs {
			if scrapeconfig.JobName == GetKubecostJobName() {
				if scrapeconfig.ScrapeInterval != "" {
					si := scrapeconfig.ScrapeInterval
					sid, err := time.ParseDuration(si)
					if err != nil {
						klog.Infof("error parseing scrapeConfig for %s", scrapeconfig.JobName)
					} else {
						klog.Infof("Found Kubecost job scrape interval of: %s", si)
						scrapeInterval = sid
					}
				}
			}
		}
	}
	klog.Infof("Using scrape interval of %f", scrapeInterval.Seconds())

	m, err := prom.Validate(promCli)
	if err != nil || m.Running == false {
		if err != nil {
			klog.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prometheusTroubleshootingEp)
		} else if m.Running == false {
			klog.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prometheusTroubleshootingEp)
		}

		api := prometheusAPI.NewAPI(promCli)
		_, err = api.Config(context.Background())
		if err != nil {
			klog.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/thanos here.", address, err.Error(), prometheusTroubleshootingEp)
		} else {
			klog.V(1).Info("Retrieved a prometheus config file from: " + address)
		}
	} else {
		klog.V(1).Info("Success: retrieved the 'up' query against prometheus at: " + address)
	}

	// Kubernetes API setup
	var kc *rest.Config
	if kubeconfig := env.GetKubeConfigPath(); kubeconfig != "" {
		kc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		kc, err = rest.InClusterConfig()
	}

	if err != nil {
		panic(err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		panic(err.Error())
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := cloud.NewProvider(k8sCache, cloudProviderKey)
	if err != nil {
		panic(err.Error())
	}

	watchConfigFunc := func(c interface{}) {
		conf := c.(*v1.ConfigMap)
		if conf.GetName() == "pricing-configs" {
			_, err := cloudProvider.UpdateConfigFromConfigMap(conf.Data)
			if err != nil {
				klog.Infof("ERROR UPDATING %s CONFIG: %s", "pricing-configs", err.Error())
			}
		}
		for _, cw := range additionalConfigWatchers {
			if conf.GetName() == cw.ConfigmapName {
				err := cw.WatchFunc(conf.GetName(), conf.Data)
				if err != nil {
					klog.Infof("ERROR UPDATING %s CONFIG: %s", cw.ConfigmapName, err.Error())
				}
			}
		}
	}

	kubecostNamespace := env.GetKubecostNamespace()
	// We need an initial invocation because the init of the cache has happened before we had access to the provider.
	configs, err := kubeClientset.CoreV1().ConfigMaps(kubecostNamespace).Get(context.Background(), "pricing-configs", metav1.GetOptions{})
	if err != nil {
		klog.Infof("No %s configmap found at installtime, using existing configs: %s", "pricing-configs", err.Error())
	} else {
		watchConfigFunc(configs)
	}

	for _, cw := range additionalConfigWatchers {
		configs, err := kubeClientset.CoreV1().ConfigMaps(kubecostNamespace).Get(context.Background(), cw.ConfigmapName, metav1.GetOptions{})
		if err != nil {
			klog.Infof("No %s configmap found at installtime, using existing configs: %s", cw.ConfigmapName, err.Error())
		} else {
			watchConfigFunc(configs)
		}
	}

	k8sCache.SetConfigMapUpdateFunc(watchConfigFunc)

	// TODO: General Architecture Note: Several passes have been made to modularize a lot of
	// TODO: our code, but the router still continues to be the obvious entry point for new \
	// TODO: features. We should look to split out the actual "router" functionality and
	// TODO: implement a builder -> controller for stitching new features and other dependencies.
	clusterManager := newClusterManager()

	// Initialize metrics here

	remoteEnabled := env.IsRemoteEnabled()
	if remoteEnabled {
		info, err := cloudProvider.ClusterInfo()
		klog.Infof("Saving cluster  with id:'%s', and name:'%s' to durable storage", info["id"], info["name"])
		if err != nil {
			klog.Infof("Error saving cluster id %s", err.Error())
		}
		_, _, err = cloud.GetOrCreateClusterMeta(info["id"], info["name"])
		if err != nil {
			klog.Infof("Unable to set cluster id '%s' for cluster '%s', %s", info["id"], info["name"], err.Error())
		}
	}

	// Thanos Client
	var thanosClient prometheusClient.Client
	if thanos.IsEnabled() {
		thanosAddress := thanos.QueryURL()

		if thanosAddress != "" {
			thanosCli, _ := thanos.NewThanosClient(thanosAddress, timeout, keepAlive, queryConcurrency, env.GetQueryLoggingFile())

			_, err = prom.Validate(thanosCli)
			if err != nil {
				klog.V(1).Infof("[Warning] Failed to query Thanos at %s. Error: %s.", thanosAddress, err.Error())
				thanosClient = thanosCli
			} else {
				klog.V(1).Info("Success: retrieved the 'up' query against Thanos at: " + thanosAddress)

				thanosClient = thanosCli
			}

		} else {
			klog.Infof("Error resolving environment variable: $%s", env.ThanosQueryUrlEnvVar)
		}
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	var clusterMap clusters.ClusterMap
	if thanosClient != nil {
		clusterMap = clusters.NewClusterMap(thanosClient, 10*time.Minute)
	} else {
		clusterMap = clusters.NewClusterMap(promCli, 5*time.Minute)
	}

	// cache responses from model and aggregation for a default of 10 minutes;
	// clear expired responses every 20 minutes
	aggregateCache := cache.New(time.Minute*10, time.Minute*20)
	costDataCache := cache.New(time.Minute*10, time.Minute*20)
	clusterCostsCache := cache.New(cache.NoExpiration, cache.NoExpiration)
	outOfClusterCache := cache.New(time.Minute*5, time.Minute*10)
	settingsCache := cache.New(cache.NoExpiration, cache.NoExpiration)

	// query durations that should be cached longer should be registered here
	// use relatively prime numbers to minimize likelihood of synchronized
	// attempts at cache warming
	day := 24 * time.Hour
	cacheExpiration := map[time.Duration]time.Duration{
		day:      maxCacheMinutes1d * time.Minute,
		2 * day:  maxCacheMinutes2d * time.Minute,
		7 * day:  maxCacheMinutes7d * time.Minute,
		30 * day: maxCacheMinutes30d * time.Minute,
	}

	var pc prometheus.Client
	if thanosClient != nil {
		pc = thanosClient
	} else {
		pc = promCli
	}
	costModel := NewCostModel(pc, cloudProvider, k8sCache, clusterMap, scrapeInterval)
	metricsEmitter := NewCostModelMetricsEmitter(promCli, k8sCache, cloudProvider, costModel)

	a := &Accesses{
		Router:            httprouter.New(),
		PrometheusClient:  promCli,
		ThanosClient:      thanosClient,
		KubeClientSet:     kubeClientset,
		ClusterManager:    clusterManager,
		ClusterMap:        clusterMap,
		CloudProvider:     cloudProvider,
		Model:             costModel,
		MetricsEmitter:    metricsEmitter,
		AggregateCache:    aggregateCache,
		CostDataCache:     costDataCache,
		ClusterCostsCache: clusterCostsCache,
		OutOfClusterCache: outOfClusterCache,
		SettingsCache:     settingsCache,
		CacheExpiration:   cacheExpiration,
	}
	// Use the Accesses instance, itself, as the CostModelAggregator. This is
	// confusing and unconventional, but necessary so that we can swap it
	// out for the ETL-adapted version elsewhere.
	// TODO clean this up once ETL is open-sourced.
	a.AggAPI = a

	// Initialize mechanism for subscribing to settings changes
	a.InitializeSettingsPubSub()

	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		klog.V(1).Info("Failed to download pricing data: " + err.Error())
	}

	// Warm the aggregate cache unless explicitly set to false
	if env.IsCacheWarmingEnabled() {
		log.Infof("Init: AggregateCostModel cache warming enabled")
		a.warmAggregateCostModelCache()
	} else {
		log.Infof("Init: AggregateCostModel cache warming disabled")
	}

	a.MetricsEmitter.Start()

	managerEndpoints := cm.NewClusterManagerEndpoints(a.ClusterManager)

	a.Router.GET("/costDataModel", a.CostDataModel)
	a.Router.GET("/costDataModelRange", a.CostDataModelRange)
	a.Router.GET("/aggregatedCostModel", a.AggregateCostModelHandler)
	a.Router.GET("/allocation/compute", a.ComputeAllocationHandler)
	a.Router.GET("/outOfClusterCosts", a.OutOfClusterCostsWithCache)
	a.Router.GET("/allNodePricing", a.GetAllNodePricing)
	a.Router.POST("/refreshPricing", a.RefreshPricingData)
	a.Router.GET("/clusterCostsOverTime", a.ClusterCostsOverTime)
	a.Router.GET("/clusterCosts", a.ClusterCosts)
	a.Router.GET("/clusterCostsFromCache", a.ClusterCostsFromCacheHandler)
	a.Router.GET("/validatePrometheus", a.GetPrometheusMetadata)
	a.Router.GET("/managementPlatform", a.ManagementPlatform)
	a.Router.GET("/clusterInfo", a.ClusterInfo)
	a.Router.GET("/clusterInfoMap", a.GetClusterInfoMap)
	a.Router.GET("/serviceAccountStatus", a.GetServiceAccountStatus)
	a.Router.GET("/pricingSourceStatus", a.GetPricingSourceStatus)
	a.Router.GET("/pricingSourceCounts", a.GetPricingSourceCounts)

	// cluster manager endpoints
	a.Router.GET("/clusters", managerEndpoints.GetAllClusters)
	a.Router.PUT("/clusters", managerEndpoints.PutCluster)
	a.Router.DELETE("/clusters/:id", managerEndpoints.DeleteCluster)

	return a
}
