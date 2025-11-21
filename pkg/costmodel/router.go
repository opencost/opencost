package costmodel

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/nodestats"
	"github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/retry"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/core/pkg/version"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/customcost"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/opencost/opencost/pkg/util/watcher"

	"github.com/julienschmidt/httprouter"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/clusters"
	sysenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/modules/collector-source/pkg/collector"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	"github.com/opencost/opencost/pkg/cloud/models"
	clusterc "github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/patrickmn/go-cache"

	"k8s.io/client-go/kubernetes"
)

const (
	RFC3339Milli         = "2006-01-02T15:04:05.000Z"
	CustomPricingSetting = "CustomPricing"
	DiscountSetting      = "Discount"
)

var (
	// gitCommit is set by the build system
	gitCommit string

	proto = protocol.HTTP()
)

// Accesses defines a singleton application instance, providing access to
// Prometheus, Kubernetes, the cloud provider, and caches.
type Accesses struct {
	DataSource          source.OpenCostDataSource
	KubeClientSet       kubernetes.Interface
	ClusterCache        clustercache.ClusterCache
	ClusterMap          clusters.ClusterMap
	CloudProvider       models.Provider
	ConfigFileManager   *config.ConfigFileManager
	ClusterInfoProvider clusters.ClusterInfoProvider
	Model               *CostModel
	MetricsEmitter      *CostModelMetricsEmitter
	// SettingsCache stores current state of app settings
	SettingsCache *cache.Cache
	// settingsSubscribers tracks channels through which changes to different
	// settings will be published in a pub/sub model
	settingsSubscribers map[string][]chan string
	settingsMutex       sync.Mutex
}

func filterFields(fields string, data map[string]*CostData) map[string]CostData {
	fs := strings.Split(fields, ",")
	fmap := make(map[string]bool)
	for _, f := range fs {
		fieldNameLower := strings.ToLower(f) // convert to go struct name by uppercasing first letter
		log.Debugf("to delete: %s", fieldNameLower)
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

func WriteData(w http.ResponseWriter, data interface{}, err error) {
	if err != nil {
		proto.WriteError(w, proto.InternalServerError(err.Error()))
		return
	}

	proto.WriteData(w, data)
}

// RefreshPricingData needs to be called when a new node joins the fleet, since we cache the relevant subsets of pricing data to avoid storing the whole thing.
func (a *Accesses) RefreshPricingData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := a.CloudProvider.DownloadPricingData()
	if err != nil {
		log.Errorf("Error refreshing pricing data: %s", err.Error())
	}

	WriteData(w, nil, err)
}

func (a *Accesses) CostDataModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("timeWindow")
	offset := r.URL.Query().Get("offset")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")

	duration, err := timeutil.ParseDuration(window)
	if err != nil {
		WriteData(w, nil, fmt.Errorf("error parsing window (%s): %s", window, err))
		return
	}

	end := time.Now()
	if offset != "" {
		offsetDur, err := timeutil.ParseDuration(offset)
		if err != nil {
			WriteData(w, nil, fmt.Errorf("error parsing offset (%s): %s", offset, err))
			return
		}

		end = end.Add(-offsetDur)
	}

	start := end.Add(-duration)

	data, err := a.Model.ComputeCostData(start, end)

	// apply filter by removing if != namespace
	if namespace != "" {
		for key, costData := range data {
			if costData.Namespace != namespace {
				delete(data, key)
			}
		}
	}

	if fields != "" {
		filteredData := filterFields(fields, data)
		WriteData(w, filteredData, err)
	} else {
		WriteData(w, data, err)
	}
}

func (a *Accesses) GetAllNodePricing(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.CloudProvider.AllNodePricing()
	WriteData(w, data, err)
}

func (a *Accesses) ManagementPlatform(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.CloudProvider.GetManagementPlatform()
	WriteData(w, data, err)
}

func (a *Accesses) ClusterInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.ClusterInfoProvider.GetClusterInfo()

	WriteData(w, data, nil)
}

func (a *Accesses) GetClusterInfoMap(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.ClusterMap.AsMap()

	WriteData(w, data, nil)
}

func (a *Accesses) GetServiceAccountStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	WriteData(w, a.CloudProvider.ServiceAccountStatus(), nil)
}

func (a *Accesses) GetPricingSourceStatus(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.CloudProvider.PricingSourceStatus()
	WriteData(w, data, nil)
}

func (a *Accesses) GetPricingSourceCounts(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := a.Model.GetPricingSourceCounts()
	WriteData(w, data, err)
}

func (a *Accesses) GetPricingSourceSummary(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data := a.CloudProvider.PricingSourceSummary()
	WriteData(w, data, nil)
}

func (a *Accesses) GetOrphanedPods(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	podlist := a.ClusterCache.GetAllPods()

	var lonePods []*clustercache.Pod
	for _, pod := range podlist {
		if len(pod.OwnerReferences) == 0 {
			lonePods = append(lonePods, pod)
		}
	}

	body, err := json.Marshal(lonePods)
	if err != nil {
		fmt.Fprintf(w, "Error decoding pod: %s", err)
	} else {
		w.Write(body)
	}
}

func (a *Accesses) GetInstallNamespace(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ns := env.GetOpencostNamespace()
	w.Write([]byte(ns))
}

type InstallInfo struct {
	Containers  []ContainerInfo   `json:"containers"`
	ClusterInfo map[string]string `json:"clusterInfo"`
	Version     string            `json:"version"`
}

type ContainerInfo struct {
	ContainerName string `json:"containerName"`
	Image         string `json:"image"`
	StartTime     string `json:"startTime"`
}

func (a *Accesses) GetInstallInfo(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	containers, err := GetKubecostContainers(a.KubeClientSet)
	if err != nil {
		http.Error(w, fmt.Sprintf("Unable to list pods: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	info := InstallInfo{
		Containers:  containers,
		ClusterInfo: make(map[string]string),
		Version:     version.FriendlyVersion(),
	}

	nodes := a.ClusterCache.GetAllNodes()
	cachePods := a.ClusterCache.GetAllPods()

	info.ClusterInfo["nodeCount"] = strconv.Itoa(len(nodes))
	info.ClusterInfo["podCount"] = strconv.Itoa(len(cachePods))

	body, err := json.Marshal(info)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error decoding pod: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	w.Write(body)
}

func GetKubecostContainers(kubeClientSet kubernetes.Interface) ([]ContainerInfo, error) {
	pods, err := kubeClientSet.CoreV1().Pods(env.GetOpencostNamespace()).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=cost-analyzer",
		FieldSelector: "status.phase=Running",
		Limit:         1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query kubernetes client for kubecost pods: %s", err)
	}

	// If we have zero pods either something is weird with the install since the app selector is not exposed in the helm
	// chart or more likely we are running locally - in either case Images field will return as null
	containers := make([]ContainerInfo, 0)
	if len(pods.Items) > 0 {
		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				c := ContainerInfo{
					ContainerName: container.Name,
					Image:         container.Image,
					StartTime:     pod.Status.StartTime.String(),
				}
				containers = append(containers, c)
			}
		}
	}

	return containers, nil
}

func (a *Accesses) AddServiceKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	r.ParseForm()

	key := r.PostForm.Get("key")
	k := []byte(key)
	err := os.WriteFile(env.GetGCPAuthSecretFilePath(), k, 0644)
	if err != nil {
		fmt.Fprintf(w, "Error writing service key: %s", err)
	}

	w.WriteHeader(http.StatusOK)
}

func (a *Accesses) GetHelmValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	encodedValues := sysenv.Get("HELM_VALUES", "")
	if encodedValues == "" {
		fmt.Fprintf(w, "Values reporting disabled")
		return
	}

	result, err := base64.StdEncoding.DecodeString(encodedValues)
	if err != nil {
		fmt.Fprintf(w, "Failed to decode encoded values: %s", err)
		return
	}

	w.Write(result)
}

func Initialize(router *httprouter.Router, additionalConfigWatchers ...*watcher.ConfigMapWatcher) *Accesses {
	var err error

	// Kubernetes API setup
	kubeClientset, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		log.Fatalf("Failed to build Kubernetes client: %s", err.Error())
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clusterc.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(nil)

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := provider.NewProvider(k8sCache, cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsClusterInfoFileEnabled() {
		clusterInfoFile := confManager.ConfigFileAt(env.GetClusterInfoFilePath())
		clusterInfoProvider = NewConfiguredClusterInfoProvider(clusterInfoFile)
	} else {
		clusterInfoProvider = NewLocalClusterInfoProvider(kubeClientset, cloudProvider)
	}

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(context.Background())
	fn := func() (source.OpenCostDataSource, error) {
		ds, e := prom.NewDefaultPrometheusDataSource(clusterInfoProvider)
		if e != nil {
			if source.IsRetryable(e) {
				return nil, e
			}
			fatalErr = e
			cancel()
		}

		return ds, e
	}
	if env.IsCollectorDataSourceEnabled() {
		fn = func() (source.OpenCostDataSource, error) {
			store := GetDefaultCollectorStorage()
			nodeStatConf, err := NewNodeClientConfigFromEnv()
			if err != nil {
				return nil, fmt.Errorf("failed to get node client config: %w", err)
			}
			clusterConfig, err := kubeconfig.LoadKubeconfig("")
			if err != nil {
				return nil, fmt.Errorf("failed to load kube config: %w", err)
			}
			nodeStatClient := nodestats.NewNodeStatsSummaryClient(k8sCache, nodeStatConf, clusterConfig)
			ds := collector.NewDefaultCollectorDataSource(
				store,
				clusterInfoProvider,
				k8sCache,
				nodeStatClient,
			)
			return ds, nil
		}
	}

	dataSource, _ := retry.Retry(
		ctx,
		fn,
		maxRetries,
		retryInterval,
	)

	if fatalErr != nil {
		log.Fatalf("Failed to create Prometheus data source: %s", fatalErr)
		panic(fatalErr)
	}

	// Append the pricing config watcher
	installNamespace := env.GetOpencostNamespace()

	configWatchers := watcher.NewConfigMapWatchers(kubeClientset, installNamespace, additionalConfigWatchers...)
	configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))
	configWatchers.AddWatcher(metrics.GetMetricsConfigWatcher())
	configWatchers.Watch()

	clusterMap := dataSource.ClusterMap()
	settingsCache := cache.New(cache.NoExpiration, cache.NoExpiration)

	costModel := NewCostModel(dataSource, cloudProvider, k8sCache, clusterMap, dataSource.BatchDuration())
	metricsEmitter := NewCostModelMetricsEmitter(k8sCache, cloudProvider, clusterInfoProvider, costModel)

	a := &Accesses{
		DataSource:          dataSource,
		KubeClientSet:       kubeClientset,
		ClusterCache:        k8sCache,
		ClusterMap:          clusterMap,
		CloudProvider:       cloudProvider,
		ConfigFileManager:   confManager,
		ClusterInfoProvider: clusterInfoProvider,
		Model:               costModel,
		MetricsEmitter:      metricsEmitter,
		SettingsCache:       settingsCache,
	}

	// Initialize mechanism for subscribing to settings changes
	a.InitializeSettingsPubSub()
	err = a.CloudProvider.DownloadPricingData()
	if err != nil {
		log.Infof("Failed to download pricing data: %s", err)
	}

	if !env.IsKubecostMetricsPodEnabled() {
		a.MetricsEmitter.Start()
	}

	a.DataSource.RegisterEndPoints(router)

	router.GET("/costDataModel", a.CostDataModel)
	router.GET("/allocation/compute", a.ComputeAllocationHandler)
	router.GET("/allocation/compute/summary", a.ComputeAllocationHandlerSummary)
	router.GET("/allNodePricing", a.GetAllNodePricing)
	router.POST("/refreshPricing", a.RefreshPricingData)
	router.GET("/managementPlatform", a.ManagementPlatform)
	router.GET("/clusterInfo", a.ClusterInfo)
	router.GET("/clusterInfoMap", a.GetClusterInfoMap)
	router.GET("/serviceAccountStatus", a.GetServiceAccountStatus)
	router.GET("/pricingSourceStatus", a.GetPricingSourceStatus)
	router.GET("/pricingSourceSummary", a.GetPricingSourceSummary)
	router.GET("/pricingSourceCounts", a.GetPricingSourceCounts)
	router.GET("/orphanedPods", a.GetOrphanedPods)
	router.GET("/installNamespace", a.GetInstallNamespace)
	router.GET("/installInfo", a.GetInstallInfo)
	router.POST("/serviceKey", a.AddServiceKey)
	router.GET("/helmValues", a.GetHelmValues)

	return a
}

// GetDefaultStorage retrieves the default shared storage which is required for running an opencost collector.
func GetDefaultCollectorStorage() storage.Storage {
	const warningMessage = `Failed to create local collector directory '%s' - %s.
		Did you mean to enable to collector? For persistent storage, it's recommended to use Prometheus, 
		or set a storage bucket configuration at %s. 

		%s`

	// Try bucket storage if it exists
	store, err := storage.TryGetDefaultStorage()
	if err == nil {
		return store
	}

	// Fallback to a local storage bucket
	dir := env.GetLocalCollectorDirectory()
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Warnf(
			warningMessage,
			dir,
			err.Error(),
			sysenv.GetDefaultStorageConfigFilePath(),
			"Falling back to an in-memory file system for collector, which will lose any persistent storage upon restart.",
		)

		return storage.NewMemoryStorage()
	}

	return storage.NewFileStorage(dir)
}

// InitializeCloudCost Initializes Cloud Cost pipeline and querier and registers endpoints
func InitializeCloudCost(router *httprouter.Router, providerConfig models.ProviderConfig) *cloudcost.PipelineService {
	log.Debugf("Cloud Cost config path: %s", env.GetCloudCostConfigPath())
	cloudConfigController := cloudconfig.NewMemoryController(providerConfig)

	repo := cloudcost.NewMemoryRepository()
	cloudCostPipelineService := cloudcost.NewPipelineService(repo, cloudConfigController, cloudcost.DefaultIngestorConfiguration())
	repoQuerier := cloudcost.NewRepositoryQuerier(repo)
	cloudCostQueryService := cloudcost.NewQueryService(repoQuerier, repoQuerier)

	router.GET("/cloud/config/export", cloudConfigController.GetExportConfigHandler())
	router.GET("/cloud/config/enable", cloudConfigController.GetEnableConfigHandler())
	router.GET("/cloud/config/disable", cloudConfigController.GetDisableConfigHandler())
	router.GET("/cloud/config/delete", cloudConfigController.GetDeleteConfigHandler())

	router.GET("/cloudCost", cloudCostQueryService.GetCloudCostHandler())
	router.GET("/cloudCost/view/graph", cloudCostQueryService.GetCloudCostViewGraphHandler())
	router.GET("/cloudCost/view/totals", cloudCostQueryService.GetCloudCostViewTotalsHandler())
	router.GET("/cloudCost/view/table", cloudCostQueryService.GetCloudCostViewTableHandler(nil))

	router.GET("/cloudCost/status", cloudCostPipelineService.GetCloudCostStatusHandler())
	router.GET("/cloudCost/rebuild", cloudCostPipelineService.GetCloudCostRebuildHandler())
	router.GET("/cloudCost/repair", cloudCostPipelineService.GetCloudCostRepairHandler())

	return cloudCostPipelineService
}

func InitializeCustomCost(router *httprouter.Router) *customcost.PipelineService {
	hourlyRepo := customcost.NewMemoryRepository()
	dailyRepo := customcost.NewMemoryRepository()
	ingConfig := customcost.DefaultIngestorConfiguration()
	var err error
	customCostPipelineService, err := customcost.NewPipelineService(hourlyRepo, dailyRepo, ingConfig)
	if err != nil {
		log.Errorf("error instantiating custom cost pipeline service: %v", err)
		return nil
	}

	customCostQuerier := customcost.NewRepositoryQuerier(hourlyRepo, dailyRepo, ingConfig.HourlyDuration, ingConfig.DailyDuration)
	customCostQueryService := customcost.NewQueryService(customCostQuerier)

	router.GET("/customCost/total", customCostQueryService.GetCustomCostTotalHandler())
	router.GET("/customCost/timeseries", customCostQueryService.GetCustomCostTimeseriesHandler())

	return customCostPipelineService
}
