package agent

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/config"
	"github.com/kubecost/cost-model/pkg/costmodel"
	"github.com/kubecost/cost-model/pkg/costmodel/clusters"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util/watcher"

	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"

	"github.com/rs/cors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// AgentOpts contain configuration options that can be passed to the Execute() method
type AgentOpts struct {
	// Stubbed for future configuration
}

// ClusterExportInterval is the interval used to export the cluster if env.IsExportClusterCacheEnabled() is true
const ClusterExportInterval = 5 * time.Minute

// clusterExporter is used if env.IsExportClusterCacheEnabled() is set to true
// it will export the kubernetes cluster data to a file on a specific interval
var clusterExporter *clustercache.ClusterExporter

func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

// initializes the kubernetes client cache
func newKubernetesClusterCache() (kubernetes.Interface, clustercache.ClusterCache, error) {
	var err error

	// Kubernetes API setup
	var kc *rest.Config
	if kubeconfig := env.GetKubeConfigPath(); kubeconfig != "" {
		kc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		kc, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, nil, err
	}

	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		return nil, nil, err
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	return kubeClientset, k8sCache, nil
}

func newPrometheusClient() (prometheus.Client, error) {
	address := env.GetPrometheusServerEndpoint()
	if address == "" {
		return nil, fmt.Errorf("No address for prometheus set in $%s. Aborting.", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	log.Infof("Prometheus Client Max Concurrency set to %d", queryConcurrency)

	timeout := 120 * time.Second
	keepAlive := 120 * time.Second

	promCli, err := prom.NewPrometheusClient(address, timeout, keepAlive, queryConcurrency, "")
	if err != nil {
		return nil, fmt.Errorf("Failed to create prometheus client, Error: %v", err)
	}

	m, err := prom.Validate(promCli)
	if err != nil || !m.Running {
		if err != nil {
			log.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prom.PrometheusTroubleshootingURL)
		} else if !m.Running {
			log.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prom.PrometheusTroubleshootingURL)
		}
	} else {
		log.Infof("Success: retrieved the 'up' query against prometheus at: %s", address)
	}

	api := prometheusAPI.NewAPI(promCli)
	_, err = api.Config(context.Background())
	if err != nil {
		log.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/thanos here.", address, err.Error(), prom.PrometheusTroubleshootingURL)
	} else {
		log.Infof("Retrieved a prometheus config file from: %s", address)
	}

	return promCli, nil
}

func Execute(opts *AgentOpts) error {
	log.Infof("Starting Kubecost Agent")

	configWatchers := watcher.NewConfigMapWatchers()

	scrapeInterval := time.Minute
	promCli, err := newPrometheusClient()
	if err != nil {
		panic(err.Error())
	}

	// Lookup scrape interval for kubecost job, update if found
	si, err := prom.ScrapeIntervalFor(promCli, env.GetKubecostJobName())
	if err == nil {
		scrapeInterval = si
	}

	klog.Infof("Using scrape interval of %f", scrapeInterval.Seconds())

	// initialize kubernetes client and cluster cache
	k8sClient, clusterCache, err := newKubernetesClusterCache()
	if err != nil {
		panic(err.Error())
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(&config.ConfigFileManagerOpts{
		BucketStoreConfig: env.GetKubecostConfigBucket(),
		LocalConfigPath:   "/",
	})

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := cloud.NewProvider(clusterCache, cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// Append the pricing config watcher
	configWatchers.AddWatcher(cloud.ConfigWatcherFor(cloudProvider))
	watchConfigFunc := configWatchers.ToWatchFunc()
	watchedConfigs := configWatchers.GetWatchedConfigs()

	kubecostNamespace := env.GetKubecostNamespace()

	// We need an initial invocation because the init of the cache has happened before we had access to the provider.
	for _, cw := range watchedConfigs {
		configs, err := k8sClient.CoreV1().ConfigMaps(kubecostNamespace).Get(context.Background(), cw, metav1.GetOptions{})
		if err != nil {
			klog.Infof("No %s configmap found at install time, using existing configs: %s", cw, err.Error())
		} else {
			watchConfigFunc(configs)
		}
	}

	clusterCache.SetConfigMapUpdateFunc(watchConfigFunc)

	// Initialize cluster exporting if it's enabled
	if env.IsExportClusterCacheEnabled() {
		cacheLocation := confManager.ConfigFileAt("/var/configs/cluster-cache.json")
		clusterExporter = clustercache.NewClusterExporter(clusterCache, cacheLocation, ClusterExportInterval)
		clusterExporter.Run()
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	localClusterInfo := costmodel.NewLocalClusterInfoProvider(k8sClient, cloudProvider)

	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsExportClusterInfoEnabled() {
		clusterInfoConf := confManager.ConfigFileAt("/var/configs/cluster-info.json")
		clusterInfoProvider = costmodel.NewClusterInfoWriteOnRequest(localClusterInfo, clusterInfoConf)
	} else {
		clusterInfoProvider = localClusterInfo
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	clusterMap := clusters.NewClusterMap(promCli, clusterInfoProvider, 5*time.Minute)

	costModel := costmodel.NewCostModel(promCli, cloudProvider, clusterCache, clusterMap, scrapeInterval)

	// initialize Kubernetes Metrics Emitter
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(promCli, clusterCache, cloudProvider, clusterInfoProvider, costModel)

	// download pricing data
	err = cloudProvider.DownloadPricingData()
	if err != nil {
		klog.Errorf("Error downloading pricing data: %s", err)
	}

	// start emitting metrics
	metricsEmitter.Start()

	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/healthz", Healthz)
	rootMux.Handle("/metrics", promhttp.Handler())
	handler := cors.AllowAll().Handler(rootMux)

	return http.ListenAndServe(fmt.Sprintf(":%d", env.GetKubecostMetricsPort()), handler)
}
