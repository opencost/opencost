package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/costmodel"
	"github.com/kubecost/cost-model/pkg/costmodel/clusters"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util/watcher"

	prometheus "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rs/cors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

// initializes the kubernetes client cache
func newKubernetesClusterCache() (clustercache.ClusterCache, error) {
	var err error

	// Kubernetes API setup
	var kc *rest.Config
	if kubeconfig := env.GetKubeConfigPath(); kubeconfig != "" {
		kc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		kc, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, err
	}

	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		return nil, err
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := clustercache.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	return k8sCache, nil
}

func newPrometheusClient() (prometheus.Client, error) {
	address := env.GetPrometheusServerEndpoint()
	if address == "" {
		return nil, fmt.Errorf("No address for prometheus set in $%s. Aborting.", env.PrometheusServerEndpointEnvVar)
	}

	queryConcurrency := env.GetMaxQueryConcurrency()
	klog.Infof("Prometheus Client Max Concurrency set to %d", queryConcurrency)

	timeout := 120 * time.Second
	keepAlive := 120 * time.Second

	promCli, err := prom.NewPrometheusClient(address, timeout, keepAlive, queryConcurrency, "")
	if err != nil {
		return nil, fmt.Errorf("Failed to create prometheus client, Error: %v", err)
	}

	m, err := prom.Validate(promCli)
	if err != nil || !m.Running {
		if err != nil {
			klog.Errorf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prom.PrometheusTroubleshootingURL)
		} else if !m.Running {
			klog.Errorf("Prometheus at %s is not running. Troubleshooting help available at: %s", address, prom.PrometheusTroubleshootingURL)
		}
	} else {
		klog.V(1).Info("Success: retrieved the 'up' query against prometheus at: " + address)
	}

	api := prometheusAPI.NewAPI(promCli)
	_, err = api.Config(context.Background())
	if err != nil {
		klog.Infof("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s. Ignore if using cortex/thanos here.", address, err.Error(), prom.PrometheusTroubleshootingURL)
	} else {
		klog.Infof("Retrieved a prometheus config file from: %s", address)
	}

	return promCli, nil
}

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()

	klog.V(1).Infof("Starting kubecost-metrics...")

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
	clusterCache, err := newKubernetesClusterCache()
	if err != nil {
		panic(err.Error())
	}

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := cloud.NewProvider(clusterCache, cloudProviderKey)
	if err != nil {
		panic(err.Error())
	}

	// Append the pricing config watcher
	configWatchers.AddWatcher(cloud.ConfigWatcherFor(cloudProvider))
	watchConfigFunc := configWatchers.ToWatchFunc()
	watchedConfigs := configWatchers.GetWatchedConfigs()

	k8sClient := clusterCache.GetClient()
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

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	clusterMap := clusters.NewClusterMap(
		promCli,
		costmodel.NewLocalClusterInfoProvider(k8sClient, cloudProvider),
		5*time.Minute)

	costModel := costmodel.NewCostModel(promCli, cloudProvider, clusterCache, clusterMap, scrapeInterval)

	// initialize Kubernetes Metrics Emitter
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(promCli, clusterCache, cloudProvider, costModel)

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

	klog.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", env.GetKubecostMetricsPort()), handler))
}
