package agent

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util/retry"
	"github.com/opencost/opencost/pkg/util/watcher"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"
	"github.com/opencost/opencost/pkg/cloud/provider"
	cluster "github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rs/cors"
	"k8s.io/client-go/kubernetes"
)

// AgentOpts contain configuration options that can be passed to the Execute() method
type AgentOpts struct {
	// Stubbed for future configuration
}

// ClusterExportInterval is the interval used to export the cluster if env.IsExportClusterCacheEnabled() is true
const ClusterExportInterval = 5 * time.Minute

// clusterExporter is used if env.IsExportClusterCacheEnabled() is set to true
// it will export the kubernetes cluster data to a file on a specific interval
var clusterExporter *cluster.ClusterExporter

func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

// initializes the kubernetes client cache
func newKubernetesClusterCache() (kubernetes.Interface, clustercache.ClusterCache, error) {
	var err error

	// Kubernetes API setup
	kubeClientset, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		return nil, nil, err
	}

	// Create Kubernetes Cluster Cache + Watchers
	k8sCache := cluster.NewKubernetesClusterCache(kubeClientset)
	k8sCache.Run()

	return kubeClientset, k8sCache, nil
}

func Execute(opts *AgentOpts) error {
	log.Infof("Starting Kubecost Agent version %s", version.FriendlyVersion())

	// initialize kubernetes client and cluster cache
	k8sClient, clusterCache, err := newKubernetesClusterCache()
	if err != nil {
		panic(err.Error())
	}

	clusterUID, err := kubeconfig.GetClusterUID(k8sClient)
	if err != nil {
		return fmt.Errorf("error getting cluster UID: %w", err)
	}

	// Create ConfigFileManager for synchronization of shared configuration
	confManager := config.NewConfigFileManager(nil)

	cloudProviderKey := env.GetCloudProviderAPIKey()
	cloudProvider, err := provider.NewProvider(clusterCache, cloudProviderKey, confManager)
	if err != nil {
		panic(err.Error())
	}

	// ClusterInfo Provider to provide the cluster map with local and remote cluster data
	localClusterInfo := costmodel.NewLocalClusterInfoProvider(k8sClient, cloudProvider)

	var clusterInfoProvider clusters.ClusterInfoProvider
	if env.IsExportClusterInfoEnabled() {
		clusterInfoConf := confManager.ConfigFileAt(env.GetClusterInfoFilePath())
		clusterInfoProvider = costmodel.NewClusterInfoWriteOnRequest(localClusterInfo, clusterInfoConf)
	} else {
		clusterInfoProvider = localClusterInfo
	}

	const maxRetries = 10
	const retryInterval = 10 * time.Second

	var fatalErr error

	ctx, cancel := context.WithCancel(context.Background())
	dataSource, err := retry.Retry(
		ctx,
		func() (source.OpenCostDataSource, error) {
			ds, e := prom.NewDefaultPrometheusDataSource(clusterInfoProvider)
			if e != nil {
				if source.IsRetryable(e) {
					return nil, e
				}
				fatalErr = e
				cancel()
			}

			return ds, e
		},
		maxRetries,
		retryInterval,
	)

	if fatalErr != nil {
		log.Fatalf("Failed to create Prometheus data source: %s", fatalErr)
		panic(fatalErr)
	}

	// Append the pricing config watcher
	installNamespace := env.GetOpencostNamespace()
	configWatchers := watcher.NewConfigMapWatchers(k8sClient, installNamespace)
	configWatchers.AddWatcher(provider.ConfigWatcherFor(cloudProvider))
	configWatchers.Watch()

	// Initialize cluster exporting if it's enabled
	if env.IsExportClusterCacheEnabled() {
		cacheLocation := confManager.ConfigFileAt(env.GetClusterCacheFilePath())
		clusterExporter = cluster.NewClusterExporter(clusterCache, cacheLocation, ClusterExportInterval)
		clusterExporter.Run()
	}

	// Initialize ClusterMap for maintaining ClusterInfo by ClusterID
	clusterMap := dataSource.ClusterMap()

	costModel := costmodel.NewCostModel(clusterUID, dataSource, cloudProvider, clusterCache, clusterMap, dataSource.BatchDuration())

	// initialize Kubernetes Metrics Emitter
	metricsEmitter := costmodel.NewCostModelMetricsEmitter(clusterCache, cloudProvider, clusterInfoProvider, costModel)

	// download pricing data
	err = cloudProvider.DownloadPricingData()
	if err != nil {
		log.Errorf("Error downloading pricing data: %s", err)
	}

	// start emitting metrics
	metricsEmitter.Start()

	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/healthz", Healthz)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(fmt.Sprintf(":%d", env.GetKubecostMetricsPort()), handler)
}
