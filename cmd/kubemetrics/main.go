package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()

	klog.V(1).Infof("Starting kubecost-metrics...")

	// initialize kubernetes client and cluster cache
	clusterCache, err := newKubernetesClusterCache()
	if err != nil {
		panic(err.Error())
	}

	// initialize Kubernetes Metrics
	metrics.InitKubeMetrics(clusterCache, &metrics.KubeMetricsOpts{
		EmitKubecostControllerMetrics: true,
		EmitNamespaceAnnotations:      env.IsEmitNamespaceAnnotationsMetric(),
		EmitPodAnnotations:            env.IsEmitPodAnnotationsMetric(),
		EmitKubeStateMetrics:          true,
	})

	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/healthz", Healthz)
	rootMux.Handle("/metrics", promhttp.Handler())
	handler := cors.AllowAll().Handler(rootMux)

	klog.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", env.GetKubecostMetricsPort()), handler))
}
