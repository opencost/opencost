package main

import (
	"net/http"

	costModel "github.com/kubecost/cost-model/costmodel"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog"
)

func main() {
	rootMux := http.NewServeMux()
	rootMux.Handle("/", costModel.Router)
	rootMux.Handle("/metrics", promhttp.Handler())
	klog.Fatal(http.ListenAndServe(":9003", rootMux))
}
