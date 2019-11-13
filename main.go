package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/kubecost/cost-model/costmodel"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog"
)

func Healthz(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

func main() {
	costmodel.Initialize()

	rootMux := http.NewServeMux()
	costmodel.Router.GET("/healthz", Healthz)
	rootMux.Handle("/", costmodel.Router)
	rootMux.Handle("/metrics", promhttp.Handler())
	klog.Fatal(http.ListenAndServe(":9003", rootMux))
}
