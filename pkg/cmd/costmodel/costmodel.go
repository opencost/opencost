package costmodel

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/kubecost/opencost/pkg/costmodel"
	"github.com/kubecost/opencost/pkg/errors"
	"github.com/kubecost/opencost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
)

// CostModelOpts contain configuration options that can be passed to the Execute() method
type CostModelOpts struct {
	// Stubbed for future configuration
}

func Healthz(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

func Execute(opts *CostModelOpts) error {
	a := costmodel.Initialize()

	rootMux := http.NewServeMux()
	a.Router.GET("/healthz", Healthz)
	a.Router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
	rootMux.Handle("/", a.Router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(":9003", errors.PanicHandlerMiddleware(handler))
}
