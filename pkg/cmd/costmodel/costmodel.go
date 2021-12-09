package costmodel

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/kubecost/cost-model/pkg/costmodel"
	"github.com/kubecost/cost-model/pkg/errors"
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
	rootMux.Handle("/", a.Router)
	rootMux.Handle("/metrics", promhttp.Handler())
	handler := cors.AllowAll().Handler(rootMux)

	return http.ListenAndServe(":9003", errors.PanicHandlerMiddleware(handler))
}
