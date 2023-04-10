package costmodel

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	"github.com/opencost/opencost/pkg/filemanager"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/metrics"
	"github.com/opencost/opencost/pkg/version"
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
	log.Infof("Starting cost-model version %s", version.FriendlyVersion())
	a := costmodel.Initialize()

	StartExportWorker(context.Background(), a.Model)

	rootMux := http.NewServeMux()
	a.Router.GET("/healthz", Healthz)
	a.Router.GET("/allocation", a.ComputeAllocationHandler)
	a.Router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
	rootMux.Handle("/", a.Router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(":9003", errors.PanicHandlerMiddleware(handler))
}

func StartExportWorker(ctx context.Context, model costmodel.AllocationModel) {
	// TODO: there should be a better way to load the configuration
	exportPath := os.Getenv(env.ExportCSVFile)
	if exportPath == "" {
		log.Infof("%s is not set, skipping CSV exporter", env.ExportCSVFile)
		return
	}

	fm, err := filemanager.NewFileManager(exportPath)
	if err != nil {
		log.Errorf("could not start CSV exporter: %v", err)
		return
	}
	go func() {
		log.Info("Starting CSV exporter worker...")

		// perform first update immediately
		nextRunAt := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(nextRunAt.Sub(time.Now())):
				err := costmodel.UpdateCSV(ctx, fm, model)
				if err != nil {
					// it's background worker, log error and carry on, maybe next time it will work
					log.Errorf("Error updating CSV: %s", err)
				}
				now := time.Now().UTC()
				// next launch is at 00:10 UTC tomorrow
				// extra 10 minutes is to let prometheus to collect all the data for the previous day
				nextRunAt = time.Date(now.Year(), now.Month(), now.Day(), 0, 10, 0, 0, now.Location()).AddDate(0, 0, 1)
			}
		}
	}()
}
