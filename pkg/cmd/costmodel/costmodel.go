package costmodel

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	"github.com/opencost/opencost/pkg/filemanager"
	"github.com/opencost/opencost/pkg/metrics"
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
	log.Infof("Kubernetes enabled: %t", env.IsKubernetesEnabled())

	var a *costmodel.Accesses

	if env.IsKubernetesEnabled() {
		a = costmodel.Initialize()
		err := StartExportWorker(context.Background(), a.Model)
		if err != nil {
			log.Errorf("couldn't start CSV export worker: %v", err)
		}
	} else {
		a = costmodel.InitializeWithoutKubernetes()
		log.Debugf("Cloud Cost config path: %s", env.GetCloudCostConfigPath())
	}

	log.Infof("Cloud Costs enabled: %t", env.IsCloudCostEnabled())
	if env.IsCloudCostEnabled() {
		repo := cloudcost.NewMemoryRepository()
		a.CloudCostPipelineService = cloudcost.NewPipelineService(repo, a.CloudConfigController, cloudcost.DefaultIngestorConfiguration())
		repoQuerier := cloudcost.NewRepositoryQuerier(repo)
		a.CloudCostQueryService = cloudcost.NewQueryService(repoQuerier, repoQuerier)
	}

	rootMux := http.NewServeMux()
	a.Router.GET("/healthz", Healthz)

	if env.IsKubernetesEnabled() {
		a.Router.GET("/allocation", a.ComputeAllocationHandler)
		a.Router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
		a.Router.GET("/assets", a.ComputeAssetsHandler)
		if env.IsCarbonEstimatesEnabled() {
			a.Router.GET("/assets/carbon", a.ComputeAssetsCarbonHandler)
		}
	}

	a.Router.GET("/cloudCost", a.CloudCostQueryService.GetCloudCostHandler())
	a.Router.GET("/cloudCost/view/graph", a.CloudCostQueryService.GetCloudCostViewGraphHandler())
	a.Router.GET("/cloudCost/view/totals", a.CloudCostQueryService.GetCloudCostViewTotalsHandler())
	a.Router.GET("/cloudCost/view/table", a.CloudCostQueryService.GetCloudCostViewTableHandler())

	a.Router.GET("/cloudCost/status", a.CloudCostPipelineService.GetCloudCostStatusHandler())
	a.Router.GET("/cloudCost/rebuild", a.CloudCostPipelineService.GetCloudCostRebuildHandler())
	a.Router.GET("/cloudCost/repair", a.CloudCostPipelineService.GetCloudCostRepairHandler())

	if env.IsPProfEnabled() {
		a.Router.HandlerFunc(http.MethodGet, "/debug/pprof/", pprof.Index)
		a.Router.HandlerFunc(http.MethodGet, "/debug/pprof/cmdline", pprof.Cmdline)
		a.Router.HandlerFunc(http.MethodGet, "/debug/pprof/profile", pprof.Profile)
		a.Router.HandlerFunc(http.MethodGet, "/debug/pprof/symbol", pprof.Symbol)
		a.Router.HandlerFunc(http.MethodGet, "/debug/pprof/trace", pprof.Trace)
		a.Router.Handler(http.MethodGet, "/debug/pprof/goroutine", pprof.Handler("goroutine"))
		a.Router.Handler(http.MethodGet, "/debug/pprof/heap", pprof.Handler("heap"))
	}

	rootMux.Handle("/", a.Router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(fmt.Sprint(":", env.GetAPIPort()), errors.PanicHandlerMiddleware(handler))
}

func StartExportWorker(ctx context.Context, model costmodel.AllocationModel) error {
	exportPath := env.GetExportCSVFile()
	if exportPath == "" {
		log.Infof("%s is not set, CSV export is disabled", env.ExportCSVFile)
		return nil
	}
	fm, err := filemanager.NewFileManager(exportPath)
	if err != nil {
		return fmt.Errorf("could not create file manager: %v", err)
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
				err := costmodel.UpdateCSV(ctx, fm, model, env.GetExportCSVLabelsAll(), env.GetExportCSVLabelsList())
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
	return nil
}
