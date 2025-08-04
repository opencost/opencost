package costmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/util/apiutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/customcost"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/opencost/opencost/core/pkg/errors"
	coreLog "github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/filemanager"
	"github.com/opencost/opencost/pkg/mcp/server"
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
	coreLog.Infof("Starting cost-model version %s", version.FriendlyVersion())
	coreLog.Infof("Kubernetes enabled: %t", env.IsKubernetesEnabled())

	router := httprouter.New()
	var a *costmodel.Accesses
	var cp models.Provider
	if env.IsKubernetesEnabled() {
		a = costmodel.Initialize(router)
		err := StartExportWorker(context.Background(), a.Model)
		if err != nil {
			coreLog.Errorf("couldn't start CSV export worker: %v", err)
		}

		// Register OpenCost Specific Endpoints
		router.GET("/allocation", a.ComputeAllocationHandler)
		router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
		router.GET("/assets", a.ComputeAssetsHandler)
		if env.IsCarbonEstimatesEnabled() {
			router.GET("/assets/carbon", a.ComputeAssetsCarbonHandler)
		}

		// set cloud provider for cloud cost
		cp = a.CloudProvider
	}

	coreLog.Infof("Cloud Costs enabled: %t", env.IsCloudCostEnabled())
	if env.IsCloudCostEnabled() {
		var providerConfig models.ProviderConfig
		if cp != nil {
			providerConfig = provider.ExtractConfigFromProviders(cp)
		}
		costmodel.InitializeCloudCost(router, providerConfig)
	}

	coreLog.Infof("Custom Costs enabled: %t", env.IsCustomCostEnabled())
	var customCostPipelineService *customcost.PipelineService
	if env.IsCustomCostEnabled() {
		customCostPipelineService = costmodel.InitializeCustomCost(router)
	}

	// this endpoint is intentionally left out of the "if env.IsCustomCostEnabled()" conditional; in the handler, it is
	// valid for CustomCostPipelineService to be nil
	router.GET("/customCost/status", customCostPipelineService.GetCustomCostStatusHandler())

	// Initialize MCP server if enabled
	coreLog.Infof("MCP Server enabled: %t", env.IsMCPEnabled())
	if env.IsMCPEnabled() {
		err := initializeMCPServer(router, a)
		if err != nil {
			coreLog.Errorf("Failed to initialize MCP server: %v", err)
		}
	}

	router.GET("/healthz", Healthz)

	router.GET("/logs/level", GetLogLevel)
	router.POST("/logs/level", SetLogLevel)

	if env.IsPProfEnabled() {
		router.HandlerFunc(http.MethodGet, "/debug/pprof/", pprof.Index)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/cmdline", pprof.Cmdline)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/profile", pprof.Profile)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/symbol", pprof.Symbol)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/trace", pprof.Trace)
		router.Handler(http.MethodGet, "/debug/pprof/goroutine", pprof.Handler("goroutine"))
		router.Handler(http.MethodGet, "/debug/pprof/heap", pprof.Handler("heap"))
	}

	apiutil.ApplyContainerDiagnosticEndpoints(router)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(fmt.Sprint(":", env.GetAPIPort()), errors.PanicHandlerMiddleware(handler))
}

func StartExportWorker(ctx context.Context, model costmodel.AllocationModel) error {
	exportPath := env.GetExportCSVFile()
	if exportPath == "" {
		coreLog.Infof("%s is not set, CSV export is disabled", env.ExportCSVFile)
		return nil
	}
	fm, err := filemanager.NewFileManager(exportPath)
	if err != nil {
		return fmt.Errorf("could not create file manager: %v", err)
	}
	go func() {
		coreLog.Info("Starting CSV exporter worker...")

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
					coreLog.Errorf("Error updating CSV: %s", err)
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
type LogLevelRequestResponse struct {
	Level string `json:"level"`
}

func GetLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	level := coreLog.GetLogLevel()
	llrr := LogLevelRequestResponse{
		Level: level,
	}

	body, err := json.Marshal(llrr)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to retrive log level"), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to write response: %s", body), http.StatusInternalServerError)
		return
	}
}

func SetLogLevel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	params := LogLevelRequestResponse{}
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to decode request body, error: %s", err), http.StatusBadRequest)
		return
	}

	err = coreLog.SetLogLevel(params.Level)
	if err != nil {
		http.Error(w, fmt.Sprintf("level must be a valid log level according to zerolog; level given: %s, error: %s", params.Level, err), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// initializeMCPServer initializes the MCP (Model Context Protocol) server
func initializeMCPServer(router *httprouter.Router, accesses *costmodel.Accesses) error {
	coreLog.Info("Initializing MCP server...")

	// Create MCP server configuration
	mcpConfig := &server.ServerConfig{
		OpenCostURL:    fmt.Sprintf("http://localhost:%d", env.GetAPIPort()),
		SessionTimeout: 30 * time.Minute,
		MaxRequestSize: 10 * 1024 * 1024, // 10MB
		EnableDebug:    false,
		CorsEnabled:    true,
		AllowedOrigins: []string{"*"},
		RateLimitRPS:   100,
	}

	// Create MCP server logger
	mcpLogger := log.New(os.Stderr, "[MCP] ", log.LstdFlags)

	// Create and configure MCP server
	mcpServer, err := server.NewMCPServer(mcpConfig, mcpLogger)
	if err != nil {
		return fmt.Errorf("failed to create MCP server: %w", err)
	}

	// Set up MCP server routes
	// Main MCP endpoint that handles all MCP protocol requests
	router.Handler("POST", "/mcp", mcpServer)
	router.Handler("OPTIONS", "/mcp", mcpServer)

	// GET endpoint for MCP server capabilities and status
	router.GET("/mcp/status", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		
		status := map[string]interface{}{
			"status":      "active",
			"version":     "1.0.0",
			"protocol":    "mcp",
			"tools":       []string{"query_allocations", "query_assets", "query_cloud_costs"},
			"description": "OpenCost Model Context Protocol server for AI-powered cost analysis",
			"endpoints": map[string]string{
				"mcp":    "/mcp",
				"status": "/mcp/status",
			},
		}
		
		body, err := json.Marshal(status)
		if err != nil {
			http.Error(w, "Failed to marshal status", http.StatusInternalServerError)
			return
		}
		
		w.Write(body)
	})

	coreLog.Info("MCP server initialized successfully")
	coreLog.Info("MCP endpoints:")
	coreLog.Info("  POST/OPTIONS /mcp - MCP JSON-RPC requests")
	coreLog.Info("  GET /mcp/status - MCP server status")
	coreLog.Infof("MCP server configured with OpenCost URL: %s", mcpConfig.OpenCostURL)
	
	return nil
}
