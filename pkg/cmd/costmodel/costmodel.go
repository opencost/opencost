package costmodel

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/util/apiutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/customcost"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	mcp_sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/opencost/opencost/core/pkg/errors"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/version"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/filemanager"
	opencost_mcp "github.com/opencost/opencost/pkg/mcp"
	"github.com/opencost/opencost/pkg/metrics"
)

func Execute(conf *Config) error {
	log.Infof("Starting cost-model version %s", version.FriendlyVersion())
	if conf == nil {
		conf = DefaultConfig()
	}
	conf.log()

	router := httprouter.New()
	var a *costmodel.Accesses
	var cp models.Provider
	if conf.KubernetesEnabled {
		a = costmodel.Initialize(router)
		err := StartExportWorker(context.Background(), a.Model)
		if err != nil {
			log.Errorf("couldn't start CSV export worker: %v", err)
		}

		// Register OpenCost Specific Endpoints
		router.GET("/allocation", a.ComputeAllocationHandler)
		router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
		router.GET("/assets", a.ComputeAssetsHandler)
		if conf.CarbonEstimatesEnabled {
			router.GET("/assets/carbon", a.ComputeAssetsCarbonHandler)
		}

		// set cloud provider for cloud cost
		cp = a.CloudProvider
	}

	var cloudCostPipelineService *cloudcost.PipelineService
	if conf.CloudCostEnabled {
		var providerConfig models.ProviderConfig
		if cp != nil {
			providerConfig = provider.ExtractConfigFromProviders(cp)
		}
		cloudCostPipelineService = costmodel.InitializeCloudCost(router, providerConfig)
	}

	var customCostPipelineService *customcost.PipelineService
	if conf.CloudCostEnabled {
		customCostPipelineService = costmodel.InitializeCustomCost(router)
	}

	// this endpoint is intentionally left out of the "if env.IsCustomCostEnabled()" conditional; in the handler, it is
	// valid for CustomCostPipelineService to be nil
	router.GET("/customCost/status", customCostPipelineService.GetCustomCostStatusHandler())

	// Initialize MCP Server if enabled and Kubernetes is available
	if conf.MCPServerEnabled && a != nil {
		// Get cloud cost querier if cloud costs are enabled
		var cloudCostQuerier cloudcost.Querier
		if conf.CloudCostEnabled && cloudCostPipelineService != nil {
			cloudCostQuerier = cloudCostPipelineService.GetCloudCostQuerier()
		}

		err := StartMCPServer(context.Background(), a, cloudCostQuerier)
		if err != nil {
			log.Errorf("Failed to start MCP server: %v", err)
		}
	} else if conf.MCPServerEnabled {
		log.Warnf("MCP Server is enabled but Kubernetes is not available. MCP server requires Kubernetes to function.")
	}

	apiutil.ApplyContainerDiagnosticEndpoints(router)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	return http.ListenAndServe(fmt.Sprint(":", conf.Port), errors.PanicHandlerMiddleware(handler))
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
			case <-time.After(time.Until(nextRunAt)):
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

// StartMCPServer starts the MCP server as a background service
func StartMCPServer(ctx context.Context, accesses *costmodel.Accesses, cloudCostQuerier cloudcost.Querier) error {
	log.Info("Initializing MCP server...")

	// Create MCP server using existing OpenCost dependencies
	mcpServer := opencost_mcp.NewMCPServer(accesses.Model, accesses.CloudProvider, cloudCostQuerier)

	// Create MCP SDK server
	sdkServer := mcp_sdk.NewServer(&mcp_sdk.Implementation{
		Name:    "opencost-mcp-server",
		Version: version.Version,
	}, nil)

	// Define tool handlers
	handleAllocationCosts := func(ctx context.Context, req *mcp_sdk.CallToolRequest, args AllocationArgs) (*mcp_sdk.CallToolResult, interface{}, error) {
		// Parse step duration if provided
		var step time.Duration
		var err error
		if args.Step != "" {
			step, err = time.ParseDuration(args.Step)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid step duration '%s': %w", args.Step, err)
			}
		}

		queryRequest := &opencost_mcp.OpenCostQueryRequest{
			QueryType: opencost_mcp.AllocationQueryType,
			Window:    args.Window,
			AllocationParams: &opencost_mcp.AllocationQuery{
				Step:                                  step,
				Accumulate:                            args.Accumulate,
				ShareIdle:                             args.ShareIdle,
				Aggregate:                             args.Aggregate,
				IncludeIdle:                           args.IncludeIdle,
				IdleByNode:                            args.IdleByNode,
				IncludeProportionalAssetResourceCosts: args.IncludeProportionalAssetResourceCosts,
				IncludeAggregatedMetadata:             args.IncludeAggregatedMetadata,
				ShareLB:                               args.ShareLB,
				Filter:                                args.Filter,
			},
		}

		mcpReq := &opencost_mcp.MCPRequest{
			Query: queryRequest,
		}

		mcpResp, err := mcpServer.ProcessMCPRequest(mcpReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process allocation request: %w", err)
		}

		return nil, mcpResp, nil
	}

	handleAssetCosts := func(ctx context.Context, req *mcp_sdk.CallToolRequest, args AssetArgs) (*mcp_sdk.CallToolResult, interface{}, error) {
		queryRequest := &opencost_mcp.OpenCostQueryRequest{
			QueryType:   opencost_mcp.AssetQueryType,
			Window:      args.Window,
			AssetParams: &opencost_mcp.AssetQuery{},
		}

		mcpReq := &opencost_mcp.MCPRequest{
			Query: queryRequest,
		}

		mcpResp, err := mcpServer.ProcessMCPRequest(mcpReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process asset request: %w", err)
		}

		return nil, mcpResp, nil
	}

	handleCloudCosts := func(ctx context.Context, req *mcp_sdk.CallToolRequest, args CloudCostArgs) (*mcp_sdk.CallToolResult, interface{}, error) {
		queryRequest := &opencost_mcp.OpenCostQueryRequest{
			QueryType: opencost_mcp.CloudCostQueryType,
			Window:    args.Window,
			CloudCostParams: &opencost_mcp.CloudCostQuery{
				Aggregate:  args.Aggregate,
				Accumulate: args.Accumulate,
				Filter:     args.Filter,
				Provider:   args.Provider,
				Service:    args.Service,
				Category:   args.Category,
				Region:     args.Region,
				AccountID:  args.Account,
			},
		}

		mcpReq := &opencost_mcp.MCPRequest{
			Query: queryRequest,
		}

		mcpResp, err := mcpServer.ProcessMCPRequest(mcpReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process cloud cost request: %w", err)
		}

		return nil, mcpResp, nil
	}

	handleEfficiency := func(ctx context.Context, req *mcp_sdk.CallToolRequest, args EfficiencyArgs) (*mcp_sdk.CallToolResult, interface{}, error) {
		queryRequest := &opencost_mcp.OpenCostQueryRequest{
			QueryType: opencost_mcp.EfficiencyQueryType,
			Window:    args.Window,
			EfficiencyParams: &opencost_mcp.EfficiencyQuery{
				Aggregate:                  args.Aggregate,
				Filter:                     args.Filter,
				EfficiencyBufferMultiplier: args.BufferMultiplier,
			},
		}

		mcpReq := &opencost_mcp.MCPRequest{
			Query: queryRequest,
		}

		mcpResp, err := mcpServer.ProcessMCPRequest(mcpReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process efficiency request: %w", err)
		}

		return nil, mcpResp, nil
	}

	// Register tools
	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name:        "get_allocation_costs",
		Description: "Retrieves allocation cost data.",
	}, handleAllocationCosts)

	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name:        "get_asset_costs",
		Description: "Retrieves asset cost data.",
	}, handleAssetCosts)

	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name:        "get_cloud_costs",
		Description: "Retrieves cloud cost data.",
	}, handleCloudCosts)

	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name:        "get_efficiency",
		Description: "Retrieves resource efficiency metrics with rightsizing recommendations and cost savings analysis. Computes CPU and memory efficiency (usage/request ratio), provides recommended resource requests, and calculates potential cost savings. Optional buffer_multiplier parameter (default: 1.2 for 20% headroom) can be set to values like 1.4 for 40% headroom.",
	}, handleEfficiency)

	// Create HTTP handler
	handler := mcp_sdk.NewStreamableHTTPHandler(func(r *http.Request) *mcp_sdk.Server {
		return sdkServer
	}, &mcp_sdk.StreamableHTTPOptions{
		JSONResponse: true,
	})

	// Add logging middleware
	loggingHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		log.Debugf("MCP HTTP request: %s %s from %s", req.Method, req.URL.Path, req.RemoteAddr)
		handler.ServeHTTP(w, req)
	})

	// Start HTTP server on configured port
	port := env.GetMCPHTTPPort()
	log.Infof("Starting MCP HTTP server on port %d...", port)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: loggingHandler,
	}

	// Start server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("MCP server failed: %v", err)
		}
	}()

	log.Info("MCP server started successfully")
	return nil
}

// Tool argument structures for MCP server
type AllocationArgs struct {
	Window    string `json:"window"`
	Aggregate string `json:"aggregate"`

	// Allocation query parameters
	Step                                  string `json:"step,omitempty"`
	Resolution                            string `json:"resolution,omitempty"`
	Accumulate                            bool   `json:"accumulate,omitempty"`
	ShareIdle                             bool   `json:"share_idle,omitempty"`
	IncludeIdle                           bool   `json:"include_idle,omitempty"`
	IdleByNode                            bool   `json:"idle_by_node,omitempty"`
	IncludeProportionalAssetResourceCosts bool   `json:"include_proportional_asset_resource_costs,omitempty"`
	IncludeAggregatedMetadata             bool   `json:"include_aggregated_metadata,omitempty"`
	ShareLB                               bool   `json:"share_lb,omitempty"`
	Filter                                string `json:"filter,omitempty"`
}

type AssetArgs struct {
	Window string `json:"window"`
}

type CloudCostArgs struct {
	Window    string `json:"window"`
	Aggregate string `json:"aggregate"`

	// Cloud cost query parameters
	Accumulate string `json:"accumulate,omitempty"`
	Filter     string `json:"filter,omitempty"`
	Provider   string `json:"provider,omitempty"`
	Service    string `json:"service,omitempty"`
	Category   string `json:"category,omitempty"`
	Region     string `json:"region,omitempty"`
	Account    string `json:"account,omitempty"`
}

type EfficiencyArgs struct {
	Window           string   `json:"window"`                      // Time window (e.g., "today", "yesterday", "7d", "lastweek")
	Aggregate        string   `json:"aggregate,omitempty"`         // Aggregation level (e.g., "pod", "namespace", "controller")
	Filter           string   `json:"filter,omitempty"`            // Filter expression (same as allocation filters)
	BufferMultiplier *float64 `json:"buffer_multiplier,omitempty"` // Buffer multiplier for recommendations (default: 1.2 for 20% headroom, e.g., 1.4 for 40%)
}
