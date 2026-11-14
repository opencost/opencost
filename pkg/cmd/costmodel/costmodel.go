package costmodel

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/util/apiutil"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
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
	"github.com/opencost/opencost/pkg/inferencecost"
	opencost_mcp "github.com/opencost/opencost/pkg/mcp"
	"github.com/opencost/opencost/pkg/metrics"
)

const shutdownTimeout = 30 * time.Second

func Execute(conf *Config) error {
	log.Infof("Starting cost-model version %s", version.FriendlyVersion())
	if conf == nil {
		conf = DefaultConfig()
	}
	conf.log()

	// Create cancellable context for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	router := httprouter.New()
	var a *costmodel.Accesses

	if conf.KubernetesEnabled {
		a = costmodel.Initialize(router)
		err := StartExportWorker(context.Background(), a.Model)
		if err != nil {
			log.Errorf("couldn't start CSV export worker: %v", err)
		}

		// Register inference cost routes unconditionally so clients receive 501
		// (not 404) when INFERENCE_COST_ENABLED=false. The QueryService nil-
		// checks in each handler produce the 501 when qs is nil.
		var inferenceCostQueryService *inferencecost.QueryService
		if conf.InferenceCostEnabled {
			if err := StartInferenceCostCollector(ctx, a, &inferenceCostQueryService); err != nil {
				log.Errorf("Failed to start inference cost collector: %v", err)
			}
		}
		router.GET("/inferenceCost/total", inferenceCostQueryService.GetInferenceCostTotalHandler())
		router.GET("/inferenceCost/timeseries", inferenceCostQueryService.GetInferenceCostTimeseriesHandler())

		// Register OpenCost Specific Endpoints
		router.GET("/allocation", a.ComputeAllocationHandler)
		router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
		router.GET("/assets", a.ComputeAssetsHandler)
		if conf.CarbonEstimatesEnabled {
			router.GET("/assets/carbon", a.ComputeAssetsCarbonHandler)
		}
		router.GET("/kubemodel", a.KubeModelHandler)

	}

	var cloudCostPipelineService *cloudcost.PipelineService
	if conf.CloudCostEnabled {

		cloudCostPipelineService = costmodel.InitializeCloudCost(router)
	}

	var customCostPipelineService *customcost.PipelineService
	if conf.CustomCostEnabled {
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

		err := StartMCPServer(ctx, a, cloudCostQuerier)
		if err != nil {
			log.Errorf("Failed to start MCP server: %v", err)
		}
	} else if conf.MCPServerEnabled {
		log.Warnf("MCP Server is enabled but Kubernetes is not available. MCP server requires Kubernetes to function.")
	} else {
		if value, exists := os.LookupEnv(env.MCPServerEnabledEnvVar); !exists || value == "" {
			log.Infof("MCP server is now disabled by default. If you wish to use the MCP server, please set the %s environment variable to true.", env.MCPServerEnabledEnvVar)
		}
	}

	apiutil.ApplyContainerDiagnosticEndpoints(router)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	server := &http.Server{
		Addr:    fmt.Sprint(":", conf.Port),
		Handler: errors.PanicHandlerMiddleware(handler),
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Infof("HTTP server starting on port %d", conf.Port)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		log.Infof("Shutdown signal received, starting graceful shutdown...")

		if a.KubeModelPipeline != nil {
			a.KubeModelPipeline.Stop()
		}

		if customCostPipelineService != nil {
			customCostPipelineService.Stop()
		}

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Errorf("Error during server shutdown: %v", err)
			server.Close()
			return err
		}

		log.Infof("Graceful shutdown completed")
		return nil
	}
}

// StartInferenceCostCollector initialises and starts the inference cost
// collection loop as a background goroutine, and populates *qs with the
// QueryService so the caller can register routes. It is a no-op if the
// collector cannot be initialised (error is logged, existing functionality
// is unaffected).
func StartInferenceCostCollector(ctx context.Context, a *costmodel.Accesses, qs **inferencecost.QueryService) error {
	cfg := inferencecost.DefaultConfig()

	// Get the MetricsQuerier from the DataSource
	metricsQuerier := a.DataSource.Metrics()

	collector, err := inferencecost.NewCollector(cfg, a.Model, metricsQuerier)
	if err != nil {
		return err
	}

	exporter := inferencecost.NewExporter()
	if err := exporter.Register(); err != nil {
		return err
	}

	calculator := inferencecost.NewCalculator(cfg)
	runner := inferencecost.NewRunner(collector, calculator, exporter, cfg.CollectionInterval)

	// The collector and calculator are shared between the background runner
	// and the API; both paths are read-only, so sharing is safe.
	*qs = inferencecost.NewQueryService(collector, calculator)

	go runner.Start(ctx)
	log.Infof("InferenceCost: collector started (interval=%s)", cfg.CollectionInterval)
	return nil
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
	addr := fmt.Sprintf(":%d", env.GetMCPHTTPPort())
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for MCP HTTP on %s: %w", addr, err)
	}
	startMCPServer(ctx, accesses, cloudCostQuerier, listener)
	return nil
}

func startMCPServer(ctx context.Context, accesses *costmodel.Accesses, cloudCostQuerier cloudcost.Querier, listener net.Listener) <-chan struct{} {
	log.Info("Initializing MCP server...")

	// Create MCP server using existing OpenCost dependencies
	mcpServer := opencost_mcp.NewMCPServer(accesses.Model, accesses.CloudProvider, cloudCostQuerier)

	// Create MCP SDK server with registered tools
	sdkServer := newMCPSDKServer(mcpServer)

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

	server := &http.Server{
		Handler: loggingHandler,
	}

	// Start server in a goroutine
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Errorf("MCP server failed: %v", err)
		}
	}()

	// Graceful shutdown goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		log.Info("Shutting down MCP server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Errorf("MCP server shutdown error: %v", err)
		} else {
			log.Info("MCP server shut down successfully")
		}
	}()

	log.Infof("MCP server listening on %s", listener.Addr())
	return done
}

func newMCPSDKServer(mcpServer *opencost_mcp.MCPServer) *mcp_sdk.Server {
	// Create MCP SDK server
	sdkServer := mcp_sdk.NewServer(&mcp_sdk.Implementation{
		Name:    "opencost-mcp-server",
		Version: version.Version,
	}, nil)

	// Define tool handlers
	handleAllocationCosts := func(ctx context.Context, req *mcp_sdk.CallToolRequest, args AllocationArgs) (*mcp_sdk.CallToolResult, interface{}, error) {
		var step time.Duration
		if args.Step != "" {
			var err error
			step, err = timeutil.ParseDuration(args.Step)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid step duration '%s': %w", args.Step, err)
			}
			if step <= 0 {
				return nil, nil, fmt.Errorf("invalid step duration '%s': must be > 0", args.Step)
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

		mcpResp, err := mcpServer.ProcessMCPRequest(ctx, mcpReq)
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

		mcpResp, err := mcpServer.ProcessMCPRequest(ctx, mcpReq)
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

		mcpResp, err := mcpServer.ProcessMCPRequest(ctx, mcpReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process cloud cost request: %w", err)
		}

		return nil, mcpResp, nil
	}

	handleEfficiency := func(ctx context.Context, req *mcp_sdk.CallToolRequest, args EfficiencyArgs) (*mcp_sdk.CallToolResult, interface{}, error) {
		var step time.Duration
		if args.Step != "" {
			var err error
			step, err = timeutil.ParseDuration(args.Step)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid step duration '%s': %w", args.Step, err)
			}
			if step <= 0 {
				return nil, nil, fmt.Errorf("invalid step duration '%s': must be > 0", args.Step)
			}
		}

		queryRequest := &opencost_mcp.OpenCostQueryRequest{
			QueryType: opencost_mcp.EfficiencyQueryType,
			Window:    args.Window,
			EfficiencyParams: &opencost_mcp.EfficiencyQuery{
				Step:                       step,
				Aggregate:                  args.Aggregate,
				Filter:                     args.Filter,
				EfficiencyBufferMultiplier: args.BufferMultiplier,
			},
		}

		mcpReq := &opencost_mcp.MCPRequest{
			Query: queryRequest,
		}

		mcpResp, err := mcpServer.ProcessMCPRequest(ctx, mcpReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process efficiency request: %w", err)
		}

		return nil, mcpResp, nil
	}

	// Register tools
	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name: "get_allocation_costs",
		Description: "Retrieves Kubernetes workload cost allocation data broken down by Kubernetes concepts such as namespace, controller, pod, container, or service. Workload costs include CPU, memory (RAM), GPU, persistent volume (PV) storage, and network costs calculated from max(request, usage) and underlying node/cloud pricing.\n\n" +
			"When to use: Use this tool when analyzing costs attributed to software workloads or applications running inside Kubernetes. Prefer this over get_asset_costs (which returns underlying infrastructure spend like physical nodes and disks) and get_cloud_costs (which returns out-of-cluster cloud provider billing like RDS, S3, or account-level bills). Use get_efficiency when seeking rightsizing or cost savings recommendations.\n\n" +
			"Key parameters:\n" +
			"- window (required): Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek', or RFC 3339 range '2023-01-01T00:00:00Z,2023-01-02T00:00:00Z').\n" +
			"- aggregate: Comma-separated list of dimensions to group by (e.g., 'namespace', 'controller', 'controllerKind', 'pod', 'container', 'cluster', 'node', 'department', 'label:<name>', 'annotation:<name>').\n" +
			"- filter: Filter expression to limit allocations (e.g., 'namespace:\"kube-system\"', 'cluster:\"prod\"', 'label[app]:\"frontend\"').\n" +
			"- accumulate: If true, sums all allocation data across the entire window into a single set (default: false).\n" +
			"- step: Duration of time buckets (e.g., '1h', '1d'). Defaults to the full window.\n" +
			"- share_idle, include_idle, idle_by_node: Options to allocate or report unallocated cluster idle capacity (note: share_idle requires include_idle=true).\n" +
			"- include_proportional_asset_resource_costs, share_lb: Advanced internal asset cost options (require include_idle=true; details omitted from MCP results).\n" +
			"- include_aggregated_metadata: Internal aggregation option to preserve labels and annotations (details omitted from MCP results).",
	}, handleAllocationCosts)

	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name: "get_asset_costs",
		Description: "Retrieves infrastructure-level asset cost data for Kubernetes cluster components: nodes (virtual machines or bare metal), disks (attached storage volumes / EBS / persistent disks), load balancers, and network assets. Asset costs reflect the actual underlying cloud infrastructure spend and hardware resource allocation (e.g., VM instance hours, disk byte-hours).\n\n" +
			"When to use: Use this tool when inspecting underlying infrastructure spend, comparing node costs across clusters, assessing host-level idle capacity, or analyzing disk and load balancer infrastructure. Prefer get_allocation_costs when attributing costs to Kubernetes workloads (pods, namespaces, controllers) and get_cloud_costs for non-Kubernetes or cloud-account-wide billing.\n\n" +
			"Key parameters:\n" +
			"- window (required): Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek', or RFC 3339 range '2023-01-01T00:00:00Z,2023-01-02T00:00:00Z').",
	}, handleAssetCosts)

	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name: "get_cloud_costs",
		Description: "Retrieves cloud-provider billing and cost data directly from integrated cloud billing APIs (e.g., AWS Cost and Usage Report / CUR via Athena, GCP BigQuery Cloud Billing Export, Azure Cost Management Export).\n\n" +
			"When to use: Use this tool when analyzing cloud provider billing spend, non-Kubernetes cloud services (such as Amazon RDS, Google Cloud SQL, S3, Cloud Storage, DynamoDB), or whole-account multi-cloud costs. Prefer get_allocation_costs for Kubernetes workload costs and get_asset_costs for cluster infrastructure assets.\n\n" +
			"Key parameters:\n" +
			"- window (required): Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek', or RFC 3339 range '2023-01-01T00:00:00Z,2023-01-02T00:00:00Z').\n" +
			"- aggregate: Comma-separated list of cloud cost dimensions to group by (e.g., 'provider', 'accountID', 'service', 'category', 'regionID', 'invoiceEntityID', 'providerID', 'label:<name>').\n" +
			"- accumulate: Aggregation interval for accumulation: 'day', 'week', 'month', or 'true' (to sum across the entire window).\n" +
			"- filter: Filter expression for cloud costs (e.g., 'provider:\"AWS\"', 'service:\"AmazonEC2\"', 'category:\"Compute\"', 'regionID:\"us-east-1\"'). When filter is set, direct shortcut parameters (provider, service, category, account) are ignored and all filter constraints must be specified inside this expression.\n" +
			"- provider, service, category, account: Direct filter shortcuts used only when filter is omitted (note: ignored if filter is set; for region filtering, use filter: 'regionID:\"us-east-1\"').",
	}, handleCloudCosts)

	mcp_sdk.AddTool(sdkServer, &mcp_sdk.Tool{
		Name: "get_efficiency",
		Description: "Retrieves resource efficiency metrics with rightsizing recommendations and cost savings analysis. Computes CPU and memory efficiency (usage/request ratio), provides recommended resource requests, and calculates potential cost savings.\n\n" +
			"When to use: Use this tool when looking for workload rightsizing, overprovisioning, or cost savings opportunities across pods, namespaces, or controllers. Prefer get_allocation_costs for standard cost attribution without rightsizing recommendations.\n\n" +
			"Key parameters:\n" +
			"- window (required): Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek').\n" +
			"- aggregate: Aggregation level (e.g., 'pod', 'namespace', 'controller', 'container').\n" +
			"- filter: Filter expression to limit allocations (same syntax as get_allocation_costs, e.g., 'namespace:\"default\"').\n" +
			"- buffer_multiplier: Headroom buffer multiplier for resource recommendations (default: 1.2 for 20% headroom, e.g., 1.4 for 40% headroom).\n" +
			"- step: Query step size (e.g., '1h', '6h') to control peak memory by batching large windows.",
	}, handleEfficiency)

	return sdkServer
}

// Tool argument structures for MCP server
type AllocationArgs struct {
	Window    string `json:"window" jsonschema:"Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek', or RFC 3339 range 'YYYY-MM-DDTHH:MM:SSZ,YYYY-MM-DDTHH:MM:SSZ')"`
	Aggregate string `json:"aggregate,omitempty" jsonschema:"Comma-separated list of dimensions to aggregate by (e.g., 'namespace', 'controller', 'controllerKind', 'pod', 'container', 'cluster', 'node', 'department', 'label:<name>', 'annotation:<name>')"`

	// Allocation query parameters
	Step                                  string `json:"step,omitempty" jsonschema:"Duration of time buckets (e.g., '1h', '1d'). Defaults to the full window."`
	Resolution                            string `json:"resolution,omitempty" jsonschema:"Ignored compatibility parameter; query resolution is determined by the underlying Prometheus data source"`
	Accumulate                            bool   `json:"accumulate,omitempty" jsonschema:"If true, sums all allocation data across the entire window into a single set (default: false)"`
	ShareIdle                             bool   `json:"share_idle,omitempty" jsonschema:"If true, shares idle resource costs proportionally across all workloads (requires include_idle=true; default: false)"`
	IncludeIdle                           bool   `json:"include_idle,omitempty" jsonschema:"If true, includes idle costs in results as a separate item (default: false)"`
	IdleByNode                            bool   `json:"idle_by_node,omitempty" jsonschema:"If true, calculates idle cost separately per node rather than cluster-wide (default: false)"`
	IncludeProportionalAssetResourceCosts bool   `json:"include_proportional_asset_resource_costs,omitempty" jsonschema:"If true, computes proportional asset resource costs internally (requires include_idle=true; details omitted from MCP results; default: false)"`
	IncludeAggregatedMetadata             bool   `json:"include_aggregated_metadata,omitempty" jsonschema:"Internal aggregation option; metadata (labels/annotations) is not exposed in MCP results (default: false)"`
	ShareLB                               bool   `json:"share_lb,omitempty" jsonschema:"If true, includes AKS load balancer totals in internal proportional asset resource cost calculations (requires include_proportional_asset_resource_costs=true and include_idle=true; details omitted from MCP results; default: false)"`
	Filter                                string `json:"filter,omitempty" jsonschema:"Filter expression to select specific allocations (e.g., 'namespace:\"kube-system\"', 'cluster:\"prod\"', 'label[app]:\"frontend\"')"`
}

type AssetArgs struct {
	Window string `json:"window" jsonschema:"Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek', or RFC 3339 range 'YYYY-MM-DDTHH:MM:SSZ,YYYY-MM-DDTHH:MM:SSZ')"`
}

type CloudCostArgs struct {
	Window    string `json:"window" jsonschema:"Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek', or RFC 3339 range 'YYYY-MM-DDTHH:MM:SSZ,YYYY-MM-DDTHH:MM:SSZ')"`
	Aggregate string `json:"aggregate,omitempty" jsonschema:"Comma-separated list of cloud cost dimensions to group by (e.g., 'provider', 'accountID', 'service', 'category', 'regionID', 'invoiceEntityID', 'providerID', 'label:<name>')"`

	// Cloud cost query parameters
	Accumulate string `json:"accumulate,omitempty" jsonschema:"Aggregation interval for accumulation: 'day', 'week', 'month', or 'true' (to sum across the entire window)"`
	Filter     string `json:"filter,omitempty" jsonschema:"Filter expression for cloud costs (e.g., 'provider:\"AWS\"', 'service:\"AmazonEC2\"', 'category:\"Compute\"', 'regionID:\"us-east-1\"'). When filter is set, direct shortcut parameters (provider, service, category, account) are ignored and all filter constraints must be specified inside this expression"`
	Provider   string `json:"provider,omitempty" jsonschema:"Filter shortcut by cloud provider name (e.g., 'AWS', 'GCP', 'Azure'); ignored if filter is set"`
	Service    string `json:"service,omitempty" jsonschema:"Filter shortcut by cloud service name (e.g., 'AmazonEC2', 'Cloud Storage', 'Virtual Machines'); ignored if filter is set"`
	Category   string `json:"category,omitempty" jsonschema:"Filter shortcut by spending category (e.g., 'Compute', 'Storage', 'Network', 'Management', 'Other'); ignored if filter is set"`
	Region     string `json:"region,omitempty" jsonschema:"Ignored by query filter; use filter parameter with regionID (e.g., filter: 'regionID:\"us-east-1\"') instead"`
	Account    string `json:"account,omitempty" jsonschema:"Filter shortcut by cloud account or subscription ID (e.g., AWS account ID, GCP project ID); ignored if filter is set"`
}

type EfficiencyArgs struct {
	Window           string   `json:"window" jsonschema:"Time window for the query (e.g., 'today', 'yesterday', '7d', '30d', 'lastweek')"`
	Aggregate        string   `json:"aggregate,omitempty" jsonschema:"Aggregation level (e.g., 'pod', 'namespace', 'controller', 'container')"`
	Filter           string   `json:"filter,omitempty" jsonschema:"Filter expression to limit allocations (same syntax as allocation filters, e.g., 'namespace:\"default\"')"`
	BufferMultiplier *float64 `json:"buffer_multiplier,omitempty" jsonschema:"Headroom buffer multiplier for resource recommendations (default: 1.2 for 20% headroom, e.g., 1.4 for 40%)"`
	Step             string   `json:"step,omitempty" jsonschema:"Query step size (e.g., '1h', '6h') to control peak memory by batching large windows"`
}
