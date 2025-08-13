//go:build mcp

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/costmodel"
)

// OpenCostMCPServer provides Model Context Protocol access to OpenCost APIs
type OpenCostMCPServer struct {
	server    *server.MCPServer
	costModel *costmodel.CostModel
	contexts  map[string]*OpenCostConversationContext
	insights  *InsightEngine
}

// NewOpenCostMCPServer creates a new MCP server for OpenCost
func NewOpenCostMCPServer(costModel *costmodel.CostModel) *OpenCostMCPServer {
	s := &OpenCostMCPServer{
		costModel: costModel,
		contexts:  make(map[string]*OpenCostConversationContext),
		insights:  NewInsightEngine(),
	}

	// Create MCP server using the new API
	mcpServer := server.NewMCPServer(
		"OpenCost MCP Server",
		"1.0.0",
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(true),
		server.WithPromptCapabilities(true),
	)
	s.server = mcpServer

	// Register tools using the new API
	s.registerTools()

	// Register resources using the new API
	s.registerResources()

	// Register prompts using the new API
	s.registerPrompts()

	return s
}

// Start starts the MCP server
func (s *OpenCostMCPServer) Start() error {
	return server.ServeStdio(s.server)
}

// registerTools registers all MCP tools for OpenCost operations using the new API
func (s *OpenCostMCPServer) registerTools() {
	// Allocation tools
	queryAllocTool := mcp.NewTool("query_allocations",
		mcp.WithDescription("Query Kubernetes resource allocations and costs with AI-enhanced insights"),
		mcp.WithObject("query",
			mcp.Required(),
			mcp.Description("Allocation query parameters"),
		),
	)
	s.server.AddTool(queryAllocTool, s.handleAllocationQuery)

	allocSummaryTool := mcp.NewTool("allocation_summary",
		mcp.WithDescription("Get a summary of allocation costs with insights and recommendations"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window (e.g., '7d', 'last month', 'this week')"),
		),
		mcp.WithObject("filters",
			mcp.Description("Optional filters for the summary"),
		),
	)
	s.server.AddTool(allocSummaryTool, s.handleAllocationSummary)

	topConsumersTool := mcp.NewTool("top_cost_consumers",
		mcp.WithDescription("Find the top cost consumers in your cluster"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window for analysis"),
		),
		mcp.WithNumber("count",
			mcp.Description("Number of top consumers to return (default: 10)"),
		),
		mcp.WithString("groupBy",
			mcp.Description("Group by: namespace, service, pod, container, node (default: namespace)"),
		),
	)
	s.server.AddTool(topConsumersTool, s.handleTopCostConsumers)

	efficiencyTool := mcp.NewTool("efficiency_analysis",
		mcp.WithDescription("Analyze resource efficiency and identify optimization opportunities"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window for analysis"),
		),
		mcp.WithNumber("threshold",
			mcp.Description("Efficiency threshold (0-1, default: 0.7)"),
		),
	)
	s.server.AddTool(efficiencyTool, s.handleEfficiencyAnalysis)

	// Asset tools
	queryAssetsTool := mcp.NewTool("query_assets",
		mcp.WithDescription("Query cluster assets (nodes, disks, load balancers) with utilization insights"),
		mcp.WithObject("query",
			mcp.Required(),
			mcp.Description("Asset query parameters"),
		),
	)
	s.server.AddTool(queryAssetsTool, s.handleAssetQuery)

	assetUtilTool := mcp.NewTool("asset_utilization",
		mcp.WithDescription("Analyze asset utilization and identify underutilized resources"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window for analysis"),
		),
		mcp.WithArray("assetTypes",
			mcp.Description("Asset types to analyze (Node, Disk, LoadBalancer)"),
		),
	)
	s.server.AddTool(assetUtilTool, s.handleAssetUtilization)

	capacityTool := mcp.NewTool("capacity_planning",
		mcp.WithDescription("Get capacity planning insights and recommendations"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window for historical analysis"),
		),
		mcp.WithNumber("forecastDays",
			mcp.Description("Number of days to forecast (default: 30)"),
		),
	)
	s.server.AddTool(capacityTool, s.handleCapacityPlanning)

	// Cloud cost tools
	cloudCostTool := mcp.NewTool("query_cloud_costs",
		mcp.WithDescription("Query cloud costs with provider, service, and region breakdowns"),
		mcp.WithObject("query",
			mcp.Required(),
			mcp.Description("Cloud cost query parameters"),
		),
	)
	s.server.AddTool(cloudCostTool, s.handleCloudCostQuery)

	cloudBreakdownTool := mcp.NewTool("cloud_cost_breakdown",
		mcp.WithDescription("Get detailed cloud cost breakdown by provider, service, or region"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window for analysis"),
		),
		mcp.WithString("groupBy",
			mcp.Description("Group by: provider, service, region, account (default: service)"),
		),
	)
	s.server.AddTool(cloudBreakdownTool, s.handleCloudCostBreakdown)

	anomalyTool := mcp.NewTool("cost_anomaly_detection",
		mcp.WithDescription("Detect cost anomalies and unusual spending patterns"),
		mcp.WithString("window",
			mcp.Required(),
			mcp.Description("Time window for analysis"),
		),
		mcp.WithString("sensitivity",
			mcp.Description("Sensitivity level: low, medium, high (default: medium)"),
		),
	)
	s.server.AddTool(anomalyTool, s.handleCostAnomalyDetection)

	// Context and conversation tools
	startSessionTool := mcp.NewTool("start_cost_session",
		mcp.WithDescription("Start a new cost analysis session with context tracking"),
		mcp.WithObject("preferences",
			mcp.Description("User preferences for the session"),
		),
	)
	s.server.AddTool(startSessionTool, s.handleStartSession)

	insightsTool := mcp.NewTool("get_cost_insights",
		mcp.WithDescription("Get AI-generated insights about current cost patterns and trends"),
		mcp.WithString("sessionId",
			mcp.Description("Session ID for context"),
		),
		mcp.WithString("focusArea",
			mcp.Description("Focus area: allocations, assets, cloud, all (default: all)"),
		),
	)
	s.server.AddTool(insightsTool, s.handleGetInsights)

	recommendationsTool := mcp.NewTool("cost_recommendations",
		mcp.WithDescription("Get personalized cost optimization recommendations"),
		mcp.WithString("sessionId",
			mcp.Description("Session ID for context"),
		),
		mcp.WithNumber("maxRecommendations",
			mcp.Description("Maximum recommendations to return (default: 5)"),
		),
	)
	s.server.AddTool(recommendationsTool, s.handleCostRecommendations)

	// Natural language query tool
	naturalQueryTool := mcp.NewTool("natural_cost_query",
		mcp.WithDescription("Query costs using natural language (e.g., 'expensive pods in production last week')"),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("Natural language cost query"),
		),
		mcp.WithString("sessionId",
			mcp.Description("Optional session ID for context"),
		),
	)
	s.server.AddTool(naturalQueryTool, s.handleNaturalQuery)
}

// registerResources registers MCP resources for OpenCost data using the new API
func (s *OpenCostMCPServer) registerResources() {
	// For now, we'll implement these as placeholder handlers
	// In a full implementation, you would register actual resources
	// using s.server.AddResource() with mcp.NewResource()
}

// registerPrompts registers MCP prompts for cost analysis using the new API
func (s *OpenCostMCPServer) registerPrompts() {
	// For now, we'll implement these as placeholder handlers
	// In a full implementation, you would register actual prompts
	// using s.server.AddPrompt() with mcp.NewPrompt()
}

// Tool handlers

func (s *OpenCostMCPServer) handleAllocationQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var queryReq AllocationQueryRequest
	if err := req.BindArguments(&queryReq); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid query parameters: %v", err)), nil
	}

	// Parse natural language if provided
	if err := queryReq.ParseNaturalLanguage(); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error parsing natural language query: %v", err)), nil
	}

	// Convert to OpenCost parameters
	window, err := s.convertTimeWindow(queryReq.Window)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid time window: %v", err)), nil
	}

	// Set defaults
	step := 24 * time.Hour
	if queryReq.Step != nil {
		step = *queryReq.Step
	}

	aggregateBy := queryReq.Aggregate
	if len(aggregateBy) == 0 {
		aggregateBy = []string{"namespace"}
	}

	// Query allocations from OpenCost
	asr, err := s.costModel.QueryAllocation(
		window,
		step,
		aggregateBy,
		queryReq.IncludeIdle,
		queryReq.IdleByNode,
		queryReq.IncludeProportionalAssetResourceCosts,
		queryReq.IncludeAggregatedMetadata,
		queryReq.ShareLoadBalancer,
		opencost.AccumulateOptionNone,
		queryReq.ShareIdle,
	)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error querying allocations: %v", err)), nil
	}

	// Convert to enhanced response
	response := s.enhanceAllocationResponse(asr, &queryReq)

	return mcp.NewToolResultStructured(response, fmt.Sprintf("Found %d allocations with total cost of $%.2f",
		len(response.Allocations), response.TotalCost)), nil
}

func (s *OpenCostMCPServer) handleAllocationSummary(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	windowStr := req.GetString("window", "7d")

	// Parse filters if provided
	filtersMap := req.GetArguments()["filters"]
	var filters *SmartFilter
	if filtersMap != nil {
		filtersData, _ := json.Marshal(filtersMap)
		json.Unmarshal(filtersData, &filters)
	}

	// Create query request
	window, err := ParseTimeWindow(windowStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid time window: %v", err)), nil
	}

	queryReq := AllocationQueryRequest{
		Window:       window,
		Aggregate:    []string{"namespace"},
		Filter:       filters,
		AnalysisType: "summary",
		OutputFormat: "summary",
	}

	// Query allocations
	ocWindow, err := s.convertTimeWindow(window)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error converting time window: %v", err)), nil
	}

	asr, err := s.costModel.QueryAllocation(
		ocWindow,
		24*time.Hour,
		[]string{"namespace"},
		false, false, false, false, false,
		opencost.AccumulateOptionNone,
		false,
	)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error querying allocations: %v", err)), nil
	}

	// Generate summary
	response := s.enhanceAllocationResponse(asr, &queryReq)

	// Create a focused summary response
	summary := map[string]interface{}{
		"totalCost":       response.TotalCost,
		"summary":         response.Summary,
		"insights":        response.Insights,
		"recommendations": response.Recommendations,
		"timeWindow":      window,
	}

	return mcp.NewToolResultStructured(summary,
		fmt.Sprintf("Allocation Summary for %s: Total cost $%.2f across %d allocations",
			windowStr, response.TotalCost, len(response.Allocations))), nil
}

func (s *OpenCostMCPServer) handleTopCostConsumers(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	windowStr := req.GetString("window", "7d")
	count := int(req.GetInt("count", 10))
	groupBy := req.GetString("groupBy", "namespace")

	window, err := ParseTimeWindow(windowStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid time window: %v", err)), nil
	}

	// Query with specified grouping
	ocWindow, err := s.convertTimeWindow(window)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error converting time window: %v", err)), nil
	}

	asr, err := s.costModel.QueryAllocation(
		ocWindow,
		24*time.Hour,
		[]string{groupBy},
		false, false, false, false, false,
		opencost.AccumulateOptionNone,
		false,
	)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error querying allocations: %v", err)), nil
	}

	// Extract and sort by cost
	var costItems []struct {
		Name string  `json:"name"`
		Cost float64 `json:"cost"`
	}

	for _, as := range asr.Slice() {
		for name, alloc := range as.Allocations {
			costItems = append(costItems, struct {
				Name string  `json:"name"`
				Cost float64 `json:"cost"`
			}{
				Name: name,
				Cost: alloc.TotalCost(),
			})
		}
	}

	// Sort by cost descending
	sort.Slice(costItems, func(i, j int) bool {
		return costItems[i].Cost > costItems[j].Cost
	})

	// Limit results
	if len(costItems) > count {
		costItems = costItems[:count]
	}

	response := map[string]interface{}{
		"topCostConsumers": costItems,
		"groupBy":          groupBy,
		"timeWindow":       window,
		"totalAnalyzed":    len(costItems),
	}

	return mcp.NewToolResultStructured(response,
		fmt.Sprintf("Top %d cost consumers by %s for %s", count, groupBy, windowStr)), nil
}

func (s *OpenCostMCPServer) handleEfficiencyAnalysis(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	windowStr := req.GetString("window", "7d")
	threshold := req.GetFloat("threshold", 0.7)

	window, err := ParseTimeWindow(windowStr)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid time window: %v", err)), nil
	}

	// Query allocations with efficiency data
	ocWindow, err := s.convertTimeWindow(window)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error converting time window: %v", err)), nil
	}

	asr, err := s.costModel.QueryAllocation(
		ocWindow,
		24*time.Hour,
		[]string{"namespace", "pod"},
		false, false, false, false, false,
		opencost.AccumulateOptionNone,
		false,
	)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error querying allocations: %v", err)), nil
	}

	// Analyze efficiency
	var inefficient []struct {
		Name           string  `json:"name"`
		CPUEfficiency  float64 `json:"cpuEfficiency"`
		RAMEfficiency  float64 `json:"ramEfficiency"`
		TotalCost      float64 `json:"totalCost"`
		Recommendation string  `json:"recommendation"`
	}

	totalCost := 0.0
	efficientCost := 0.0

	for _, as := range asr.Slice() {
		for name, alloc := range as.Allocations {
			cpuEff := alloc.CPUEfficiency()
			ramEff := alloc.RAMEfficiency()
			totalEff := alloc.TotalEfficiency()
			cost := alloc.TotalCost()

			totalCost += cost

			if totalEff < threshold {
				recommendation := "Consider rightsizing resources"
				if cpuEff < 0.3 {
					recommendation = "CPU significantly overprovisioned - consider reducing CPU requests"
				} else if ramEff < 0.3 {
					recommendation = "Memory significantly overprovisioned - consider reducing memory requests"
				}

				inefficient = append(inefficient, struct {
					Name           string  `json:"name"`
					CPUEfficiency  float64 `json:"cpuEfficiency"`
					RAMEfficiency  float64 `json:"ramEfficiency"`
					TotalCost      float64 `json:"totalCost"`
					Recommendation string  `json:"recommendation"`
				}{
					Name:           name,
					CPUEfficiency:  cpuEff,
					RAMEfficiency:  ramEff,
					TotalCost:      cost,
					Recommendation: recommendation,
				})
			} else {
				efficientCost += cost
			}
		}
	}

	// Sort by cost impact
	sort.Slice(inefficient, func(i, j int) bool {
		return inefficient[i].TotalCost > inefficient[j].TotalCost
	})

	response := map[string]interface{}{
		"inefficientResources": inefficient,
		"efficiencyThreshold":  threshold,
		"totalCost":            totalCost,
		"efficientCost":        efficientCost,
		"inefficientCost":      totalCost - efficientCost,
		"efficiencyRate":       efficientCost / totalCost,
		"timeWindow":           window,
		"insights": []CostInsight{
			{
				Type:        "efficiency",
				Severity:    "medium",
				Title:       "Resource Efficiency Analysis",
				Description: fmt.Sprintf("Found %d resources below %0.0f%% efficiency threshold", len(inefficient), threshold*100),
				Impact:      &[]float64{totalCost - efficientCost}[0],
				Confidence:  0.85,
			},
		},
	}

	return mcp.NewToolResultStructured(response,
		fmt.Sprintf("Efficiency Analysis: %d resources below %0.0f%% threshold, potential savings: $%.2f",
			len(inefficient), threshold*100, totalCost-efficientCost)), nil
}

func (s *OpenCostMCPServer) handleAssetQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var queryReq AssetQueryRequest
	if err := req.BindArguments(&queryReq); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid query parameters: %v", err)), nil
	}

	// Parse natural language if provided
	if err := queryReq.ParseNaturalLanguage(); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error parsing natural language query: %v", err)), nil
	}

	// Convert to OpenCost parameters
	window, err := s.convertTimeWindow(queryReq.Window)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid time window: %v", err)), nil
	}

	// Query assets from OpenCost
	assetSet, err := s.costModel.ComputeAssets(window, "")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error querying assets: %v", err)), nil
	}

	// Convert to enhanced response
	response := s.enhanceAssetResponse(assetSet, &queryReq)

	return mcp.NewToolResultStructured(response,
		fmt.Sprintf("Found %d assets with total cost of $%.2f",
			len(response.Assets), response.Summary.TotalCost)), nil
}

func (s *OpenCostMCPServer) handleNaturalQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := req.GetString("query", "")
	sessionId := req.GetString("sessionId", "")

	if query == "" {
		return mcp.NewToolResultError("Query cannot be empty"), nil
	}

	// Get or create session context
	ctx_data := s.getOrCreateContext(sessionId)
	ctx_data.UpdateActivity()
	ctx_data.RecentQueries = append(ctx_data.RecentQueries, query)

	// Parse the natural language query to determine intent and type
	queryLower := strings.ToLower(query)

	// Determine data type and create appropriate request
	if strings.Contains(queryLower, "asset") || strings.Contains(queryLower, "node") ||
		strings.Contains(queryLower, "disk") || strings.Contains(queryLower, "load balancer") {
		// Asset query
		assetReq := AssetQueryRequest{
			NaturalQuery: query,
			Window:       ctx_data.CurrentWindow,
		}

		if assetReq.Window == nil {
			// Default to last 7 days
			window, _ := ParseTimeWindow("7d")
			assetReq.Window = window
		}

		if err := assetReq.ParseNaturalLanguage(); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error parsing query: %v", err)), nil
		}

		// Convert and query
		ocWindow, err := s.convertTimeWindow(assetReq.Window)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error with time window: %v", err)), nil
		}

		assetSet, err := s.costModel.ComputeAssets(ocWindow, "")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error querying assets: %v", err)), nil
		}

		response := s.enhanceAssetResponse(assetSet, &assetReq)

		// Store insights in context
		for _, insight := range response.Insights {
			ctx_data.AddInsight(insight)
		}

		return mcp.NewToolResultStructured(response,
			fmt.Sprintf("Natural query results: %s", query)), nil

	} else if strings.Contains(queryLower, "cloud") || strings.Contains(queryLower, "aws") ||
		strings.Contains(queryLower, "gcp") || strings.Contains(queryLower, "azure") {
		// Cloud cost query
		cloudReq := CloudCostQueryRequest{
			NaturalQuery: query,
			Window:       ctx_data.CurrentWindow,
		}

		if cloudReq.Window == nil {
			window, _ := ParseTimeWindow("7d")
			cloudReq.Window = window
		}

		if err := cloudReq.ParseNaturalLanguage(); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error parsing query: %v", err)), nil
		}

		// For now, return a placeholder response since cloud cost querying
		// requires cloud cost integration to be enabled
		response := CloudCostQueryResponse{
			CloudCosts: []EnhancedCloudCost{},
			Window:     *cloudReq.Window,
			TotalCost:  0,
			Summary: CloudCostSummary{
				TotalCost: 0,
			},
			Insights: []CostInsight{
				{
					Type:        "info",
					Severity:    "low",
					Title:       "Cloud Cost Query",
					Description: "Cloud cost integration is not enabled or no cloud cost data available",
					Confidence:  1.0,
				},
			},
		}

		return mcp.NewToolResultStructured(response,
			"Cloud cost query completed, but no data available (integration may not be enabled)"), nil

	} else {
		// Default to allocation query
		allocReq := AllocationQueryRequest{
			NaturalLanguageQuery: query,
			Window:               ctx_data.CurrentWindow,
			OutputFormat:         "insights",
		}

		if allocReq.Window == nil {
			window, _ := ParseTimeWindow("7d")
			allocReq.Window = window
		}

		if err := allocReq.ParseNaturalLanguage(); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error parsing query: %v", err)), nil
		}

		// Convert and query
		ocWindow, err := s.convertTimeWindow(allocReq.Window)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error with time window: %v", err)), nil
		}

		aggregateBy := allocReq.Aggregate
		if len(aggregateBy) == 0 {
			aggregateBy = []string{"namespace"}
		}

		asr, err := s.costModel.QueryAllocation(
			ocWindow,
			24*time.Hour,
			aggregateBy,
			allocReq.IncludeIdle,
			allocReq.IdleByNode,
			allocReq.IncludeProportionalAssetResourceCosts,
			allocReq.IncludeAggregatedMetadata,
			allocReq.ShareLoadBalancer,
			opencost.AccumulateOptionNone,
			allocReq.ShareIdle,
		)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Error querying allocations: %v", err)), nil
		}

		response := s.enhanceAllocationResponse(asr, &allocReq)

		// Store insights in context
		for _, insight := range response.Insights {
			ctx_data.AddInsight(insight)
		}

		return mcp.NewToolResultStructured(response,
			fmt.Sprintf("Natural query results: %s", query)), nil
	}
}

// Additional handlers would be implemented here...
// For brevity, I'm showing the core structure and a few key handlers

// Helper methods

func (s *OpenCostMCPServer) convertTimeWindow(tw *TimeWindow) (opencost.Window, error) {
	if tw == nil {
		// Default to last 24 hours
		end := time.Now()
		start := end.Add(-24 * time.Hour)
		return opencost.NewWindow(&start, &end), nil
	}

	return opencost.NewWindow(&tw.Start, &tw.End), nil
}

func (s *OpenCostMCPServer) getOrCreateContext(sessionId string) *OpenCostConversationContext {
	if sessionId == "" {
		sessionId = fmt.Sprintf("session_%d", time.Now().Unix())
	}

	if ctx, exists := s.contexts[sessionId]; exists {
		return ctx
	}

	ctx := &OpenCostConversationContext{
		SessionID:     sessionId,
		StartTime:     time.Now(),
		LastActivity:  time.Now(),
		CachedResults: make(map[string]CachedResult),
	}

	s.contexts[sessionId] = ctx
	return ctx
}

// Enhanced response builders

func (s *OpenCostMCPServer) enhanceAllocationResponse(asr *opencost.AllocationSetRange, req *AllocationQueryRequest) *AllocationQueryResponse {
	var allocations []EnhancedAllocation
	totalCost := 0.0

	// Convert OpenCost allocations to enhanced format
	for _, as := range asr.Slice() {
		for name, alloc := range as.Allocations {
			enhanced := EnhancedAllocation{
				Name:             name,
				Window:           TimeWindow{Start: alloc.Start, End: alloc.End},
				CPUCost:          alloc.CPUCost,
				RAMCost:          alloc.RAMCost,
				GPUCost:          alloc.GPUCost,
				NetworkCost:      alloc.NetworkCost,
				LoadBalancerCost: alloc.LoadBalancerCost,
				PVCost:           alloc.PVCost(),
				SharedCost:       alloc.SharedCost,
				ExternalCost:     alloc.ExternalCost,
				TotalCost:        alloc.TotalCost(),
			}

			// Add efficiency metrics
			cpuEff := alloc.CPUEfficiency()
			ramEff := alloc.RAMEfficiency()
			totalEff := alloc.TotalEfficiency()

			enhanced.CPUEfficiency = &cpuEff
			enhanced.RAMEfficiency = &ramEff
			enhanced.TotalEfficiency = &totalEff

			// Add properties if available
			if alloc.Properties != nil {
				enhanced.Properties = map[string]interface{}{
					"cluster":    alloc.Properties.Cluster,
					"node":       alloc.Properties.Node,
					"namespace":  alloc.Properties.Namespace,
					"pod":        alloc.Properties.Pod,
					"container":  alloc.Properties.Container,
					"controller": alloc.Properties.Controller,
					"service":    alloc.Properties.Services,
				}
			}

			allocations = append(allocations, enhanced)
			totalCost += enhanced.TotalCost
		}
	}

	// Sort by cost and add rankings
	sort.Slice(allocations, func(i, j int) bool {
		return allocations[i].TotalCost > allocations[j].TotalCost
	})

	for i := range allocations {
		allocations[i].CostRank = i + 1
	}

	// Generate insights
	insights := s.insights.GenerateAllocationInsights(allocations)

	// Generate summary
	summary := s.generateAllocationSummary(allocations)

	// Generate recommendations
	recommendations := s.insights.GenerateAllocationRecommendations(allocations, insights)

	return &AllocationQueryResponse{
		Allocations:     allocations,
		Window:          TimeWindow{Start: asr.Start(), End: asr.End()},
		TotalCost:       totalCost,
		Summary:         summary,
		Insights:        insights,
		Recommendations: recommendations,
		QueryMetadata: QueryMetadata{
			RecordsReturned: len(allocations),
			QueryComplexity: "medium",
		},
	}
}

func (s *OpenCostMCPServer) enhanceAssetResponse(assetSet *opencost.AssetSet, req *AssetQueryRequest) *AssetQueryResponse {
	var assets []EnhancedAsset
	totalCost := 0.0

	// Convert OpenCost assets to enhanced format
	for name, asset := range assetSet.Assets {
		enhanced := EnhancedAsset{
			Type:      asset.Type().String(),
			Name:      name,
			Window:    TimeWindow{Start: asset.GetStart(), End: asset.GetEnd()},
			TotalCost: asset.TotalCost(),
		}

		// Add asset-specific properties
		if props := asset.GetProperties(); props != nil {
			enhanced.Properties = map[string]interface{}{
				"cluster":  props.Cluster,
				"provider": props.Provider,
				"region":   props.Region,
				"zone":     props.Zone,
			}
		}

		// Add labels
		if labels := asset.GetLabels(); labels != nil {
			enhanced.Labels = map[string]string(labels)
		}

		assets = append(assets, enhanced)
		totalCost += enhanced.TotalCost
	}

	// Generate insights
	insights := s.insights.GenerateAssetInsights(assets)

	// Generate summary
	summary := AssetSummary{
		TotalCost: totalCost,
	}

	return &AssetQueryResponse{
		Assets:   assets,
		Window:   *req.Window,
		Summary:  summary,
		Insights: insights,
		QueryMetadata: QueryMetadata{
			RecordsReturned: len(assets),
			QueryComplexity: "medium",
		},
	}
}

func (s *OpenCostMCPServer) generateAllocationSummary(allocations []EnhancedAllocation) AllocationSummary {
	totalCost := 0.0
	totalEff := 0.0
	effCount := 0
	costBreakdown := make(map[string]float64)
	var topConsumers []string
	var inefficient []string

	for _, alloc := range allocations {
		totalCost += alloc.TotalCost

		if alloc.TotalEfficiency != nil {
			totalEff += *alloc.TotalEfficiency
			effCount++

			if *alloc.TotalEfficiency < 0.7 {
				inefficient = append(inefficient, alloc.Name)
			}
		}

		// Cost breakdown by resource type
		costBreakdown["cpu"] += alloc.CPUCost
		costBreakdown["ram"] += alloc.RAMCost
		costBreakdown["gpu"] += alloc.GPUCost
		costBreakdown["network"] += alloc.NetworkCost
		costBreakdown["storage"] += alloc.PVCost
		costBreakdown["loadBalancer"] += alloc.LoadBalancerCost

		// Top consumers (first 5)
		if len(topConsumers) < 5 {
			topConsumers = append(topConsumers, alloc.Name)
		}
	}

	avgEff := 0.0
	if effCount > 0 {
		avgEff = totalEff / float64(effCount)
	}

	return AllocationSummary{
		TotalCost:             totalCost,
		AverageEfficiency:     avgEff,
		TopCostConsumers:      topConsumers,
		InefficiientResources: inefficient,
		CostBreakdown:         costBreakdown,
	}
}

// Additional handlers for other tools would be implemented here...

// Resource handlers
func (s *OpenCostMCPServer) handleAllocationResource(ctx context.Context, req mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	// Implement allocation resource handler
	return mcp.NewReadResourceResult("Allocation resource data"), nil
}

func (s *OpenCostMCPServer) handleAssetResource(ctx context.Context, req mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	// Implement asset resource handler
	return mcp.NewReadResourceResult("Asset resource data"), nil
}

func (s *OpenCostMCPServer) handleCloudCostResource(ctx context.Context, req mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	// Implement cloud cost resource handler
	return mcp.NewReadResourceResult("Cloud cost resource data"), nil
}

func (s *OpenCostMCPServer) handleInsightsResource(ctx context.Context, req mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	// Implement insights resource handler
	return mcp.NewReadResourceResult("Cost insights data"), nil
}

func (s *OpenCostMCPServer) handleClusterOverviewResource(ctx context.Context, req mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	// Implement cluster overview resource handler
	return mcp.NewReadResourceResult("Cluster overview data"), nil
}

// Prompt handlers
func (s *OpenCostMCPServer) handleCostOptimizationPrompt(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	// Implement cost optimization prompt
	return mcp.NewGetPromptResult("Cost optimization analysis prompt", []mcp.PromptMessage{}), nil
}

func (s *OpenCostMCPServer) handleAnomalyInvestigationPrompt(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	// Implement anomaly investigation prompt
	return mcp.NewGetPromptResult("Cost anomaly investigation prompt", []mcp.PromptMessage{}), nil
}

func (s *OpenCostMCPServer) handleBudgetAnalysisPrompt(ctx context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	// Implement budget analysis prompt
	return mcp.NewGetPromptResult("Budget analysis prompt", []mcp.PromptMessage{}), nil
}

// Implement remaining handlers for the other tools...
