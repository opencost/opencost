package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/opencost/opencost/pkg/mcp/processors"
	"github.com/opencost/opencost/pkg/mcp/utils"
)

// handleQueryAllocations handles allocation cost queries with natural language processing
func (s *MCPServer) handleQueryAllocations(ctx *types.QueryContext, args map[string]interface{}) (*types.CallToolResult, error) {
	startTime := time.Now()

	// Extract and validate parameters
	query, ok := args["query"].(string)
	if !ok || query == "" {
		return s.createErrorResult("Query parameter is required and must be a non-empty string"), nil
	}

	// Parse natural language query
	queryParams, err := utils.ParseAllocationQuery(query, args)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to parse query: %v", err)), nil
	}

	// Execute query through OpenCost client
	allocationData, err := s.openCostClient.QueryAllocations(ctx.Context, queryParams)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to query allocations: %v", err)), nil
	}

	// Process data for AI consumption
	processor := processors.NewAllocationProcessor()
	processedData, err := processor.ProcessAllocations(allocationData, queryParams, ctx)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to process allocation data: %v", err)), nil
	}

	// Add conversation context and insights
	s.enhanceAllocationResponse(processedData, ctx, queryParams)

	// Format response
	formatter := utils.NewResponseFormatter()
	responseText := formatter.FormatAllocationResponse(processedData, query)

	executionTime := time.Since(startTime)
	s.logger.Printf("Allocation query executed in %v: %s", executionTime, query)

	return &types.CallToolResult{
		Content: []types.ToolContent{
			{
				Type: "text",
				Text: responseText,
			},
		},
	}, nil
}

// handleQueryAssets handles asset cost queries with utilization analysis
func (s *MCPServer) handleQueryAssets(ctx *types.QueryContext, args map[string]interface{}) (*types.CallToolResult, error) {
	startTime := time.Now()

	// Extract and validate parameters
	query, ok := args["query"].(string)
	if !ok || query == "" {
		return s.createErrorResult("Query parameter is required and must be a non-empty string"), nil
	}

	// Parse natural language query
	queryParams, err := utils.ParseAssetQuery(query, args)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to parse query: %v", err)), nil
	}

	// Execute query through OpenCost client
	assetData, err := s.openCostClient.QueryAssets(ctx.Context, queryParams)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to query assets: %v", err)), nil
	}

	// Process data for AI consumption
	processor := processors.NewAssetProcessor()
	processedData, err := processor.ProcessAssets(assetData, queryParams, ctx)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to process asset data: %v", err)), nil
	}

	// Add conversation context and insights
	s.enhanceAssetResponse(processedData, ctx, queryParams)

	// Format response
	formatter := utils.NewResponseFormatter()
	responseText := formatter.FormatAssetResponse(processedData, query)

	executionTime := time.Since(startTime)
	s.logger.Printf("Asset query executed in %v: %s", executionTime, query)

	return &types.CallToolResult{
		Content: []types.ToolContent{
			{
				Type: "text",
				Text: responseText,
			},
		},
	}, nil
}

// handleQueryCloudCosts handles cloud cost queries with trend analysis and anomaly detection
func (s *MCPServer) handleQueryCloudCosts(ctx *types.QueryContext, args map[string]interface{}) (*types.CallToolResult, error) {
	startTime := time.Now()

	// Extract and validate parameters
	query, ok := args["query"].(string)
	if !ok || query == "" {
		return s.createErrorResult("Query parameter is required and must be a non-empty string"), nil
	}

	// Parse natural language query
	queryParams, err := utils.ParseCloudCostQuery(query, args)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to parse query: %v", err)), nil
	}

	// Execute query through OpenCost client
	cloudCostData, err := s.openCostClient.QueryCloudCosts(ctx.Context, queryParams)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to query cloud costs: %v", err)), nil
	}

	// Process data for AI consumption
	processor := processors.NewCloudCostProcessor()
	processedData, err := processor.ProcessCloudCosts(cloudCostData, queryParams, ctx)
	if err != nil {
		return s.createErrorResult(fmt.Sprintf("Failed to process cloud cost data: %v", err)), nil
	}

	// Add conversation context and insights
	s.enhanceCloudCostResponse(processedData, ctx, queryParams)

	// Format response
	formatter := utils.NewResponseFormatter()
	responseText := formatter.FormatCloudCostResponse(processedData, query)

	executionTime := time.Since(startTime)
	s.logger.Printf("Cloud cost query executed in %v: %s", executionTime, query)

	return &types.CallToolResult{
		Content: []types.ToolContent{
			{
				Type: "text",
				Text: responseText,
			},
		},
	}, nil
}

// Response enhancement methods

func (s *MCPServer) enhanceAllocationResponse(data *types.AllocationResponseData, ctx *types.QueryContext, params *utils.AllocationQueryParams) {
	// Add conversation hints based on data
	data.Metadata.ConversationHints = s.generateAllocationConversationHints(data, ctx)

	// Generate follow-up questions
	data.FollowUpQuestions = s.generateAllocationFollowUpQuestions(data, ctx, params)

	// Add context from conversation history
	s.addConversationContext(data, ctx)

	// Generate AI insights based on patterns
	s.generateAllocationInsights(data, ctx)
}

func (s *MCPServer) enhanceAssetResponse(data *types.AssetResponseData, ctx *types.QueryContext, params *utils.AssetQueryParams) {
	// Add conversation hints based on data
	data.Metadata.ConversationHints = s.generateAssetConversationHints(data, ctx)

	// Generate follow-up questions
	data.FollowUpQuestions = s.generateAssetFollowUpQuestions(data, ctx, params)

	// Add context from conversation history
	s.addConversationContext(data, ctx)

	// Generate AI insights based on utilization patterns
	s.generateAssetInsights(data, ctx)
}

func (s *MCPServer) enhanceCloudCostResponse(data *types.CloudCostResponseData, ctx *types.QueryContext, params *utils.CloudCostQueryParams) {
	// Add conversation hints based on data
	data.Metadata.ConversationHints = s.generateCloudCostConversationHints(data, ctx)

	// Generate follow-up questions
	data.FollowUpQuestions = s.generateCloudCostFollowUpQuestions(data, ctx, params)

	// Add context from conversation history
	s.addConversationContext(data, ctx)

	// Generate AI insights based on spending patterns
	s.generateCloudCostInsights(data, ctx)
}

// Conversation hint generators

func (s *MCPServer) generateAllocationConversationHints(data *types.AllocationResponseData, ctx *types.QueryContext) []string {
	hints := []string{}

	if data.TotalCost > 1000 {
		hints = append(hints, "High cost detected - consider asking about optimization opportunities")
	}

	if len(data.TopCostDrivers) > 0 {
		topDriver := data.TopCostDrivers[0]
		if topDriver.Percentage > 50 {
			hints = append(hints, fmt.Sprintf("Single entity (%s) drives %s of costs - investigate further", topDriver.Name, fmt.Sprintf("%.1f%%", topDriver.Percentage)))
		}
	}

	// Check for potential efficiency issues
	inefficientCount := 0
	for _, allocation := range data.Allocations {
		if allocation.Efficiency != nil && allocation.Efficiency.Overall < 0.5 {
			inefficientCount++
		}
	}

	if inefficientCount > len(data.Allocations)/2 {
		hints = append(hints, "Many allocations show low efficiency - ask about resource optimization")
	}

	return hints
}

func (s *MCPServer) generateAssetConversationHints(data *types.AssetResponseData, ctx *types.QueryContext) []string {
	hints := []string{}

	if data.Utilization.AverageCPU < 0.3 {
		hints = append(hints, "Low CPU utilization detected - consider right-sizing recommendations")
	}

	if data.Utilization.AverageRAM < 0.4 {
		hints = append(hints, "Low memory utilization detected - potential for downsizing")
	}

	if data.AssetSummary.UnderutilizedCount > 0 {
		hints = append(hints, fmt.Sprintf("%d underutilized assets found - ask for specific optimization recommendations", data.AssetSummary.UnderutilizedCount))
	}

	if data.Optimization.TotalPotentialSavings > 100 {
		hints = append(hints, fmt.Sprintf("$%.2f potential savings identified - request detailed optimization plan", data.Optimization.TotalPotentialSavings))
	}

	return hints
}

func (s *MCPServer) generateCloudCostConversationHints(data *types.CloudCostResponseData, ctx *types.QueryContext) []string {
	hints := []string{}

	if data.BillingAnalysis.PeriodComparison.ChangePercent > 20 {
		hints = append(hints, fmt.Sprintf("Cost increase of %.1f%% detected - investigate causes", data.BillingAnalysis.PeriodComparison.ChangePercent))
	}

	if len(data.Anomalies) > 0 {
		hints = append(hints, fmt.Sprintf("%d cost anomalies detected - ask for detailed analysis", len(data.Anomalies)))
	}

	if data.BillingAnalysis.CostOptimization.PotentialSavings > 500 {
		hints = append(hints, fmt.Sprintf("$%.2f in optimization opportunities - request action plan", data.BillingAnalysis.CostOptimization.PotentialSavings))
	}

	// Check for budget overruns
	if data.BillingAnalysis.BudgetTracking != nil && data.BillingAnalysis.BudgetTracking.IsOverBudget {
		hints = append(hints, "Budget exceeded - ask for cost reduction strategies")
	}

	return hints
}

// Follow-up question generators

func (s *MCPServer) generateAllocationFollowUpQuestions(data *types.AllocationResponseData, ctx *types.QueryContext, params *utils.AllocationQueryParams) []string {
	questions := []string{}

	// Cost-related questions
	if data.TotalCost > 100 {
		questions = append(questions, "What are the main drivers of these costs?")
		questions = append(questions, "How do these costs compare to previous periods?")
	}

	// Efficiency-related questions
	lowEfficiencyCount := 0
	for _, allocation := range data.Allocations {
		if allocation.Efficiency != nil && allocation.Efficiency.Overall < 0.6 {
			lowEfficiencyCount++
		}
	}

	if lowEfficiencyCount > 0 {
		questions = append(questions, "Which allocations have the lowest resource efficiency?")
		questions = append(questions, "What specific optimization recommendations do you have?")
	}

	// Namespace-specific questions
	if params.Aggregate == "namespace" && len(data.Breakdown.ByNamespace) > 1 {
		questions = append(questions, "Can you break this down by individual pods or services?")
		questions = append(questions, "Which namespace has the highest cost per resource unit?")
	}

	// Time-based questions
	if strings.Contains(params.Window, "d") {
		questions = append(questions, "How do these costs trend over time?")
		questions = append(questions, "Are there any cost spikes or anomalies?")
	}

	return questions
}

func (s *MCPServer) generateAssetFollowUpQuestions(data *types.AssetResponseData, ctx *types.QueryContext, params *utils.AssetQueryParams) []string {
	questions := []string{}

	// Utilization-related questions
	if data.Utilization.AverageCPU < 0.5 || data.Utilization.AverageRAM < 0.5 {
		questions = append(questions, "Which specific assets are underutilized?")
		questions = append(questions, "What would be the impact of right-sizing these assets?")
	}

	// Cost optimization questions
	if data.Optimization.TotalPotentialSavings > 0 {
		questions = append(questions, "Can you provide a detailed optimization plan?")
		questions = append(questions, "What are the risks of implementing these optimizations?")
	}

	// Asset type questions
	if len(data.AssetSummary.AssetTypes) > 1 {
		questions = append(questions, "Which asset types are the most cost-effective?")
		questions = append(questions, "Are there any oversized assets that could be downsized?")
	}

	// Comparison questions
	questions = append(questions, "How do these utilization rates compare to industry benchmarks?")
	questions = append(questions, "What would be the cost impact of improving utilization to 70%?")

	return questions
}

func (s *MCPServer) generateCloudCostFollowUpQuestions(data *types.CloudCostResponseData, ctx *types.QueryContext, params *utils.CloudCostQueryParams) []string {
	questions := []string{}

	// Cost trend questions
	if data.BillingAnalysis.PeriodComparison.ChangePercent != 0 {
		if data.BillingAnalysis.PeriodComparison.ChangePercent > 0 {
			questions = append(questions, "What caused the cost increase?")
		} else {
			questions = append(questions, "What drove the cost reduction?")
		}
	}

	// Service-specific questions
	if len(data.CostByService) > 1 {
		questions = append(questions, "Which cloud services offer the best cost optimization opportunities?")
		questions = append(questions, "Are there any services with unusually high costs?")
	}

	// Anomaly questions
	if len(data.Anomalies) > 0 {
		questions = append(questions, "Can you explain the cost anomalies in detail?")
		questions = append(questions, "How can we prevent these anomalies in the future?")
	}

	// Optimization questions
	if data.BillingAnalysis.CostOptimization.PotentialSavings > 0 {
		questions = append(questions, "What's the quickest way to realize these savings?")
		questions = append(questions, "Which optimization opportunities have the lowest risk?")
	}

	// Budget questions
	if data.BillingAnalysis.BudgetTracking != nil {
		questions = append(questions, "How can we stay within budget for the rest of the period?")
		questions = append(questions, "What would a reasonable budget be for next period?")
	}

	return questions
}

// Context and insights methods

func (s *MCPServer) addConversationContext(data interface{}, ctx *types.QueryContext) {
	// Get recent queries from session
	recentQueries := ctx.Session.GetRecentQueries(5)

	contextData := map[string]interface{}{
		"recent_queries":     len(recentQueries),
		"session_duration":   time.Since(ctx.Session.StartTime).String(),
		"conversation_topic": s.inferConversationTopic(recentQueries),
	}

	// Add context to response data based on type
	switch v := data.(type) {
	case *types.AllocationResponseData:
		v.Context = contextData
	case *types.AssetResponseData:
		v.Context = contextData
	case *types.CloudCostResponseData:
		v.Context = contextData
	}
}

func (s *MCPServer) inferConversationTopic(queries []types.QueryHistoryEntry) string {
	if len(queries) == 0 {
		return "cost_analysis"
	}

	// Simple topic inference based on query patterns
	topics := map[string]int{
		"optimization": 0,
		"analysis":     0,
		"troubleshooting": 0,
		"planning":     0,
	}

	for _, query := range queries {
		queryLower := strings.ToLower(query.Query)
		if strings.Contains(queryLower, "optim") || strings.Contains(queryLower, "save") || strings.Contains(queryLower, "reduc") {
			topics["optimization"]++
		} else if strings.Contains(queryLower, "analyz") || strings.Contains(queryLower, "break") || strings.Contains(queryLower, "compar") {
			topics["analysis"]++
		} else if strings.Contains(queryLower, "issue") || strings.Contains(queryLower, "problem") || strings.Contains(queryLower, "spike") {
			topics["troubleshooting"]++
		} else if strings.Contains(queryLower, "plan") || strings.Contains(queryLower, "budget") || strings.Contains(queryLower, "forecast") {
			topics["planning"]++
		}
	}

	maxTopic := "analysis"
	maxCount := 0
	for topic, count := range topics {
		if count > maxCount {
			maxCount = count
			maxTopic = topic
		}
	}

	return maxTopic
}

func (s *MCPServer) generateAllocationInsights(data *types.AllocationResponseData, ctx *types.QueryContext) {
	// This would contain AI-powered insight generation logic
	// For now, implement basic rule-based insights
	
	insights := []types.Insight{}

	// Cost concentration insight
	if len(data.TopCostDrivers) > 0 && data.TopCostDrivers[0].Percentage > 70 {
		insights = append(insights, types.Insight{
			Type:        "cost_concentration",
			Title:       "High Cost Concentration",
			Description: fmt.Sprintf("%s accounts for %.1f%% of total costs", data.TopCostDrivers[0].Name, data.TopCostDrivers[0].Percentage),
			Severity:    "high",
			Confidence:  0.9,
			ActionItems: []string{
				"Investigate resource usage patterns",
				"Consider cost allocation policies",
				"Review resource limits and requests",
			},
		})
	}

	// Efficiency insight
	inefficientCount := 0
	totalAllocations := len(data.Allocations)
	for _, allocation := range data.Allocations {
		if allocation.Efficiency != nil && allocation.Efficiency.Overall < 0.4 {
			inefficientCount++
		}
	}

	if inefficientCount > totalAllocations/2 {
		insights = append(insights, types.Insight{
			Type:        "efficiency",
			Title:       "Low Resource Efficiency",
			Description: fmt.Sprintf("%d out of %d allocations show low efficiency", inefficientCount, totalAllocations),
			Severity:    "medium",
			Confidence:  0.8,
			ActionItems: []string{
				"Review resource requests and limits",
				"Implement horizontal pod autoscaling",
				"Consider vertical pod autoscaling",
			},
		})
	}

	data.Insights = insights
}

func (s *MCPServer) generateAssetInsights(data *types.AssetResponseData, ctx *types.QueryContext) {
	insights := []types.Insight{}

	// Utilization insight
	if data.Utilization.AverageCPU < 0.3 {
		insights = append(insights, types.Insight{
			Type:        "utilization",
			Title:       "Low CPU Utilization",
			Description: fmt.Sprintf("Average CPU utilization is %.1f%%, well below optimal range", data.Utilization.AverageCPU*100),
			Severity:    "high",
			Value:       data.Utilization.AverageCPU,
			Confidence:  0.9,
			ActionItems: []string{
				"Consider downsizing instances",
				"Implement auto-scaling policies",
				"Consolidate workloads",
			},
		})
	}

	// Cost optimization insight
	if data.Optimization.TotalPotentialSavings > data.TotalCost*0.2 {
		insights = append(insights, types.Insight{
			Type:        "cost_optimization",
			Title:       "Significant Savings Opportunity",
			Description: fmt.Sprintf("Potential savings of $%.2f (%.1f%% of total cost)", data.Optimization.TotalPotentialSavings, (data.Optimization.TotalPotentialSavings/data.TotalCost)*100),
			Severity:    "high",
			Value:       data.Optimization.TotalPotentialSavings,
			Confidence:  0.8,
			ActionItems: []string{
				"Implement right-sizing recommendations",
				"Review and optimize storage usage",
				"Consider reserved instance purchases",
			},
		})
	}

	data.Insights = insights
}

func (s *MCPServer) generateCloudCostInsights(data *types.CloudCostResponseData, ctx *types.QueryContext) {
	insights := []types.Insight{}

	// Cost trend insight
	if data.BillingAnalysis.PeriodComparison.ChangePercent > 25 {
		insights = append(insights, types.Insight{
			Type:        "cost_trend",
			Title:       "Significant Cost Increase",
			Description: fmt.Sprintf("Costs increased by %.1f%% compared to previous period", data.BillingAnalysis.PeriodComparison.ChangePercent),
			Severity:    "high",
			Value:       data.BillingAnalysis.PeriodComparison.ChangePercent,
			Confidence:  0.95,
			ActionItems: []string{
				"Analyze service-level cost changes",
				"Review recent infrastructure changes",
				"Implement cost alerting and budgets",
			},
		})
	}

	// Anomaly insight
	if len(data.Anomalies) > 0 {
		highSeverityAnomalies := 0
		for _, anomaly := range data.Anomalies {
			if anomaly.Impact == "high" {
				highSeverityAnomalies++
			}
		}

		if highSeverityAnomalies > 0 {
			insights = append(insights, types.Insight{
				Type:        "anomaly",
				Title:       "Cost Anomalies Detected",
				Description: fmt.Sprintf("%d high-impact cost anomalies detected", highSeverityAnomalies),
				Severity:    "high",
				Value:       highSeverityAnomalies,
				Confidence:  0.9,
				ActionItems: []string{
					"Investigate anomaly root causes",
					"Implement automated anomaly detection",
					"Set up cost spike alerts",
				},
			})
		}
	}

	data.Insights = insights
}

// Utility methods

func (s *MCPServer) createErrorResult(message string) *types.CallToolResult {
	return &types.CallToolResult{
		Content: []types.ToolContent{
			{
				Type: "text",
				Text: message,
			},
		},
		IsError: true,
	}
}