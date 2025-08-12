// package mcp provides structs for integrating opencost with ai agents.
// maps opencost api parameters to model context protocol format.
package mcp

import (
	"fmt"
	"time"
)

// conversation tracking

// tracks conversation state between ai and opencost
type ConversationState struct {
	SessionID    string    `json:"sessionId"`
	CreatedAt    time.Time `json:"createdAt"`
	LastActivity time.Time `json:"lastActivity"`

	CurrentDomain   string            `json:"currentDomain,omitempty"`
	ActiveFilters   map[string]string `json:"activeFilters,omitempty"`
	TimeContext     *TimeWindow       `json:"timeContext,omitempty"`
	BusinessContext *BusinessContext  `json:"businessContext,omitempty"`
}

type TimeWindow struct {
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	WindowType string    `json:"windowType"`
	Timezone   string    `json:"timezone"`
}

type BusinessContext struct {
	Organization string   `json:"organization,omitempty"`
	Team         string   `json:"team,omitempty"`
	CostCenter   string   `json:"costCenter,omitempty"`
	Environment  string   `json:"environment,omitempty"`
	Budget       *float64 `json:"budget,omitempty"`
}

// opencost api structs

// allocation query with ai extras
type AllocationQuery struct {
	Window            string   `json:"window"`
	Resolution        string   `json:"resolution,omitempty"`
	Aggregate         []string `json:"aggregate,omitempty"`
	Step              string   `json:"step,omitempty"`
	Filter            string   `json:"filter,omitempty"`
	IncludeIdle       *bool    `json:"includeIdle,omitempty"`
	IncludeSharedCost *bool    `json:"includeSharedCost,omitempty"`
	ShareTenancyCosts *bool    `json:"shareTenancyCosts,omitempty"`

	Accumulate                            *string `json:"accumulate,omitempty"`         
	AccumulateBy                          *string `json:"accumulateBy,omitempty"`       
	IdleByNode                            *bool   `json:"idleByNode,omitempty"`         
	SharedLoadBalancer                    *bool   `json:"sharedLoadBalancer,omitempty"` 
	IncludeProportionalAssetResourceCosts *bool   `json:"includeProportionalAssetResourceCosts,omitempty"`
	IncludeAggregatedMetadata             *bool   `json:"includeAggregatedMetadata,omitempty"`
	ShareIdle                             *bool   `json:"shareIdle,omitempty"`        
	IncludeExternal                       *bool   `json:"includeExternal,omitempty"`  
	Reconcile                             *bool   `json:"reconcile,omitempty"`        
	ReconcileNetwork                      *bool   `json:"reconcileNetwork,omitempty"` 
	MergeUnallocated                      *bool   `json:"mergeUnallocated,omitempty"` 
	SplitIdle                             *bool   `json:"splitIdle,omitempty"`        

	BusinessIntent   string     `json:"businessIntent,omitempty"`
	ExpectedRange    *CostRange `json:"expectedRange,omitempty"`
	ComparisonPeriod *string    `json:"comparisonPeriod,omitempty"`
}

type AssetQuery struct {
	Window    string   `json:"window"`
	Aggregate []string `json:"aggregate,omitempty"`
	Filter    string   `json:"filter,omitempty"`

	AssetTypes       []string `json:"assetTypes,omitempty"`
	IncludeBreakdown *bool    `json:"includeBreakdown,omitempty"`

	Accumulate              *bool   `json:"accumulate,omitempty"`
	Step                    *string `json:"step,omitempty"`
	IncludeCloud            *bool   `json:"includeCloud,omitempty"` // include cloud assets
	DisableAdjustments      *bool   `json:"disableAdjustments,omitempty"`
	DisableAggregatedStores *bool   `json:"disableAggregatedStores,omitempty"`

	OptimizationFocus string   `json:"optimizationFocus,omitempty"`
	CostThreshold     *float64 `json:"costThreshold,omitempty"`
}

// cloud cost query params
type CloudCostQuery struct {
	Window    string   `json:"window"`
	Aggregate []string `json:"aggregate,omitempty"`
	Filter    string   `json:"filter,omitempty"`

	Providers    []string `json:"providers,omitempty"`
	Services     []string `json:"services,omitempty"`
	IncludeUsage *bool    `json:"includeUsage,omitempty"`

	Accumulate  *string `json:"accumulate,omitempty"`
	Step        *string `json:"step,omitempty"`
	CostMetric  *string `json:"costMetric,omitempty"`
	Limit       *int    `json:"limit,omitempty"`
	Offset      *int    `json:"offset,omitempty"`
	SortBy      *string `json:"sortBy,omitempty"`
	SortByOrder *string `json:"sortByOrder,omitempty"`
	Format      *string `json:"format,omitempty"`

	BudgetAlert      *float64 `json:"budgetAlert,omitempty"`
	TrendAnalysis    *bool    `json:"trendAnalysis,omitempty"`
	AnomalyDetection *bool    `json:"anomalyDetection,omitempty"`
}

// response types

type MCPResponse struct {
	Data       interface{} `json:"data"`
	QueryType  string      `json:"queryType"`
	ExecutedAt time.Time   `json:"executedAt"`

	Summary     *ResponseSummary `json:"summary,omitempty"`
	Insights    []AIInsight      `json:"insights,omitempty"`
	Suggestions []string         `json:"suggestions,omitempty"`

	SessionID       string           `json:"sessionId"`
	FollowUpOptions []FollowUpOption `json:"followUpOptions,omitempty"`
}

type ResponseSummary struct {
	TotalCost      float64 `json:"totalCost"`
	Currency       string  `json:"currency"`
	TimeRange      string  `json:"timeRange"`
	TopContributor string  `json:"topContributor,omitempty"`
	ItemCount      int     `json:"itemCount"`
	CostTrend      string  `json:"costTrend,omitempty"`
}

// ai insights from cost data
type AIInsight struct {
	Type        string  `json:"type"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Impact      string  `json:"impact"`
	Confidence  float64 `json:"confidence"`
	Action      string  `json:"action,omitempty"`
}

// suggests next queries
type FollowUpOption struct {
	Label       string      `json:"label"`
	QueryType   string      `json:"queryType"`
	Parameters  interface{} `json:"parameters"`
	Explanation string      `json:"explanation"`
}

// cost range for anomaly detection
type CostRange struct {
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Currency string  `json:"currency"`
}

// mcp tool setup

// tool definition for mcp
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema ToolInputSchema `json:"inputSchema"`
}

// json schema for tool inputs
type ToolInputSchema struct {
	Type       string                        `json:"type"`
	Properties map[string]PropertyDefinition `json:"properties,omitempty"`
	Required   []string                      `json:"required,omitempty"`
}

// property in tool schema
type PropertyDefinition struct {
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Items       *ItemDef `json:"items,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

// array item types
type ItemDef struct {
	Type string `json:"type"`
}

// creates allocation tool
func NewAllocationTool() Tool {
	return Tool{
		Name:        "query_allocations",
		Description: "Query Kubernetes cost allocations with AI-enhanced parameters",
		InputSchema: ToolInputSchema{
			Type: "object",
			Properties: map[string]PropertyDefinition{
				"window": {
					Type:        "string",
					Description: "Time window (e.g., '7d', '1h')",
				},
				"aggregate": {
					Type:        "array",
					Description: "Group by dimensions (namespace, pod, etc.)",
					Items:       &ItemDef{Type: "string"},
				},
				"filter": {
					Type:        "string",
					Description: "OpenCost filter expression",
				},
				"step": {
					Type:        "string",
					Description: "Step duration for allocation sets",
				},
				"resolution": {
					Type:        "string",
					Description: "Prometheus query resolution",
				},
				"includeIdle": {
					Type:        "boolean",
					Description: "Include idle allocation costs",
				},
				"accumulate": {
					Type:        "string",
					Description: "Accumulation option (all, hour, day, week, month, quarter)",
					Enum:        []string{"all", "hour", "day", "week", "month", "quarter"},
				},
				"accumulateBy": {
					Type:        "string",
					Description: "Accumulate by specific option",
				},
				"idleByNode": {
					Type:        "boolean",
					Description: "Compute idle allocations at node level",
				},
				"sharedLoadBalancer": {
					Type:        "boolean",
					Description: "Include shared load balancer costs",
				},
				"includeProportionalAssetResourceCosts": {
					Type:        "boolean",
					Description: "Include proportional asset resource costs",
				},
				"includeAggregatedMetadata": {
					Type:        "boolean",
					Description: "Include aggregated labels/annotations",
				},
				"shareIdle": {
					Type:        "boolean",
					Description: "Share idle costs",
				},
				"includeExternal": {
					Type:        "boolean",
					Description: "Include external costs",
				},
				"reconcile": {
					Type:        "boolean",
					Description: "Reconcile costs",
				},
				"reconcileNetwork": {
					Type:        "boolean",
					Description: "Reconcile network costs",
				},
				"mergeUnallocated": {
					Type:        "boolean",
					Description: "Merge unallocated costs",
				},
				"splitIdle": {
					Type:        "boolean",
					Description: "Split idle costs",
				},
				"businessIntent": {
					Type:        "string",
					Description: "AI context: cost_optimization, budget_tracking, etc.",
				},
			},
			Required: []string{"window"},
		},
	}
}

// creates asset tool
func NewAssetTool() Tool {
	return Tool{
		Name:        "query_assets",
		Description: "Query infrastructure assets and their costs",
		InputSchema: ToolInputSchema{
			Type: "object",
			Properties: map[string]PropertyDefinition{
				"window": {
					Type:        "string",
					Description: "Time window for asset cost analysis",
				},
				"aggregate": {
					Type:        "array",
					Description: "Group by dimensions (cluster, node, etc.)",
					Items:       &ItemDef{Type: "string"},
				},
				"filter": {
					Type:        "string",
					Description: "Asset filter expression",
				},
				"assetTypes": {
					Type:        "array",
					Description: "Asset types to include (Node, Disk, Network)",
					Items:       &ItemDef{Type: "string"},
				},
				"includeBreakdown": {
					Type:        "boolean",
					Description: "Include detailed cost breakdown",
				},
				"accumulate": {
					Type:        "boolean",
					Description: "Accumulate results over time",
				},
				"step": {
					Type:        "string",
					Description: "Step duration for asset sets",
				},
				"includeCloud": {
					Type:        "boolean",
					Description: "Include cloud assets",
				},
				"disableAdjustments": {
					Type:        "boolean",
					Description: "Disable cost adjustments",
				},
				"disableAggregatedStores": {
					Type:        "boolean",
					Description: "Disable aggregated stores",
				},
				"optimizationFocus": {
					Type:        "string",
					Description: "Focus area: underutilized, oversized, etc.",
				},
			},
			Required: []string{"window"},
		},
	}
}

// creates cloud cost tool
func NewCloudCostTool() Tool {
	return Tool{
		Name:        "query_cloudcosts",
		Description: "Query cloud provider costs with AI insights",
		InputSchema: ToolInputSchema{
			Type: "object",
			Properties: map[string]PropertyDefinition{
				"window": {
					Type:        "string",
					Description: "Time window for cloud cost analysis",
				},
				"aggregate": {
					Type:        "array",
					Description: "Group by dimensions (provider, service, etc.)",
					Items:       &ItemDef{Type: "string"},
				},
				"filter": {
					Type:        "string",
					Description: "Cloud cost filter expression",
				},
				"providers": {
					Type:        "array",
					Description: "Cloud providers (AWS, GCP, Azure)",
					Items:       &ItemDef{Type: "string"},
				},
				"services": {
					Type:        "array",
					Description: "Cloud services to include",
					Items:       &ItemDef{Type: "string"},
				},
				"includeUsage": {
					Type:        "boolean",
					Description: "Include usage data",
				},
				"accumulate": {
					Type:        "string",
					Description: "Accumulation option (all, hour, day, week, month, quarter)",
					Enum:        []string{"all", "hour", "day", "week", "month", "quarter"},
				},
				"step": {
					Type:        "string",
					Description: "Step duration for cloud cost sets",
				},
				"costMetric": {
					Type:        "string",
					Description: "Cost metric (amortized, list, etc.)",
					Enum:        []string{"amortized", "list", "net", "blended"},
				},
				"limit": {
					Type:        "integer",
					Description: "Result limit",
				},
				"offset": {
					Type:        "integer",
					Description: "Result offset",
				},
				"sortBy": {
					Type:        "string",
					Description: "Sort field",
				},
				"sortByOrder": {
					Type:        "string",
					Description: "Sort direction",
					Enum:        []string{"asc", "desc"},
				},
				"format": {
					Type:        "string",
					Description: "Output format",
					Enum:        []string{"json", "csv"},
				},
				"budgetAlert": {
					Type:        "number",
					Description: "Budget threshold for AI alerts",
				},
			},
			Required: []string{"window"},
		},
	}
}

// mcp tool request
type CallToolRequest struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments"`
}

// mcp tool result
type CallToolResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError,omitempty"`
}

// content types for responses
type Content struct {
	Type string      `json:"type"` // "text", "image", "resource"
	Text string      `json:"text,omitempty"`
	Data interface{} `json:"data,omitempty"`
}

// tool handler function signature
type ToolHandler func(request CallToolRequest) (*CallToolResult, error)

// helper for text content
func NewTextContent(text string) Content {
	return Content{
		Type: "text",
		Text: text,
	}
}

// helper for success result
func NewToolResult(content ...Content) *CallToolResult {
	return &CallToolResult{
		Content: content,
		IsError: false,
	}
}

// helper for error result
func NewToolError(message string) *CallToolResult {
	return &CallToolResult{
		Content: []Content{{Type: "text", Text: message}},
		IsError: true,
	}
}

// example usage

// example allocation handler
func ExampleAllocationHandler(request CallToolRequest) (*CallToolResult, error) {
	window, ok := request.Arguments["window"].(string)
	if !ok {
		return NewToolError("window parameter is required"), nil
	}

	businessIntent, _ := request.Arguments["businessIntent"].(string)

	// This would integrate with actual OpenCost API
	result := fmt.Sprintf("Allocation query for window=%s with intent=%s", window, businessIntent)

	// Return AI-enhanced response
	return NewToolResult(NewTextContent(result)), nil
}

// validates tool requests
func ValidateToolRequest(request CallToolRequest, tool Tool) error {
	for _, required := range tool.InputSchema.Required {
		if _, exists := request.Arguments[required]; !exists {
			return fmt.Errorf("required parameter '%s' is missing", required)
		}
	}
	return nil
}

// returns all opencost tools
func GetAllOpenCostTools() []Tool {
	return []Tool{
		NewAllocationTool(),
		NewAssetTool(),
		NewCloudCostTool(),
	}
}
