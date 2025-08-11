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
	Type        string  `json:"type"` // "optimization", "anomaly", "trend"
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Impact      string  `json:"impact"`           // "high", "medium", "low"
	Confidence  float64 `json:"confidence"`       // 0.0 to 1.0
	Action      string  `json:"action,omitempty"` // recommended action
}

// suggests next queries
type FollowUpOption struct {
	Label       string      `json:"label"`       // "Drill down by namespace"
	QueryType   string      `json:"queryType"`   // "allocations"
	Parameters  interface{} `json:"parameters"`  // pre-filled query parameters
	Explanation string      `json:"explanation"` // why this is suggested
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
				"assetTypes": {
					Type:        "array",
					Description: "Asset types to include (Node, Disk, Network)",
					Items:       &ItemDef{Type: "string"},
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
				"providers": {
					Type:        "array",
					Description: "Cloud providers (AWS, GCP, Azure)",
					Items:       &ItemDef{Type: "string"},
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
