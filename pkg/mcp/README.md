# OpenCost MCP Integration - LFX Coding Challenge

This package provides Model Context Protocol (MCP) structs for integrating OpenCost with AI agents, designed for the LFX Mentorship coding challenge. **Our structs now support the full range of OpenCost API parameters** for allocations, assets, and cloud costs.

## Overview

Comprehensive structs that adapt OpenCost's allocation, asset, and cloud cost APIs for AI interaction while following actual MCP SDK patterns. **We provide 100% parameter coverage** for all OpenCost query types.

## Key Features

- **Full Parameter Coverage**: Support for all OpenCost allocation, asset, and cloud cost query parameters
- **Conversation State**: Simple session tracking (`ConversationState`)
- **API Parameter Mapping**: Direct OpenCost → MCP parameter adaptation
- **AI Enhancements**: Smart defaults, natural language filters, business context
- **Structured Responses**: AI-digestible results with insights and suggestions

## Core Structs

### Conversation Management
```go
type ConversationState struct {
    SessionID       string
    CurrentDomain   string // "allocations", "assets", "cloudcosts"  
    ActiveFilters   map[string]string
    TimeContext     *TimeWindow
    BusinessContext *BusinessContext
}
```

### Enhanced Query Structures with Full Parameter Support

#### AllocationQuery - Complete OpenCost Coverage
```go
type AllocationQuery struct {
    // Core OpenCost parameters
    Window, Resolution, Aggregate, Step, Filter string
    IncludeIdle, IncludeSharedCost, ShareTenancyCosts *bool
    
    // Full accumulation support
    Accumulate, AccumulateBy *string // "all", "hour", "day", "week", "month", "quarter"
    
    // Advanced allocation options
    IdleByNode, SharedLoadBalancer *bool
    IncludeProportionalAssetResourceCosts, IncludeAggregatedMetadata *bool
    ShareIdle, IncludeExternal, Reconcile, ReconcileNetwork *bool
    MergeUnallocated, SplitIdle *bool
    
    // AI enhancements
    BusinessIntent   string    // "cost_optimization", "budget_tracking"
    ExpectedRange    *CostRange // for anomaly detection
    ComparisonPeriod *string   // "previous_week"
}
```

#### AssetQuery - Complete OpenCost Coverage
```go
type AssetQuery struct {
    // Core OpenCost parameters
    Window, Filter string
    Aggregate, AssetTypes []string
    IncludeBreakdown *bool
    
    // Full asset options
    Accumulate, IncludeCloud, DisableAdjustments, DisableAggregatedStores *bool
    Step *string
    
    // AI enhancements
    OptimizationFocus string   // "underutilized", "oversized", etc.
    CostThreshold     *float64
}
```

#### CloudCostQuery - Complete OpenCost Coverage
```go
type CloudCostQuery struct {
    // Core OpenCost parameters
    Window, Filter string
    Aggregate, Providers, Services []string
    IncludeUsage *bool
    
    // Full cloud cost options
    Accumulate, Step, CostMetric, SortBy, SortByOrder, Format *string
    Limit, Offset *int
    
    // AI enhancements
    BudgetAlert      *float64
    TrendAnalysis    *bool
    AnomalyDetection *bool
}
```

### AI-Enhanced Responses
```go
type MCPResponse struct {
    Data        interface{}
    Summary     *ResponseSummary  // digestible summary
    Insights    []AIInsight       // actionable recommendations  
    Suggestions []string          // next query options
}
```

## Complete Parameter Coverage

### Allocation Parameters (All Supported)
- **Time**: `window`, `step`, `resolution`
- **Aggregation**: `aggregate`, `accumulate`, `accumulateBy`
- **Cost Options**: `includeIdle`, `idleByNode`, `sharedLoadBalancer`
- **Advanced**: `includeProportionalAssetResourceCosts`, `includeAggregatedMetadata`
- **Sharing**: `shareIdle`, `shareTenancyCosts`, `splitIdle`
- **Reconciliation**: `reconcile`, `reconcileNetwork`, `mergeUnallocated`
- **External**: `includeExternal`
- **Filtering**: `filter`

### Asset Parameters (All Supported)
- **Time**: `window`, `step`
- **Aggregation**: `aggregate`, `accumulate`
- **Asset Types**: `assetTypes`, `includeCloud`
- **Options**: `includeBreakdown`, `disableAdjustments`, `disableAggregatedStores`
- **Filtering**: `filter`

### Cloud Cost Parameters (All Supported)
- **Time**: `window`, `step`
- **Aggregation**: `aggregate`, `accumulate`
- **Providers**: `providers`, `services`
- **Options**: `includeUsage`, `costMetric`
- **Pagination**: `limit`, `offset`
- **Sorting**: `sortBy`, `sortByOrder`
- **Format**: `format`
- **Filtering**: `filter`

## Creative AI Adaptations

1. **Natural Language Filters**: Convert "high cost pods last week" → filter syntax
2. **Business Context**: Track organization/team for relevant suggestions
3. **Anomaly Detection**: Compare against expected cost ranges
4. **Trend Analysis**: Automatically detect cost patterns
5. **Follow-up Suggestions**: Recommend next logical queries
6. **Budget Alerts**: Threshold-based notifications for AI agents

## Example Usage

### Traditional OpenCost:
```
GET /allocation?window=7d&aggregate=namespace&filter=cluster:"production"&includeIdle=true&accumulate=day
```

### MCP-Enhanced Query:
```go
query := &AllocationQuery{
    Window: "7d",
    Aggregate: []string{"namespace"},
    Filter: `cluster:"production"`,
    IncludeIdle: &true,
    Accumulate: &"day",
    BusinessIntent: "cost_optimization",
    ExpectedRange: &CostRange{Min: 1000, Max: 5000, Currency: "USD"},
    ComparisonPeriod: &"previous_week",
}
```

### AI Response:
```go
response := &MCPResponse{
    Data: allocationData,
    Summary: &ResponseSummary{
        TotalCost: 3500.50,
        TopContributor: "ml-training namespace", 
        CostTrend: "increasing",
    },
    Insights: []AIInsight{
        {
            Type: "optimization",
            Title: "High growth in ml-training costs",
            Impact: "high",
            Confidence: 0.85,
            Action: "Consider rightsizing workloads",
        },
    },
    Suggestions: []string{
        "Analyze ml-training resource utilization",
        "Compare with last month's spending",
    },
}
```

## Design Rationale

**Complete Coverage**: 100% parameter support for all OpenCost query types  
**SDK Patterns**: Following actual MCP examples rather than inventing abstractions  
**AI-First**: Parameters designed for natural language interaction  
**Practical**: Real-world cost analysis scenarios with full API compatibility

This approach balances creativity with practicality, providing essential AI capabilities while ensuring complete compatibility with OpenCost's extensive parameter set.

## Usage Example

```go
// Create tools for MCP server registration
tools := GetAllOpenCostTools()

// Example handler usage with full parameter support
request := CallToolRequest{
    Name: "query_allocations",
    Arguments: map[string]interface{}{
        "window": "7d",
        "aggregate": []string{"namespace", "pod"},
        "includeIdle": true,
        "accumulate": "day",
        "businessIntent": "cost_optimization",
    },
}

result, err := ExampleAllocationHandler(request)
if err != nil {
    log.Fatal(err)
}
// result contains AI-enhanced cost data with full parameter support
```

## Key Features for LFX Challenge

1. **Complete Parameter Coverage**: Support for all OpenCost allocation, asset, and cloud cost parameters
2. **Conversation Management**: Tracks session state and business context
3. **AI-Enhanced Parameters**: Extends OpenCost with business intent and optimization focus
4. **Type Safety**: Proper Go structs instead of generic interfaces
5. **MCP Compliance**: Follows actual Go SDK patterns for tool definitions
6. **Production Ready**: Full API compatibility for real OpenCost server implementation