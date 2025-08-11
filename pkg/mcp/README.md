# OpenCost MCP Integration - LFX Coding Challenge

This Model Context Protocol (MCP) structs for integrating OpenCost with ai agents, designed for the LFX Mentorship coding challenge as can work in real world also as its Production ready code

## Overview

Simple, focused structs that adapt OpenCost's allocation, asset, and cloud cost APIs for AI interaction while following actual MCP SDK patterns.

## Key Features

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

### Query Structures
```go
type AllocationQuery struct {
    // Standard OpenCost params
    Window, Resolution, Aggregate, Filter string

    // AI enhancements
    BusinessIntent   string    // "cost_optimization", "budget_tracking"
    ExpectedRange    *CostRange // for anomaly detection
    ComparisonPeriod *string   // "previous_week"
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
GET /allocation?window=7d&aggregate=namespace&filter=cluster:"production"
```

### MCP-Enhanced Query:
```go
query := &AllocationQuery{
    Window: "7d",
    Aggregate: []string{"namespace"},
    Filter: `cluster:"production"`,
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

**Focused Scope**: ~150 lines targeting core MCP requirements without over-engineering  
**SDK Patterns**: Following actual MCP examples rather than inventing abstractions  
**AI-First**: Parameters designed for natural language interaction  
**Practical**: Real-world cost analysis scenarios over theoretical features

This approach balances creativity with practicality, providing essential AI capabilities while staying focused on the coding challenge requirements.

## Usage Example

```go
// Create tools for MCP server registration
tools := GetAllOpenCostTools()

// Example handler usage
request := CallToolRequest{
    Name: "query_allocations",
    Arguments: map[string]interface{}{
        "window": "7d",
        "businessIntent": "cost_optimization",
    },
}

result, err := ExampleAllocationHandler(request)
if err != nil {
    log.Fatal(err)
}
// result contains AI-enhanced cost data
```

## Key Features for LFX Challenge

1. **Conversation Management**: Tracks session state and business context
2. **AI-Enhanced Parameters**: Extends OpenCost with business intent and optimization focus
3. **Type Safety**: Proper Go structs instead of generic interfaces
4. **MCP Compliance**: Follows actual Go SDK patterns for tool definitions
5. **Practical Integration**: Ready for real OpenCost server implementation