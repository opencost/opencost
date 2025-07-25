package mcp

type QueryContext struct {
    QueryType    string          `json:"queryType"`
    TimeWindow   string          `json:"timeWindow,omitempty"`
    Aggregations []string        `json:"aggregations,omitempty"`
    Filters      map[string]string `json:"filters,omitempty"`
    Granularity  string          `json:"granularity,omitempty"`
    Step         string          `json:"step,omitempty"`
    ResponseHint string          `json:"responseHint,omitempty"`
}

type AllocationResponse struct {
    TotalCost     float64            `json:"totalCost"`
    CostBreakdown map[string]float64 `json:"costBreakdown"`
    TimeSeries    []TimeSeriesPoint  `json:"timeSeries,omitempty"`
}

type AssetResponse struct {
    AssetType     string             `json:"assetType"`
    TotalCost     float64            `json:"totalCost"`
    CostPerAsset  map[string]float64 `json:"costPerAsset"`
    Region        string             `json:"region,omitempty"`
}

type CloudCostResponse struct {
    Provider        string             `json:"provider"`
    Services        map[string]float64 `json:"services"`
    TotalCloudCost  float64            `json:"totalCloudCost"`
}

type TimeSeriesPoint struct {
    Timestamp string  `json:"timestamp"`
    Value     float64 `json:"value"`
}

type ConversationState struct {
    LastQuery      QueryContext `json:"lastQuery"`
    LastIntent     string       `json:"lastIntent"`
    FollowUpNeeded bool         `json:"followUpNeeded"`
    Summary        string       `json:"summary,omitempty"`
}
