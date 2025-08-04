package types

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// AI-Optimized Response Types

// MCPResponseData represents the base structure for all MCP tool responses
type MCPResponseData struct {
	Summary           string                 `json:"summary"`
	Data              interface{}            `json:"data"`
	Insights          []Insight              `json:"insights,omitempty"`
	Trends            []Trend                `json:"trends,omitempty"`
	Recommendations   []Recommendation       `json:"recommendations,omitempty"`
	FollowUpQuestions []string               `json:"follow_up_questions,omitempty"`
	Metadata          ResponseMetadata       `json:"metadata"`
	Context           map[string]interface{} `json:"context,omitempty"`
}

// ResponseMetadata contains metadata about the response
type ResponseMetadata struct {
	QueryType         string        `json:"query_type"`
	ExecutionTime     time.Duration `json:"execution_time"`
	DataPoints        int           `json:"data_points"`
	TimeRange         TimeRange     `json:"time_range"`
	Filters           []FilterInfo  `json:"filters,omitempty"`
	Aggregation       string        `json:"aggregation,omitempty"`
	Currency          string        `json:"currency,omitempty"`
	ConversationHints []string      `json:"conversation_hints,omitempty"`
}

// TimeRange represents a time range for queries
type TimeRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
	Step  string `json:"step,omitempty"`
}

// FilterInfo represents applied filters
type FilterInfo struct {
	Field string      `json:"field"`
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

// Insight represents an AI-generated insight
type Insight struct {
	Type        string      `json:"type"`
	Title       string      `json:"title"`
	Description string      `json:"description"`
	Severity    string      `json:"severity"`
	Value       interface{} `json:"value,omitempty"`
	Confidence  float64     `json:"confidence"`
	ActionItems []string    `json:"action_items,omitempty"`
}

// Trend represents a detected trend in the data
type Trend struct {
	Type        string      `json:"type"`
	Direction   string      `json:"direction"`
	Magnitude   float64     `json:"magnitude"`
	Description string      `json:"description"`
	Period      string      `json:"period"`
	Confidence  float64     `json:"confidence"`
	Impact      string      `json:"impact"`
	Data        interface{} `json:"data,omitempty"`
}

// Recommendation represents an AI-generated recommendation
type Recommendation struct {
	Type         string    `json:"type"`
	Title        string    `json:"title"`
	Description  string    `json:"description"`
	Priority     string    `json:"priority"`
	Impact       string    `json:"impact"`
	Effort       string    `json:"effort"`
	PotentialSavings *float64 `json:"potential_savings,omitempty"`
	Tags         []string  `json:"tags,omitempty"`
	Steps        []string  `json:"steps,omitempty"`
}

// Allocation Response Types

// AllocationResponseData represents processed allocation data
type AllocationResponseData struct {
	MCPResponseData
	Allocations    []ProcessedAllocation `json:"allocations"`
	TotalCost      float64               `json:"total_cost"`
	TopCostDrivers []CostDriver          `json:"top_cost_drivers"`
	Breakdown      AllocationBreakdown   `json:"breakdown"`
}

// ProcessedAllocation represents an allocation with AI enhancements
type ProcessedAllocation struct {
	Name           string                 `json:"name"`
	Properties     map[string]interface{} `json:"properties"`
	Start          time.Time              `json:"start"`
	End            time.Time              `json:"end"`
	CPUCoreHours   float64                `json:"cpu_core_hours"`
	RAMByteHours   float64                `json:"ram_byte_hours"`
	GPUHours       float64                `json:"gpu_hours,omitempty"`
	PVByteHours    float64                `json:"pv_byte_hours,omitempty"`
	NetworkCost    float64                `json:"network_cost,omitempty"`
	LoadBalancerCost float64              `json:"load_balancer_cost,omitempty"`
	TotalCost      float64                `json:"total_cost"`
	CPUCost        float64                `json:"cpu_cost"`
	RAMCost        float64                `json:"ram_cost"`
	GPUCost        float64                `json:"gpu_cost,omitempty"`
	PVCost         float64                `json:"pv_cost,omitempty"`
	SharedCost     float64                `json:"shared_cost,omitempty"`
	ExternalCost   float64                `json:"external_cost,omitempty"`
	Efficiency     *ResourceEfficiency    `json:"efficiency,omitempty"`
	CostPerUnit    map[string]float64     `json:"cost_per_unit,omitempty"`
	Tags           []string               `json:"tags,omitempty"`
}

// ResourceEfficiency represents resource utilization efficiency
type ResourceEfficiency struct {
	CPUEfficiency float64 `json:"cpu_efficiency"`
	RAMEfficiency float64 `json:"ram_efficiency"`
	Overall       float64 `json:"overall"`
	Recommendations []string `json:"recommendations,omitempty"`
}

// CostDriver represents a primary cost contributor
type CostDriver struct {
	Name        string  `json:"name"`
	Cost        float64 `json:"cost"`
	Percentage  float64 `json:"percentage"`
	Type        string  `json:"type"`
	Trend       string  `json:"trend,omitempty"`
	Description string  `json:"description,omitempty"`
}

// AllocationBreakdown represents cost breakdown by different dimensions
type AllocationBreakdown struct {
	ByNamespace  []CostDriver `json:"by_namespace,omitempty"`
	ByPod        []CostDriver `json:"by_pod,omitempty"`
	ByService    []CostDriver `json:"by_service,omitempty"`
	ByCluster    []CostDriver `json:"by_cluster,omitempty"`
	ByNode       []CostDriver `json:"by_node,omitempty"`
	ByController []CostDriver `json:"by_controller,omitempty"`
	ByLabel      map[string][]CostDriver `json:"by_label,omitempty"`
}

// Asset Response Types

// AssetResponseData represents processed asset data
type AssetResponseData struct {
	MCPResponseData
	Assets         []ProcessedAsset  `json:"assets"`
	TotalCost      float64           `json:"total_cost"`
	AssetSummary   AssetSummary      `json:"asset_summary"`
	Utilization    UtilizationStats  `json:"utilization"`
	Optimization   OptimizationData  `json:"optimization"`
}

// ProcessedAsset represents an asset with AI enhancements
type ProcessedAsset struct {
	Name           string                 `json:"name"`
	Type           string                 `json:"type"`
	Properties     map[string]interface{} `json:"properties"`
	Start          time.Time              `json:"start"`
	End            time.Time              `json:"end"`
	Minutes        float64                `json:"minutes"`
	CPUCores       float64                `json:"cpu_cores,omitempty"`
	RAMBytes       float64                `json:"ram_bytes,omitempty"`
	GPUCount       float64                `json:"gpu_count,omitempty"`
	CPUBreakdown   *opencost.Breakdown    `json:"cpu_breakdown,omitempty"`
	RAMBreakdown   *opencost.Breakdown    `json:"ram_breakdown,omitempty"`
	GPUBreakdown   *opencost.Breakdown    `json:"gpu_breakdown,omitempty"`
	NetworkBreakdown *opencost.Breakdown  `json:"network_breakdown,omitempty"`
	StorageBreakdown *opencost.Breakdown  `json:"storage_breakdown,omitempty"`
	TotalCost      float64                `json:"total_cost"`
	Adjustment     float64                `json:"adjustment,omitempty"`
	Utilization    *AssetUtilization      `json:"utilization,omitempty"`
	RightSizing    *RightSizingRecommendation `json:"right_sizing,omitempty"`
	Tags           []string               `json:"tags,omitempty"`
}

// AssetUtilization represents asset utilization metrics
type AssetUtilization struct {
	CPUUtilization float64 `json:"cpu_utilization"`
	RAMUtilization float64 `json:"ram_utilization"`
	StorageUtilization float64 `json:"storage_utilization,omitempty"`
	NetworkUtilization float64 `json:"network_utilization,omitempty"`
	OverallScore   float64 `json:"overall_score"`
}

// RightSizingRecommendation represents right-sizing recommendations
type RightSizingRecommendation struct {
	CurrentSize    string  `json:"current_size"`
	RecommendedSize string `json:"recommended_size"`
	PotentialSavings float64 `json:"potential_savings"`
	Confidence     float64 `json:"confidence"`
	Reasoning      string  `json:"reasoning"`
}

// AssetSummary represents a summary of assets
type AssetSummary struct {
	TotalAssets    int              `json:"total_assets"`
	AssetTypes     map[string]int   `json:"asset_types"`
	CostByType     map[string]float64 `json:"cost_by_type"`
	UnderutilizedCount int          `json:"underutilized_count"`
	OverprovisionedCount int        `json:"overprovisioned_count"`
}

// UtilizationStats represents utilization statistics
type UtilizationStats struct {
	AverageCPU     float64 `json:"average_cpu"`
	AverageRAM     float64 `json:"average_ram"`
	AverageStorage float64 `json:"average_storage"`
	LowUtilizationThreshold float64 `json:"low_utilization_threshold"`
	HighUtilizationThreshold float64 `json:"high_utilization_threshold"`
}

// OptimizationData represents asset optimization opportunities
type OptimizationData struct {
	TotalPotentialSavings float64 `json:"total_potential_savings"`
	RightSizingOpportunities int   `json:"right_sizing_opportunities"`
	IdleResources        int       `json:"idle_resources"`
	Recommendations      []AssetRecommendation `json:"recommendations"`
}

// AssetRecommendation represents an asset-specific recommendation
type AssetRecommendation struct {
	AssetName    string  `json:"asset_name"`
	Type         string  `json:"type"`
	Description  string  `json:"description"`
	Impact       string  `json:"impact"`
	Savings      float64 `json:"savings"`
	Action       string  `json:"action"`
}

// Cloud Cost Response Types

// CloudCostResponseData represents processed cloud cost data
type CloudCostResponseData struct {
	MCPResponseData
	CloudCosts     []ProcessedCloudCost `json:"cloud_costs"`
	TotalCost      float64              `json:"total_cost"`
	CostByProvider map[string]float64   `json:"cost_by_provider"`
	CostByService  map[string]float64   `json:"cost_by_service"`
	BillingAnalysis BillingAnalysis     `json:"billing_analysis"`
	Anomalies      []CostAnomaly        `json:"anomalies,omitempty"`
}

// ProcessedCloudCost represents cloud cost data with AI enhancements
type ProcessedCloudCost struct {
	Properties        map[string]interface{} `json:"properties"`
	Provider          string                 `json:"provider"`
	AccountID         string                 `json:"account_id"`
	InvoiceEntityID   string                 `json:"invoice_entity_id"`
	Service           string                 `json:"service"`
	SKU               string                 `json:"sku,omitempty"`
	Region            string                 `json:"region,omitempty"`
	UsageType         string                 `json:"usage_type,omitempty"`
	UsageUnit         string                 `json:"usage_unit,omitempty"`
	Domain            string                 `json:"domain,omitempty"`
	Start             time.Time              `json:"start"`
	End               time.Time              `json:"end"`
	ListCost          float64                `json:"list_cost"`
	NetCost           float64                `json:"net_cost"`
	AmortizedCost     float64                `json:"amortized_cost"`
	InvoicedCost      float64                `json:"invoiced_cost"`
	AmortizedNetCost  float64                `json:"amortized_net_cost"`
	Usage             float64                `json:"usage"`
	CostPerUnit       float64                `json:"cost_per_unit"`
	Trend             *CostTrend             `json:"trend,omitempty"`
	Tags              []string               `json:"tags,omitempty"`
}

// CostTrend represents cost trend information
type CostTrend struct {
	Direction     string  `json:"direction"`
	ChangePercent float64 `json:"change_percent"`
	Period        string  `json:"period"`
	Volatility    string  `json:"volatility"`
}

// BillingAnalysis represents billing analysis insights
type BillingAnalysis struct {
	TotalSpend        float64            `json:"total_spend"`
	PeriodComparison  PeriodComparison   `json:"period_comparison"`
	SpendDistribution SpendDistribution  `json:"spend_distribution"`
	CostOptimization  CostOptimization   `json:"cost_optimization"`
	BudgetTracking    *BudgetTracking    `json:"budget_tracking,omitempty"`
}

// PeriodComparison represents period-over-period comparison
type PeriodComparison struct {
	CurrentPeriod   float64 `json:"current_period"`
	PreviousPeriod  float64 `json:"previous_period"`
	Change          float64 `json:"change"`
	ChangePercent   float64 `json:"change_percent"`
	Trend           string  `json:"trend"`
}

// SpendDistribution represents spend distribution analysis
type SpendDistribution struct {
	TopServices   []CostDriver `json:"top_services"`
	TopRegions    []CostDriver `json:"top_regions"`
	TopAccounts   []CostDriver `json:"top_accounts"`
	SpendPattern  string       `json:"spend_pattern"`
}

// CostOptimization represents cost optimization opportunities
type CostOptimization struct {
	PotentialSavings     float64                 `json:"potential_savings"`
	Opportunities        []OptimizationOpportunity `json:"opportunities"`
	ReservedInstanceGaps []RIRecommendation      `json:"reserved_instance_gaps,omitempty"`
	UnusedResources      []UnusedResource        `json:"unused_resources,omitempty"`
}

// OptimizationOpportunity represents a cost optimization opportunity
type OptimizationOpportunity struct {
	Type         string  `json:"type"`
	Service      string  `json:"service"`
	Description  string  `json:"description"`
	Savings      float64 `json:"savings"`
	Confidence   float64 `json:"confidence"`
	ActionPlan   []string `json:"action_plan"`
}

// RIRecommendation represents reserved instance recommendations
type RIRecommendation struct {
	Service          string  `json:"service"`
	InstanceType     string  `json:"instance_type"`
	Region           string  `json:"region"`
	RecommendedQuantity int  `json:"recommended_quantity"`
	PotentialSavings float64 `json:"potential_savings"`
	PaybackPeriod    string  `json:"payback_period"`
}

// UnusedResource represents an unused cloud resource
type UnusedResource struct {
	ResourceID   string  `json:"resource_id"`
	ResourceType string  `json:"resource_type"`
	Service      string  `json:"service"`
	Region       string  `json:"region"`
	MonthlyCost  float64 `json:"monthly_cost"`
	LastUsed     *time.Time `json:"last_used,omitempty"`
	Reason       string  `json:"reason"`
}

// BudgetTracking represents budget tracking information
type BudgetTracking struct {
	BudgetAmount   float64 `json:"budget_amount"`
	SpentAmount    float64 `json:"spent_amount"`
	RemainingAmount float64 `json:"remaining_amount"`
	UtilizationPercent float64 `json:"utilization_percent"`
	ForecastedSpend float64 `json:"forecasted_spend"`
	IsOverBudget   bool    `json:"is_over_budget"`
	DaysRemaining  int     `json:"days_remaining"`
}

// CostAnomaly represents a detected cost anomaly
type CostAnomaly struct {
	Type         string    `json:"type"`
	Service      string    `json:"service"`
	Region       string    `json:"region,omitempty"`
	DetectedAt   time.Time `json:"detected_at"`
	AnomalyScore float64   `json:"anomaly_score"`
	ExpectedCost float64   `json:"expected_cost"`
	ActualCost   float64   `json:"actual_cost"`
	Impact       string    `json:"impact"`
	Description  string    `json:"description"`
	PossibleCauses []string `json:"possible_causes"`
	Recommendations []string `json:"recommendations"`
}

// Error Response Types

// MCPErrorResponse represents an error response with AI-friendly context
type MCPErrorResponse struct {
	Error            string              `json:"error"`
	ErrorCode        string              `json:"error_code"`
	Description      string              `json:"description"`
	Suggestions      []string            `json:"suggestions,omitempty"`
	RelatedQueries   []string            `json:"related_queries,omitempty"`
	Documentation    []DocumentationLink `json:"documentation,omitempty"`
	TroubleshootingSteps []string        `json:"troubleshooting_steps,omitempty"`
}

// DocumentationLink represents a link to relevant documentation
type DocumentationLink struct {
	Title string `json:"title"`
	URL   string `json:"url"`
	Description string `json:"description,omitempty"`
}