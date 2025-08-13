//go:build mcp

package mcp

import (
	"fmt"
	"strings"
	"time"
)

// =============================================================================
// CONVERSATION MANAGEMENT
// =============================================================================

// OpenCostConversationContext manages the state and context of ongoing
// AI conversations about cost data, enabling multi-turn interactions
// and context-aware responses.
type OpenCostConversationContext struct {
	SessionID    string    `json:"sessionId"`
	StartTime    time.Time `json:"startTime"`
	LastActivity time.Time `json:"lastActivity"`

	// Conversation state management
	CurrentWindow      *TimeWindow            `json:"currentWindow,omitempty"`
	PreferredCurrency  string                 `json:"preferredCurrency,omitempty"`
	DefaultAggregation []string               `json:"defaultAggregation,omitempty"`
	ActiveFilters      map[string]interface{} `json:"activeFilters,omitempty"`

	// AI-specific context
	UserPreferences *UserCostPreferences `json:"userPreferences,omitempty"`
	RecentQueries   []string             `json:"recentQueries,omitempty"`
	CostInsights    []CostInsight        `json:"costInsights,omitempty"`

	// Query optimization
	CachedResults    map[string]CachedResult `json:"cachedResults,omitempty"`
	PerformanceHints []string                `json:"performanceHints,omitempty"`
}

// UserCostPreferences captures user-specific preferences for cost analysis
type UserCostPreferences struct {
	PreferredTimeRange  string           `json:"preferredTimeRange"` // "7d", "30d", "90d"
	CostThresholds      CostThresholds   `json:"costThresholds"`
	ImportantNamespaces []string         `json:"importantNamespaces"`
	AlertPreferences    AlertPreferences `json:"alertPreferences"`
	ReportingFormat     string           `json:"reportingFormat"` // "detailed", "summary", "trends"
}

type CostThresholds struct {
	HighCostAlert    float64 `json:"highCostAlert"`    // Alert if cost exceeds this
	AnomalyThreshold float64 `json:"anomalyThreshold"` // Percentage change for anomaly detection
	EfficiencyTarget float64 `json:"efficiencyTarget"` // Target efficiency percentage
}

type AlertPreferences struct {
	EnableCostSpikes     bool     `json:"enableCostSpikes"`
	EnableEfficiencyDrop bool     `json:"enableEfficiencyDrop"`
	NotificationChannels []string `json:"notificationChannels"`
}

// =============================================================================
// ALLOCATION QUERY STRUCTURES
// =============================================================================

// AllocationQueryRequest represents a comprehensive allocation query with
// AI-friendly features for natural language interpretation and smart defaults
type AllocationQueryRequest struct {
	// Core OpenCost parameters
	Window       *TimeWindow    `json:"window"`
	Step         *time.Duration `json:"step,omitempty"`
	Aggregate    []string       `json:"aggregate,omitempty"`
	Accumulate   bool           `json:"accumulate,omitempty"`
	AccumulateBy string         `json:"accumulateBy,omitempty"`

	// Advanced parameters
	IncludeIdle                           bool `json:"includeIdle,omitempty"`
	IdleByNode                            bool `json:"idleByNode,omitempty"`
	ShareIdle                             bool `json:"shareIdle,omitempty"`
	ShareLoadBalancer                     bool `json:"shareLoadBalancer,omitempty"`
	IncludeProportionalAssetResourceCosts bool `json:"includeProportionalAssetResourceCosts,omitempty"`
	IncludeAggregatedMetadata             bool `json:"includeAggregatedMetadata,omitempty"`

	// Filtering and selection
	Filter *SmartFilter `json:"filter,omitempty"`

	// AI-enhanced features
	NaturalLanguageQuery string `json:"naturalLanguageQuery,omitempty"`
	AnalysisType         string `json:"analysisType,omitempty"`   // "cost-breakdown", "efficiency", "trends", "anomalies"
	ComparisonMode       string `json:"comparisonMode,omitempty"` // "previous-period", "budget", "target"
	OutputFormat         string `json:"outputFormat,omitempty"`   // "summary", "detailed", "insights"

	// Context from conversation
	Context *QueryContext `json:"context,omitempty"`
}

// SmartFilter provides AI-friendly filtering with natural language support
type SmartFilter struct {
	// Traditional filters
	Namespace []string `json:"namespace,omitempty"`
	Service   []string `json:"service,omitempty"`
	Pod       []string `json:"pod,omitempty"`
	Container []string `json:"container,omitempty"`
	Node      []string `json:"node,omitempty"`

	// Cost-based filters
	MinCost *float64 `json:"minCost,omitempty"`
	MaxCost *float64 `json:"maxCost,omitempty"`
	TopK    *int     `json:"topK,omitempty"`    // Get top K by cost
	BottomK *int     `json:"bottomK,omitempty"` // Get bottom K by cost

	// Efficiency filters
	MinEfficiency *float64 `json:"minEfficiency,omitempty"`
	MaxEfficiency *float64 `json:"maxEfficiency,omitempty"`

	// Natural language interpretation
	NaturalFilter string `json:"naturalFilter,omitempty"` // "expensive services", "inefficient pods"

	// Label-based filtering with smart matching
	Labels      map[string]interface{} `json:"labels,omitempty"`
	LabelExists []string               `json:"labelExists,omitempty"`
}

// QueryContext provides context for interpreting and optimizing queries
type QueryContext struct {
	UserIntent      string           `json:"userIntent,omitempty"` // "troubleshoot", "optimize", "report"
	PreviousResults *PreviousResults `json:"previousResults,omitempty"`
	BusinessContext *BusinessContext `json:"businessContext,omitempty"`
	TimeComparison  *TimeComparison  `json:"timeComparison,omitempty"`
}

type PreviousResults struct {
	LastQuery        *AllocationQueryRequest `json:"lastQuery,omitempty"`
	LastWindow       *TimeWindow             `json:"lastWindow,omitempty"`
	IdentifiedIssues []string                `json:"identifiedIssues,omitempty"`
}

type BusinessContext struct {
	Department   string   `json:"department,omitempty"`
	Project      string   `json:"project,omitempty"`
	CostCenter   string   `json:"costCenter,omitempty"`
	BudgetPeriod string   `json:"budgetPeriod,omitempty"`
	BudgetLimit  *float64 `json:"budgetLimit,omitempty"`
}

type TimeComparison struct {
	CompareWith    *TimeWindow `json:"compareWith,omitempty"`
	ComparisonType string      `json:"comparisonType,omitempty"` // "previous", "same-period-last-year"
}

// AllocationQueryResponse provides structured, AI-optimized allocation data
type AllocationQueryResponse struct {
	// Core data
	Allocations []EnhancedAllocation `json:"allocations"`
	Window      TimeWindow           `json:"window"`
	TotalCost   float64              `json:"totalCost"`

	// AI-enhanced insights
	Summary         AllocationSummary `json:"summary"`
	Insights        []CostInsight     `json:"insights"`
	Recommendations []Recommendation  `json:"recommendations"`

	// Metadata for follow-up queries
	QueryMetadata   QueryMetadata    `json:"queryMetadata"`
	Warnings        []string         `json:"warnings,omitempty"`
	PerformanceInfo *PerformanceInfo `json:"performanceInfo,omitempty"`
}

// EnhancedAllocation extends OpenCost allocation with AI-friendly features
type EnhancedAllocation struct {
	// Core OpenCost fields
	Name             string     `json:"name"`
	Window           TimeWindow `json:"window"`
	CPUCost          float64    `json:"cpuCost"`
	RAMCost          float64    `json:"ramCost"`
	GPUCost          float64    `json:"gpuCost"`
	NetworkCost      float64    `json:"networkCost"`
	LoadBalancerCost float64    `json:"loadBalancerCost"`
	PVCost           float64    `json:"pvCost"`
	SharedCost       float64    `json:"sharedCost"`
	ExternalCost     float64    `json:"externalCost"`
	TotalCost        float64    `json:"totalCost"`

	// Efficiency metrics (AI-enhanced)
	CPUEfficiency   *float64 `json:"cpuEfficiency,omitempty"`
	RAMEfficiency   *float64 `json:"ramEfficiency,omitempty"`
	TotalEfficiency *float64 `json:"totalEfficiency,omitempty"`

	// AI insights
	CostRank        int    `json:"costRank"`            // Rank by cost in result set
	EfficiencyRank  int    `json:"efficiencyRank"`      // Rank by efficiency
	CostTrend       string `json:"costTrend,omitempty"` // "increasing", "decreasing", "stable"
	EfficiencyTrend string `json:"efficiencyTrend,omitempty"`

	// Contextual information
	Properties  map[string]interface{} `json:"properties,omitempty"`
	Labels      map[string]string      `json:"labels,omitempty"`
	Annotations []string               `json:"annotations,omitempty"` // AI-generated notes
}

// =============================================================================
// ASSET QUERY STRUCTURES
// =============================================================================

// AssetQueryRequest provides comprehensive asset querying with AI enhancements
type AssetQueryRequest struct {
	// Core parameters
	Window *TimeWindow  `json:"window"`
	Filter *SmartFilter `json:"filter,omitempty"`

	// Asset-specific parameters
	AssetTypes    []string `json:"assetTypes,omitempty"` // "Node", "Disk", "LoadBalancer", etc.
	IncludeCarbon bool     `json:"includeCarbon,omitempty"`

	// AI-enhanced features
	AnalysisType string `json:"analysisType,omitempty"` // "utilization", "cost-efficiency", "capacity-planning"
	OutputFormat string `json:"outputFormat,omitempty"` // "summary", "detailed", "optimization"

	// Natural language support
	NaturalQuery string        `json:"naturalQuery,omitempty"` // "underutilized nodes", "expensive storage"
	Context      *QueryContext `json:"context,omitempty"`
}

// AssetQueryResponse provides structured asset data with AI insights
type AssetQueryResponse struct {
	Assets          []EnhancedAsset  `json:"assets"`
	Window          TimeWindow       `json:"window"`
	Summary         AssetSummary     `json:"summary"`
	Insights        []CostInsight    `json:"insights"`
	Recommendations []Recommendation `json:"recommendations"`
	QueryMetadata   QueryMetadata    `json:"queryMetadata"`
}

// EnhancedAsset extends OpenCost asset data with AI-friendly features
type EnhancedAsset struct {
	// Core OpenCost fields
	Type       string                 `json:"type"`
	Name       string                 `json:"name"`
	Window     TimeWindow             `json:"window"`
	TotalCost  float64                `json:"totalCost"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Labels     map[string]string      `json:"labels,omitempty"`

	// Asset-specific costs
	CPUCost     *float64 `json:"cpuCost,omitempty"`
	RAMCost     *float64 `json:"ramCost,omitempty"`
	GPUCost     *float64 `json:"gpuCost,omitempty"`
	StorageCost *float64 `json:"storageCost,omitempty"`
	NetworkCost *float64 `json:"networkCost,omitempty"`

	// Utilization and efficiency (AI-enhanced)
	CPUUtilization   *float64 `json:"cpuUtilization,omitempty"`
	RAMUtilization   *float64 `json:"ramUtilization,omitempty"`
	UtilizationScore *float64 `json:"utilizationScore,omitempty"`

	// Carbon footprint (if enabled)
	CarbonEmission *float64 `json:"carbonEmission,omitempty"`

	// AI insights
	UtilizationTrend      string   `json:"utilizationTrend,omitempty"` // "increasing", "decreasing", "stable"
	CostEfficiencyRank    int      `json:"costEfficiencyRank"`
	OptimizationPotential *float64 `json:"optimizationPotential,omitempty"` // Potential savings
	Annotations           []string `json:"annotations,omitempty"`
}

// =============================================================================
// CLOUD COST QUERY STRUCTURES
// =============================================================================

// CloudCostQueryRequest provides comprehensive cloud cost querying with AI features
type CloudCostQueryRequest struct {
	// Core parameters
	Window     *TimeWindow           `json:"window"`
	Aggregate  []string              `json:"aggregate,omitempty"`
	Accumulate bool                  `json:"accumulate,omitempty"`
	Filter     *CloudCostSmartFilter `json:"filter,omitempty"`

	// Cost metrics selection
	CostMetric string `json:"costMetric,omitempty"` // "amortizedNetCost", "listCost", etc.

	// AI-enhanced features
	AnalysisType   string `json:"analysisType,omitempty"` // "cost-breakdown", "trends", "anomalies", "attribution"
	ComparisonMode string `json:"comparisonMode,omitempty"`
	OutputFormat   string `json:"outputFormat,omitempty"`

	// Natural language support
	NaturalQuery string        `json:"naturalQuery,omitempty"` // "AWS S3 costs this month", "compute costs by region"
	Context      *QueryContext `json:"context,omitempty"`
}

// CloudCostSmartFilter provides AI-friendly cloud cost filtering
type CloudCostSmartFilter struct {
	// Provider filtering
	Provider []string `json:"provider,omitempty"` // "aws", "gcp", "azure"
	Account  []string `json:"account,omitempty"`
	Region   []string `json:"region,omitempty"`

	// Service filtering
	Service  []string `json:"service,omitempty"`  // "EC2", "S3", "RDS", etc.
	Category []string `json:"category,omitempty"` // "compute", "storage", "network"

	// Cost-based filtering
	MinCost *float64 `json:"minCost,omitempty"`
	MaxCost *float64 `json:"maxCost,omitempty"`
	TopK    *int     `json:"topK,omitempty"`

	// Natural language interpretation
	NaturalFilter string `json:"naturalFilter,omitempty"` // "expensive storage services", "compute in us-east-1"

	// Labels and tags
	Labels map[string]interface{} `json:"labels,omitempty"`
}

// CloudCostQueryResponse provides structured cloud cost data with AI insights
type CloudCostQueryResponse struct {
	CloudCosts      []EnhancedCloudCost `json:"cloudCosts"`
	Window          TimeWindow          `json:"window"`
	TotalCost       float64             `json:"totalCost"`
	Summary         CloudCostSummary    `json:"summary"`
	Insights        []CostInsight       `json:"insights"`
	Recommendations []Recommendation    `json:"recommendations"`
	QueryMetadata   QueryMetadata       `json:"queryMetadata"`
}

// EnhancedCloudCost extends OpenCost cloud cost data with AI features
type EnhancedCloudCost struct {
	// Core OpenCost fields
	Properties       CloudCostProperties `json:"properties"`
	Window           TimeWindow          `json:"window"`
	ListCost         float64             `json:"listCost"`
	NetCost          float64             `json:"netCost"`
	AmortizedNetCost float64             `json:"amortizedNetCost"`
	InvoicedCost     float64             `json:"invoicedCost"`
	AmortizedCost    float64             `json:"amortizedCost"`

	// AI-enhanced insights
	CostRank       int      `json:"costRank"`
	CostTrend      string   `json:"costTrend,omitempty"`
	PercentOfTotal float64  `json:"percentOfTotal"`
	Annotations    []string `json:"annotations,omitempty"`
}

type CloudCostProperties struct {
	ProviderID       string            `json:"providerID,omitempty"`
	Provider         string            `json:"provider,omitempty"`
	AccountID        string            `json:"accountID,omitempty"`
	AccountName      string            `json:"accountName,omitempty"`
	RegionID         string            `json:"regionID,omitempty"`
	AvailabilityZone string            `json:"availabilityZone,omitempty"`
	Service          string            `json:"service,omitempty"`
	Category         string            `json:"category,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
}

// =============================================================================
// SHARED HELPER STRUCTURES
// =============================================================================

// TimeWindow provides flexible time window specification with AI-friendly parsing
type TimeWindow struct {
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Duration string    `json:"duration,omitempty"` // Human-readable like "7d", "1w", "1m"
}

// CostInsight represents AI-generated insights about cost data
type CostInsight struct {
	Type        string      `json:"type"`     // "anomaly", "trend", "optimization", "alert"
	Severity    string      `json:"severity"` // "low", "medium", "high", "critical"
	Title       string      `json:"title"`
	Description string      `json:"description"`
	Impact      *float64    `json:"impact,omitempty"`   // Financial impact
	Confidence  float64     `json:"confidence"`         // AI confidence score (0-1)
	Actions     []string    `json:"actions,omitempty"`  // Suggested actions
	Metadata    interface{} `json:"metadata,omitempty"` // Additional structured data
}

// Recommendation provides AI-generated recommendations for cost optimization
type Recommendation struct {
	ID               string      `json:"id"`
	Type             string      `json:"type"` // "rightsizing", "scheduling", "termination"
	Title            string      `json:"title"`
	Description      string      `json:"description"`
	Effort           string      `json:"effort"` // "low", "medium", "high"
	Impact           string      `json:"impact"` // "low", "medium", "high"
	PotentialSavings *float64    `json:"potentialSavings,omitempty"`
	Implementation   []string    `json:"implementation,omitempty"`
	RiskLevel        string      `json:"riskLevel"` // "low", "medium", "high"
	Metadata         interface{} `json:"metadata,omitempty"`
}

// Summary structures for different data types
type AllocationSummary struct {
	TotalCost             float64            `json:"totalCost"`
	AverageEfficiency     float64            `json:"averageEfficiency"`
	TopCostConsumers      []string           `json:"topCostConsumers"`
	InefficiientResources []string           `json:"inefficientResources"`
	CostBreakdown         map[string]float64 `json:"costBreakdown"`
}

type AssetSummary struct {
	TotalCost           float64            `json:"totalCost"`
	AverageUtilization  float64            `json:"averageUtilization"`
	UnderutilizedAssets []string           `json:"underutilizedAssets"`
	OverutilizedAssets  []string           `json:"overutilizedAssets"`
	AssetTypeBreakdown  map[string]float64 `json:"assetTypeBreakdown"`
}

type CloudCostSummary struct {
	TotalCost         float64            `json:"totalCost"`
	TopServices       []string           `json:"topServices"`
	TopRegions        []string           `json:"topRegions"`
	ProviderBreakdown map[string]float64 `json:"providerBreakdown"`
	ServiceBreakdown  map[string]float64 `json:"serviceBreakdown"`
}

// QueryMetadata provides information about query execution and optimization
type QueryMetadata struct {
	ExecutionTime     time.Duration `json:"executionTime"`
	RecordsReturned   int           `json:"recordsReturned"`
	RecordsFiltered   int           `json:"recordsFiltered"`
	CacheHit          bool          `json:"cacheHit"`
	OptimizationHints []string      `json:"optimizationHints,omitempty"`
	QueryComplexity   string        `json:"queryComplexity"` // "simple", "medium", "complex"
}

type PerformanceInfo struct {
	QueryDuration    time.Duration `json:"queryDuration"`
	ProcessingTime   time.Duration `json:"processingTime"`
	CacheStatus      string        `json:"cacheStatus"`
	OptimizationTips []string      `json:"optimizationTips,omitempty"`
}

// CachedResult for storing frequently accessed results
type CachedResult struct {
	Query     string        `json:"query"`
	Result    interface{}   `json:"result"`
	Timestamp time.Time     `json:"timestamp"`
	TTL       time.Duration `json:"ttl"`
}

// Helper methods for natural language processing and AI interactions
func (req *AllocationQueryRequest) ParseNaturalLanguage() error {
	if req.NaturalLanguageQuery == "" {
		return nil
	}

	query := strings.ToLower(req.NaturalLanguageQuery)

	// Parse common patterns
	if strings.Contains(query, "expensive") || strings.Contains(query, "high cost") {
		if req.Filter == nil {
			req.Filter = &SmartFilter{}
		}
		// Set to get top 10 by cost if not specified
		if req.Filter.TopK == nil {
			topK := 10
			req.Filter.TopK = &topK
		}
	}

	if strings.Contains(query, "inefficient") || strings.Contains(query, "low efficiency") {
		if req.Filter == nil {
			req.Filter = &SmartFilter{}
		}
		maxEff := 0.7 // Less than 70% efficiency
		req.Filter.MaxEfficiency = &maxEff
	}

	// Parse namespace references
	if strings.Contains(query, "production") || strings.Contains(query, "prod") {
		if req.Filter == nil {
			req.Filter = &SmartFilter{}
		}
		req.Filter.Namespace = append(req.Filter.Namespace, "production", "prod")
	}

	// Parse time references
	if strings.Contains(query, "last week") {
		now := time.Now()
		start := now.AddDate(0, 0, -7)
		req.Window = &TimeWindow{
			Start:    start,
			End:      now,
			Duration: "7d",
		}
	}

	return nil
}

func (req *AssetQueryRequest) ParseNaturalLanguage() error {
	if req.NaturalQuery == "" {
		return nil
	}

	query := strings.ToLower(req.NaturalQuery)

	// Parse asset type references
	if strings.Contains(query, "node") || strings.Contains(query, "server") {
		req.AssetTypes = append(req.AssetTypes, "Node")
	}
	if strings.Contains(query, "disk") || strings.Contains(query, "storage") {
		req.AssetTypes = append(req.AssetTypes, "Disk")
	}
	if strings.Contains(query, "load balancer") || strings.Contains(query, "lb") {
		req.AssetTypes = append(req.AssetTypes, "LoadBalancer")
	}

	// Parse utilization references
	if strings.Contains(query, "underutilized") || strings.Contains(query, "unused") {
		req.AnalysisType = "utilization"
		if req.Filter == nil {
			req.Filter = &SmartFilter{}
		}
		req.Filter.NaturalFilter = "underutilized"
	}

	return nil
}

func (req *CloudCostQueryRequest) ParseNaturalLanguage() error {
	if req.NaturalQuery == "" {
		return nil
	}

	query := strings.ToLower(req.NaturalQuery)

	// Parse provider references
	if strings.Contains(query, "aws") || strings.Contains(query, "amazon") {
		if req.Filter == nil {
			req.Filter = &CloudCostSmartFilter{}
		}
		req.Filter.Provider = append(req.Filter.Provider, "aws")
	}
	if strings.Contains(query, "gcp") || strings.Contains(query, "google") {
		if req.Filter == nil {
			req.Filter = &CloudCostSmartFilter{}
		}
		req.Filter.Provider = append(req.Filter.Provider, "gcp")
	}
	if strings.Contains(query, "azure") || strings.Contains(query, "microsoft") {
		if req.Filter == nil {
			req.Filter = &CloudCostSmartFilter{}
		}
		req.Filter.Provider = append(req.Filter.Provider, "azure")
	}

	// Parse service references
	if strings.Contains(query, "s3") || strings.Contains(query, "storage") {
		if req.Filter == nil {
			req.Filter = &CloudCostSmartFilter{}
		}
		req.Filter.Service = append(req.Filter.Service, "S3", "Storage")
	}
	if strings.Contains(query, "ec2") || strings.Contains(query, "compute") {
		if req.Filter == nil {
			req.Filter = &CloudCostSmartFilter{}
		}
		req.Filter.Service = append(req.Filter.Service, "EC2", "Compute")
	}

	return nil
}

// Methods for AI-specific features
func (ctx *OpenCostConversationContext) UpdateActivity() {
	ctx.LastActivity = time.Now()
}

func (ctx *OpenCostConversationContext) AddInsight(insight CostInsight) {
	ctx.CostInsights = append(ctx.CostInsights, insight)
	// Keep only recent insights to prevent context bloat
	if len(ctx.CostInsights) > 10 {
		ctx.CostInsights = ctx.CostInsights[1:]
	}
}

func (ctx *OpenCostConversationContext) SuggestOptimizations() []Recommendation {
	var recommendations []Recommendation

	// Analyze recent insights for optimization opportunities
	for _, insight := range ctx.CostInsights {
		if insight.Type == "anomaly" && insight.Severity == "high" {
			recommendations = append(recommendations, Recommendation{
				ID:          fmt.Sprintf("opt-%d", time.Now().Unix()),
				Type:        "investigation",
				Title:       "Investigate Cost Anomaly",
				Description: fmt.Sprintf("Cost anomaly detected: %s", insight.Description),
				Effort:      "medium",
				Impact:      "high",
				RiskLevel:   "low",
			})
		}
	}

	return recommendations
}

// ParseDuration converts human-readable duration strings to time.Duration
func ParseDuration(duration string) (time.Duration, error) {
	switch strings.ToLower(duration) {
	case "1h", "hour":
		return time.Hour, nil
	case "1d", "day":
		return 24 * time.Hour, nil
	case "7d", "week", "1w":
		return 7 * 24 * time.Hour, nil
	case "30d", "month", "1m":
		return 30 * 24 * time.Hour, nil
	case "90d", "quarter", "3m":
		return 90 * 24 * time.Hour, nil
	case "365d", "year", "1y":
		return 365 * 24 * time.Hour, nil
	default:
		return time.ParseDuration(duration)
	}
}

// ParseTimeWindow creates a TimeWindow from various inputs
func ParseTimeWindow(input string) (*TimeWindow, error) {
	now := time.Now()

	switch strings.ToLower(input) {
	case "today":
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return &TimeWindow{
			Start:    start,
			End:      now,
			Duration: "1d",
		}, nil
	case "yesterday":
		start := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, now.Location())
		end := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return &TimeWindow{
			Start:    start,
			End:      end,
			Duration: "1d",
		}, nil
	case "this week":
		// Start of week (Monday)
		weekday := int(now.Weekday())
		if weekday == 0 {
			weekday = 7 // Sunday = 7
		}
		start := now.AddDate(0, 0, -(weekday - 1))
		start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
		return &TimeWindow{
			Start:    start,
			End:      now,
			Duration: "1w",
		}, nil
	case "last week":
		weekday := int(now.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		end := now.AddDate(0, 0, -(weekday - 1))
		end = time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, end.Location())
		start := end.AddDate(0, 0, -7)
		return &TimeWindow{
			Start:    start,
			End:      end,
			Duration: "1w",
		}, nil
	case "this month":
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
		return &TimeWindow{
			Start:    start,
			End:      now,
			Duration: "1m",
		}, nil
	case "last month":
		start := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, now.Location())
		end := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
		return &TimeWindow{
			Start:    start,
			End:      end,
			Duration: "1m",
		}, nil
	default:
		// Try to parse as duration from now
		duration, err := ParseDuration(input)
		if err != nil {
			return nil, fmt.Errorf("invalid time window: %s", input)
		}
		start := now.Add(-duration)
		return &TimeWindow{
			Start:    start,
			End:      now,
			Duration: input,
		}, nil
	}
}
