package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/allocation"
	cloudcostfilter "github.com/opencost/opencost/core/pkg/filter/cloudcost"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	models "github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/costmodel"
)

// QueryType defines the type of query to be executed.
type QueryType string

const (
	AllocationQueryType      QueryType = "allocation"
	AssetQueryType           QueryType = "asset"
	CloudCostQueryType       QueryType = "cloudcost"
	EfficiencyQueryType      QueryType = "efficiency"
	RecommendationsQueryType QueryType = "recommendations"
)

// Efficiency calculation constants
const (
	efficiencyBufferMultiplier = 1.2         // 20% headroom for stability
	efficiencyMinCPU           = 0.001       // minimum CPU cores
	efficiencyMinRAM           = 1024 * 1024 // 1 MB minimum RAM
)

// Recommendation thresholds
const (
	// Idle detection thresholds
	idleCPUThreshold    = 0.05 // <5% CPU usage considered idle
	idleMemoryThreshold = 0.05 // <5% memory usage considered idle

	// Oversized detection thresholds
	oversizedCPUThreshold    = 0.3 // <30% CPU efficiency is oversized
	oversizedMemoryThreshold = 0.3 // <30% memory efficiency is oversized

	// Underprovisioned detection thresholds
	underprovisionedCPUThreshold    = 0.9 // >90% CPU efficiency may be underprovisioned
	underprovisionedMemoryThreshold = 0.9 // >90% memory efficiency may be underprovisioned

	// Minimum savings threshold for recommendations
	minSavingsThreshold = 0.01 // Minimum $0.01 savings to generate recommendation
)

// RecommendationType defines the type of cost recommendation
type RecommendationType string

const (
	RecommendationTypeIdle             RecommendationType = "idle"
	RecommendationTypeOversized        RecommendationType = "oversized"
	RecommendationTypeRightsize        RecommendationType = "rightsize"
	RecommendationTypeUnderprovisioned RecommendationType = "underprovisioned"
)

// RecommendationPriority defines the priority level of a recommendation
type RecommendationPriority string

const (
	RecommendationPriorityHigh   RecommendationPriority = "high"
	RecommendationPriorityMedium RecommendationPriority = "medium"
	RecommendationPriorityLow    RecommendationPriority = "low"
)

// MCPRequest represents a single turn in a conversation with the OpenCost MCP server.
type MCPRequest struct {
	SessionID string                `json:"sessionId"`
	Query     *OpenCostQueryRequest `json:"query"`
}

// MCPResponse is the response from the OpenCost MCP server for a single turn.
type MCPResponse struct {
	Data      interface{}   `json:"data"`
	QueryInfo QueryMetadata `json:"queryInfo"`
}

// QueryMetadata contains metadata about the query execution.
type QueryMetadata struct {
	QueryID        string        `json:"queryId"`
	Timestamp      time.Time     `json:"timestamp"`
	ProcessingTime time.Duration `json:"processingTime"`
}

// OpenCostQueryRequest provides a unified interface for all OpenCost query types.
type OpenCostQueryRequest struct {
	QueryType QueryType `json:"queryType" validate:"required,oneof=allocation asset cloudcost efficiency recommendations"`

	Window string `json:"window" validate:"required"`

	AllocationParams      *AllocationQuery      `json:"allocationParams,omitempty"`
	AssetParams           *AssetQuery           `json:"assetParams,omitempty"`
	CloudCostParams       *CloudCostQuery       `json:"cloudCostParams,omitempty"`
	EfficiencyParams      *EfficiencyQuery      `json:"efficiencyParams,omitempty"`
	RecommendationsParams *RecommendationsQuery `json:"recommendationsParams,omitempty"`
}

// AllocationQuery contains the parameters for an allocation query.
type AllocationQuery struct {
	Step                                  time.Duration `json:"step,omitempty"`
	Accumulate                            bool          `json:"accumulate,omitempty"`
	ShareIdle                             bool          `json:"shareIdle,omitempty"`
	Aggregate                             string        `json:"aggregate,omitempty"`
	IncludeIdle                           bool          `json:"includeIdle,omitempty"`
	IdleByNode                            bool          `json:"idleByNode,omitempty"`
	IncludeProportionalAssetResourceCosts bool          `json:"includeProportionalAssetResourceCosts,omitempty"`
	IncludeAggregatedMetadata             bool          `json:"includeAggregatedMetadata,omitempty"`
	ShareLB                               bool          `json:"sharelb,omitempty"`
	Filter                                string        `json:"filter,omitempty"` // Filter expression for allocations (e.g., "cluster:production", "namespace:kube-system")
}

// AssetQuery contains the parameters for an asset query.
type AssetQuery struct {
	// Currently no specific parameters needed for asset queries as it only takes window as parameter
}

// CloudCostQuery contains the parameters for a cloud cost query.
type CloudCostQuery struct {
	Aggregate  string `json:"aggregate,omitempty"`  // Comma-separated list of aggregation properties
	Accumulate string `json:"accumulate,omitempty"` // e.g., "week", "day", "month"
	Filter     string `json:"filter,omitempty"`     // Filter expression for cloud costs
	Provider   string `json:"provider,omitempty"`   // Cloud provider filter (aws, gcp, azure, etc.)
	Service    string `json:"service,omitempty"`    // Service filter (ec2, s3, compute, etc.)
	Category   string `json:"category,omitempty"`   // Category filter (compute, storage, network, etc.)
	Region     string `json:"region,omitempty"`     // Region filter
	// Additional explicit fields for filtering
	AccountID       string            `json:"accountID,omitempty"`       // Alias of Account; maps to accountID
	InvoiceEntityID string            `json:"invoiceEntityID,omitempty"` // Invoice entity ID filter
	ProviderID      string            `json:"providerID,omitempty"`      // Cloud provider resource ID filter
	Labels          map[string]string `json:"labels,omitempty"`          // Label filters (key->value)
}

// EfficiencyQuery contains the parameters for an efficiency query.
type EfficiencyQuery struct {
	Aggregate                  string   `json:"aggregate,omitempty"`                  // Aggregation properties (e.g., "pod", "namespace", "controller")
	Filter                     string   `json:"filter,omitempty"`                     // Filter expression for allocations (same as AllocationQuery)
	EfficiencyBufferMultiplier *float64 `json:"efficiencyBufferMultiplier,omitempty"` // Buffer multiplier for recommendations (default: 1.2 for 20% headroom)
}

// RecommendationsQuery contains the parameters for a cost recommendations query.
type RecommendationsQuery struct {
	Aggregate        string   `json:"aggregate,omitempty"`        // Aggregation level (e.g., "pod", "namespace", "controller")
	Filter           string   `json:"filter,omitempty"`           // Filter expression for allocations
	BufferMultiplier *float64 `json:"bufferMultiplier,omitempty"` // Buffer multiplier for sizing recommendations (default: 1.2)
	MinSavings       *float64 `json:"minSavings,omitempty"`       // Minimum savings threshold to include recommendation (default: 0.01)
	IncludeIdle      bool     `json:"includeIdle,omitempty"`      // Include idle resource detection (default: true)
	IncludeOversized bool     `json:"includeOversized,omitempty"` // Include oversized resource detection (default: true)
	IncludeRightsize bool     `json:"includeRightsize,omitempty"` // Include rightsizing recommendations (default: true)
	TopN             *int     `json:"topN,omitempty"`             // Limit to top N recommendations by savings (default: no limit)
}

// AllocationResponse represents the allocation data returned to the AI agent.
type AllocationResponse struct {
	// The allocation data, as a map of allocation sets.
	Allocations map[string]*AllocationSet `json:"allocations"`
}

// AllocationSet represents a set of allocation data.
type AllocationSet struct {
	// The name of the allocation set.
	Name        string            `json:"name"`
	Properties  map[string]string `json:"properties"`
	Allocations []*Allocation     `json:"allocations"`
}

// TotalCost calculates the total cost of all allocations in the set.
func (as *AllocationSet) TotalCost() float64 {
	var total float64
	for _, alloc := range as.Allocations {
		total += alloc.TotalCost
	}
	return total
}

// Allocation represents a single allocation data point.

type Allocation struct {
	Name string `json:"name"` // Allocation key (namespace, cluster, etc.)

	CPUCost      float64 `json:"cpuCost"`      // Cost of CPU usage
	GPUCost      float64 `json:"gpuCost"`      // Cost of GPU usage
	RAMCost      float64 `json:"ramCost"`      // Cost of memory usage
	PVCost       float64 `json:"pvCost"`       // Cost of persistent volumes
	NetworkCost  float64 `json:"networkCost"`  // Cost of network usage
	SharedCost   float64 `json:"sharedCost"`   // Shared/unallocated costs assigned here
	ExternalCost float64 `json:"externalCost"` // External costs (cloud services, etc.)
	TotalCost    float64 `json:"totalCost"`    // Sum of all costs above

	CPUCoreHours float64 `json:"cpuCoreHours"` // Usage metrics: CPU core-hours
	RAMByteHours float64 `json:"ramByteHours"` // Usage metrics: RAM byte-hours
	GPUHours     float64 `json:"gpuHours"`     // Usage metrics: GPU-hours
	PVByteHours  float64 `json:"pvByteHours"`  // Usage metrics: PV byte-hours

	Start time.Time `json:"start"` // Start timestamp for this allocation
	End   time.Time `json:"end"`   // End timestamp for this allocation
}

// AssetResponse represents the asset data returned to the AI agent.
type AssetResponse struct {
	// The asset data, as a map of asset sets.
	Assets map[string]*AssetSet `json:"assets"`
}

// AssetSet represents a set of asset data.
type AssetSet struct {
	// The name of the asset set.
	Name string `json:"name"`

	// The asset data for the set.
	Assets []*Asset `json:"assets"`
}

// Asset represents a single asset data point.
type Asset struct {
	Type       string            `json:"type"`
	Properties AssetProperties   `json:"properties"`
	Labels     map[string]string `json:"labels,omitempty"`

	Start time.Time `json:"start"`
	End   time.Time `json:"end"`

	Minutes    float64 `json:"minutes"`
	Adjustment float64 `json:"adjustment"`
	TotalCost  float64 `json:"totalCost"`

	// Disk-specific fields
	ByteHours      float64  `json:"byteHours,omitempty"`
	ByteHoursUsed  *float64 `json:"byteHoursUsed,omitempty"`
	ByteUsageMax   *float64 `json:"byteUsageMax,omitempty"`
	StorageClass   string   `json:"storageClass,omitempty"`
	VolumeName     string   `json:"volumeName,omitempty"`
	ClaimName      string   `json:"claimName,omitempty"`
	ClaimNamespace string   `json:"claimNamespace,omitempty"`
	Local          float64  `json:"local,omitempty"`

	// Node-specific fields
	NodeType     string  `json:"nodeType,omitempty"`
	CPUCoreHours float64 `json:"cpuCoreHours,omitempty"`
	RAMByteHours float64 `json:"ramByteHours,omitempty"`
	GPUHours     float64 `json:"gpuHours,omitempty"`
	GPUCount     float64 `json:"gpuCount,omitempty"`
	CPUCost      float64 `json:"cpuCost,omitempty"`
	GPUCost      float64 `json:"gpuCost,omitempty"`
	RAMCost      float64 `json:"ramCost,omitempty"`
	Discount     float64 `json:"discount,omitempty"`
	Preemptible  float64 `json:"preemptible,omitempty"`

	// Breakdown fields (can be used for different types)
	Breakdown    *AssetBreakdown `json:"breakdown,omitempty"`
	CPUBreakdown *AssetBreakdown `json:"cpuBreakdown,omitempty"`
	RAMBreakdown *AssetBreakdown `json:"ramBreakdown,omitempty"`

	// Overhead (Node-specific)
	Overhead *NodeOverhead `json:"overhead,omitempty"`

	// LoadBalancer-specific fields
	Private bool   `json:"private,omitempty"`
	Ip      string `json:"ip,omitempty"`

	// Cloud-specific fields
	Credit float64 `json:"credit,omitempty"`
}

// NodeOverhead represents node overhead information
type NodeOverhead struct {
	RamOverheadFraction  float64 `json:"ramOverheadFraction"`
	CpuOverheadFraction  float64 `json:"cpuOverheadFraction"`
	OverheadCostFraction float64 `json:"overheadCostFraction"`
}
type AssetProperties struct {
	Category   string `json:"category,omitempty"`
	Provider   string `json:"provider,omitempty"`
	Account    string `json:"account,omitempty"`
	Project    string `json:"project,omitempty"`
	Service    string `json:"service,omitempty"`
	Cluster    string `json:"cluster,omitempty"`
	Name       string `json:"name,omitempty"`
	ProviderID string `json:"providerID,omitempty"`
}

type AssetBreakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

// CloudCostResponse represents the cloud cost data returned to the AI agent.
type CloudCostResponse struct {
	// The cloud cost data, as a map of cloud cost sets.
	CloudCosts map[string]*CloudCostSet `json:"cloudCosts"`
	// Summary information
	Summary *CloudCostSummary `json:"summary,omitempty"`
}

// CloudCostSummary provides summary information about cloud costs
type CloudCostSummary struct {
	TotalNetCost       float64            `json:"totalNetCost"`
	TotalAmortizedCost float64            `json:"totalAmortizedCost"`
	TotalInvoicedCost  float64            `json:"totalInvoicedCost"`
	KubernetesPercent  float64            `json:"kubernetesPercent"`
	ProviderBreakdown  map[string]float64 `json:"providerBreakdown,omitempty"`
	ServiceBreakdown   map[string]float64 `json:"serviceBreakdown,omitempty"`
	RegionBreakdown    map[string]float64 `json:"regionBreakdown,omitempty"`
}

// CloudCostSet represents a set of cloud cost data.
type CloudCostSet struct {
	// The name of the cloud cost set.
	Name string `json:"name"`

	// The cloud cost data for the set.
	CloudCosts []*CloudCost `json:"cloudCosts"`

	// Aggregation information
	AggregationProperties []string `json:"aggregationProperties,omitempty"`

	// Time window
	Window *TimeWindow `json:"window,omitempty"`
}

// TimeWindow represents a time range
type TimeWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// CloudCostProperties defines the properties of a cloud cost item.
type CloudCostProperties struct {
	ProviderID        string            `json:"providerID,omitempty"`
	Provider          string            `json:"provider,omitempty"`
	AccountID         string            `json:"accountID,omitempty"`
	AccountName       string            `json:"accountName,omitempty"`
	InvoiceEntityID   string            `json:"invoiceEntityID,omitempty"`
	InvoiceEntityName string            `json:"invoiceEntityName,omitempty"`
	RegionID          string            `json:"regionID,omitempty"`
	AvailabilityZone  string            `json:"availabilityZone,omitempty"`
	Service           string            `json:"service,omitempty"`
	Category          string            `json:"category,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
}

// CloudCost represents a single cloud cost data point.
type CloudCost struct {
	Properties       CloudCostProperties `json:"properties"`
	Window           TimeWindow          `json:"window"`
	ListCost         CostMetric          `json:"listCost"`
	NetCost          CostMetric          `json:"netCost"`
	AmortizedNetCost CostMetric          `json:"amortizedNetCost"`
	InvoicedCost     CostMetric          `json:"invoicedCost"`
	AmortizedCost    CostMetric          `json:"amortizedCost"`
}

// CostMetric represents a cost value with Kubernetes percentage
type CostMetric struct {
	Cost              float64 `json:"cost"`
	KubernetesPercent float64 `json:"kubernetesPercent"`
}

// EfficiencyResponse represents the efficiency data returned to the AI agent.
type EfficiencyResponse struct {
	Efficiencies []*EfficiencyMetric `json:"efficiencies"`
}

// EfficiencyMetric represents efficiency data for a single pod/workload.
type EfficiencyMetric struct {
	Name string `json:"name"` // Pod/namespace/controller name based on aggregation

	// Current state
	CPUEfficiency    float64 `json:"cpuEfficiency"`    // Usage / Request ratio (0-1+)
	MemoryEfficiency float64 `json:"memoryEfficiency"` // Usage / Request ratio (0-1+)

	// Current requests and usage
	CPUCoresRequested float64 `json:"cpuCoresRequested"`
	CPUCoresUsed      float64 `json:"cpuCoresUsed"`
	RAMBytesRequested float64 `json:"ramBytesRequested"`
	RAMBytesUsed      float64 `json:"ramBytesUsed"`

	// Recommendations (based on actual usage with buffer)
	RecommendedCPURequest float64 `json:"recommendedCpuRequest"` // Recommended CPU cores
	RecommendedRAMRequest float64 `json:"recommendedRamRequest"` // Recommended RAM bytes

	// Resulting efficiency after applying recommendations
	ResultingCPUEfficiency    float64 `json:"resultingCpuEfficiency"`
	ResultingMemoryEfficiency float64 `json:"resultingMemoryEfficiency"`

	// Cost analysis
	CurrentTotalCost   float64 `json:"currentTotalCost"`   // Current total cost
	RecommendedCost    float64 `json:"recommendedCost"`    // Estimated cost with recommendations
	CostSavings        float64 `json:"costSavings"`        // Potential savings
	CostSavingsPercent float64 `json:"costSavingsPercent"` // Savings as percentage

	// Buffer multiplier used for recommendations
	EfficiencyBufferMultiplier float64 `json:"efficiencyBufferMultiplier"` // Buffer multiplier applied (e.g., 1.2 for 20% headroom)

	// Time window
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// RecommendationsResponse represents cost optimization recommendations returned to the AI agent.
type RecommendationsResponse struct {
	Recommendations []*Recommendation        `json:"recommendations"` // List of recommendations sorted by savings
	Summary         *RecommendationsSummary  `json:"summary"`         // Summary of all recommendations
	Window          *TimeWindow              `json:"window"`          // Time window analyzed
}

// RecommendationsSummary provides summary statistics for recommendations.
type RecommendationsSummary struct {
	TotalRecommendations int     `json:"totalRecommendations"` // Total number of recommendations
	TotalPotentialSavings float64 `json:"totalPotentialSavings"` // Sum of all potential savings
	ByType               map[string]int     `json:"byType"`               // Count by recommendation type
	ByPriority           map[string]int     `json:"byPriority"`           // Count by priority level
	IdleResourceCount    int     `json:"idleResourceCount"`    // Number of idle resources identified
	OversizedCount       int     `json:"oversizedCount"`       // Number of oversized resources
	RightsizeCount       int     `json:"rightsizeCount"`       // Number of rightsizing opportunities
}

// Recommendation represents a single cost optimization recommendation.
type Recommendation struct {
	ID          string                 `json:"id"`          // Unique identifier for the recommendation
	Type        RecommendationType     `json:"type"`        // Type of recommendation (idle, oversized, rightsize)
	Priority    RecommendationPriority `json:"priority"`    // Priority level (high, medium, low)
	ResourceName string                `json:"resourceName"` // Name of the resource (pod, namespace, etc.)
	Description string                 `json:"description"` // Human-readable description of the recommendation
	Action      string                 `json:"action"`      // Recommended action to take

	// Current state
	CurrentCPURequest float64 `json:"currentCpuRequest"` // Current CPU request in cores
	CurrentRAMRequest float64 `json:"currentRamRequest"` // Current RAM request in bytes
	CurrentCPUUsage   float64 `json:"currentCpuUsage"`   // Current CPU usage in cores
	CurrentRAMUsage   float64 `json:"currentRamUsage"`   // Current RAM usage in bytes
	CurrentCost       float64 `json:"currentCost"`       // Current cost

	// Efficiency metrics
	CPUEfficiency    float64 `json:"cpuEfficiency"`    // Current CPU efficiency (0-1+)
	MemoryEfficiency float64 `json:"memoryEfficiency"` // Current memory efficiency (0-1+)

	// Recommendation details
	RecommendedCPURequest float64 `json:"recommendedCpuRequest"` // Recommended CPU request
	RecommendedRAMRequest float64 `json:"recommendedRamRequest"` // Recommended RAM request
	RecommendedCost       float64 `json:"recommendedCost"`       // Estimated cost after optimization

	// Savings analysis
	EstimatedSavings       float64 `json:"estimatedSavings"`       // Estimated cost savings
	EstimatedSavingsPercent float64 `json:"estimatedSavingsPercent"` // Savings as percentage

	// Time window
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// MCPServer holds the dependencies for the MCP API server.
type MCPServer struct {
	costModel    *costmodel.CostModel
	provider     models.Provider
	cloudQuerier cloudcost.Querier
}

// NewMCPServer creates a new MCP Server.
func NewMCPServer(costModel *costmodel.CostModel, provider models.Provider, cloudQuerier cloudcost.Querier) *MCPServer {
	return &MCPServer{
		costModel:    costModel,
		provider:     provider,
		cloudQuerier: cloudQuerier,
	}
}

// ProcessMCPRequest processes an MCP request and returns an MCP response.

func (s *MCPServer) ProcessMCPRequest(request *MCPRequest) (*MCPResponse, error) {
	// 1. Validate Request
	if err := validate.Struct(request); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// 2. Query Dispatching
	var data interface{}
	var err error

	queryStart := time.Now()

	switch request.Query.QueryType {
	case AllocationQueryType:
		data, err = s.QueryAllocations(request.Query)
	case AssetQueryType:
		data, err = s.QueryAssets(request.Query)
	case CloudCostQueryType:
		data, err = s.QueryCloudCosts(request.Query)
	case EfficiencyQueryType:
		data, err = s.QueryEfficiency(request.Query)
	case RecommendationsQueryType:
		data, err = s.QueryRecommendations(request.Query)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", request.Query.QueryType)
	}

	if err != nil {
		// Handle error appropriately, maybe return a JSON-RPC error response
		return nil, err
	}

	processingTime := time.Since(queryStart)

	// 3. Construct Final Response
	mcpResponse := &MCPResponse{
		Data: data,
		QueryInfo: QueryMetadata{
			QueryID:        generateQueryID(),
			Timestamp:      time.Now(),
			ProcessingTime: processingTime,
		},
	}
	return mcpResponse, nil
}

// validate is the singleton validator instance.
var validate = validator.New()

func generateQueryID() string {
	bytes := make([]byte, 8) // 16 hex characters
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID if crypto/rand fails
		return fmt.Sprintf("query-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("query-%s", hex.EncodeToString(bytes))
}

func (s *MCPServer) QueryAllocations(query *OpenCostQueryRequest) (*AllocationResponse, error) {
	// 1. Parse Window
	window, err := opencost.ParseWindowWithOffset(query.Window, 0) // 0 offset for UTC
	if err != nil {
		return nil, fmt.Errorf("failed to parse window '%s': %w", query.Window, err)
	}

	// 2. Set default parameters
	var step time.Duration
	var aggregateBy []string
	var includeIdle, idleByNode, includeProportionalAssetResourceCosts, includeAggregatedMetadata, sharedLoadBalancer, shareIdle bool
	var accumulateBy opencost.AccumulateOption
	var filterString string

	// 3. Parse allocation parameters if provided
	if query.AllocationParams != nil {
		// Set step duration (default to window duration if not specified)
		if query.AllocationParams.Step > 0 {
			step = query.AllocationParams.Step
		} else {
			step = window.Duration()
		}

		// Parse aggregation properties
		if query.AllocationParams.Aggregate != "" {
			aggregateBy = strings.Split(query.AllocationParams.Aggregate, ",")
		}

		// Set boolean parameters
		includeIdle = query.AllocationParams.IncludeIdle
		idleByNode = query.AllocationParams.IdleByNode
		includeProportionalAssetResourceCosts = query.AllocationParams.IncludeProportionalAssetResourceCosts
		includeAggregatedMetadata = query.AllocationParams.IncludeAggregatedMetadata
		sharedLoadBalancer = query.AllocationParams.ShareLB
		shareIdle = query.AllocationParams.ShareIdle

		// Set filter string
		filterString = query.AllocationParams.Filter

		// Validate filter string if provided
		if filterString != "" {
			parser := allocation.NewAllocationFilterParser()
			_, err := parser.Parse(filterString)
			if err != nil {
				return nil, fmt.Errorf("invalid allocation filter '%s': %w", filterString, err)
			}
		}

		// Set accumulation option
		if query.AllocationParams.Accumulate {
			accumulateBy = opencost.AccumulateOptionAll
		} else {
			accumulateBy = opencost.AccumulateOptionNone
		}
	} else {
		// Default values when no parameters provided
		step = window.Duration()
		accumulateBy = opencost.AccumulateOptionNone
		filterString = ""
	}

	// 4. Call the existing QueryAllocation function with all parameters
	asr, err := s.costModel.QueryAllocation(
		window,
		step,
		aggregateBy,
		includeIdle,
		idleByNode,
		includeProportionalAssetResourceCosts,
		includeAggregatedMetadata,
		sharedLoadBalancer,
		accumulateBy,
		shareIdle,
		filterString,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query allocations: %w", err)
	}

	// 5. Handle the AllocationSetRange result
	if asr == nil || len(asr.Allocations) == 0 {
		return &AllocationResponse{
			Allocations: make(map[string]*AllocationSet),
		}, nil
	}

	// 6. Transform the result to MCP format
	// If we have multiple sets, we'll combine them or return the first one
	// For now, let's return the first allocation set
	firstSet := asr.Allocations[0]
	return transformAllocationSet(firstSet), nil
}

// transformAllocationSet converts an opencost.AllocationSet into the MCP's AllocationResponse format.
func transformAllocationSet(allocSet *opencost.AllocationSet) *AllocationResponse {
	if allocSet == nil {
		return &AllocationResponse{Allocations: make(map[string]*AllocationSet)}
	}

	mcpAllocations := make(map[string]*AllocationSet)

	// Create a single set for all allocations
	mcpSet := &AllocationSet{
		Name:        "allocations",
		Allocations: []*Allocation{},
	}

	// Convert each allocation
	for _, alloc := range allocSet.Allocations {
		if alloc == nil {
			continue
		}

		mcpAlloc := &Allocation{
			Name:         alloc.Name,
			CPUCost:      alloc.CPUCost,
			GPUCost:      alloc.GPUCost,
			RAMCost:      alloc.RAMCost,
			PVCost:       alloc.PVCost(), // Call the method
			NetworkCost:  alloc.NetworkCost,
			SharedCost:   alloc.SharedCost,
			ExternalCost: alloc.ExternalCost,
			TotalCost:    alloc.TotalCost(),
			CPUCoreHours: alloc.CPUCoreHours,
			RAMByteHours: alloc.RAMByteHours,
			GPUHours:     alloc.GPUHours,
			PVByteHours:  alloc.PVBytes(), // Use the method directly
			Start:        alloc.Start,
			End:          alloc.End,
		}
		mcpSet.Allocations = append(mcpSet.Allocations, mcpAlloc)
	}

	mcpAllocations["allocations"] = mcpSet

	return &AllocationResponse{
		Allocations: mcpAllocations,
	}
}

func (s *MCPServer) QueryAssets(query *OpenCostQueryRequest) (*AssetResponse, error) {
	// 1. Parse Window
	window, err := opencost.ParseWindowWithOffset(query.Window, 0) // 0 offset for UTC
	if err != nil {
		return nil, fmt.Errorf("failed to parse window '%s': %w", query.Window, err)
	}

	// 2. Set Query Options
	start := *window.Start()
	end := *window.End()

	// 3. Call CostModel to get the asset set
	assetSet, err := s.costModel.ComputeAssets(start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to compute assets: %w", err)
	}

	// 4. Transform Response for the MCP API
	return transformAssetSet(assetSet), nil
}

// transformAssetSet converts a opencost.AssetSet into the MCP's AssetResponse format.
func transformAssetSet(assetSet *opencost.AssetSet) *AssetResponse {
	if assetSet == nil {
		return &AssetResponse{Assets: make(map[string]*AssetSet)}
	}

	mcpAssets := make(map[string]*AssetSet)

	// Create a single set for all assets
	mcpSet := &AssetSet{
		Name:   "assets",
		Assets: []*Asset{},
	}

	for _, asset := range assetSet.Assets {
		if asset == nil {
			continue
		}

		properties := asset.GetProperties()
		labels := asset.GetLabels()

		mcpAsset := &Asset{
			Type: asset.Type().String(),
			Properties: AssetProperties{
				Category:   properties.Category,
				Provider:   properties.Provider,
				Account:    properties.Account,
				Project:    properties.Project,
				Service:    properties.Service,
				Cluster:    properties.Cluster,
				Name:       properties.Name,
				ProviderID: properties.ProviderID,
			},
			Labels:     labels,
			Start:      asset.GetStart(),
			End:        asset.GetEnd(),
			Minutes:    asset.Minutes(),
			Adjustment: asset.GetAdjustment(),
			TotalCost:  asset.TotalCost(),
		}

		// Handle type-specific fields
		switch a := asset.(type) {
		case *opencost.Disk:
			mcpAsset.ByteHours = a.ByteHours
			mcpAsset.ByteHoursUsed = a.ByteHoursUsed
			mcpAsset.ByteUsageMax = a.ByteUsageMax
			mcpAsset.StorageClass = a.StorageClass
			mcpAsset.VolumeName = a.VolumeName
			mcpAsset.ClaimName = a.ClaimName
			mcpAsset.ClaimNamespace = a.ClaimNamespace
			mcpAsset.Local = a.Local
			if a.Breakdown != nil {
				mcpAsset.Breakdown = &AssetBreakdown{
					Idle:   a.Breakdown.Idle,
					Other:  a.Breakdown.Other,
					System: a.Breakdown.System,
					User:   a.Breakdown.User,
				}
			}
		case *opencost.Node:
			mcpAsset.NodeType = a.NodeType
			mcpAsset.CPUCoreHours = a.CPUCoreHours
			mcpAsset.RAMByteHours = a.RAMByteHours
			mcpAsset.GPUHours = a.GPUHours
			mcpAsset.GPUCount = a.GPUCount
			mcpAsset.CPUCost = a.CPUCost
			mcpAsset.GPUCost = a.GPUCost
			mcpAsset.RAMCost = a.RAMCost
			mcpAsset.Discount = a.Discount
			mcpAsset.Preemptible = a.Preemptible
			if a.CPUBreakdown != nil {
				mcpAsset.CPUBreakdown = &AssetBreakdown{
					Idle:   a.CPUBreakdown.Idle,
					Other:  a.CPUBreakdown.Other,
					System: a.CPUBreakdown.System,
					User:   a.CPUBreakdown.User,
				}
			}
			if a.RAMBreakdown != nil {
				mcpAsset.RAMBreakdown = &AssetBreakdown{
					Idle:   a.RAMBreakdown.Idle,
					Other:  a.RAMBreakdown.Other,
					System: a.RAMBreakdown.System,
					User:   a.RAMBreakdown.User,
				}
			}
			if a.Overhead != nil {
				mcpAsset.Overhead = &NodeOverhead{
					RamOverheadFraction:  a.Overhead.RamOverheadFraction,
					CpuOverheadFraction:  a.Overhead.CpuOverheadFraction,
					OverheadCostFraction: a.Overhead.OverheadCostFraction,
				}
			}
		case *opencost.LoadBalancer:
			mcpAsset.Private = a.Private
			mcpAsset.Ip = a.Ip
		case *opencost.Network:
			// Network assets have no specific fields beyond the base asset structure
			// All relevant data is in Properties, Labels, Cost, etc.
		case *opencost.Cloud:
			mcpAsset.Credit = a.Credit
		case *opencost.ClusterManagement:
			// ClusterManagement assets have no specific fields beyond the base asset structure
			// All relevant data is in Properties, Labels, Cost, etc.
		}

		mcpSet.Assets = append(mcpSet.Assets, mcpAsset)
	}

	mcpAssets["assets"] = mcpSet

	return &AssetResponse{
		Assets: mcpAssets,
	}
}

// QueryCloudCosts translates an MCP query into a CloudCost repository query and transforms the result.
func (s *MCPServer) QueryCloudCosts(query *OpenCostQueryRequest) (*CloudCostResponse, error) {
	// 1. Check if cloud cost querier is available
	if s.cloudQuerier == nil {
		return nil, fmt.Errorf("cloud cost querier not configured - check cloud-integration.json file")
	}

	// 2. Parse Window
	window, err := opencost.ParseWindowWithOffset(query.Window, 0) // 0 offset for UTC
	if err != nil {
		return nil, fmt.Errorf("failed to parse window '%s': %w", query.Window, err)
	}

	// 3. Build query request
	request := cloudcost.QueryRequest{
		Start:  *window.Start(),
		End:    *window.End(),
		Filter: nil, // Will be set from CloudCostParams if provided
	}

	// 4. Apply filtering and aggregation from CloudCostParams
	if query.CloudCostParams != nil {
		request = s.buildCloudCostQueryRequest(request, query.CloudCostParams)
	}

	// 5. Query the repository (this handles multiple cloud providers automatically)
	ccsr, err := s.cloudQuerier.Query(context.TODO(), request)
	if err != nil {
		return nil, fmt.Errorf("failed to query cloud costs: %w", err)
	}

	// 6. Transform Response
	return transformCloudCostSetRange(ccsr), nil
}

// buildCloudCostQueryRequest builds a QueryRequest from CloudCostParams
func (s *MCPServer) buildCloudCostQueryRequest(request cloudcost.QueryRequest, params *CloudCostQuery) cloudcost.QueryRequest {
	// Set aggregation
	if params.Aggregate != "" {
		aggregateBy := strings.Split(params.Aggregate, ",")
		request.AggregateBy = aggregateBy
	}

	// Set accumulation
	if params.Accumulate != "" {
		request.Accumulate = opencost.ParseAccumulate(params.Accumulate)
	}

	// Build filter from individual parameters or filter string
	var filter filter.Filter
	var err error

	if params.Filter != "" {
		// Parse the filter string directly
		parser := cloudcostfilter.NewCloudCostFilterParser()
		filter, err = parser.Parse(params.Filter)
		if err != nil {
			// Log error but continue without filter rather than failing the entire request
			log.Warnf("failed to parse filter string '%s': %v", params.Filter, err)
		}
	} else {
		// Build filter from individual parameters
		filter = s.buildFilterFromParams(params)
	}

	request.Filter = filter
	return request
}

// buildFilterFromParams creates a filter from individual CloudCostQuery parameters
func (s *MCPServer) buildFilterFromParams(params *CloudCostQuery) filter.Filter {
	var filterParts []string

	// Add provider filter
	if params.Provider != "" {
		filterParts = append(filterParts, fmt.Sprintf(`provider:"%s"`, params.Provider))
	}

	// Add providerID filter
	if params.ProviderID != "" {
		filterParts = append(filterParts, fmt.Sprintf(`providerID:"%s"`, params.ProviderID))
	}

	// Add service filter
	if params.Service != "" {
		filterParts = append(filterParts, fmt.Sprintf(`service:"%s"`, params.Service))
	}

	// Add category filter
	if params.Category != "" {
		filterParts = append(filterParts, fmt.Sprintf(`category:"%s"`, params.Category))
	}

	// Region is intentionally not supported here

	// Add account filter (maps to accountID)
	if params.AccountID != "" {
		filterParts = append(filterParts, fmt.Sprintf(`accountID:"%s"`, params.AccountID))
	}

	// Add invoiceEntityID filter
	if params.InvoiceEntityID != "" {
		filterParts = append(filterParts, fmt.Sprintf(`invoiceEntityID:"%s"`, params.InvoiceEntityID))
	}

	// Add label filters (label[key]:"value")
	if len(params.Labels) > 0 {
		for k, v := range params.Labels {
			if k == "" {
				continue
			}
			filterParts = append(filterParts, fmt.Sprintf(`label[%s]:"%s"`, k, v))
		}
	}

	// If no filters specified, return nil
	if len(filterParts) == 0 {
		return nil
	}

	// Combine all filter parts with AND logic (parser expects 'and')
	filterString := strings.Join(filterParts, " and ")

	// Parse the combined filter string
	parser := cloudcostfilter.NewCloudCostFilterParser()
	filter, err := parser.Parse(filterString)
	if err != nil {
		// Log error but return nil rather than failing
		log.Warnf("failed to parse combined filter '%s': %v", filterString, err)
		return nil
	}

	return filter
}

// transformCloudCostSetRange converts a opencost.CloudCostSetRange into the MCP's CloudCostResponse format.
func transformCloudCostSetRange(ccsr *opencost.CloudCostSetRange) *CloudCostResponse {
	if ccsr == nil || len(ccsr.CloudCostSets) == 0 {
		return &CloudCostResponse{
			CloudCosts: make(map[string]*CloudCostSet),
			Summary: &CloudCostSummary{
				TotalNetCost: 0,
			},
		}
	}

	mcpCloudCosts := make(map[string]*CloudCostSet)
	var totalNetCost, totalAmortizedCost, totalInvoicedCost float64
	providerBreakdown := make(map[string]float64)
	serviceBreakdown := make(map[string]float64)
	regionBreakdown := make(map[string]float64)

	// Process each cloud cost set in the range
	for i, ccSet := range ccsr.CloudCostSets {
		if ccSet == nil {
			continue
		}

		setName := fmt.Sprintf("cloudcosts_%d", i)
		mcpSet := &CloudCostSet{
			Name:                  setName,
			CloudCosts:            []*CloudCost{},
			AggregationProperties: ccSet.AggregationProperties,
			Window: &TimeWindow{
				Start: *ccSet.Window.Start(),
				End:   *ccSet.Window.End(),
			},
		}

		// Convert each cloud cost item
		for _, item := range ccSet.CloudCosts {
			if item == nil {
				continue
			}

			mcpCC := &CloudCost{
				Properties: CloudCostProperties{
					ProviderID:        item.Properties.ProviderID,
					Provider:          item.Properties.Provider,
					AccountID:         item.Properties.AccountID,
					AccountName:       item.Properties.AccountName,
					InvoiceEntityID:   item.Properties.InvoiceEntityID,
					InvoiceEntityName: item.Properties.InvoiceEntityName,
					RegionID:          item.Properties.RegionID,
					AvailabilityZone:  item.Properties.AvailabilityZone,
					Service:           item.Properties.Service,
					Category:          item.Properties.Category,
					Labels:            item.Properties.Labels,
				},
				Window: TimeWindow{
					Start: *item.Window.Start(),
					End:   *item.Window.End(),
				},
				ListCost: CostMetric{
					Cost:              item.ListCost.Cost,
					KubernetesPercent: item.ListCost.KubernetesPercent,
				},
				NetCost: CostMetric{
					Cost:              item.NetCost.Cost,
					KubernetesPercent: item.NetCost.KubernetesPercent,
				},
				AmortizedNetCost: CostMetric{
					Cost:              item.AmortizedNetCost.Cost,
					KubernetesPercent: item.AmortizedNetCost.KubernetesPercent,
				},
				InvoicedCost: CostMetric{
					Cost:              item.InvoicedCost.Cost,
					KubernetesPercent: item.InvoicedCost.KubernetesPercent,
				},
				AmortizedCost: CostMetric{
					Cost:              item.AmortizedCost.Cost,
					KubernetesPercent: item.AmortizedCost.KubernetesPercent,
				},
			}
			mcpSet.CloudCosts = append(mcpSet.CloudCosts, mcpCC)

			// Update summary totals
			totalNetCost += item.NetCost.Cost
			totalAmortizedCost += item.AmortizedNetCost.Cost
			totalInvoicedCost += item.InvoicedCost.Cost

			// Update breakdowns
			providerBreakdown[item.Properties.Provider] += item.NetCost.Cost
			serviceBreakdown[item.Properties.Service] += item.NetCost.Cost
			regionBreakdown[item.Properties.RegionID] += item.NetCost.Cost
		}

		mcpCloudCosts[setName] = mcpSet
	}

	// Calculate cost-weighted average Kubernetes percentage (by NetCost)
	var avgKubernetesPercent float64
	var numerator, denominator float64
	for _, ccSet := range ccsr.CloudCostSets {
		for _, item := range ccSet.CloudCosts {
			if item == nil {
				continue
			}
			cost := item.NetCost.Cost
			percent := item.NetCost.KubernetesPercent
			if cost <= 0 {
				continue
			}
			numerator += cost * percent
			denominator += cost
		}
	}
	if denominator > 0 {
		avgKubernetesPercent = numerator / denominator
	}

	summary := &CloudCostSummary{
		TotalNetCost:       totalNetCost,
		TotalAmortizedCost: totalAmortizedCost,
		TotalInvoicedCost:  totalInvoicedCost,
		KubernetesPercent:  avgKubernetesPercent,
		ProviderBreakdown:  providerBreakdown,
		ServiceBreakdown:   serviceBreakdown,
		RegionBreakdown:    regionBreakdown,
	}

	return &CloudCostResponse{
		CloudCosts: mcpCloudCosts,
		Summary:    summary,
	}
}

// QueryEfficiency queries allocation data and computes efficiency metrics with recommendations.
func (s *MCPServer) QueryEfficiency(query *OpenCostQueryRequest) (*EfficiencyResponse, error) {
	// 1. Parse Window
	window, err := opencost.ParseWindowWithOffset(query.Window, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to parse window '%s': %w", query.Window, err)
	}

	// 2. Set default parameters
	var aggregateBy []string
	var filterString string
	var bufferMultiplier float64 = efficiencyBufferMultiplier // Default to 1.2 (20% headroom)

	// 3. Parse efficiency parameters if provided
	if query.EfficiencyParams != nil {
		// Parse aggregation properties (default to pod if not specified)
		if query.EfficiencyParams.Aggregate != "" {
			aggregateBy = strings.Split(query.EfficiencyParams.Aggregate, ",")
		} else {
			aggregateBy = []string{"pod"}
		}

		// Set filter string
		filterString = query.EfficiencyParams.Filter

		// Validate filter string if provided
		if filterString != "" {
			parser := allocation.NewAllocationFilterParser()
			_, err := parser.Parse(filterString)
			if err != nil {
				return nil, fmt.Errorf("invalid allocation filter '%s': %w", filterString, err)
			}
		}

		// Set buffer multiplier if provided, otherwise use default
		if query.EfficiencyParams.EfficiencyBufferMultiplier != nil {
			bufferMultiplier = *query.EfficiencyParams.EfficiencyBufferMultiplier
		}
	} else {
		// Default to pod-level aggregation
		aggregateBy = []string{"pod"}
		filterString = ""
	}

	// 4. Query allocations with the specified parameters
	// Use the entire window as step to get aggregated data
	step := window.Duration()
	asr, err := s.costModel.QueryAllocation(
		window,
		step,
		aggregateBy,
		false, // includeIdle
		false, // idleByNode
		false, // includeProportionalAssetResourceCosts
		false, // includeAggregatedMetadata
		false, // sharedLoadBalancer
		opencost.AccumulateOptionNone,
		false, // shareIdle
		filterString,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query allocations: %w", err)
	}

	// 5. Handle empty results
	if asr == nil || len(asr.Allocations) == 0 {
		return &EfficiencyResponse{
			Efficiencies: []*EfficiencyMetric{},
		}, nil
	}

	// 6. Compute efficiency metrics from allocations using concurrent processing
	var (
		mu           sync.Mutex
		wg           sync.WaitGroup
		efficiencies = make([]*EfficiencyMetric, 0)
	)

	// Process each allocation set (typically one per time window) concurrently
	for _, allocSet := range asr.Allocations {
		if allocSet == nil {
			continue
		}

		// Process this allocation set in a goroutine
		wg.Add(1)
		go func(allocSet *opencost.AllocationSet) {
			defer wg.Done()

			// Compute metrics for all allocations in this set
			localMetrics := make([]*EfficiencyMetric, 0, len(allocSet.Allocations))
			for _, alloc := range allocSet.Allocations {
				if metric := computeEfficiencyMetric(alloc, bufferMultiplier); metric != nil {
					localMetrics = append(localMetrics, metric)
				}
			}

			// Append results to shared slice (thread-safe)
			if len(localMetrics) > 0 {
				mu.Lock()
				efficiencies = append(efficiencies, localMetrics...)
				mu.Unlock()
			}
		}(allocSet)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	return &EfficiencyResponse{
		Efficiencies: efficiencies,
	}, nil
}

// safeDiv performs division and returns 0 if denominator is 0.
func safeDiv(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

// computeEfficiencyMetric calculates efficiency metrics for a single allocation.
func computeEfficiencyMetric(alloc *opencost.Allocation, bufferMultiplier float64) *EfficiencyMetric {
	if alloc == nil {
		return nil
	}

	// Calculate time duration in hours
	hours := alloc.Minutes() / 60.0
	if hours <= 0 {
		return nil
	}

	// Get current usage (average over the period)
	cpuCoresUsed := alloc.CPUCoreHours / hours
	ramBytesUsed := alloc.RAMByteHours / hours

	// Get requested amounts
	cpuCoresRequested := alloc.CPUCoreRequestAverage
	ramBytesRequested := alloc.RAMBytesRequestAverage

	// Calculate current efficiency (will be 0 if no requests are set)
	cpuEfficiency := safeDiv(cpuCoresUsed, cpuCoresRequested)
	memoryEfficiency := safeDiv(ramBytesUsed, ramBytesRequested)

	// Calculate recommendations with buffer for headroom
	recommendedCPU := cpuCoresUsed * bufferMultiplier
	recommendedRAM := ramBytesUsed * bufferMultiplier

	// Ensure recommendations meet minimum thresholds
	if recommendedCPU < efficiencyMinCPU {
		recommendedCPU = efficiencyMinCPU
	}
	if recommendedRAM < efficiencyMinRAM {
		recommendedRAM = efficiencyMinRAM
	}

	// Calculate resulting efficiency after applying recommendations
	resultingCPUEff := safeDiv(cpuCoresUsed, recommendedCPU)
	resultingMemEff := safeDiv(ramBytesUsed, recommendedRAM)

	// Calculate cost per unit based on REQUESTED amounts (not used amounts)
	// This gives us the cost per core-hour or byte-hour that the cluster charges
	cpuCostPerCoreHour := safeDiv(alloc.CPUCost, cpuCoresRequested*hours)
	ramCostPerByteHour := safeDiv(alloc.RAMCost, ramBytesRequested*hours)

	// Current total cost
	currentTotalCost := alloc.TotalCost()

	// Estimate recommended cost based on recommended requests
	recommendedCPUCost := recommendedCPU * hours * cpuCostPerCoreHour
	recommendedRAMCost := recommendedRAM * hours * ramCostPerByteHour
	// Keep other costs the same (PV, network, shared, external, GPU)
	otherCosts := alloc.PVCost() + alloc.NetworkCost + alloc.SharedCost + alloc.ExternalCost + alloc.GPUCost
	recommendedTotalCost := recommendedCPUCost + recommendedRAMCost + otherCosts

	// Clamp recommended cost to avoid rounding issues making it higher than current
	if recommendedTotalCost > currentTotalCost && (recommendedTotalCost-currentTotalCost) < 0.0001 {
		recommendedTotalCost = currentTotalCost
	}

	// Calculate savings
	costSavings := currentTotalCost - recommendedTotalCost
	costSavingsPercent := safeDiv(costSavings, currentTotalCost) * 100

	return &EfficiencyMetric{
		Name:                       alloc.Name,
		CPUEfficiency:              cpuEfficiency,
		MemoryEfficiency:           memoryEfficiency,
		CPUCoresRequested:          cpuCoresRequested,
		CPUCoresUsed:               cpuCoresUsed,
		RAMBytesRequested:          ramBytesRequested,
		RAMBytesUsed:               ramBytesUsed,
		RecommendedCPURequest:      recommendedCPU,
		RecommendedRAMRequest:      recommendedRAM,
		ResultingCPUEfficiency:     resultingCPUEff,
		ResultingMemoryEfficiency:  resultingMemEff,
		CurrentTotalCost:           currentTotalCost,
		RecommendedCost:            recommendedTotalCost,
		CostSavings:                costSavings,
		CostSavingsPercent:         costSavingsPercent,
		EfficiencyBufferMultiplier: bufferMultiplier,
		Start:                      alloc.Start,
		End:                        alloc.End,
	}
}

// QueryRecommendations generates cost optimization recommendations based on allocation data.
func (s *MCPServer) QueryRecommendations(query *OpenCostQueryRequest) (*RecommendationsResponse, error) {
	// 1. Parse Window
	window, err := opencost.ParseWindowWithOffset(query.Window, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to parse window '%s': %w", query.Window, err)
	}

	// 2. Set default parameters
	var aggregateBy []string
	var filterString string
	var bufferMultiplier float64 = efficiencyBufferMultiplier
	var minSavings float64 = minSavingsThreshold
	includeIdle := true
	includeOversized := true
	includeRightsize := true
	var topN *int

	// 3. Parse recommendations parameters if provided
	if query.RecommendationsParams != nil {
		if query.RecommendationsParams.Aggregate != "" {
			aggregateBy = strings.Split(query.RecommendationsParams.Aggregate, ",")
		} else {
			aggregateBy = []string{"pod"}
		}

		filterString = query.RecommendationsParams.Filter
		if filterString != "" {
			parser := allocation.NewAllocationFilterParser()
			_, err := parser.Parse(filterString)
			if err != nil {
				return nil, fmt.Errorf("invalid allocation filter '%s': %w", filterString, err)
			}
		}

		if query.RecommendationsParams.BufferMultiplier != nil {
			bufferMultiplier = *query.RecommendationsParams.BufferMultiplier
		}
		if query.RecommendationsParams.MinSavings != nil {
			minSavings = *query.RecommendationsParams.MinSavings
		}
		// Use explicit booleans (default to true if not specified via empty params)
		includeIdle = query.RecommendationsParams.IncludeIdle || (!query.RecommendationsParams.IncludeIdle && !query.RecommendationsParams.IncludeOversized && !query.RecommendationsParams.IncludeRightsize)
		includeOversized = query.RecommendationsParams.IncludeOversized || (!query.RecommendationsParams.IncludeIdle && !query.RecommendationsParams.IncludeOversized && !query.RecommendationsParams.IncludeRightsize)
		includeRightsize = query.RecommendationsParams.IncludeRightsize || (!query.RecommendationsParams.IncludeIdle && !query.RecommendationsParams.IncludeOversized && !query.RecommendationsParams.IncludeRightsize)
		topN = query.RecommendationsParams.TopN
	} else {
		aggregateBy = []string{"pod"}
	}

	// 4. Query allocations
	step := window.Duration()
	asr, err := s.costModel.QueryAllocation(
		window,
		step,
		aggregateBy,
		false, // includeIdle
		false, // idleByNode
		false, // includeProportionalAssetResourceCosts
		false, // includeAggregatedMetadata
		false, // sharedLoadBalancer
		opencost.AccumulateOptionNone,
		false, // shareIdle
		filterString,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query allocations: %w", err)
	}

	// 5. Handle empty results
	if asr == nil || len(asr.Allocations) == 0 {
		return &RecommendationsResponse{
			Recommendations: []*Recommendation{},
			Summary:         createEmptyRecommendationsSummary(),
			Window: &TimeWindow{
				Start: *window.Start(),
				End:   *window.End(),
			},
		}, nil
	}

	// 6. Generate recommendations from allocations
	var recommendations []*Recommendation
	for _, allocSet := range asr.Allocations {
		if allocSet == nil {
			continue
		}
		for _, alloc := range allocSet.Allocations {
			recs := generateRecommendationsFromAllocation(alloc, bufferMultiplier, minSavings, includeIdle, includeOversized, includeRightsize)
			recommendations = append(recommendations, recs...)
		}
	}

	// 7. Sort recommendations by estimated savings (highest first)
	sortRecommendationsBySavings(recommendations)

	// 8. Apply topN limit if specified
	if topN != nil && *topN > 0 && len(recommendations) > *topN {
		recommendations = recommendations[:*topN]
	}

	// 9. Build summary
	summary := buildRecommendationsSummary(recommendations)

	return &RecommendationsResponse{
		Recommendations: recommendations,
		Summary:         summary,
		Window: &TimeWindow{
			Start: *window.Start(),
			End:   *window.End(),
		},
	}, nil
}

// generateRecommendationsFromAllocation creates recommendations for a single allocation.
func generateRecommendationsFromAllocation(alloc *opencost.Allocation, bufferMultiplier, minSavings float64, includeIdle, includeOversized, includeRightsize bool) []*Recommendation {
	if alloc == nil {
		return nil
	}

	hours := alloc.Minutes() / 60.0
	if hours <= 0 {
		return nil
	}

	var recommendations []*Recommendation

	// Calculate usage metrics
	cpuCoresUsed := alloc.CPUCoreHours / hours
	ramBytesUsed := alloc.RAMByteHours / hours
	cpuCoresRequested := alloc.CPUCoreRequestAverage
	ramBytesRequested := alloc.RAMBytesRequestAverage

	// Calculate efficiency
	cpuEfficiency := safeDiv(cpuCoresUsed, cpuCoresRequested)
	memoryEfficiency := safeDiv(ramBytesUsed, ramBytesRequested)

	// Calculate recommended values
	recommendedCPU := cpuCoresUsed * bufferMultiplier
	recommendedRAM := ramBytesUsed * bufferMultiplier
	if recommendedCPU < efficiencyMinCPU {
		recommendedCPU = efficiencyMinCPU
	}
	if recommendedRAM < efficiencyMinRAM {
		recommendedRAM = efficiencyMinRAM
	}

	// Calculate costs
	cpuCostPerCoreHour := safeDiv(alloc.CPUCost, cpuCoresRequested*hours)
	ramCostPerByteHour := safeDiv(alloc.RAMCost, ramBytesRequested*hours)
	currentTotalCost := alloc.TotalCost()

	recommendedCPUCost := recommendedCPU * hours * cpuCostPerCoreHour
	recommendedRAMCost := recommendedRAM * hours * ramCostPerByteHour
	otherCosts := alloc.PVCost() + alloc.NetworkCost + alloc.SharedCost + alloc.ExternalCost + alloc.GPUCost
	recommendedTotalCost := recommendedCPUCost + recommendedRAMCost + otherCosts

	if recommendedTotalCost > currentTotalCost && (recommendedTotalCost-currentTotalCost) < 0.0001 {
		recommendedTotalCost = currentTotalCost
	}

	savings := currentTotalCost - recommendedTotalCost
	savingsPercent := safeDiv(savings, currentTotalCost) * 100

	// Skip if savings are below threshold
	if savings < minSavings {
		return nil
	}

	// Check for idle resources
	if includeIdle && cpuEfficiency < idleCPUThreshold && memoryEfficiency < idleMemoryThreshold && cpuCoresRequested > 0 && ramBytesRequested > 0 {
		rec := &Recommendation{
			ID:                      generateRecommendationID(),
			Type:                    RecommendationTypeIdle,
			Priority:                RecommendationPriorityHigh,
			ResourceName:           alloc.Name,
			Description:            fmt.Sprintf("Resource '%s' appears to be idle with <%.0f%% CPU and <%.0f%% memory utilization", alloc.Name, idleCPUThreshold*100, idleMemoryThreshold*100),
			Action:                  "Consider removing or scaling down this resource. Verify it's not needed before deletion.",
			CurrentCPURequest:       cpuCoresRequested,
			CurrentRAMRequest:       ramBytesRequested,
			CurrentCPUUsage:         cpuCoresUsed,
			CurrentRAMUsage:         ramBytesUsed,
			CurrentCost:             currentTotalCost,
			CPUEfficiency:           cpuEfficiency,
			MemoryEfficiency:        memoryEfficiency,
			RecommendedCPURequest:   recommendedCPU,
			RecommendedRAMRequest:   recommendedRAM,
			RecommendedCost:         recommendedTotalCost,
			EstimatedSavings:        savings,
			EstimatedSavingsPercent: savingsPercent,
			Start:                   alloc.Start,
			End:                     alloc.End,
		}
		recommendations = append(recommendations, rec)
		return recommendations // Return early, idle is the most critical
	}

	// Check for oversized resources
	if includeOversized && (cpuEfficiency < oversizedCPUThreshold || memoryEfficiency < oversizedMemoryThreshold) && cpuCoresRequested > 0 && ramBytesRequested > 0 {
		priority := RecommendationPriorityMedium
		if cpuEfficiency < 0.1 || memoryEfficiency < 0.1 {
			priority = RecommendationPriorityHigh
		}

		rec := &Recommendation{
			ID:                      generateRecommendationID(),
			Type:                    RecommendationTypeOversized,
			Priority:                priority,
			ResourceName:           alloc.Name,
			Description:            fmt.Sprintf("Resource '%s' is significantly oversized with %.1f%% CPU and %.1f%% memory efficiency", alloc.Name, cpuEfficiency*100, memoryEfficiency*100),
			Action:                  fmt.Sprintf("Reduce CPU request from %.3f to %.3f cores and RAM request from %.0f to %.0f bytes", cpuCoresRequested, recommendedCPU, ramBytesRequested, recommendedRAM),
			CurrentCPURequest:       cpuCoresRequested,
			CurrentRAMRequest:       ramBytesRequested,
			CurrentCPUUsage:         cpuCoresUsed,
			CurrentRAMUsage:         ramBytesUsed,
			CurrentCost:             currentTotalCost,
			CPUEfficiency:           cpuEfficiency,
			MemoryEfficiency:        memoryEfficiency,
			RecommendedCPURequest:   recommendedCPU,
			RecommendedRAMRequest:   recommendedRAM,
			RecommendedCost:         recommendedTotalCost,
			EstimatedSavings:        savings,
			EstimatedSavingsPercent: savingsPercent,
			Start:                   alloc.Start,
			End:                     alloc.End,
		}
		recommendations = append(recommendations, rec)
		return recommendations
	}

	// General rightsizing recommendation
	if includeRightsize && savings >= minSavings {
		priority := RecommendationPriorityLow
		if savingsPercent >= 30 {
			priority = RecommendationPriorityMedium
		}
		if savingsPercent >= 50 {
			priority = RecommendationPriorityHigh
		}

		rec := &Recommendation{
			ID:                      generateRecommendationID(),
			Type:                    RecommendationTypeRightsize,
			Priority:                priority,
			ResourceName:           alloc.Name,
			Description:            fmt.Sprintf("Resource '%s' can be rightsized for %.1f%% cost savings", alloc.Name, savingsPercent),
			Action:                  fmt.Sprintf("Adjust CPU request to %.3f cores (from %.3f) and RAM request to %.0f bytes (from %.0f)", recommendedCPU, cpuCoresRequested, recommendedRAM, ramBytesRequested),
			CurrentCPURequest:       cpuCoresRequested,
			CurrentRAMRequest:       ramBytesRequested,
			CurrentCPUUsage:         cpuCoresUsed,
			CurrentRAMUsage:         ramBytesUsed,
			CurrentCost:             currentTotalCost,
			CPUEfficiency:           cpuEfficiency,
			MemoryEfficiency:        memoryEfficiency,
			RecommendedCPURequest:   recommendedCPU,
			RecommendedRAMRequest:   recommendedRAM,
			RecommendedCost:         recommendedTotalCost,
			EstimatedSavings:        savings,
			EstimatedSavingsPercent: savingsPercent,
			Start:                   alloc.Start,
			End:                     alloc.End,
		}
		recommendations = append(recommendations, rec)
	}

	return recommendations
}

// generateRecommendationID creates a unique ID for a recommendation.
func generateRecommendationID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("rec-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("rec-%s", hex.EncodeToString(bytes))
}

// sortRecommendationsBySavings sorts recommendations by estimated savings in descending order.
func sortRecommendationsBySavings(recommendations []*Recommendation) {
	for i := 0; i < len(recommendations); i++ {
		for j := i + 1; j < len(recommendations); j++ {
			if recommendations[j].EstimatedSavings > recommendations[i].EstimatedSavings {
				recommendations[i], recommendations[j] = recommendations[j], recommendations[i]
			}
		}
	}
}

// createEmptyRecommendationsSummary creates an empty summary for when there are no recommendations.
func createEmptyRecommendationsSummary() *RecommendationsSummary {
	return &RecommendationsSummary{
		TotalRecommendations:  0,
		TotalPotentialSavings: 0,
		ByType:                make(map[string]int),
		ByPriority:            make(map[string]int),
		IdleResourceCount:     0,
		OversizedCount:        0,
		RightsizeCount:        0,
	}
}

// buildRecommendationsSummary creates a summary from a list of recommendations.
func buildRecommendationsSummary(recommendations []*Recommendation) *RecommendationsSummary {
	summary := &RecommendationsSummary{
		TotalRecommendations: len(recommendations),
		ByType:               make(map[string]int),
		ByPriority:           make(map[string]int),
	}

	for _, rec := range recommendations {
		summary.TotalPotentialSavings += rec.EstimatedSavings
		summary.ByType[string(rec.Type)]++
		summary.ByPriority[string(rec.Priority)]++

		switch rec.Type {
		case RecommendationTypeIdle:
			summary.IdleResourceCount++
		case RecommendationTypeOversized:
			summary.OversizedCount++
		case RecommendationTypeRightsize:
			summary.RightsizeCount++
		}
	}

	return summary
}
