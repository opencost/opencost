package mcp

import (
	"time"
	models "github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloudcost"
	"github.com/opencost/opencost/pkg/costmodel"
)

// QueryType defines the type of query to be executed.
type QueryType string

const (
	AllocationQueryType QueryType = "allocation"
	AssetQueryType      QueryType = "asset"
	CloudCostQueryType  QueryType = "cloudcost"
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
	Summary   *DataSummary  `json:"summary,omitempty"`
}

// QueryMetadata contains metadata about the query execution.
type QueryMetadata struct {
	QueryID        string        `json:"queryId"`
	Timestamp      time.Time     `json:"timestamp"`
	ProcessingTime time.Duration `json:"processingTime"`
}

// DataSummary provides a summary of the data.
type DataSummary struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

// OpenCostQueryRequest provides a unified interface for all OpenCost query types.
type OpenCostQueryRequest struct {
	QueryType QueryType `json:"queryType" validate:"required,oneof=allocation asset cloudcost"`

	Window string `json:"window" validate:"required"`

	AllocationParams *AllocationQuery `json:"allocationParams,omitempty"`
	AssetParams      *AssetQuery      `json:"assetParams,omitempty"`
	CloudCostParams  *CloudCostQuery  `json:"cloudCostParams,omitempty"`
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
	Account    string `json:"account,omitempty"`    // Account filter
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

// MCPServer holds the dependencies for the MCP API server.
type MCPServer struct {
	costModel   *costmodel.CostModel
	provider    models.Provider
	integration cloudcost.CloudCostIntegration
}