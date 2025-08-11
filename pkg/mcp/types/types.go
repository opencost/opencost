package types

import (
	"fmt"
	"time"
)

// Window represents a time range for queries
type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// String returns the window in OpenCost format
func (w Window) String() string {
	if w.Start.IsZero() || w.End.IsZero() {
		return "today"
	}
	return fmt.Sprintf("%s,%s", 
		w.Start.Format("2006-01-02T15:04:05Z"), 
		w.End.Format("2006-01-02T15:04:05Z"))
}

// Session represents an MCP session
type Session struct {
	ID                 string                 `json:"id"`
	StartTime          time.Time              `json:"startTime"`
	LastActivity       time.Time              `json:"lastActivity"`
	QueryHistory       []QueryHistoryItem     `json:"queryHistory"`
	ActiveFilters      map[string]interface{} `json:"activeFilters"`
	PreferredUnits     string                 `json:"preferredUnits"`
	UserExpertiseLevel string                 `json:"userExpertiseLevel"`
}

// QueryHistoryItem represents a single query in the session history
type QueryHistoryItem struct {
	Timestamp   time.Time              `json:"timestamp"`
	QueryType   string                 `json:"queryType"`
	Parameters  map[string]interface{} `json:"parameters"`
	ResultCount int                    `json:"resultCount"`
	Duration    time.Duration          `json:"duration"`
}

// Request types

type AllocationRequest struct {
	Window               string            `json:"window,omitempty"`
	Filters              map[string]string `json:"filters,omitempty"`
	NaturalLanguageQuery string            `json:"naturalLanguageQuery,omitempty"`
	Aggregate            string            `json:"aggregate,omitempty"`
	Resolution           string            `json:"resolution,omitempty"`
}

type AssetRequest struct {
	Window               string            `json:"window,omitempty"`
	Filters              map[string]string `json:"filters,omitempty"`
	NaturalLanguageQuery string            `json:"naturalLanguageQuery,omitempty"`
	Aggregate            string            `json:"aggregate,omitempty"`
	Resolution           string            `json:"resolution,omitempty"`
}

type CloudCostRequest struct {
	Window               string            `json:"window,omitempty"`
	Filters              map[string]string `json:"filters,omitempty"`
	NaturalLanguageQuery string            `json:"naturalLanguageQuery,omitempty"`
	Provider             string            `json:"provider,omitempty"`
	Service              string            `json:"service,omitempty"`
}

type ChatRequest struct {
	SessionID string `json:"sessionId"`
	Message   string `json:"message"`
}

// Response types

type MCPResponse struct {
	QueryType string           `json:"queryType"`
	Data      interface{}      `json:"data"`
	Summary   Summary          `json:"summary"`
	Insights  []Insight        `json:"insights"`
	Metadata  ResponseMetadata `json:"metadata"`
}

type Summary struct {
	TotalCost   float64 `json:"totalCost"`
	Currency    string  `json:"currency"`
	Period      string  `json:"period"`
	ItemCount   int     `json:"itemCount,omitempty"`
	TimeRange   Window  `json:"timeRange,omitempty"`
	TopItems    []Item  `json:"topItems,omitempty"`
}

type Item struct {
	Name        string  `json:"name"`
	Cost        float64 `json:"cost"`
	Percentage  float64 `json:"percentage"`
	Type        string  `json:"type"`
}

type Insight struct {
	Type        string  `json:"type"`        // "warning", "info", "optimization", "anomaly"
	Severity    string  `json:"severity"`    // "high", "medium", "low"
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Confidence  float64 `json:"confidence"`
	ActionItems []string `json:"actionItems,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

type ResponseMetadata struct {
	QueryTime          time.Time     `json:"queryTime"`
	ProcessingTime     time.Duration `json:"processingTime"`
	DataSources        []string      `json:"dataSources"`
	Confidence         float64       `json:"confidence"`
	NextSuggestedQuery string        `json:"nextSuggestedQuery,omitempty"`
	TotalResults       int           `json:"totalResults,omitempty"`
	ResultsReturned    int           `json:"resultsReturned,omitempty"`
}

// Natural language processing types

type NLQueryResult struct {
	Window      string            `json:"window"`
	Filters     map[string]string `json:"filters"`
	Aggregate   string            `json:"aggregate,omitempty"`
	Resolution  string            `json:"resolution,omitempty"`
	Confidence  float64           `json:"confidence"`
}

type IntentResult struct {
	QueryType  string  `json:"queryType"`  // "allocation", "asset", "cloud", "general"
	Confidence float64 `json:"confidence"`
	Entities   []Entity `json:"entities,omitempty"`
}

type Entity struct {
	Type       string  `json:"type"`       // "namespace", "cluster", "service", etc.
	Value      string  `json:"value"`
	Confidence float64 `json:"confidence"`
}

// OpenCost data types (simplified representations)

type AllocationSet struct {
	Allocations map[string]*Allocation `json:"allocations"`
	Window      Window                 `json:"window"`
	TotalCost   float64                `json:"totalCost"`
}

type Allocation struct {
	Name                string             `json:"name"`
	Properties          *AllocationProperties `json:"properties"`
	Start               time.Time          `json:"start"`
	End                 time.Time          `json:"end"`
	CPUCoreHours        float64            `json:"cpuCoreHours"`
	CPUCost             float64            `json:"cpuCost"`
	GPUHours            float64            `json:"gpuHours"`
	GPUCost             float64            `json:"gpuCost"`
	NetworkCost         float64            `json:"networkCost"`
	LoadBalancerCost    float64            `json:"loadBalancerCost"`
	PVCost              float64            `json:"pvCost"`
	RAMByteHours        float64            `json:"ramByteHours"`
	RAMCost             float64            `json:"ramCost"`
	SharedCost          float64            `json:"sharedCost"`
	ExternalCost        float64            `json:"externalCost"`
	TotalCost           float64            `json:"totalCost"`
	TotalEfficiency     float64            `json:"totalEfficiency"`
}

type AllocationProperties struct {
	Cluster     string            `json:"cluster,omitempty"`
	Node        string            `json:"node,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	Controller  string            `json:"controller,omitempty"`
	Pod         string            `json:"pod,omitempty"`
	Container   string            `json:"container,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type AssetSet struct {
	Assets    map[string]*Asset `json:"assets"`
	Window    Window            `json:"window"`
	TotalCost float64           `json:"totalCost"`
}

type Asset struct {
	Properties    *AssetProperties `json:"properties"`
	Labels        map[string]string `json:"labels"`
	Start         time.Time        `json:"start"`
	End           time.Time        `json:"end"`
	Minutes       float64          `json:"minutes"`
	CPUCores      float64          `json:"cpuCores"`
	CPUCoreHours  float64          `json:"cpuCoreHours"`
	CPUCost       float64          `json:"cpuCost"`
	GPUCount      float64          `json:"gpuCount"`
	GPUHours      float64          `json:"gpuHours"`
	GPUCost       float64          `json:"gpuCost"`
	RAMBytes      float64          `json:"ramBytes"`
	RAMByteHours  float64          `json:"ramByteHours"`
	RAMCost       float64          `json:"ramCost"`
	StorageBytes  float64          `json:"storageBytes"`
	StorageCost   float64          `json:"storageCost"`
	NetworkCost   float64          `json:"networkCost"`
	TotalCost     float64          `json:"totalCost"`
	Adjustment    float64          `json:"adjustment"`
}

type AssetProperties struct {
	Category    string `json:"category,omitempty"`    // "Compute", "Storage", "Network", etc.
	Provider    string `json:"provider,omitempty"`    // "AWS", "GCP", "Azure", etc.
	Account     string `json:"account,omitempty"`
	Project     string `json:"project,omitempty"`
	Service     string `json:"service,omitempty"`
	Cluster     string `json:"cluster,omitempty"`
	Node        string `json:"node,omitempty"`
	Name        string `json:"name,omitempty"`
	ProviderID  string `json:"providerID,omitempty"`
}