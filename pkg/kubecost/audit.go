package kubecost

import (
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

// AuditType the types of Audits, each of which should be contained in an AuditSet
type AuditType string

const (
	AuditAllocationReconciliation AuditType = "AuditAllocationReconciliation"
	AuditAllocationTotalStore     AuditType = "AuditAllocationTotalStore"
	AuditAllocationAggStore       AuditType = "AuditAllocationAggStore"
	AuditAssetReconciliation      AuditType = "AuditAssetReconciliation"
	AuditAssetTotalStore          AuditType = "AuditAssetTotalStore"
	AuditAssetAggStore            AuditType = "AuditAssetAggStore"
	AuditClusterEquality          AuditType = "AuditClusterEquality"

	AuditAll         AuditType = ""
	AuditInvalidType AuditType = "InvalidType"
)

// ToAuditType converts a string to an Audit type
func ToAuditType(check string) AuditType {
	switch check {
	case string(AuditAllocationReconciliation):
		return AuditAllocationReconciliation
	case string(AuditAllocationTotalStore):
		return AuditAllocationTotalStore
	case string(AuditAllocationAggStore):
		return AuditAllocationAggStore
	case string(AuditAssetReconciliation):
		return AuditAssetReconciliation
	case string(AuditAssetTotalStore):
		return AuditAssetTotalStore
	case string(AuditAssetAggStore):
		return AuditAssetAggStore
	case string(AuditClusterEquality):
		return AuditClusterEquality
	case string(AuditAll):
		return AuditAll
	default:
		return AuditInvalidType
	}
}

// AuditStatus are possible outcomes of an audit
type AuditStatus string

const (
	FailedStatus  AuditStatus = "Failed"
	WarningStatus             = "Warning"
	PassedStatus              = "Passed"
)

// AuditMissingValue records when a value that should be present in a store or in the audit generated results are missing
type AuditMissingValue struct {
	Description string
	Key         string
}

// AuditFloatResult structure for holding the results of a failed audit on a float value, Expected should be the value
// calculated by the Audit func while Actual is what is contained in the relevant store.
type AuditFloatResult struct {
	Expected float64
	Actual   float64
}

// Clone returns a deep copy of the caller
func (afr *AuditFloatResult) Clone() *AuditFloatResult {
	return &AuditFloatResult{
		Expected: afr.Expected,
		Actual:   afr.Actual,
	}
}

// AllocationReconciliationAudit records the differences of between compute resources (cpu, ram, gpu) costs between
// allocations by nodes and node assets keyed on node name and compute resource
type AllocationReconciliationAudit struct {
	Status        AuditStatus
	Description   string
	LastRun       time.Time
	Resources     map[string]map[string]*AuditFloatResult
	MissingValues []*AuditMissingValue
}

// Clone returns a deep copy of the caller
func (ara *AllocationReconciliationAudit) Clone() *AllocationReconciliationAudit {
	if ara == nil {
		return nil
	}

	resources := make(map[string]map[string]*AuditFloatResult, len(ara.Resources))
	for node, resourceMap := range ara.Resources {
		copyResourceMap := make(map[string]*AuditFloatResult, len(resourceMap))
		for resourceName, val := range resourceMap {
			copyResourceMap[resourceName] = val.Clone()
		}
		resources[node] = copyResourceMap
	}
	return &AllocationReconciliationAudit{
		Status:        ara.Status,
		Description:   ara.Description,
		LastRun:       ara.LastRun,
		Resources:     resources,
		MissingValues: slices.Clone(ara.MissingValues),
	}
}

// TotalAudit records the differences between a total store and the totaled results of the store that it is based on
// keyed by cluster and node names
type TotalAudit struct {
	Status         AuditStatus
	Description    string
	LastRun        time.Time
	TotalByNode    map[string]*AuditFloatResult
	TotalByCluster map[string]*AuditFloatResult
	MissingValues  []*AuditMissingValue
}

// Clone returns a deep copy of the caller
func (ta *TotalAudit) Clone() *TotalAudit {
	if ta == nil {
		return nil
	}

	tbn := make(map[string]*AuditFloatResult, len(ta.TotalByNode))
	for k, v := range ta.TotalByNode {
		tbn[k] = v
	}
	tbc := make(map[string]*AuditFloatResult, len(ta.TotalByNode))
	for k, v := range ta.TotalByCluster {
		tbc[k] = v
	}

	return &TotalAudit{
		Status:         ta.Status,
		Description:    ta.Description,
		LastRun:        ta.LastRun,
		TotalByNode:    tbn,
		TotalByCluster: tbc,
		MissingValues:  slices.Clone(ta.MissingValues),
	}
}

// AggAudit contains the results of an Audit on an AggStore keyed on aggregation prop and Allocation key
type AggAudit struct {
	Status        AuditStatus
	Description   string
	LastRun       time.Time
	Results       map[string]map[string]*AuditFloatResult
	MissingValues []*AuditMissingValue
}

// Clone returns a deep copy of the caller
func (aa *AggAudit) Clone() *AggAudit {
	if aa == nil {
		return nil
	}
	res := make(map[string]map[string]*AuditFloatResult, len(aa.Results))
	for aggType, aggResults := range aa.Results {
		copyAggResult := make(map[string]*AuditFloatResult, len(aggResults))
		for aggName, auditFloatResult := range aggResults {
			copyAggResult[aggName] = auditFloatResult
		}
		res[aggType] = copyAggResult
	}

	return &AggAudit{
		Status:        aa.Status,
		Description:   aa.Description,
		LastRun:       aa.LastRun,
		Results:       res,
		MissingValues: slices.Clone(aa.MissingValues),
	}
}

// AssetReconciliationAudit records differences in assets and the Cloud
type AssetReconciliationAudit struct {
	Status        AuditStatus
	Description   string
	LastRun       time.Time
	Results       map[string]map[string]*AuditFloatResult
	MissingValues []*AuditMissingValue
}

// Clone returns a deep copy of the caller
func (ara *AssetReconciliationAudit) Clone() *AssetReconciliationAudit {
	res := make(map[string]map[string]*AuditFloatResult, len(ara.Results))
	for aggType, aggResults := range ara.Results {
		copyAggResult := make(map[string]*AuditFloatResult, len(aggResults))
		for aggName, auditFloatResult := range aggResults {
			copyAggResult[aggName] = auditFloatResult
		}
		res[aggType] = copyAggResult
	}

	return &AssetReconciliationAudit{
		Status:        ara.Status,
		Description:   ara.Description,
		LastRun:       ara.LastRun,
		Results:       res,
		MissingValues: slices.Clone(ara.MissingValues),
	}
}

// EqualityAudit records the difference in cost between Allocations and Assets aggregated by cluster and keyed on cluster
type EqualityAudit struct {
	Status        AuditStatus
	Description   string
	LastRun       time.Time
	Clusters      map[string]*AuditFloatResult
	MissingValues []*AuditMissingValue
}

// Clone returns a deep copy of the caller
func (ea *EqualityAudit) Clone() *EqualityAudit {
	if ea == nil {
		return nil
	}
	clusters := make(map[string]*AuditFloatResult, len(ea.Clusters))
	for k, v := range ea.Clusters {
		clusters[k] = v
	}
	return &EqualityAudit{
		Status:        ea.Status,
		Description:   ea.Description,
		LastRun:       ea.LastRun,
		Clusters:      clusters,
		MissingValues: slices.Clone(ea.MissingValues),
	}
}

// AuditCoverage tracks coverage of each audit type
type AuditCoverage struct {
	AllocationReconciliation Window `json:"allocationReconciliation"`
	AllocationAgg            Window `json:"allocationAgg"`
	AllocationTotal          Window `json:"allocationTotal"`
	AssetTotal               Window `json:"assetTotal"`
	AssetReconciliation      Window `json:"assetReconciliation"`
	ClusterEquality          Window `json:"clusterEquality"`
}

// NewAuditCoverage create default AuditCoverage
func NewAuditCoverage() *AuditCoverage {
	return &AuditCoverage{}
}

// Update expands the coverage of each Window in the coverage that the given AuditSet's Window if the corresponding Audit is not nil
// Note: This means of determining coverage can lead to holes in the given window
func (ac *AuditCoverage) Update(as *AuditSet) {
	if as != nil && as.AllocationReconciliation != nil {
		ac.AllocationReconciliation.Expand(as.Window)
		ac.AllocationAgg.Expand(as.Window)
		ac.AllocationTotal.Expand(as.Window)
		ac.AssetTotal.Expand(as.Window)
		ac.AssetReconciliation.Expand(as.Window)
		ac.ClusterEquality.Expand(as.Window)
	}

}

// AuditSet is a ETLSet which contains all kind of Audits for a given Window
type AuditSet struct {
	sync.RWMutex
	AllocationReconciliation *AllocationReconciliationAudit `json:"allocationReconciliation"`
	AllocationAgg            *AggAudit                      `json:"allocationAgg"`
	AllocationTotal          *TotalAudit                    `json:"allocationTotal"`
	AssetTotal               *TotalAudit                    `json:"assetTotal"`
	AssetReconciliation      *AssetReconciliationAudit      `json:"assetReconciliation"`
	ClusterEquality          *EqualityAudit                 `json:"clusterEquality"`
	Window                   Window                         `json:"window"`
}

// NewAuditSet creates an empty AuditSet with the given window
func NewAuditSet(start, end time.Time) *AuditSet {
	return &AuditSet{
		Window: NewWindow(&start, &end),
	}
}

// UpdateAuditSet overwrites any audit fields in the caller with those in the given AuditSet which are not nil
func (as *AuditSet) UpdateAuditSet(that *AuditSet) *AuditSet {
	if as == nil {
		return that
	}

	if that.AllocationReconciliation != nil {
		as.AllocationReconciliation = that.AllocationReconciliation
	}
	if that.AllocationAgg != nil {
		as.AllocationAgg = that.AllocationAgg
	}
	if that.AllocationTotal != nil {
		as.AllocationTotal = that.AllocationTotal
	}
	if that.AssetTotal != nil {
		as.AssetTotal = that.AssetTotal
	}
	if that.AssetReconciliation != nil {
		as.AssetReconciliation = that.AssetReconciliation
	}

	if that.ClusterEquality != nil {
		as.ClusterEquality = that.ClusterEquality
	}

	return as
}

// ConstructSet fulfills the ETLSet interface to provide an empty version of itself so that it can be initialized in its
// generic form.
func (as *AuditSet) ConstructSet() ETLSet {
	return &AuditSet{}
}

// IsEmpty returns true if any of the audits are non-nil
func (as *AuditSet) IsEmpty() bool {
	return as == nil || (as.AllocationReconciliation == nil &&
		as.AllocationAgg == nil &&
		as.AllocationTotal == nil &&
		as.AssetTotal == nil &&
		as.AssetReconciliation == nil &&
		as.ClusterEquality == nil)
}

// GetWindow returns AuditSet Window
func (as *AuditSet) GetWindow() Window {
	return as.Window
}

// Clone returns a deep copy of the caller
func (as *AuditSet) Clone() *AuditSet {
	if as == nil {
		return nil
	}

	as.RLock()
	defer as.RUnlock()

	return &AuditSet{
		AllocationReconciliation: as.AllocationReconciliation.Clone(),
		AllocationAgg:            as.AllocationAgg.Clone(),
		AllocationTotal:          as.AllocationTotal.Clone(),
		AssetTotal:               as.AssetTotal.Clone(),
		AssetReconciliation:      as.AssetReconciliation.Clone(),
		ClusterEquality:          as.ClusterEquality.Clone(),
		Window:                   as.Window.Clone(),
	}
}

// CloneSet returns a deep copy of the caller and returns set
func (as *AuditSet) CloneSet() ETLSet {
	return as.Clone()
}

// AuditSetRange SetRange of AuditSets
type AuditSetRange struct {
	SetRange[*AuditSet]
}
