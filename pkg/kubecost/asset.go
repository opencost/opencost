package kubecost

import (
	"encoding"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util/json"
)

// UndefinedKey is used in composing Asset group keys if the group does not have that property defined.
// E.g. if aggregating on Cluster, Assets in the AssetSet where Asset has no cluster will be grouped under key "__undefined__"
const UndefinedKey = "__undefined__"

// Asset defines an entity within a cluster that has a defined cost over a
// given period of time.
type Asset interface {
	// Type identifies the kind of Asset, which must always exist and should
	// be defined by the underlying type implementing the interface.
	Type() AssetType

	// Properties are a map of predefined traits, which may or may not exist,
	// but must conform to the AssetProperty schema
	Properties() *AssetProperties
	SetProperties(*AssetProperties)

	// Labels are a map of undefined string-to-string values
	Labels() AssetLabels
	SetLabels(AssetLabels)

	// Monetary values
	Adjustment() float64
	SetAdjustment(float64)
	TotalCost() float64

	// Temporal values
	Start() time.Time
	End() time.Time
	SetStartEnd(time.Time, time.Time)
	Window() Window
	ExpandWindow(Window)
	Minutes() float64

	// Operations and comparisons
	Add(Asset) Asset
	Clone() Asset
	Equal(Asset) bool

	// Representations
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	json.Marshaler
	fmt.Stringer
}

// AssetToExternalAllocation converts the given asset to an Allocation, given
// the properties to use to aggregate, and the mapping from Allocation property
// to Asset label. For example, consider this asset:
//
// CURRENT: Asset ETL stores its data ALREADY MAPPED from label to k8s concept. This isn't ideal-- see the TOOD.
//   Cloud {
// 	   TotalCost: 10.00,
// 	   Labels{
//       "kubernetes_namespace":"monitoring",
// 	     "env":"prod"
// 	   }
//   }
//
// Given the following parameters, we expect to return:
//
//   1) single-prop full match
//   aggregateBy = ["namespace"]
//   => Allocation{Name: "monitoring", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//   2) multi-prop full match
//   aggregateBy = ["namespace", "label:env"]
//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//   => Allocation{Name: "monitoring/env=prod", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//   3) multi-prop partial match
//   aggregateBy = ["namespace", "label:foo"]
//   => Allocation{Name: "monitoring/__unallocated__", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//   4) no match
//   aggregateBy = ["cluster"]
//   => nil, err
//
// TODO:
//   Cloud {
// 	   TotalCost: 10.00,
// 	   Labels{
//       "kubernetes_namespace":"monitoring",
// 	     "env":"prod"
// 	   }
//   }
//
// Given the following parameters, we expect to return:
//
//   1) single-prop full match
//   aggregateBy = ["namespace"]
//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//   => Allocation{Name: "monitoring", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//   2) multi-prop full match
//   aggregateBy = ["namespace", "label:env"]
//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//   => Allocation{Name: "monitoring/env=prod", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//   3) multi-prop partial match
//   aggregateBy = ["namespace", "label:foo"]
//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//   => Allocation{Name: "monitoring/__unallocated__", ExternalCost: 10.00, TotalCost: 10.00}, nil
//
//   4) no match
//   aggregateBy = ["cluster"]
//   allocationPropertyLabels = {"namespace":"kubernetes_namespace"}
//   => nil, err
//
// (See asset_test.go for assertions of these examples and more.)
func AssetToExternalAllocation(asset Asset, aggregateBy []string, labelConfig *LabelConfig) (*Allocation, error) {
	if asset == nil {
		return nil, fmt.Errorf("asset is nil")
	}

	// Use default label config if one is not provided.
	if labelConfig == nil {
		labelConfig = NewLabelConfig()
	}

	// names will collect the slash-separated names accrued by iterating over
	// aggregateBy and checking the relevant labels.
	names := []string{}

	// match records whether or not a match was found in the Asset labels,
	// such that is can genuinely be turned into an external Allocation.
	match := false

	// props records the relevant Properties to set on the resultant Allocation
	props := AllocationProperties{}

	// For each aggregation parameter, try to find a match in the asset's
	// labels, using the labelConfig to translate. For an aggregation parameter
	// defined by a label (e.g. "label:app") this is simple: look for the label
	// and use it (e.g. if "app" is a defined label on the asset, then use its
	// value). For an aggregation parameter defined by a non-label property
	// (e.g. "namespace") this requires using the labelConfig to look up the
	// label name associated with that property and to use the value under that
	// label, if set (e.g. if the aggregation property is "namespace" and the
	// labelConfig is configured with "namespace_external_label" => "kubens"
	// and the asset has label "kubens":"kubecost", then file the asset as an
	// external cost under "kubecost").
	for _, aggBy := range aggregateBy {
		name := labelConfig.GetExternalAllocationName(asset.Labels(), aggBy)

		if name == "" {
			// No matching label has been defined in the cost-analyzer label config
			// relating to the given aggregateBy property.
			names = append(names, UnallocatedSuffix)
			continue
		} else {
			names = append(names, name)
			match = true

			// Default labels to an empty map, if necessary
			if props.Labels == nil {
				props.Labels = map[string]string{}
			}

			// Set the corresponding property on props
			switch aggBy {
			case AllocationClusterProp:
				props.Cluster = name
			case AllocationNodeProp:
				props.Node = name
			case AllocationNamespaceProp:
				props.Namespace = name
			case AllocationControllerKindProp:
				props.ControllerKind = name
			case AllocationControllerProp:
				props.Controller = name
			case AllocationPodProp:
				props.Pod = name
			case AllocationContainerProp:
				props.Container = name
			case AllocationServiceProp:
				props.Services = []string{name}
			case AllocationDeploymentProp:
				props.Controller = name
				props.ControllerKind = "deployment"
			case AllocationStatefulSetProp:
				props.Controller = name
				props.ControllerKind = "statefulset"
			case AllocationDaemonSetProp:
				props.Controller = name
				props.ControllerKind = "daemonset"
			case AllocationDepartmentProp:
				props.Labels[labelConfig.DepartmentLabel] = name
			case AllocationEnvironmentProp:
				props.Labels[labelConfig.EnvironmentLabel] = name
			case AllocationOwnerProp:
				props.Labels[labelConfig.OwnerLabel] = name
			case AllocationProductProp:
				props.Labels[labelConfig.ProductLabel] = name
			case AllocationTeamProp:
				props.Labels[labelConfig.TeamLabel] = name
			default:
				if strings.HasPrefix(aggBy, "label:") {
					// Set the corresponding label in props
					labelName := strings.TrimPrefix(aggBy, "label:")
					labelValue := strings.TrimPrefix(name, labelName+"=")
					props.Labels[labelName] = labelValue
				}
			}
		}
	}

	// If not a signle aggregation property generated a matching label property,
	// then consider the asset ineligible to be treated as an external allocation.
	if !match {
		return nil, fmt.Errorf("asset does not qualify as an external allocation")
	}

	// Use naming to label as an external allocation. See IsExternal() for more.
	names = append(names, ExternalSuffix)

	// TODO: external allocation: efficiency?
	// TODO: external allocation: resource totals?
	return &Allocation{
		Name:         strings.Join(names, "/"),
		Properties:   &props,
		Window:       asset.Window().Clone(),
		Start:        asset.Start(),
		End:          asset.End(),
		ExternalCost: asset.TotalCost(),
	}, nil
}

// key is used to determine uniqueness of an Asset, for instance during Insert
// to determine if two Assets should be combined. Passing `nil` `aggregateBy` indicates
// that all available `AssetProperty` keys should be used. Passing empty `aggregateBy` indicates that
// no key should be used (e.g. to aggregate all assets). Passing one or more `aggregateBy`
// values will key by only those values.
// Valid values of `aggregateBy` elements are strings which are an `AssetProperty`, and strings prefixed
// with `"label:"`.
func key(a Asset, aggregateBy []string) (string, error) {
	var buffer strings.Builder

	if aggregateBy == nil {
		aggregateBy = []string{
			string(AssetProviderProp),
			string(AssetAccountProp),
			string(AssetProjectProp),
			string(AssetCategoryProp),
			string(AssetClusterProp),
			string(AssetTypeProp),
			string(AssetServiceProp),
			string(AssetProviderIDProp),
			string(AssetNameProp),
		}
	}

	for i, s := range aggregateBy {
		key := ""
		switch true {
		case s == string(AssetProviderProp):
			key = a.Properties().Provider
		case s == string(AssetAccountProp):
			key = a.Properties().Account
		case s == string(AssetProjectProp):
			key = a.Properties().Project
		case s == string(AssetClusterProp):
			key = a.Properties().Cluster
		case s == string(AssetCategoryProp):
			key = a.Properties().Category
		case s == string(AssetTypeProp):
			key = a.Type().String()
		case s == string(AssetServiceProp):
			key = a.Properties().Service
		case s == string(AssetProviderIDProp):
			key = a.Properties().ProviderID
		case s == string(AssetNameProp):
			key = a.Properties().Name
		case strings.HasPrefix(s, "label:"):
			if labelKey := strings.TrimPrefix(s, "label:"); labelKey != "" {
				labelVal := a.Labels()[labelKey]
				if labelVal == "" {
					key = "__undefined__"
				} else {
					key = fmt.Sprintf("%s=%s", labelKey, labelVal)
				}
			} else {
				// Don't allow aggregating on label ""
				return "", fmt.Errorf("attempted to aggregate on invalid key: %s", s)
			}
		default:
			return "", fmt.Errorf("attempted to aggregate on invalid key: %s", s)
		}

		if key != "" {
			buffer.WriteString(key)
		} else {
			buffer.WriteString(UndefinedKey)
		}
		if i != (len(aggregateBy) - 1) {
			buffer.WriteString("/")
		}
	}
	return buffer.String(), nil
}

func toString(a Asset) string {
	return fmt.Sprintf("%s{%s}%s=%.2f", a.Type().String(), a.Properties(), a.Window(), a.TotalCost())
}

// AssetLabels is a schema-free mapping of key/value pairs that can be
// attributed to an Asset as a flexible a
type AssetLabels map[string]string

// Clone returns a cloned map of labels
func (al AssetLabels) Clone() AssetLabels {
	clone := make(AssetLabels, len(al))

	for label, value := range al {
		clone[label] = value
	}

	return clone
}

// Equal returns true only if the two set of labels are exact matches
func (al AssetLabels) Equal(that AssetLabels) bool {
	if len(al) != len(that) {
		return false
	}

	for label, value := range al {
		if thatValue, ok := that[label]; !ok || thatValue != value {
			return false
		}
	}

	return true
}

// Merge retains only the labels shared with the given AssetLabels
func (al AssetLabels) Merge(that AssetLabels) AssetLabels {
	result := AssetLabels{}

	for label, value := range al {
		if thatValue, ok := that[label]; ok && thatValue == value {
			result[label] = value
		}
	}

	return result
}

// Append joins AssetLabels with a given map of labels
func (al AssetLabels) Append(newLabels map[string]string, overwrite bool) {
	if len(newLabels) == 0 {
		return
	}

	for label, value := range newLabels {
		if _, ok := al[label]; ok {
			if overwrite {
				al[label] = value
			}
		} else {
			al[label] = value
		}
	}
}

// AssetMatchFunc is a function that can be used to match Assets by
// returning true for any given Asset if a condition is met.
type AssetMatchFunc func(Asset) bool

// AssetType identifies a type of Asset
type AssetType int

const (
	// AnyAssetType describes the Any AssetType
	AnyAssetType AssetType = iota

	// CloudAssetType describes the Cloud AssetType
	CloudAssetType

	// ClusterManagementAssetType describes the ClusterManagement AssetType
	ClusterManagementAssetType

	// DiskAssetType describes the Disk AssetType
	DiskAssetType

	// LoadBalancerAssetType describes the LoadBalancer AssetType
	LoadBalancerAssetType

	// NetworkAssetType describes the Network AssetType
	NetworkAssetType

	// NodeAssetType describes the Node AssetType
	NodeAssetType

	// SharedAssetType describes the Shared AssetType
	SharedAssetType
)

// ParseAssetType attempts to parse the given string into an AssetType
func ParseAssetType(text string) (AssetType, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case "cloud":
		return CloudAssetType, nil
	case "clustermanagement":
		return ClusterManagementAssetType, nil
	case "disk":
		return DiskAssetType, nil
	case "loadbalancer":
		return LoadBalancerAssetType, nil
	case "network":
		return NetworkAssetType, nil
	case "node":
		return NodeAssetType, nil
	case "shared":
		return SharedAssetType, nil
	}
	return AnyAssetType, fmt.Errorf("invalid asset type: %s", text)
}

// String converts the given AssetType to a string
func (at AssetType) String() string {
	return [...]string{
		"Asset",
		"Cloud",
		"ClusterManagement",
		"Disk",
		"LoadBalancer",
		"Network",
		"Node",
		"Shared",
	}[at]
}

// Any is the most general Asset, which is usually created as a result of
// adding two Assets of different types.
type Any struct {
	labels     AssetLabels
	properties *AssetProperties
	start      time.Time
	end        time.Time
	window     Window
	adjustment float64
	Cost       float64
}

// NewAsset creates a new Any-type Asset for the given period of time
func NewAsset(start, end time.Time, window Window) *Any {
	return &Any{
		labels:     AssetLabels{},
		properties: &AssetProperties{},
		start:      start,
		end:        end,
		window:     window.Clone(),
	}
}

// Type returns the Asset's type
func (a *Any) Type() AssetType {
	return AnyAssetType
}

// Properties returns the Asset's properties
func (a *Any) Properties() *AssetProperties {
	return a.properties
}

// SetProperties sets the Asset's properties
func (a *Any) SetProperties(props *AssetProperties) {
	a.properties = props
}

// Labels returns the Asset's labels
func (a *Any) Labels() AssetLabels {
	return a.labels
}

// SetLabels sets the Asset's labels
func (a *Any) SetLabels(labels AssetLabels) {
	a.labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (a *Any) Adjustment() float64 {
	return a.adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (a *Any) SetAdjustment(adj float64) {
	a.adjustment = adj
}

// TotalCost returns the Asset's TotalCost
func (a *Any) TotalCost() float64 {
	return a.Cost + a.adjustment
}

// Start returns the Asset's start time within the window
func (a *Any) Start() time.Time {
	return a.start
}

// End returns the Asset's end time within the window
func (a *Any) End() time.Time {
	return a.end
}

// Minutes returns the number of minutes the Asset was active within the window
func (a *Any) Minutes() float64 {
	return a.End().Sub(a.Start()).Minutes()
}

// Window returns the Asset's window
func (a *Any) Window() Window {
	return a.window
}

// ExpandWindow expands the Asset's window by the given window
func (a *Any) ExpandWindow(window Window) {
	a.window = a.window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (a *Any) SetStartEnd(start, end time.Time) {
	if a.Window().Contains(start) {
		a.start = start
	} else {
		log.Warningf("Any.SetStartEnd: start %s not in %s", start, a.Window())
	}

	if a.Window().Contains(end) {
		a.end = end
	} else {
		log.Warningf("Any.SetStartEnd: end %s not in %s", end, a.Window())
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (a *Any) Add(that Asset) Asset {
	this := a.Clone().(*Any)

	props := a.Properties().Merge(that.Properties())
	labels := a.Labels().Merge(that.Labels())

	start := a.Start()
	if that.Start().Before(start) {
		start = that.Start()
	}
	end := a.End()
	if that.End().After(end) {
		end = that.End()
	}
	window := a.Window().Expand(that.Window())

	this.start = start
	this.end = end
	this.window = window
	this.SetProperties(props)
	this.SetLabels(labels)
	this.adjustment += that.Adjustment()
	this.Cost += (that.TotalCost() - that.Adjustment())

	return this
}

// Clone returns a cloned instance of the Asset
func (a *Any) Clone() Asset {
	return &Any{
		labels:     a.labels.Clone(),
		properties: a.properties.Clone(),
		start:      a.start,
		end:        a.end,
		window:     a.window.Clone(),
		adjustment: a.adjustment,
		Cost:       a.Cost,
	}
}

// Equal returns true if the given Asset is an exact match of the receiver
func (a *Any) Equal(that Asset) bool {
	t, ok := that.(*Any)
	if !ok {
		return false
	}

	if !a.Labels().Equal(that.Labels()) {
		return false
	}
	if !a.Properties().Equal(that.Properties()) {
		return false
	}

	if !a.start.Equal(t.start) {
		return false
	}
	if !a.end.Equal(t.end) {
		return false
	}
	if !a.window.Equal(t.window) {
		return false
	}

	if a.Cost != t.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (a *Any) String() string {
	return toString(a)
}

// Cloud describes a cloud asset
type Cloud struct {
	labels     AssetLabels
	properties *AssetProperties
	start      time.Time
	end        time.Time
	window     Window
	adjustment float64
	Cost       float64
	Credit     float64 // Credit is a negative value representing dollars credited back to a given line-item
}

// NewCloud returns a new Cloud Asset
func NewCloud(category, providerID string, start, end time.Time, window Window) *Cloud {
	properties := &AssetProperties{
		Category:   category,
		ProviderID: providerID,
	}

	return &Cloud{
		labels:     AssetLabels{},
		properties: properties,
		start:      start,
		end:        end,
		window:     window.Clone(),
	}
}

// Type returns the AssetType
func (ca *Cloud) Type() AssetType {
	return CloudAssetType
}

// Properties returns the AssetProperties
func (ca *Cloud) Properties() *AssetProperties {
	return ca.properties
}

// SetProperties sets the Asset's properties
func (ca *Cloud) SetProperties(props *AssetProperties) {
	ca.properties = props
}

// Labels returns the AssetLabels
func (ca *Cloud) Labels() AssetLabels {
	return ca.labels
}

// SetLabels sets the Asset's labels
func (ca *Cloud) SetLabels(labels AssetLabels) {
	ca.labels = labels
}

// Adjustment returns the Asset's adjustment value
func (ca *Cloud) Adjustment() float64 {
	return ca.adjustment
}

// SetAdjustment sets the Asset's adjustment value
func (ca *Cloud) SetAdjustment(adj float64) {
	ca.adjustment = adj
}

// TotalCost returns the Asset's total cost
func (ca *Cloud) TotalCost() float64 {
	return ca.Cost + ca.adjustment + ca.Credit
}

// Start returns the Asset's precise start time within the window
func (ca *Cloud) Start() time.Time {
	return ca.start
}

// End returns the Asset's precise end time within the window
func (ca *Cloud) End() time.Time {
	return ca.end
}

// Minutes returns the number of Minutes the Asset ran
func (ca *Cloud) Minutes() float64 {
	return ca.End().Sub(ca.Start()).Minutes()
}

// Window returns the window within which the Asset ran
func (ca *Cloud) Window() Window {
	return ca.window
}

// ExpandWindow expands the Asset's window by the given window
func (ca *Cloud) ExpandWindow(window Window) {
	ca.window = ca.window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (ca *Cloud) SetStartEnd(start, end time.Time) {
	if ca.Window().Contains(start) {
		ca.start = start
	} else {
		log.Warningf("Cloud.SetStartEnd: start %s not in %s", start, ca.Window())
	}

	if ca.Window().Contains(end) {
		ca.end = end
	} else {
		log.Warningf("Cloud.SetStartEnd: end %s not in %s", end, ca.Window())
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (ca *Cloud) Add(a Asset) Asset {
	// Cloud + Cloud = Cloud
	if that, ok := a.(*Cloud); ok {
		this := ca.Clone().(*Cloud)
		this.add(that)
		return this
	}

	props := ca.Properties().Merge(a.Properties())
	labels := ca.Labels().Merge(a.Labels())

	start := ca.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := ca.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := ca.Window().Expand(a.Window())

	// Cloud + !Cloud = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = ca.Adjustment() + a.Adjustment()
	any.Cost = (ca.TotalCost() - ca.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (ca *Cloud) add(that *Cloud) {
	if ca == nil {
		ca = that
		return
	}

	props := ca.Properties().Merge(that.Properties())
	labels := ca.Labels().Merge(that.Labels())

	start := ca.Start()
	if that.Start().Before(start) {
		start = that.Start()
	}
	end := ca.End()
	if that.End().After(end) {
		end = that.End()
	}
	window := ca.Window().Expand(that.Window())

	ca.start = start
	ca.end = end
	ca.window = window
	ca.SetProperties(props)
	ca.SetLabels(labels)
	ca.adjustment += that.adjustment
	ca.Cost += that.Cost
	ca.Credit += that.Credit
}

// Clone returns a cloned instance of the Asset
func (ca *Cloud) Clone() Asset {
	return &Cloud{
		labels:     ca.labels.Clone(),
		properties: ca.properties.Clone(),
		start:      ca.start,
		end:        ca.end,
		window:     ca.window.Clone(),
		adjustment: ca.adjustment,
		Cost:       ca.Cost,
		Credit:     ca.Credit,
	}
}

// Equal returns true if the given Asset precisely equals the Asset
func (ca *Cloud) Equal(a Asset) bool {
	that, ok := a.(*Cloud)
	if !ok {
		return false
	}

	if !ca.Labels().Equal(that.Labels()) {
		return false
	}
	if !ca.Properties().Equal(that.Properties()) {
		return false
	}

	if !ca.start.Equal(that.start) {
		return false
	}
	if !ca.end.Equal(that.end) {
		return false
	}
	if !ca.window.Equal(that.window) {
		return false
	}

	if ca.adjustment != that.adjustment {
		return false
	}

	if ca.Cost != that.Cost {
		return false
	}
	if ca.Credit != that.Credit {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (ca *Cloud) String() string {
	return toString(ca)
}

// ClusterManagement describes a provider's cluster management fee
type ClusterManagement struct {
	labels     AssetLabels
	properties *AssetProperties
	window     Window
	Cost       float64
}

// NewClusterManagement creates and returns a new ClusterManagement instance
func NewClusterManagement(provider, cluster string, window Window) *ClusterManagement {
	properties := &AssetProperties{
		Category: ManagementCategory,
		Provider: ParseProvider(provider),
		Cluster:  cluster,
		Service:  KubernetesService,
	}

	return &ClusterManagement{
		labels:     AssetLabels{},
		properties: properties,
		window:     window.Clone(),
	}
}

// Type returns the Asset's type
func (cm *ClusterManagement) Type() AssetType {
	return ClusterManagementAssetType
}

// Properties returns the Asset's properties
func (cm *ClusterManagement) Properties() *AssetProperties {
	return cm.properties
}

// SetProperties sets the Asset's properties
func (cm *ClusterManagement) SetProperties(props *AssetProperties) {
	cm.properties = props
}

// Labels returns the Asset's labels
func (cm *ClusterManagement) Labels() AssetLabels {
	return cm.labels
}

// SetLabels sets the Asset's properties
func (cm *ClusterManagement) SetLabels(props AssetLabels) {
	cm.labels = props
}

// Adjustment does not apply to ClusterManagement
func (cm *ClusterManagement) Adjustment() float64 {
	return 0.0
}

// SetAdjustment does not apply to ClusterManagement
func (cm *ClusterManagement) SetAdjustment(float64) {
	return
}

// TotalCost returns the Asset's total cost
func (cm *ClusterManagement) TotalCost() float64 {
	return cm.Cost
}

// Start returns the Asset's precise start time within the window
func (cm *ClusterManagement) Start() time.Time {
	return *cm.window.Start()
}

// End returns the Asset's precise end time within the window
func (cm *ClusterManagement) End() time.Time {
	return *cm.window.End()
}

// Minutes returns the number of minutes the Asset ran
func (cm *ClusterManagement) Minutes() float64 {
	return cm.Window().Minutes()
}

// Window return the Asset's window
func (cm *ClusterManagement) Window() Window {
	return cm.window
}

// ExpandWindow expands the Asset's window by the given window
func (cm *ClusterManagement) ExpandWindow(window Window) {
	cm.window = cm.window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields (not applicable here)
func (cm *ClusterManagement) SetStartEnd(start, end time.Time) {
	return
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (cm *ClusterManagement) Add(a Asset) Asset {
	// ClusterManagement + ClusterManagement = ClusterManagement
	if that, ok := a.(*ClusterManagement); ok {
		this := cm.Clone().(*ClusterManagement)
		this.add(that)
		return this
	}

	props := cm.Properties().Merge(a.Properties())
	labels := cm.Labels().Merge(a.Labels())

	start := cm.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := cm.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := cm.Window().Expand(a.Window())

	// ClusterManagement + !ClusterManagement = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = cm.Adjustment() + a.Adjustment()
	any.Cost = (cm.TotalCost() - cm.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (cm *ClusterManagement) add(that *ClusterManagement) {
	if cm == nil {
		cm = that
		return
	}

	props := cm.Properties().Merge(that.Properties())
	labels := cm.Labels().Merge(that.Labels())
	window := cm.Window().Expand(that.Window())

	cm.window = window
	cm.SetProperties(props)
	cm.SetLabels(labels)
	cm.Cost += that.Cost
}

// Clone returns a cloned instance of the Asset
func (cm *ClusterManagement) Clone() Asset {
	return &ClusterManagement{
		labels:     cm.labels.Clone(),
		properties: cm.properties.Clone(),
		window:     cm.window.Clone(),
		Cost:       cm.Cost,
	}
}

// Equal returns true if the given Asset exactly matches the Asset
func (cm *ClusterManagement) Equal(a Asset) bool {
	that, ok := a.(*ClusterManagement)
	if !ok {
		return false
	}

	if !cm.Labels().Equal(that.Labels()) {
		return false
	}
	if !cm.Properties().Equal(that.Properties()) {
		return false
	}

	if !cm.window.Equal(that.window) {
		return false
	}

	if cm.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (cm *ClusterManagement) String() string {
	return toString(cm)
}

// Disk represents an in-cluster disk Asset
type Disk struct {
	labels     AssetLabels
	properties *AssetProperties
	start      time.Time
	end        time.Time
	window     Window
	adjustment float64
	Cost       float64
	ByteHours  float64
	Local      float64
	Breakdown  *Breakdown
}

// NewDisk creates and returns a new Disk Asset
func NewDisk(name, cluster, providerID string, start, end time.Time, window Window) *Disk {
	properties := &AssetProperties{
		Category:   StorageCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &Disk{
		labels:     AssetLabels{},
		properties: properties,
		start:      start,
		end:        end,
		window:     window,
		Breakdown:  &Breakdown{},
	}
}

// Type returns the AssetType of the Asset
func (d *Disk) Type() AssetType {
	return DiskAssetType
}

// Properties returns the Asset's properties
func (d *Disk) Properties() *AssetProperties {
	return d.properties
}

// SetProperties sets the Asset's properties
func (d *Disk) SetProperties(props *AssetProperties) {
	d.properties = props
}

// Labels returns the Asset's labels
func (d *Disk) Labels() AssetLabels {
	return d.labels
}

// SetLabels sets the Asset's labels
func (d *Disk) SetLabels(labels AssetLabels) {
	d.labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (d *Disk) Adjustment() float64 {
	return d.adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (d *Disk) SetAdjustment(adj float64) {
	d.adjustment = adj
}

// TotalCost returns the Asset's total cost
func (d *Disk) TotalCost() float64 {
	return d.Cost + d.adjustment
}

// Start returns the precise start time of the Asset within the window
func (d *Disk) Start() time.Time {
	return d.start
}

// End returns the precise start time of the Asset within the window
func (d *Disk) End() time.Time {
	return d.end
}

// Minutes returns the number of minutes the Asset ran
func (d *Disk) Minutes() float64 {
	diskMins := d.end.Sub(d.start).Minutes()
	windowMins := d.window.Minutes()

	if diskMins > windowMins {
		log.Warningf("Asset ETL: Disk.Minutes exceeds window: %.2f > %.2f", diskMins, windowMins)
		diskMins = windowMins
	}

	if diskMins < 0 {
		diskMins = 0
	}

	return diskMins
}

// Window returns the window within which the Asset
func (d *Disk) Window() Window {
	return d.window
}

// ExpandWindow expands the Asset's window by the given window
func (d *Disk) ExpandWindow(window Window) {
	d.window = d.window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (d *Disk) SetStartEnd(start, end time.Time) {
	if d.Window().Contains(start) {
		d.start = start
	} else {
		log.Warningf("Disk.SetStartEnd: start %s not in %s", start, d.Window())
	}

	if d.Window().Contains(end) {
		d.end = end
	} else {
		log.Warningf("Disk.SetStartEnd: end %s not in %s", end, d.Window())
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (d *Disk) Add(a Asset) Asset {
	// Disk + Disk = Disk
	if that, ok := a.(*Disk); ok {
		this := d.Clone().(*Disk)
		this.add(that)
		return this
	}

	props := d.Properties().Merge(a.Properties())
	labels := d.Labels().Merge(a.Labels())

	start := d.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := d.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := d.Window().Expand(a.Window())

	// Disk + !Disk = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = d.Adjustment() + a.Adjustment()
	any.Cost = (d.TotalCost() - d.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (d *Disk) add(that *Disk) {
	if d == nil {
		d = that
		return
	}

	props := d.Properties().Merge(that.Properties())
	labels := d.Labels().Merge(that.Labels())
	d.SetProperties(props)
	d.SetLabels(labels)

	start := d.Start()
	if that.Start().Before(start) {
		start = that.Start()
	}
	end := d.End()
	if that.End().After(end) {
		end = that.End()
	}
	window := d.Window().Expand(that.Window())
	d.start = start
	d.end = end
	d.window = window

	totalCost := d.Cost + that.Cost
	if totalCost > 0.0 {
		d.Breakdown.Idle = (d.Breakdown.Idle*d.Cost + that.Breakdown.Idle*that.Cost) / totalCost
		d.Breakdown.Other = (d.Breakdown.Other*d.Cost + that.Breakdown.Other*that.Cost) / totalCost
		d.Breakdown.System = (d.Breakdown.System*d.Cost + that.Breakdown.System*that.Cost) / totalCost
		d.Breakdown.User = (d.Breakdown.User*d.Cost + that.Breakdown.User*that.Cost) / totalCost

		d.Local = (d.TotalCost()*d.Local + that.TotalCost()*that.Local) / (d.TotalCost() + that.TotalCost())
	} else {
		d.Local = (d.Local + that.Local) / 2.0
	}

	d.adjustment += that.adjustment
	d.Cost += that.Cost

	d.ByteHours += that.ByteHours
}

// Clone returns a cloned instance of the Asset
func (d *Disk) Clone() Asset {
	return &Disk{
		properties: d.properties.Clone(),
		labels:     d.labels.Clone(),
		start:      d.start,
		end:        d.end,
		window:     d.window.Clone(),
		adjustment: d.adjustment,
		Cost:       d.Cost,
		ByteHours:  d.ByteHours,
		Local:      d.Local,
		Breakdown:  d.Breakdown.Clone(),
	}
}

// Equal returns true if the two Assets match exactly
func (d *Disk) Equal(a Asset) bool {
	that, ok := a.(*Disk)
	if !ok {
		return false
	}

	if !d.Labels().Equal(that.Labels()) {
		return false
	}
	if !d.Properties().Equal(that.Properties()) {
		return false
	}

	if !d.Start().Equal(that.Start()) {
		return false
	}
	if !d.End().Equal(that.End()) {
		return false
	}
	if !d.window.Equal(that.window) {
		return false
	}

	if d.adjustment != that.adjustment {
		return false
	}
	if d.Cost != that.Cost {
		return false
	}

	if d.ByteHours != that.ByteHours {
		return false
	}
	if d.Local != that.Local {
		return false
	}
	if !d.Breakdown.Equal(that.Breakdown) {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (d *Disk) String() string {
	return toString(d)
}

// Bytes returns the number of bytes belonging to the disk. This could be
// fractional because it's the number of byte*hours divided by the number of
// hours running; e.g. the sum of a 100GiB disk running for the first 10 hours
// and a 30GiB disk running for the last 20 hours of the same 24-hour window
// would produce:
//   (100*10 + 30*20) / 24 = 66.667GiB
// However, any number of disks running for the full span of a window will
// report the actual number of bytes of the static disk; e.g. the above
// scenario for one entire 24-hour window:
//   (100*24 + 30*24) / 24 = (100 + 30) = 130GiB
func (d *Disk) Bytes() float64 {
	// [b*hr]*([min/hr]*[1/min]) = [b*hr]/[hr] = b
	return d.ByteHours * (60.0 / d.Minutes())
}

// Breakdown describes a resource's use as a percentage of various usage types
type Breakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

// Clone returns a cloned instance of the Breakdown
func (b *Breakdown) Clone() *Breakdown {
	if b == nil {
		return nil
	}

	return &Breakdown{
		Idle:   b.Idle,
		Other:  b.Other,
		System: b.System,
		User:   b.User,
	}
}

// Equal returns true if the two Breakdowns are exact matches
func (b *Breakdown) Equal(that *Breakdown) bool {
	if b == nil || that == nil {
		return false
	}

	if b.Idle != that.Idle {
		return false
	}
	if b.Other != that.Other {
		return false
	}
	if b.System != that.System {
		return false
	}
	if b.User != that.User {
		return false
	}

	return true
}

// Network is an Asset representing a single node's network costs
type Network struct {
	properties *AssetProperties
	labels     AssetLabels
	start      time.Time
	end        time.Time
	window     Window
	adjustment float64
	Cost       float64
}

// NewNetwork creates and returns a new Network Asset
func NewNetwork(name, cluster, providerID string, start, end time.Time, window Window) *Network {
	properties := &AssetProperties{
		Category:   NetworkCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &Network{
		properties: properties,
		labels:     AssetLabels{},
		start:      start,
		end:        end,
		window:     window.Clone(),
	}
}

// Type returns the AssetType of the Asset
func (n *Network) Type() AssetType {
	return NetworkAssetType
}

// Properties returns the Asset's properties
func (n *Network) Properties() *AssetProperties {
	return n.properties
}

// SetProperties sets the Asset's properties
func (n *Network) SetProperties(props *AssetProperties) {
	n.properties = props
}

// Labels returns the Asset's labels
func (n *Network) Labels() AssetLabels {
	return n.labels
}

// SetLabels sets the Asset's labels
func (n *Network) SetLabels(labels AssetLabels) {
	n.labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (n *Network) Adjustment() float64 {
	return n.adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (n *Network) SetAdjustment(adj float64) {
	n.adjustment = adj
}

// TotalCost returns the Asset's total cost
func (n *Network) TotalCost() float64 {
	return n.Cost + n.adjustment
}

// Start returns the precise start time of the Asset within the window
func (n *Network) Start() time.Time {
	return n.start
}

// End returns the precise end time of the Asset within the window
func (n *Network) End() time.Time {
	return n.end
}

// Minutes returns the number of minutes the Asset ran within the window
func (n *Network) Minutes() float64 {
	netMins := n.end.Sub(n.start).Minutes()
	windowMins := n.window.Minutes()

	if netMins > windowMins {
		log.Warningf("Asset ETL: Network.Minutes exceeds window: %.2f > %.2f", netMins, windowMins)
		netMins = windowMins
	}

	if netMins < 0 {
		netMins = 0
	}

	return netMins
}

// Window returns the window within which the Asset ran
func (n *Network) Window() Window {
	return n.window
}

// ExpandWindow expands the Asset's window by the given window
func (n *Network) ExpandWindow(window Window) {
	n.window = n.window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (n *Network) SetStartEnd(start, end time.Time) {
	if n.Window().Contains(start) {
		n.start = start
	} else {
		log.Warningf("Disk.SetStartEnd: start %s not in %s", start, n.Window())
	}

	if n.Window().Contains(end) {
		n.end = end
	} else {
		log.Warningf("Disk.SetStartEnd: end %s not in %s", end, n.Window())
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (n *Network) Add(a Asset) Asset {
	// Network + Network = Network
	if that, ok := a.(*Network); ok {
		this := n.Clone().(*Network)
		this.add(that)
		return this
	}

	props := n.Properties().Merge(a.Properties())
	labels := n.Labels().Merge(a.Labels())

	start := n.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := n.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := n.Window().Expand(a.Window())

	// Network + !Network = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = n.Adjustment() + a.Adjustment()
	any.Cost = (n.TotalCost() - n.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (n *Network) add(that *Network) {
	if n == nil {
		n = that
		return
	}

	props := n.Properties().Merge(that.Properties())
	labels := n.Labels().Merge(that.Labels())
	n.SetProperties(props)
	n.SetLabels(labels)

	start := n.Start()
	if that.Start().Before(start) {
		start = that.Start()
	}
	end := n.End()
	if that.End().After(end) {
		end = that.End()
	}
	window := n.Window().Expand(that.Window())
	n.start = start
	n.end = end
	n.window = window

	n.Cost += that.Cost
	n.adjustment += that.adjustment
}

// Clone returns a deep copy of the given Network
func (n *Network) Clone() Asset {
	if n == nil {
		return nil
	}

	return &Network{
		properties: n.properties.Clone(),
		labels:     n.labels.Clone(),
		start:      n.start,
		end:        n.end,
		window:     n.window.Clone(),
		adjustment: n.adjustment,
		Cost:       n.Cost,
	}
}

// Equal returns true if the tow Assets match exactly
func (n *Network) Equal(a Asset) bool {
	that, ok := a.(*Network)
	if !ok {
		return false
	}

	if !n.Labels().Equal(that.Labels()) {
		return false
	}
	if !n.Properties().Equal(that.Properties()) {
		return false
	}

	if !n.Start().Equal(that.Start()) {
		return false
	}
	if !n.End().Equal(that.End()) {
		return false
	}
	if !n.window.Equal(that.window) {
		return false
	}

	if n.adjustment != that.adjustment {
		return false
	}
	if n.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (n *Network) String() string {
	return toString(n)
}

// Node is an Asset representing a single node in a cluster
type Node struct {
	properties   *AssetProperties
	labels       AssetLabels
	start        time.Time
	end          time.Time
	window       Window
	adjustment   float64
	NodeType     string
	CPUCoreHours float64
	RAMByteHours float64
	GPUHours     float64
	CPUBreakdown *Breakdown
	RAMBreakdown *Breakdown
	CPUCost      float64
	GPUCost      float64
	GPUCount     float64
	RAMCost      float64
	Discount     float64
	Preemptible  float64
}

// NewNode creates and returns a new Node Asset
func NewNode(name, cluster, providerID string, start, end time.Time, window Window) *Node {
	properties := &AssetProperties{
		Category:   ComputeCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &Node{
		properties:   properties,
		labels:       AssetLabels{},
		start:        start,
		end:          end,
		window:       window.Clone(),
		CPUBreakdown: &Breakdown{},
		RAMBreakdown: &Breakdown{},
	}
}

// Type returns the AssetType of the Asset
func (n *Node) Type() AssetType {
	return NodeAssetType
}

// Properties returns the Asset's properties
func (n *Node) Properties() *AssetProperties {
	return n.properties
}

// SetProperties sets the Asset's properties
func (n *Node) SetProperties(props *AssetProperties) {
	n.properties = props
}

// Labels returns the Asset's labels
func (n *Node) Labels() AssetLabels {
	return n.labels
}

// SetLabels sets the Asset's labels
func (n *Node) SetLabels(labels AssetLabels) {
	n.labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (n *Node) Adjustment() float64 {
	return n.adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (n *Node) SetAdjustment(adj float64) {
	n.adjustment = adj
}

// TotalCost returns the Asset's total cost
func (n *Node) TotalCost() float64 {
	return ((n.CPUCost + n.RAMCost) * (1.0 - n.Discount)) + n.GPUCost + n.adjustment
}

// Start returns the precise start time of the Asset within the window
func (n *Node) Start() time.Time {
	return n.start
}

// End returns the precise end time of the Asset within the window
func (n *Node) End() time.Time {
	return n.end
}

// Minutes returns the number of minutes the Asset ran within the window
func (n *Node) Minutes() float64 {
	nodeMins := n.end.Sub(n.start).Minutes()
	windowMins := n.window.Minutes()

	if nodeMins > windowMins {
		log.Warningf("Asset ETL: Node.Minutes exceeds window: %.2f > %.2f", nodeMins, windowMins)
		nodeMins = windowMins
	}

	if nodeMins < 0 {
		nodeMins = 0
	}

	return nodeMins
}

// Window returns the window within which the Asset ran
func (n *Node) Window() Window {
	return n.window
}

// ExpandWindow expands the Asset's window by the given window
func (n *Node) ExpandWindow(window Window) {
	n.window = n.window.Expand(window)
}

// SetStartEnd sets the Asset's Start and End fields
func (n *Node) SetStartEnd(start, end time.Time) {
	if n.Window().Contains(start) {
		n.start = start
	} else {
		log.Warningf("Disk.SetStartEnd: start %s not in %s", start, n.Window())
	}

	if n.Window().Contains(end) {
		n.end = end
	} else {
		log.Warningf("Disk.SetStartEnd: end %s not in %s", end, n.Window())
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (n *Node) Add(a Asset) Asset {
	// Node + Node = Node
	if that, ok := a.(*Node); ok {
		this := n.Clone().(*Node)
		this.add(that)
		return this
	}

	props := n.Properties().Merge(a.Properties())
	labels := n.Labels().Merge(a.Labels())

	start := n.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := n.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := n.Window().Expand(a.Window())

	// Node + !Node = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = n.Adjustment() + a.Adjustment()
	any.Cost = (n.TotalCost() - n.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (n *Node) add(that *Node) {
	if n == nil {
		n = that
		return
	}

	props := n.Properties().Merge(that.Properties())
	labels := n.Labels().Merge(that.Labels())
	n.SetProperties(props)
	n.SetLabels(labels)

	if n.NodeType != that.NodeType {
		n.NodeType = ""
	}

	start := n.Start()
	if that.Start().Before(start) {
		start = that.Start()
	}
	end := n.End()
	if that.End().After(end) {
		end = that.End()
	}
	window := n.Window().Expand(that.Window())
	n.start = start
	n.end = end
	n.window = window

	// Order of operations for node costs is:
	//   Discount(CPU + RAM) + GPU + Adjustment

	// Combining discounts, then involves weighting each discount by each
	// respective (CPU + RAM) cost. Combining preemptible, on the other
	// hand, is done with all three (but not Adjustment, which can change
	// without triggering a re-computation of Preemtible).

	disc := (n.CPUCost+n.RAMCost)*(1.0-n.Discount) + (that.CPUCost+that.RAMCost)*(1.0-that.Discount)
	nonDisc := (n.CPUCost + n.RAMCost) + (that.CPUCost + that.RAMCost)
	if nonDisc > 0 {
		n.Discount = 1.0 - (disc / nonDisc)
	} else {
		n.Discount = (n.Discount + that.Discount) / 2.0
	}

	nNoAdj := n.TotalCost() - n.Adjustment()
	thatNoAdj := that.TotalCost() - that.Adjustment()
	if (nNoAdj + thatNoAdj) > 0 {
		n.Preemptible = (nNoAdj*n.Preemptible + thatNoAdj*that.Preemptible) / (nNoAdj + thatNoAdj)
	} else {
		n.Preemptible = (n.Preemptible + that.Preemptible) / 2.0
	}

	totalCPUCost := n.CPUCost + that.CPUCost
	if totalCPUCost > 0.0 {
		n.CPUBreakdown.Idle = (n.CPUBreakdown.Idle*n.CPUCost + that.CPUBreakdown.Idle*that.CPUCost) / totalCPUCost
		n.CPUBreakdown.Other = (n.CPUBreakdown.Other*n.CPUCost + that.CPUBreakdown.Other*that.CPUCost) / totalCPUCost
		n.CPUBreakdown.System = (n.CPUBreakdown.System*n.CPUCost + that.CPUBreakdown.System*that.CPUCost) / totalCPUCost
		n.CPUBreakdown.User = (n.CPUBreakdown.User*n.CPUCost + that.CPUBreakdown.User*that.CPUCost) / totalCPUCost
	}

	totalRAMCost := n.RAMCost + that.RAMCost
	if totalRAMCost > 0.0 {
		n.RAMBreakdown.Idle = (n.RAMBreakdown.Idle*n.RAMCost + that.RAMBreakdown.Idle*that.RAMCost) / totalRAMCost
		n.RAMBreakdown.Other = (n.RAMBreakdown.Other*n.RAMCost + that.RAMBreakdown.Other*that.RAMCost) / totalRAMCost
		n.RAMBreakdown.System = (n.RAMBreakdown.System*n.RAMCost + that.RAMBreakdown.System*that.RAMCost) / totalRAMCost
		n.RAMBreakdown.User = (n.RAMBreakdown.User*n.RAMCost + that.RAMBreakdown.User*that.RAMCost) / totalRAMCost
	}

	n.CPUCoreHours += that.CPUCoreHours
	n.RAMByteHours += that.RAMByteHours
	n.GPUHours += that.GPUHours

	n.CPUCost += that.CPUCost
	n.GPUCost += that.GPUCost
	n.RAMCost += that.RAMCost
	n.adjustment += that.adjustment
}

// Clone returns a deep copy of the given Node
func (n *Node) Clone() Asset {
	if n == nil {
		return nil
	}

	return &Node{
		properties:   n.properties.Clone(),
		labels:       n.labels.Clone(),
		start:        n.start,
		end:          n.end,
		window:       n.window.Clone(),
		adjustment:   n.adjustment,
		NodeType:     n.NodeType,
		CPUCoreHours: n.CPUCoreHours,
		RAMByteHours: n.RAMByteHours,
		GPUHours:     n.GPUHours,
		CPUBreakdown: n.CPUBreakdown.Clone(),
		RAMBreakdown: n.RAMBreakdown.Clone(),
		CPUCost:      n.CPUCost,
		GPUCost:      n.GPUCost,
		GPUCount:     n.GPUCount,
		RAMCost:      n.RAMCost,
		Preemptible:  n.Preemptible,
		Discount:     n.Discount,
	}
}

// Equal returns true if the tow Assets match exactly
func (n *Node) Equal(a Asset) bool {
	that, ok := a.(*Node)
	if !ok {
		return false
	}

	if !n.Labels().Equal(that.Labels()) {
		return false
	}
	if !n.Properties().Equal(that.Properties()) {
		return false
	}

	if !n.Start().Equal(that.Start()) {
		return false
	}
	if !n.End().Equal(that.End()) {
		return false
	}
	if !n.window.Equal(that.window) {
		return false
	}

	if n.adjustment != that.adjustment {
		return false
	}

	if n.NodeType != that.NodeType {
		return false
	}
	if n.CPUCoreHours != that.CPUCoreHours {
		return false
	}
	if n.RAMByteHours != that.RAMByteHours {
		return false
	}
	if n.GPUHours != that.GPUHours {
		return false
	}
	if !n.CPUBreakdown.Equal(that.CPUBreakdown) {
		return false
	}
	if !n.RAMBreakdown.Equal(that.RAMBreakdown) {
		return false
	}
	if n.CPUCost != that.CPUCost {
		return false
	}
	if n.GPUCost != that.GPUCost {
		return false
	}
	if n.RAMCost != that.RAMCost {
		return false
	}
	if n.Discount != that.Discount {
		return false
	}
	if n.Preemptible != that.Preemptible {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (n *Node) String() string {
	return toString(n)
}

// IsPreemptible returns true if the node is 100% preemptible. It's possible
// to be "partially preemptible" by adding a preemptible node with a
// non-preemptible node.
func (n *Node) IsPreemptible() bool {
	return n.Preemptible == 1.0
}

// CPUCores returns the number of cores belonging to the node. This could be
// fractional because it's the number of core*hours divided by the number of
// hours running; e.g. the sum of a 4-core node running for the first 10 hours
// and a 3-core node running for the last 20 hours of the same 24-hour window
// would produce:
//   (4*10 + 3*20) / 24 = 4.167 cores
// However, any number of cores running for the full span of a window will
// report the actual number of cores of the static node; e.g. the above
// scenario for one entire 24-hour window:
//   (4*24 + 3*24) / 24 = (4 + 3) = 7 cores
func (n *Node) CPUCores() float64 {
	// [core*hr]*([min/hr]*[1/min]) = [core*hr]/[hr] = core
	return n.CPUCoreHours * (60.0 / n.Minutes())
}

// RAMBytes returns the amount of RAM belonging to the node. This could be
// fractional because it's the number of byte*hours divided by the number of
// hours running; e.g. the sum of a 12GiB-RAM node running for the first 10 hours
// and a 16GiB-RAM node running for the last 20 hours of the same 24-hour window
// would produce:
//   (12*10 + 16*20) / 24 = 18.333GiB RAM
// However, any number of bytes running for the full span of a window will
// report the actual number of bytes of the static node; e.g. the above
// scenario for one entire 24-hour window:
//   (12*24 + 16*24) / 24 = (12 + 16) = 28GiB RAM
func (n *Node) RAMBytes() float64 {
	// [b*hr]*([min/hr]*[1/min]) = [b*hr]/[hr] = b
	return n.RAMByteHours * (60.0 / n.Minutes())
}

// GPUs returns the amount of GPUs belonging to the node. This could be
// fractional because it's the number of gpu*hours divided by the number of
// hours running; e.g. the sum of a 2 gpu node running for the first 10 hours
// and a 1 gpu node running for the last 20 hours of the same 24-hour window
// would produce:
//   (2*10 + 1*20) / 24 = 1.667 GPUs
// However, any number of GPUs running for the full span of a window will
// report the actual number of GPUs of the static node; e.g. the above
// scenario for one entire 24-hour window:
//   (2*24 + 1*24) / 24 = (2 + 1) = 3 GPUs
func (n *Node) GPUs() float64 {
	// [b*hr]*([min/hr]*[1/min]) = [b*hr]/[hr] = b
	return n.GPUHours * (60.0 / n.Minutes())
}

// LoadBalancer is an Asset representing a single load balancer in a cluster
// TODO: add GB of ingress processed, numForwardingRules once we start recording those to prometheus metric
type LoadBalancer struct {
	properties *AssetProperties
	labels     AssetLabels
	start      time.Time
	end        time.Time
	window     Window
	adjustment float64
	Cost       float64
}

// NewLoadBalancer instantiates and returns a new LoadBalancer
func NewLoadBalancer(name, cluster, providerID string, start, end time.Time, window Window) *LoadBalancer {
	properties := &AssetProperties{
		Category:   NetworkCategory,
		Name:       name,
		Cluster:    cluster,
		ProviderID: providerID,
		Service:    KubernetesService,
	}

	return &LoadBalancer{
		properties: properties,
		labels:     AssetLabels{},
		start:      start,
		end:        end,
		window:     window,
	}
}

// Type returns the AssetType of the Asset
func (lb *LoadBalancer) Type() AssetType {
	return LoadBalancerAssetType
}

// Properties returns the Asset's properties
func (lb *LoadBalancer) Properties() *AssetProperties {
	return lb.properties
}

// SetProperties sets the Asset's properties
func (lb *LoadBalancer) SetProperties(props *AssetProperties) {
	lb.properties = props
}

// Labels returns the Asset's labels
func (lb *LoadBalancer) Labels() AssetLabels {
	return lb.labels
}

// SetLabels sets the Asset's labels
func (lb *LoadBalancer) SetLabels(labels AssetLabels) {
	lb.labels = labels
}

// Adjustment returns the Asset's cost adjustment
func (lb *LoadBalancer) Adjustment() float64 {
	return lb.adjustment
}

// SetAdjustment sets the Asset's cost adjustment
func (lb *LoadBalancer) SetAdjustment(adj float64) {
	lb.adjustment = adj
}

// TotalCost returns the total cost of the Asset
func (lb *LoadBalancer) TotalCost() float64 {
	return lb.Cost + lb.adjustment
}

// Start returns the preceise start point of the Asset within the window
func (lb *LoadBalancer) Start() time.Time {
	return lb.start
}

// End returns the preceise end point of the Asset within the window
func (lb *LoadBalancer) End() time.Time {
	return lb.end
}

// Minutes returns the number of minutes the Asset ran within the window
func (lb *LoadBalancer) Minutes() float64 {
	return lb.end.Sub(lb.start).Minutes()
}

// Window returns the window within which the Asset ran
func (lb *LoadBalancer) Window() Window {
	return lb.window
}

// ExpandWindow expands the Asset's window by the given window
func (lb *LoadBalancer) ExpandWindow(w Window) {
	lb.window = lb.window.Expand(w)
}

// SetStartEnd sets the Asset's Start and End fields
func (lb *LoadBalancer) SetStartEnd(start, end time.Time) {
	if lb.Window().Contains(start) {
		lb.start = start
	} else {
		log.Warningf("Disk.SetStartEnd: start %s not in %s", start, lb.Window())
	}

	if lb.Window().Contains(end) {
		lb.end = end
	} else {
		log.Warningf("Disk.SetStartEnd: end %s not in %s", end, lb.Window())
	}
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (lb *LoadBalancer) Add(a Asset) Asset {
	// LoadBalancer + LoadBalancer = LoadBalancer
	if that, ok := a.(*LoadBalancer); ok {
		this := lb.Clone().(*LoadBalancer)
		this.add(that)
		return this
	}

	props := lb.Properties().Merge(a.Properties())
	labels := lb.Labels().Merge(a.Labels())

	start := lb.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := lb.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := lb.Window().Expand(a.Window())

	// LoadBalancer + !LoadBalancer = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = lb.Adjustment() + a.Adjustment()
	any.Cost = (lb.TotalCost() - lb.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (lb *LoadBalancer) add(that *LoadBalancer) {
	if lb == nil {
		lb = that
		return
	}

	props := lb.Properties().Merge(that.Properties())
	labels := lb.Labels().Merge(that.Labels())
	lb.SetProperties(props)
	lb.SetLabels(labels)

	start := lb.Start()
	if that.Start().Before(start) {
		start = that.Start()
	}
	end := lb.End()
	if that.End().After(end) {
		end = that.End()
	}
	window := lb.Window().Expand(that.Window())
	lb.start = start
	lb.end = end
	lb.window = window

	lb.Cost += that.Cost
	lb.adjustment += that.adjustment
}

// Clone returns a cloned instance of the given Asset
func (lb *LoadBalancer) Clone() Asset {
	return &LoadBalancer{
		properties: lb.properties.Clone(),
		labels:     lb.labels.Clone(),
		start:      lb.start,
		end:        lb.end,
		window:     lb.window.Clone(),
		adjustment: lb.adjustment,
		Cost:       lb.Cost,
	}
}

// Equal returns true if the tow Assets match precisely
func (lb *LoadBalancer) Equal(a Asset) bool {
	that, ok := a.(*LoadBalancer)
	if !ok {
		return false
	}

	if !lb.Labels().Equal(that.Labels()) {
		return false
	}
	if !lb.Properties().Equal(that.Properties()) {
		return false
	}

	if !lb.Start().Equal(that.Start()) {
		return false
	}
	if !lb.End().Equal(that.End()) {
		return false
	}
	if !lb.window.Equal(that.window) {
		return false
	}

	if lb.adjustment != that.adjustment {
		return false
	}
	if lb.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (lb *LoadBalancer) String() string {
	return toString(lb)
}

// SharedAsset is an Asset representing a shared cost
type SharedAsset struct {
	properties *AssetProperties
	labels     AssetLabels
	window     Window
	Cost       float64
}

// NewSharedAsset creates and returns a new SharedAsset
func NewSharedAsset(name string, window Window) *SharedAsset {
	properties := &AssetProperties{
		Name:     name,
		Category: SharedCategory,
		Service:  OtherCategory,
	}

	return &SharedAsset{
		properties: properties,
		labels:     AssetLabels{},
		window:     window.Clone(),
	}
}

// Type returns the AssetType of the Asset
func (sa *SharedAsset) Type() AssetType {
	return SharedAssetType
}

// Properties returns the Asset's properties
func (sa *SharedAsset) Properties() *AssetProperties {
	return sa.properties
}

// SetProperties sets the Asset's properties
func (sa *SharedAsset) SetProperties(props *AssetProperties) {
	sa.properties = props
}

// Labels returns the Asset's labels
func (sa *SharedAsset) Labels() AssetLabels {
	return sa.labels
}

// SetLabels sets the Asset's labels
func (sa *SharedAsset) SetLabels(labels AssetLabels) {
	sa.labels = labels
}

// Adjustment is not relevant to SharedAsset, but required to implement Asset
func (sa *SharedAsset) Adjustment() float64 {
	return 0.0
}

// SetAdjustment is not relevant to SharedAsset, but required to implement Asset
func (sa *SharedAsset) SetAdjustment(float64) {
	return
}

// TotalCost returns the Asset's total cost
func (sa *SharedAsset) TotalCost() float64 {
	return sa.Cost
}

// Start returns the start time of the Asset
func (sa *SharedAsset) Start() time.Time {
	return *sa.window.start
}

// End returns the end time of the Asset
func (sa *SharedAsset) End() time.Time {
	return *sa.window.end
}

// Minutes returns the number of minutes the SharedAsset ran within the window
func (sa *SharedAsset) Minutes() float64 {
	return sa.window.Minutes()
}

// Window returns the window within the SharedAsset ran
func (sa *SharedAsset) Window() Window {
	return sa.window
}

// ExpandWindow expands the Asset's window
func (sa *SharedAsset) ExpandWindow(w Window) {
	sa.window = sa.window.Expand(w)
}

// SetStartEnd sets the Asset's Start and End fields (not applicable here)
func (sa *SharedAsset) SetStartEnd(start, end time.Time) {
	return
}

// Add sums the Asset with the given Asset to produce a new Asset, maintaining
// as much relevant information as possible (i.e. type, properties, labels).
func (sa *SharedAsset) Add(a Asset) Asset {
	// SharedAsset + SharedAsset = SharedAsset
	if that, ok := a.(*SharedAsset); ok {
		this := sa.Clone().(*SharedAsset)
		this.add(that)
		return this
	}

	props := sa.Properties().Merge(a.Properties())
	labels := sa.Labels().Merge(a.Labels())

	start := sa.Start()
	if a.Start().Before(start) {
		start = a.Start()
	}
	end := sa.End()
	if a.End().After(end) {
		end = a.End()
	}
	window := sa.Window().Expand(a.Window())

	// SharedAsset + !SharedAsset = Any
	any := NewAsset(start, end, window)
	any.SetProperties(props)
	any.SetLabels(labels)
	any.adjustment = sa.Adjustment() + a.Adjustment()
	any.Cost = (sa.TotalCost() - sa.Adjustment()) + (a.TotalCost() - a.Adjustment())

	return any
}

func (sa *SharedAsset) add(that *SharedAsset) {
	if sa == nil {
		sa = that
		return
	}

	props := sa.Properties().Merge(that.Properties())
	labels := sa.Labels().Merge(that.Labels())
	sa.SetProperties(props)
	sa.SetLabels(labels)

	window := sa.Window().Expand(that.Window())
	sa.window = window

	sa.Cost += that.Cost
}

// Clone returns a deep copy of the given SharedAsset
func (sa *SharedAsset) Clone() Asset {
	if sa == nil {
		return nil
	}

	return &SharedAsset{
		properties: sa.properties.Clone(),
		labels:     sa.labels.Clone(),
		window:     sa.window.Clone(),
		Cost:       sa.Cost,
	}
}

// Equal returns true if the two Assets are exact matches
func (sa *SharedAsset) Equal(a Asset) bool {
	that, ok := a.(*SharedAsset)
	if !ok {
		return false
	}

	if !sa.Labels().Equal(that.Labels()) {
		return false
	}
	if !sa.Properties().Equal(that.Properties()) {
		return false
	}

	if !sa.Start().Equal(that.Start()) {
		return false
	}
	if !sa.End().Equal(that.End()) {
		return false
	}
	if !sa.window.Equal(that.window) {
		return false
	}

	if sa.Cost != that.Cost {
		return false
	}

	return true
}

// String implements fmt.Stringer
func (sa *SharedAsset) String() string {
	return toString(sa)
}

// This type exists because only the assets map of AssetSet is marshaled to
// json, which makes it impossible to recreate an AssetSet struct. Thus,
// the type when unmarshaling a marshaled AssetSet,is AssetSetResponse
type AssetSetResponse struct {
	Assets map[string]Asset
}

// AssetSet stores a set of Assets, each with a unique name, that share
// a window. An AssetSet is mutable, so treat it like a threadsafe map.
type AssetSet struct {
	sync.RWMutex
	aggregateBy []string
	assets      map[string]Asset
	FromSource  string // stores the name of the source used to compute the data
	Window      Window
	Warnings    []string
	Errors      []string
}

// NewAssetSet instantiates a new AssetSet and, optionally, inserts
// the given list of Assets
func NewAssetSet(start, end time.Time, assets ...Asset) *AssetSet {
	as := &AssetSet{
		assets: map[string]Asset{},
		Window: NewWindow(&start, &end),
	}

	for _, a := range assets {
		as.Insert(a)
	}

	return as
}

// AggregateBy aggregates the Assets in the AssetSet by the given list of
// AssetProperties, such that each asset is binned by a key determined by its
// relevant property values.
func (as *AssetSet) AggregateBy(aggregateBy []string, opts *AssetAggregationOptions) error {
	if opts == nil {
		opts = &AssetAggregationOptions{}
	}

	if as.IsEmpty() && len(opts.SharedHourlyCosts) == 0 {
		return nil
	}

	as.Lock()
	defer as.Unlock()

	aggSet := NewAssetSet(as.Start(), as.End())
	aggSet.aggregateBy = aggregateBy

	// Compute hours of the given AssetSet, and if it ends in the future,
	// adjust the hours accordingly
	hours := as.Window.Minutes() / 60.0
	diff := time.Since(as.End())
	if diff < 0.0 {
		hours += diff.Hours()
	}

	// Insert a shared asset for each shared cost
	for name, hourlyCost := range opts.SharedHourlyCosts {
		sa := NewSharedAsset(name, as.Window.Clone())
		sa.Cost = hourlyCost * hours

		err := aggSet.Insert(sa)
		if err != nil {
			return err
		}
	}

	// Delete the Assets that don't pass each filter
	for _, ff := range opts.FilterFuncs {
		for key, asset := range as.assets {
			if !ff(asset) {
				delete(as.assets, key)
			}
		}
	}

	// Insert each asset into the new set, which will be keyed by the `aggregateBy`
	// on aggSet, resulting in aggregation.
	for _, asset := range as.assets {
		err := aggSet.Insert(asset)
		if err != nil {
			return err
		}
	}

	// Assign the aggregated values back to the original set
	as.assets = aggSet.assets
	as.aggregateBy = aggregateBy

	return nil
}

// Clone returns a new AssetSet with a deep copy of the given
// AssetSet's assets.
func (as *AssetSet) Clone() *AssetSet {
	if as == nil {
		return nil
	}

	as.RLock()
	defer as.RUnlock()

	var aggregateBy []string
	if as.aggregateBy != nil {
		aggregateBy = append([]string{}, as.aggregateBy...)
	}

	assets := make(map[string]Asset, len(as.assets))
	for k, v := range as.assets {
		assets[k] = v.Clone()
	}

	s := as.Start()
	e := as.End()

	var errors []string
	var warnings []string

	if as.Errors != nil {
		errors = make([]string, len(as.Errors))
		copy(errors, as.Errors)
	} else {
		errors = nil
	}

	if as.Warnings != nil {
		warnings := make([]string, len(as.Warnings))
		copy(warnings, as.Warnings)
	} else {
		warnings = nil
	}

	return &AssetSet{
		Window:      NewWindow(&s, &e),
		aggregateBy: aggregateBy,
		assets:      assets,
		Errors:      errors,
		Warnings:    warnings,
	}
}

// Each invokes the given function for each Asset in the set
func (as *AssetSet) Each(f func(string, Asset)) {
	if as == nil {
		return
	}

	for k, a := range as.assets {
		f(k, a)
	}
}

// End returns the end time of the AssetSet's window
func (as *AssetSet) End() time.Time {
	return *as.Window.End()
}

// FindMatch attempts to find a match in the AssetSet for the given Asset on
// the provided properties and labels. If a match is not found, FindMatch
// returns nil and a Not Found error.
func (as *AssetSet) FindMatch(query Asset, aggregateBy []string) (Asset, error) {
	as.RLock()
	defer as.RUnlock()

	matchKey, err := key(query, aggregateBy)
	if err != nil {
		return nil, err
	}
	for _, asset := range as.assets {
		if k, err := key(asset, aggregateBy); err != nil {
			return nil, err
		} else if k == matchKey {
			return asset, nil
		}
	}

	return nil, fmt.Errorf("Asset not found to match %s on %v", query, aggregateBy)
}

// ReconciliationMatch attempts to find an exact match in the AssetSet on
// (Category, ProviderID). If a match is found, it returns the Asset with the
// intent to adjust it. If no match exists, it attempts to find one on only
// (ProviderID). If that match is found, it returns the Asset with the intent
// to insert the associated Cloud cost.
func (as *AssetSet) ReconciliationMatch(query Asset) (Asset, bool, error) {
	as.RLock()
	defer as.RUnlock()

	// Full match means matching on (Category, ProviderID)
	fullMatchProps := []string{string(AssetCategoryProp), string(AssetProviderIDProp)}
	fullMatchKey, err := key(query, fullMatchProps)

	// This should never happen because we are using enumerated properties,
	// but the check is here in case that changes
	if err != nil {
		return nil, false, err
	}

	// Partial match means matching only on (ProviderID)
	providerIDMatchProps := []string{string(AssetProviderIDProp)}
	providerIDMatchKey, err := key(query, providerIDMatchProps)

	// This should never happen because we are using enumerated properties,
	// but the check is here in case that changes
	if err != nil {
		return nil, false, err
	}

	var providerIDMatch Asset
	for _, asset := range as.assets {
		// Ignore cloud assets when looking for reconciliation matches
		if asset.Type() == CloudAssetType {
			continue
		}
		if k, err := key(asset, fullMatchProps); err != nil {
			return nil, false, err
		} else if k == fullMatchKey {
			log.DedupedInfof(10, "Asset ETL: Reconciliation[rcnw]: ReconcileRange Match: %s", fullMatchKey)
			return asset, true, nil
		}
		if k, err := key(asset, providerIDMatchProps); err != nil {
			return nil, false, err
		} else if k == providerIDMatchKey {
			// Found a partial match. Save it until after all other options
			// have been checked for full matches.
			providerIDMatch = asset
		}
	}

	// No full match was found, so return partial match, if found.
	if providerIDMatch != nil {
		return providerIDMatch, false, nil
	}

	return nil, false, fmt.Errorf("Asset not found to match %s", query)
}

// Get returns the Asset in the AssetSet at the given key, or nil and false
// if no Asset exists for the given key
func (as *AssetSet) Get(key string) (Asset, bool) {
	as.RLock()
	defer as.RUnlock()

	if a, ok := as.assets[key]; ok {
		return a, true
	}
	return nil, false
}

// Insert inserts the given Asset into the AssetSet, using the AssetSet's
// configured properties to determine the key under which the Asset will
// be inserted.
func (as *AssetSet) Insert(asset Asset) error {
	if as == nil {
		return fmt.Errorf("cannot Insert into nil AssetSet")
	}

	as.Lock()
	defer as.Unlock()

	if as.assets == nil {
		as.assets = map[string]Asset{}
	}

	// Determine key into which to Insert the Asset.
	k, err := key(asset, as.aggregateBy)
	if err != nil {
		return err
	}

	// Add the given Asset to the existing entry, if there is one;
	// otherwise just set directly into assets
	if _, ok := as.assets[k]; !ok {
		as.assets[k] = asset
	} else {
		as.assets[k] = as.assets[k].Add(asset)
	}

	// Expand the window, just to be safe. It's possible that the asset will
	// be set into the map without expanding it to the AssetSet's window.
	as.assets[k].ExpandWindow(as.Window)

	return nil
}

// IsEmpty returns true if the AssetSet is nil, or if it contains
// zero assets.
func (as *AssetSet) IsEmpty() bool {
	if as == nil || len(as.assets) == 0 {
		return true
	}

	as.RLock()
	defer as.RUnlock()
	return as.assets == nil || len(as.assets) == 0
}

func (as *AssetSet) Length() int {
	if as == nil {
		return 0
	}

	as.RLock()
	defer as.RUnlock()
	return len(as.assets)
}

// Map clones and returns a map of the AssetSet's Assets
func (as *AssetSet) Map() map[string]Asset {
	if as.IsEmpty() {
		return map[string]Asset{}
	}

	return as.Clone().assets
}

func (as *AssetSet) Set(asset Asset, aggregateBy []string) error {
	if as.IsEmpty() {
		as.Lock()
		as.assets = map[string]Asset{}
		as.Unlock()
	}

	as.Lock()
	defer as.Unlock()

	// Expand the window to match the AssetSet, then set it
	asset.ExpandWindow(as.Window)
	k, err := key(asset, aggregateBy)
	if err != nil {
		return err
	}
	as.assets[k] = asset
	return nil
}

func (as *AssetSet) Start() time.Time {
	return *as.Window.Start()
}

func (as *AssetSet) TotalCost() float64 {
	tc := 0.0

	as.Lock()
	defer as.Unlock()

	for _, a := range as.assets {
		tc += a.TotalCost()
	}

	return tc
}

func (as *AssetSet) UTCOffset() time.Duration {
	_, zone := as.Start().Zone()
	return time.Duration(zone) * time.Second
}

func (as *AssetSet) accumulate(that *AssetSet) (*AssetSet, error) {
	if as == nil {
		return that, nil
	}

	if that == nil {
		return as, nil
	}

	// In the case of an AssetSetRange with empty entries, we may end up with
	// an incoming `as` without an `aggregateBy`, even though we are tring to
	// aggregate here. This handles that case by assigning the correct `aggregateBy`.
	if !sameContents(as.aggregateBy, that.aggregateBy) {
		if len(as.aggregateBy) == 0 {
			as.aggregateBy = that.aggregateBy
		}
	}

	// Set start, end to min(start), max(end)
	start := as.Start()
	end := as.End()

	if that.Start().Before(start) {
		start = that.Start()
	}

	if that.End().After(end) {
		end = that.End()
	}

	if as.IsEmpty() && that.IsEmpty() {
		return NewAssetSet(start, end), nil
	}

	acc := NewAssetSet(start, end)
	acc.aggregateBy = as.aggregateBy

	as.RLock()
	defer as.RUnlock()

	that.RLock()
	defer that.RUnlock()

	for _, asset := range as.assets {
		err := acc.Insert(asset)
		if err != nil {
			return nil, err
		}
	}

	for _, asset := range that.assets {
		err := acc.Insert(asset)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// AssetSetRange is a thread-safe slice of AssetSets. It is meant to
// be used such that the AssetSets held are consecutive and coherent with
// respect to using the same aggregation properties, UTC offset, and
// resolution. However these rules are not necessarily enforced, so use wisely.
type AssetSetRange struct {
	sync.RWMutex
	assets    []*AssetSet
	FromStore string // stores the name of the store used to retrieve the data
}

// NewAssetSetRange instantiates a new range composed of the given
// AssetSets in the order provided.
func NewAssetSetRange(assets ...*AssetSet) *AssetSetRange {
	return &AssetSetRange{
		assets: assets,
	}
}

// Accumulate sums each AssetSet in the given range, returning a single cumulative
// AssetSet for the entire range.
func (asr *AssetSetRange) Accumulate() (*AssetSet, error) {
	var assetSet *AssetSet
	var err error

	asr.RLock()
	defer asr.RUnlock()

	for _, as := range asr.assets {
		assetSet, err = assetSet.accumulate(as)
		if err != nil {
			return nil, err
		}
	}

	return assetSet, nil
}

type AssetAggregationOptions struct {
	SharedHourlyCosts map[string]float64
	FilterFuncs       []AssetMatchFunc
}

func (asr *AssetSetRange) AggregateBy(aggregateBy []string, opts *AssetAggregationOptions) error {
	aggRange := &AssetSetRange{assets: []*AssetSet{}}

	asr.Lock()
	defer asr.Unlock()

	for _, as := range asr.assets {
		err := as.AggregateBy(aggregateBy, opts)
		if err != nil {
			return err
		}

		aggRange.assets = append(aggRange.assets, as)
	}

	asr.assets = aggRange.assets

	return nil
}

func (asr *AssetSetRange) Append(that *AssetSet) {
	asr.Lock()
	defer asr.Unlock()
	asr.assets = append(asr.assets, that)
}

// Each invokes the given function for each AssetSet in the range
func (asr *AssetSetRange) Each(f func(int, *AssetSet)) {
	if asr == nil {
		return
	}

	for i, as := range asr.assets {
		f(i, as)
	}
}

func (asr *AssetSetRange) Get(i int) (*AssetSet, error) {
	if i < 0 || i >= len(asr.assets) {
		return nil, fmt.Errorf("AssetSetRange: index out of range: %d", i)
	}

	asr.RLock()
	defer asr.RUnlock()
	return asr.assets[i], nil
}

func (asr *AssetSetRange) Length() int {
	if asr == nil || asr.assets == nil {
		return 0
	}

	asr.RLock()
	defer asr.RUnlock()
	return len(asr.assets)
}

// InsertRange merges the given AssetSetRange into the receiving one by
// lining up sets with matching windows, then inserting each asset from
// the given ASR into the respective set in the receiving ASR. If the given
// ASR contains an AssetSetRange from a window that does not exist in the
// receiving ASR, then an error is returned. However, the given ASR does not
// need to cover the full range of the receiver.
func (asr *AssetSetRange) InsertRange(that *AssetSetRange) error {
	if asr == nil {
		return fmt.Errorf("cannot insert range into nil AssetSetRange")
	}

	// keys maps window to index in asr
	keys := map[string]int{}
	asr.Each(func(i int, as *AssetSet) {
		if as == nil {
			return
		}
		keys[as.Window.String()] = i
	})

	// Nothing to merge, so simply return
	if len(keys) == 0 {
		return nil
	}

	var err error
	that.Each(func(j int, thatAS *AssetSet) {
		if thatAS == nil || err != nil {
			return
		}

		// Find matching AssetSet in asr
		i, ok := keys[thatAS.Window.String()]
		if !ok {
			err = fmt.Errorf("cannot merge AssetSet into window that does not exist: %s", thatAS.Window.String())
			return
		}
		as, err := asr.Get(i)
		if err != nil {
			err = fmt.Errorf("AssetSetRange index does not exist: %d", i)
			return
		}

		// Insert each Asset from the given set
		thatAS.Each(func(k string, asset Asset) {
			err = as.Insert(asset)
			if err != nil {
				err = fmt.Errorf("error inserting asset: %s", err)
				return
			}
		})
	})

	// err might be nil
	return err
}

// IsEmpty returns false if AssetSetRange contains a single AssetSet that is not empty
func (asr *AssetSetRange) IsEmpty() bool {
	if asr == nil || asr.Length() == 0 {
		return true
	}
	asr.RLock()
	defer asr.RUnlock()
	for _, asset := range asr.assets {
		if !asset.IsEmpty() {
			return false
		}
	}
	return true
}

func (asr *AssetSetRange) MarshalJSON() ([]byte, error) {
	asr.RLock()
	defer asr.RUnlock()
	return json.Marshal(asr.assets)
}

// As with AssetSet, AssetSetRange does not serialize all its fields,
// making it impossible to reconstruct the AssetSetRange from its json.
// Therefore, the type a marshaled AssetSetRange unmarshals to is
// AssetSetRangeResponse
type AssetSetRangeResponse struct {
	Assets []*AssetSetResponse
}

func (asr *AssetSetRange) UTCOffset() time.Duration {
	if asr.Length() == 0 {
		return 0
	}

	as, err := asr.Get(0)
	if err != nil {
		return 0
	}
	return as.UTCOffset()
}

// Window returns the full window that the AssetSetRange spans, from the
// start of the first AssetSet to the end of the last one.
func (asr *AssetSetRange) Window() Window {
	if asr == nil || asr.Length() == 0 {
		return NewWindow(nil, nil)
	}

	start := asr.assets[0].Start()
	end := asr.assets[asr.Length()-1].End()

	return NewWindow(&start, &end)
}

// Start returns the earliest start of all Assets in the AssetSetRange.
// It returns an error if there are no assets
func (asr *AssetSetRange) Start() (time.Time, error) {
	start := time.Time{}
	firstStartNotSet := true
	asr.Each(func(i int, as *AssetSet) {
		as.Each(func(s string, a Asset) {
			if firstStartNotSet {
				start = a.Start()
				firstStartNotSet = false
			}
			if a.Start().Before(start) {
				start = a.Start()
			}
		})
	})

	if firstStartNotSet {
		return start, fmt.Errorf("had no data to compute a start from")
	}

	return start, nil
}

// End returns the latest end of all Assets in the AssetSetRange.
// It returns an error if there are no assets.
func (asr *AssetSetRange) End() (time.Time, error) {
	end := time.Time{}
	firstEndNotSet := true
	asr.Each(func(i int, as *AssetSet) {
		as.Each(func(s string, a Asset) {
			if firstEndNotSet {
				end = a.End()
				firstEndNotSet = false
			}
			if a.End().After(end) {
				end = a.End()
			}
		})
	})

	if firstEndNotSet {
		return end, fmt.Errorf("had no data to compute an end from")
	}

	return end, nil
}

// Minutes returns the duration, in minutes, between the earliest start
// and the latest end of all assets in the AssetSetRange.
func (asr *AssetSetRange) Minutes() float64 {
	start, err := asr.Start()
	if err != nil {
		return 0
	}
	end, err := asr.End()
	if err != nil {
		return 0
	}

	duration := end.Sub(start)

	return duration.Minutes()
}

// TotalCost returns the AssetSetRange's total cost
func (asr *AssetSetRange) TotalCost() float64 {
	if asr == nil {
		return 0.0
	}

	asr.RLock()
	defer asr.RUnlock()

	tc := 0.0
	for _, as := range asr.assets {
		tc += as.TotalCost()
	}

	return tc
}

// This is a helper type. The Asset API returns a json which cannot be natively
// unmarshaled into any Asset struct. Therefore, this struct IN COMBINATION WITH
// DESERIALIZATION LOGIC DEFINED IN asset_unmarshal.go can unmarshal a json directly
// from an Assets API query
type AssetAPIResponse struct {
	Code int                   `json:"code"`
	Data AssetSetRangeResponse `json:"data"`
}

// Returns true if string slices a and b contain all of the same strings, in any order.
func sameContents(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !contains(b, a[i]) {
			return false
		}
	}
	return true
}

func contains(slice []string, item string) bool {
	for _, element := range slice {
		if element == item {
			return true
		}
	}
	return false
}
