package kubecost

import (
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/log"
)

// CloudCost represents a CUR line item, identifying a cloud resource and
// its cost over some period of time.
type CloudCost struct {
	Properties       *CloudCostProperties `json:"properties"`
	Window           Window               `json:"window"`
	ListCost         CostMetric           `json:"listCost"`
	NetCost          CostMetric           `json:"netCost"`
	AmortizedNetCost CostMetric           `json:"amortizedNetCost"`
	InvoicedCost     CostMetric           `json:"invoicedCost"`
	AmortizedCost    CostMetric           `json:"amortizedCost"`
}

// NewCloudCost instantiates a new CloudCost
func NewCloudCost(start, end time.Time, ccProperties *CloudCostProperties, kubernetesPercent, listCost, netCost, amortizedNetCost, invoicedCost, amortizedCost float64) *CloudCost {
	return &CloudCost{
		Properties: ccProperties,
		Window:     NewWindow(&start, &end),
		ListCost: CostMetric{
			Cost:              listCost,
			KubernetesPercent: kubernetesPercent,
		},
		NetCost: CostMetric{
			Cost:              netCost,
			KubernetesPercent: kubernetesPercent,
		},
		AmortizedNetCost: CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: kubernetesPercent,
		},
		InvoicedCost: CostMetric{
			Cost:              invoicedCost,
			KubernetesPercent: kubernetesPercent,
		},
		AmortizedCost: CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: kubernetesPercent,
		},
	}
}

func (cc *CloudCost) Clone() *CloudCost {
	return &CloudCost{
		Properties:       cc.Properties.Clone(),
		Window:           cc.Window.Clone(),
		ListCost:         cc.ListCost.Clone(),
		NetCost:          cc.NetCost.Clone(),
		AmortizedNetCost: cc.AmortizedNetCost.Clone(),
		InvoicedCost:     cc.InvoicedCost.Clone(),
		AmortizedCost:    cc.AmortizedCost.Clone(),
	}
}

func (cc *CloudCost) Equal(that *CloudCost) bool {
	if that == nil {
		return false
	}

	return cc.Properties.Equal(that.Properties) &&
		cc.Window.Equal(that.Window) &&
		cc.ListCost.Equal(that.ListCost) &&
		cc.NetCost.Equal(that.NetCost) &&
		cc.AmortizedNetCost.Equal(that.AmortizedNetCost) &&
		cc.InvoicedCost.Equal(that.InvoicedCost) &&
		cc.AmortizedCost.Equal(that.AmortizedCost)
}

func (cc *CloudCost) add(that *CloudCost) {
	if cc == nil {
		log.Warnf("cannot add to nil CloudCost")
		return
	}

	// Preserve properties of cloud cost  that are matching between the two CloudCost
	cc.Properties = cc.Properties.Intersection(that.Properties)

	cc.ListCost = cc.ListCost.add(that.ListCost)
	cc.NetCost = cc.NetCost.add(that.NetCost)
	cc.AmortizedNetCost = cc.AmortizedNetCost.add(that.AmortizedNetCost)
	cc.InvoicedCost = cc.InvoicedCost.add(that.InvoicedCost)
	cc.AmortizedCost = cc.AmortizedCost.add(that.AmortizedCost)

	cc.Window = cc.Window.Expand(that.Window)
}

func (cc *CloudCost) StringProperty(prop string) (string, error) {
	if cc == nil {
		return "", nil
	}

	switch prop {
	case CloudCostInvoiceEntityIDProp:
		return cc.Properties.InvoiceEntityID, nil
	case CloudCostAccountIDProp:
		return cc.Properties.AccountID, nil
	case CloudCostProviderProp:
		return cc.Properties.Provider, nil
	case CloudCostProviderIDProp:
		return cc.Properties.ProviderID, nil
	case CloudCostServiceProp:
		return cc.Properties.Service, nil
	case CloudCostCategoryProp:
		return cc.Properties.Category, nil
	default:
		return "", fmt.Errorf("invalid property name: %s", prop)
	}
}

func (cc *CloudCost) StringMapProperty(property string) (map[string]string, error) {
	switch property {
	case CloudCostLabelProp:
		if cc.Properties == nil {
			return nil, nil
		}
		return cc.Properties.Labels, nil

	default:
		return nil, fmt.Errorf("CloudCost: StringMapProperty: invalid property name: %s", property)
	}
}

func (cc *CloudCost) GetCostMetric(costMetricName string) (CostMetric, error) {
	switch costMetricName {
	case ListCostMetric:
		return cc.ListCost, nil
	case NetCostMetric:
		return cc.NetCost, nil
	case AmortizedNetCostMetric:
		return cc.AmortizedNetCost, nil
	case InvoicedCostMetric:
		return cc.InvoicedCost, nil
	case AmortizedCostMetric:
		return cc.AmortizedCost, nil
	}
	return CostMetric{}, fmt.Errorf("invalid Cost Metric: %s", costMetricName)
}

// CloudCostSet follows the established set pattern of windowed data types. It has addition metadata types that can be
// used to preserve data consistency and be used for validation.
// - Integration is the ID for the integration that a CloudCostSet was sourced from, this value is cleared if when a
// set is joined with another with a different key
// - AggregationProperties is set by the Aggregate function and ensures that any additional inserts are keyed correctly
type CloudCostSet struct {
	CloudCosts            map[string]*CloudCost `json:"cloudCosts"`
	Window                Window                `json:"window"`
	Integration           string                `json:"-"`
	AggregationProperties []string              `json:"aggregationProperties"`
}

// NewCloudCostSet instantiates a new CloudCostSet and, optionally, inserts
// the given list of CloudCosts
func NewCloudCostSet(start, end time.Time, cloudCosts ...*CloudCost) *CloudCostSet {
	ccs := &CloudCostSet{
		CloudCosts: map[string]*CloudCost{},
		Window:     NewWindow(&start, &end),
	}

	for _, cc := range cloudCosts {
		ccs.Insert(cc)
	}

	return ccs
}

func (ccs *CloudCostSet) Aggregate(props []string) (*CloudCostSet, error) {
	if ccs == nil {
		return nil, errors.New("cannot aggregate a nil CloudCostSet")
	}

	if ccs.Window.IsOpen() {
		return nil, fmt.Errorf("cannot aggregate a CloudCostSet with an open window: %s", ccs.Window)
	}

	// Create a new result set, with the given aggregation property
	result := ccs.cloneSet()
	result.AggregationProperties = props

	// Insert clones of each item in the set, keyed by the given property.
	// The underlying insert logic will add binned items together.
	for name, cc := range ccs.CloudCosts {
		ccClone := cc.Clone()
		err := result.Insert(ccClone)
		if err != nil {
			return nil, fmt.Errorf("error aggregating %s by %v: %s", name, props, err)
		}
	}

	return result, nil
}

func (ccs *CloudCostSet) Accumulate(that *CloudCostSet) (*CloudCostSet, error) {
	if ccs.IsEmpty() {
		return that.Clone(), nil
	}
	acc := ccs.Clone()
	err := acc.accumulateInto(that)
	if err == nil {
		return nil, err
	}
	return acc, nil
}

// accumulateInto accumulates a the arg CloudCostSet Into the receiver
func (ccs *CloudCostSet) accumulateInto(that *CloudCostSet) error {
	if ccs == nil {
		return fmt.Errorf("CloudCost: cannot accumulate into nil set")
	}

	if that.IsEmpty() {
		return nil
	}

	if ccs.Integration != that.Integration {
		ccs.Integration = ""
	}

	ccs.Window.Expand(that.Window)

	for _, cc := range that.CloudCosts {
		err := ccs.Insert(cc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ccs *CloudCostSet) Equal(that *CloudCostSet) bool {
	if ccs.Integration != that.Integration {
		return false
	}

	if !ccs.Window.Equal(that.Window) {
		return false
	}

	// Check Aggregation Properties, slice order is grounds for inequality
	if len(ccs.AggregationProperties) != len(that.AggregationProperties) {
		return false
	}
	for i, prop := range ccs.AggregationProperties {
		if that.AggregationProperties[i] != prop {
			return false
		}
	}

	if len(ccs.CloudCosts) != len(that.CloudCosts) {
		return false
	}

	for k, cc := range ccs.CloudCosts {
		if tcc, ok := that.CloudCosts[k]; !ok || !cc.Equal(tcc) {
			return false
		}
	}

	return true
}

func (ccs *CloudCostSet) Filter(filters filter.Filter[*CloudCost]) *CloudCostSet {
	if ccs == nil {
		return nil
	}

	if filters == nil {
		return ccs.Clone()
	}

	result := ccs.cloneSet()

	for _, cc := range ccs.CloudCosts {
		if filters.Matches(cc) {
			result.Insert(cc.Clone())
		}
	}

	return result
}

// Insert adds a CloudCost to a CloudCostSet using its AggregationProperties and LabelConfig
// to determine the key where it will be inserted
func (ccs *CloudCostSet) Insert(cc *CloudCost) error {
	if ccs == nil {
		return fmt.Errorf("cannot insert into nil CloudCostSet")
	}

	if cc == nil {
		return fmt.Errorf("cannot insert nil CloudCost into CloudCostSet")
	}

	if ccs.CloudCosts == nil {
		ccs.CloudCosts = map[string]*CloudCost{}
	}

	ccKey := cc.Properties.GenerateKey(ccs.AggregationProperties)

	// Add the given CloudCost to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := ccs.CloudCosts[ccKey]; !ok {
		ccs.CloudCosts[ccKey] = cc.Clone()
	} else {
		ccs.CloudCosts[ccKey].add(cc)
	}

	return nil
}

func (ccs *CloudCostSet) Clone() *CloudCostSet {
	cloudCosts := make(map[string]*CloudCost, len(ccs.CloudCosts))
	for k, v := range ccs.CloudCosts {
		cloudCosts[k] = v.Clone()
	}

	cloneCCS := ccs.cloneSet()
	cloneCCS.CloudCosts = cloudCosts

	return cloneCCS
}

// cloneSet creates a copy of the receiver without any of its CloudCosts
func (ccs *CloudCostSet) cloneSet() *CloudCostSet {
	aggProps := make([]string, len(ccs.AggregationProperties))
	for i, v := range ccs.AggregationProperties {
		aggProps[i] = v
	}
	return &CloudCostSet{
		CloudCosts:            make(map[string]*CloudCost),
		Integration:           ccs.Integration,
		AggregationProperties: aggProps,
		Window:                ccs.Window.Clone(),
	}
}

func (ccs *CloudCostSet) IsEmpty() bool {
	if ccs == nil {
		return true
	}

	if len(ccs.CloudCosts) == 0 {
		return true
	}

	return false
}

func (ccs *CloudCostSet) Length() int {
	if ccs == nil {
		return 0
	}
	return len(ccs.CloudCosts)
}

func (ccs *CloudCostSet) GetWindow() Window {
	return ccs.Window
}

func (ccs *CloudCostSet) Merge(that *CloudCostSet) (*CloudCostSet, error) {
	if ccs == nil {
		return nil, fmt.Errorf("cannot merge nil CloudCostSets")
	}

	if that.IsEmpty() {
		return ccs.Clone(), nil
	}

	if !ccs.Window.Equal(that.Window) {
		return nil, fmt.Errorf("cannot merge CloudCostSets with different windows")
	}

	result := ccs.cloneSet()
	// clear integration if it is not equal
	if ccs.Integration != that.Integration {
		result.Integration = ""
	}

	for _, cc := range ccs.CloudCosts {
		result.Insert(cc)
	}

	for _, cc := range that.CloudCosts {
		result.Insert(cc)
	}

	return result, nil
}

type CloudCostSetRange struct {
	CloudCostSets []*CloudCostSet `json:"sets"`
	Window        Window          `json:"window"`
}

// NewCloudCostSetRange create a CloudCostSetRange containing CloudCostSets with windows of equal duration
// the duration between start and end must be divisible by the window duration argument
func NewCloudCostSetRange(start time.Time, end time.Time, window time.Duration, integration string) (*CloudCostSetRange, error) {
	windows, err := GetWindows(start, end, window)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostSet to cover the range
	cloudCostItemSets := make([]*CloudCostSet, len(windows))
	for i, w := range windows {
		ccs := NewCloudCostSet(*w.Start(), *w.End())
		ccs.Integration = integration
		cloudCostItemSets[i] = ccs
	}
	return &CloudCostSetRange{
		Window:        NewWindow(&start, &end),
		CloudCostSets: cloudCostItemSets,
	}, nil
}

func (ccsr *CloudCostSetRange) Clone() *CloudCostSetRange {
	ccsSlice := make([]*CloudCostSet, len(ccsr.CloudCostSets))
	for i, ccs := range ccsr.CloudCostSets {
		ccsSlice[i] = ccs.Clone()
	}
	return &CloudCostSetRange{
		Window:        ccsr.Window.Clone(),
		CloudCostSets: ccsSlice,
	}
}

func (ccsr *CloudCostSetRange) IsEmpty() bool {
	for _, ccs := range ccsr.CloudCostSets {
		if !ccs.IsEmpty() {
			return false
		}
	}
	return true
}

// Accumulate sums each CloudCostSet in the given range, returning a single cumulative
// CloudCostSet for the entire range.
func (ccsr *CloudCostSetRange) Accumulate() (*CloudCostSet, error) {
	var cloudCostSet *CloudCostSet
	var err error

	for _, ccs := range ccsr.CloudCostSets {
		if cloudCostSet == nil {
			cloudCostSet = ccs.Clone()
			continue
		}
		err = cloudCostSet.accumulateInto(ccs)
		if err != nil {
			return nil, err
		}
	}

	return cloudCostSet, nil
}

// LoadCloudCost loads CloudCosts into existing CloudCostSets of the CloudCostSetRange.
// This function service to aggregate and distribute costs over predefined windows
// are accumulated here so that the resulting CloudCost with the 1d window has the correct price for the entire day.
// If all or a portion of the window of the CloudCost is outside of the windows of the existing CloudCostSets,
// that portion of the CloudCost's cost will not be inserted
func (ccsr *CloudCostSetRange) LoadCloudCost(cloudCost *CloudCost) {
	window := cloudCost.Window
	if window.IsOpen() {
		log.Errorf("CloudCostSetRange: LoadCloudCost: invalid window %s", window.String())
		return
	}

	totalPct := 0.0

	// Distribute cost of the current item across one or more CloudCosts in
	// across each relevant CloudCostSet. Stop when the end of the current
	// block reaches the item's end time or the end of the range.
	for _, ccs := range ccsr.CloudCostSets {
		setWindow := ccs.Window

		// get percent of item window contained in set window
		pct := setWindow.GetPercentInWindow(window)
		if pct == 0 {
			continue
		}

		cc := cloudCost
		// If the current set Window only contains a portion of the CloudCost Window, insert costs relative to that portion
		if pct < 1.0 {
			cc = &CloudCost{
				Properties:       cloudCost.Properties,
				Window:           window.Contract(setWindow),
				ListCost:         cloudCost.ListCost.percent(pct),
				NetCost:          cloudCost.NetCost.percent(pct),
				AmortizedNetCost: cloudCost.AmortizedNetCost.percent(pct),
				InvoicedCost:     cloudCost.InvoicedCost.percent(pct),
				AmortizedCost:    cloudCost.AmortizedCost.percent(pct),
			}
		}

		err := ccs.Insert(cc)
		if err != nil {
			log.Errorf("CloudCostSetRange: LoadCloudCost: failed to load CloudCost with window %s: %s", setWindow.String(), err.Error())
		}

		// If all cost has been inserted, then there is no need to check later days in the range
		totalPct += pct
		if totalPct >= 1.0 {
			return
		}
	}
}

const (
	ListCostMetric         string = "ListCost"
	NetCostMetric          string = "NetCost"
	AmortizedNetCostMetric string = "AmortizedNetCost"
	InvoicedCostMetric     string = "InvoicedCost"
	AmortizedCostMetric    string = "AmortizedCost"
)

type CostMetric struct {
	Cost              float64 `json:"cost"`
	KubernetesPercent float64 `json:"kubernetesPercent"`
}

func (cm CostMetric) Equal(that CostMetric) bool {
	return cm.Cost == that.Cost && cm.KubernetesPercent == that.KubernetesPercent
}

func (cm CostMetric) Clone() CostMetric {
	return CostMetric{
		Cost:              cm.Cost,
		KubernetesPercent: cm.KubernetesPercent,
	}
}

func (cm CostMetric) add(that CostMetric) CostMetric {
	// Compute KubernetesPercent for sum
	k8sPct := 0.0
	sumCost := cm.Cost + that.Cost
	if sumCost > 0.0 {
		thisK8sCost := cm.Cost * cm.KubernetesPercent
		thatK8sCost := that.Cost * that.KubernetesPercent
		k8sPct = (thisK8sCost + thatK8sCost) / sumCost
	}

	return CostMetric{
		Cost:              sumCost,
		KubernetesPercent: k8sPct,
	}
}

// percent returns the product of the given percent and the cost, KubernetesPercent remains the same
func (cm CostMetric) percent(pct float64) CostMetric {
	return CostMetric{
		Cost:              cm.Cost * pct,
		KubernetesPercent: cm.KubernetesPercent,
	}
}
