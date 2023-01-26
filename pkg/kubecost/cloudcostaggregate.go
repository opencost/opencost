package kubecost

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/log"
)

const (
	CloudCostBillingIDProp   string = "billingID"
	CloudCostWorkGroupIDProp string = "workGroupID"
	CloudCostProviderProp    string = "provider"
	CloudCostServiceProp     string = "service"
	CloudCostLabelProp       string = "label"
)

// CloudCostAggregateProperties unique property set for CloudCostAggregate within a window
type CloudCostAggregateProperties struct {
	Provider    string `json:"provider"`
	WorkGroupID string `json:"workGroupID"`
	BillingID   string `json:"billingID"`
	Service     string `json:"service"`
	LabelValue  string `json:"label"`
}

func (ccap CloudCostAggregateProperties) Equal(that CloudCostAggregateProperties) bool {
	return ccap.Provider == that.Provider &&
		ccap.WorkGroupID == that.WorkGroupID &&
		ccap.BillingID == that.BillingID &&
		ccap.Service == that.Service &&
		ccap.LabelValue == that.LabelValue
}

func (ccap CloudCostAggregateProperties) Key(props []string) string {
	if len(props) == 0 {
		return fmt.Sprintf("%s/%s/%s/%s/%s", ccap.Provider, ccap.BillingID, ccap.WorkGroupID, ccap.Service, ccap.LabelValue)
	}

	keys := make([]string, len(props))
	for i, prop := range props {
		key := UnallocatedSuffix

		switch prop {
		case CloudCostProviderProp:
			if ccap.Provider != "" {
				key = ccap.Provider
			}
		case CloudCostBillingIDProp:
			if ccap.BillingID != "" {
				key = ccap.BillingID
			}
		case CloudCostWorkGroupIDProp:
			if ccap.WorkGroupID != "" {
				key = ccap.WorkGroupID
			}
		case CloudCostServiceProp:
			if ccap.Service != "" {
				key = ccap.Service
			}
		case CloudCostLabelProp:
			if ccap.LabelValue != "" {
				key = ccap.LabelValue
			}
		}

		keys[i] = key
	}

	return strings.Join(keys, "/")
}

// CloudCostAggregate represents an aggregation of Billing Integration data on the properties listed
// - KubernetesPercent is the percent of the CloudCostAggregates cost which was from an item which could be identified
//   as coming from a kubernetes resources.
// - Cost is the sum of the cost of each item in the CloudCostAggregate
// - Credit is the sum of credits applied to each item in the CloudCostAggregate

type CloudCostAggregate struct {
	Properties        CloudCostAggregateProperties `json:"properties"`
	KubernetesPercent float64                      `json:"kubernetesPercent"`
	Cost              float64                      `json:"cost"`
	NetCost           float64                      `json:"netCost"`
}

func (cca *CloudCostAggregate) Clone() *CloudCostAggregate {
	return &CloudCostAggregate{
		Properties:        cca.Properties,
		KubernetesPercent: cca.KubernetesPercent,
		Cost:              cca.Cost,
		NetCost:           cca.NetCost,
	}
}

func (cca *CloudCostAggregate) Equal(that *CloudCostAggregate) bool {
	if that == nil {
		return false
	}

	return cca.Cost == that.Cost &&
		cca.NetCost == that.NetCost &&
		cca.Properties.Equal(that.Properties)
}

func (cca *CloudCostAggregate) Key(props []string) string {
	return cca.Properties.Key(props)
}

func (cca *CloudCostAggregate) StringProperty(prop string) (string, error) {
	if cca == nil {
		return "", nil
	}

	switch prop {
	case CloudCostBillingIDProp:
		return cca.Properties.BillingID, nil
	case CloudCostWorkGroupIDProp:
		return cca.Properties.WorkGroupID, nil
	case CloudCostProviderProp:
		return cca.Properties.Provider, nil
	case CloudCostServiceProp:
		return cca.Properties.Service, nil
	case CloudCostLabelProp:
		return cca.Properties.LabelValue, nil
	default:
		return "", fmt.Errorf("invalid property name: %s", prop)
	}
}

func (cca *CloudCostAggregate) add(that *CloudCostAggregate) {
	if cca == nil {
		log.Warnf("cannot add to nil CloudCostAggregate")
		return
	}

	// Compute KubernetesPercent for sum
	k8sPct := 0.0
	sumCost := cca.Cost + that.Cost
	if sumCost > 0.0 {
		thisK8sCost := cca.Cost * cca.KubernetesPercent
		thatK8sCost := that.Cost * that.KubernetesPercent
		k8sPct = (thisK8sCost + thatK8sCost) / sumCost
	}

	cca.Cost = sumCost
	cca.NetCost += that.NetCost
	cca.KubernetesPercent = k8sPct
}

type CloudCostAggregateSet struct {
	CloudCostAggregates   map[string]*CloudCostAggregate `json:"items"`
	AggregationProperties []string                       `json:"-"`
	Integration           string                         `json:"-"`
	LabelName             string                         `json:"labelName,omitempty"`
	Window                Window                         `json:"window"`
}

func NewCloudCostAggregateSet(start, end time.Time, cloudCostAggregates ...*CloudCostAggregate) *CloudCostAggregateSet {
	ccas := &CloudCostAggregateSet{
		CloudCostAggregates: map[string]*CloudCostAggregate{},
		Window:              NewWindow(&start, &end),
	}

	for _, cca := range cloudCostAggregates {
		ccas.insertByProperty(cca, nil)
	}

	return ccas
}

func (ccas *CloudCostAggregateSet) Aggregate(props []string) (*CloudCostAggregateSet, error) {
	if ccas == nil {
		return nil, errors.New("cannot aggregate a nil CloudCostAggregateSet")
	}

	if ccas.Window.IsOpen() {
		return nil, fmt.Errorf("cannot aggregate a CloudCostAggregateSet with an open window: %s", ccas.Window)
	}

	// Create a new result set, with the given aggregation property
	result := NewCloudCostAggregateSet(*ccas.Window.Start(), *ccas.Window.End())
	result.AggregationProperties = props
	result.LabelName = ccas.LabelName
	result.Integration = ccas.Integration

	// Insert clones of each item in the set, keyed by the given property.
	// The underlying insert logic will add binned items together.
	for name, cca := range ccas.CloudCostAggregates {
		ccaClone := cca.Clone()
		err := result.insertByProperty(ccaClone, props)
		if err != nil {
			return nil, fmt.Errorf("error aggregating %s by %v: %s", name, props, err)
		}
	}

	return result, nil
}

func (ccas *CloudCostAggregateSet) Filter(filters filter.Filter[*CloudCostAggregate]) *CloudCostAggregateSet {
	if ccas == nil {
		return nil
	}

	result := ccas.Clone()
	result.filter(filters)

	return result
}

func (ccas *CloudCostAggregateSet) filter(filters filter.Filter[*CloudCostAggregate]) {
	if ccas == nil {
		return
	}

	if filters == nil {
		return
	}

	for name, cca := range ccas.CloudCostAggregates {
		if !filters.Matches(cca) {
			delete(ccas.CloudCostAggregates, name)
		}
	}
}

func (ccas *CloudCostAggregateSet) Insert(that *CloudCostAggregate) error {
	// Publicly, only allow Inserting as a basic operation (i.e. without causing
	// an aggregation on a property).
	return ccas.insertByProperty(that, nil)
}

func (ccas *CloudCostAggregateSet) insertByProperty(that *CloudCostAggregate, props []string) error {
	if ccas == nil {
		return fmt.Errorf("cannot insert into nil CloudCostAggregateSet")
	}

	if ccas.CloudCostAggregates == nil {
		ccas.CloudCostAggregates = map[string]*CloudCostAggregate{}
	}

	// Add the given CloudCostAggregate to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := ccas.CloudCostAggregates[that.Key(props)]; !ok {
		ccas.CloudCostAggregates[that.Key(props)] = that
	} else {
		ccas.CloudCostAggregates[that.Key(props)].add(that)
	}

	return nil
}

func (ccas *CloudCostAggregateSet) Clone() *CloudCostAggregateSet {
	aggs := make(map[string]*CloudCostAggregate, len(ccas.CloudCostAggregates))
	for k, v := range ccas.CloudCostAggregates {
		aggs[k] = v.Clone()
	}

	return &CloudCostAggregateSet{
		CloudCostAggregates: aggs,
		Integration:         ccas.Integration,
		LabelName:           ccas.LabelName,
		Window:              ccas.Window.Clone(),
	}
}

func (ccas *CloudCostAggregateSet) Equal(that *CloudCostAggregateSet) bool {
	if ccas.Integration != that.Integration {
		return false
	}

	if ccas.LabelName != that.LabelName {
		return false
	}

	if !ccas.Window.Equal(that.Window) {
		return false
	}

	if len(ccas.CloudCostAggregates) != len(that.CloudCostAggregates) {
		return false
	}

	for k, cca := range ccas.CloudCostAggregates {
		tcca, ok := that.CloudCostAggregates[k]
		if !ok {
			return false
		}
		if !cca.Equal(tcca) {
			return false
		}
	}

	return true
}

func (ccas *CloudCostAggregateSet) IsEmpty() bool {
	if ccas == nil {
		return true
	}

	if len(ccas.CloudCostAggregates) == 0 {
		return true
	}

	return false
}

func (ccas *CloudCostAggregateSet) Length() int {
	if ccas == nil {
		return 0
	}
	return len(ccas.CloudCostAggregates)
}

func (ccas *CloudCostAggregateSet) GetWindow() Window {
	return ccas.Window
}

func (ccas *CloudCostAggregateSet) Merge(that *CloudCostAggregateSet) (*CloudCostAggregateSet, error) {
	if ccas == nil || that == nil {
		return nil, fmt.Errorf("cannot merge nil CloudCostAggregateSets")
	}

	if that.IsEmpty() {
		return ccas.Clone(), nil
	}

	if !ccas.Window.Equal(that.Window) {
		return nil, fmt.Errorf("cannot merge CloudCostAggregateSets with different windows")
	}

	if ccas.LabelName != that.LabelName {
		return nil, fmt.Errorf("cannot merge CloudCostAggregateSets with different label names: '%s' != '%s'", ccas.LabelName, that.LabelName)
	}

	start, end := *ccas.Window.Start(), *ccas.Window.End()
	result := NewCloudCostAggregateSet(start, end)
	result.LabelName = ccas.LabelName

	for _, cca := range ccas.CloudCostAggregates {
		result.insertByProperty(cca, nil)
	}

	for _, cca := range that.CloudCostAggregates {
		result.insertByProperty(cca, nil)
	}

	return result, nil
}

type CloudCostAggregateSetRange struct {
	CloudCostAggregateSets []*CloudCostAggregateSet `json:"sets"`
	Window                 Window                   `json:"window"`
}

// NewCloudCostAggregateSetRange create a CloudCostAggregateSetRange containing CloudCostItemSets with windows of equal duration
// the duration between start and end must be divisible by the window duration argument
func NewCloudCostAggregateSetRange(start, end time.Time, window time.Duration, integration string, labelName string) (*CloudCostAggregateSetRange, error) {
	windows, err := GetWindows(start, end, window)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostAggregateSet to cover the range
	cloudCostAggregateSets := []*CloudCostAggregateSet{}
	for _, w := range windows {
		ccas := NewCloudCostAggregateSet(*w.Start(), *w.End())
		ccas.Integration = integration
		ccas.LabelName = labelName
		cloudCostAggregateSets = append(cloudCostAggregateSets, ccas)
	}
	return &CloudCostAggregateSetRange{
		Window:                 NewWindow(&start, &end),
		CloudCostAggregateSets: cloudCostAggregateSets,
	}, nil
}

// LoadCloudCostAggregate loads CloudCostAggregates into existing CloudCostAggregateSets of the CloudCostAggregateSetRange.
// This function service to aggregate and distribute costs over predefined windows
// If all or a portion of the window of the CloudCostAggregate is outside of the windows of the existing CloudCostAggregateSets,
// that portion of the CloudCostAggregate's cost will not be inserted
func (ccasr *CloudCostAggregateSetRange) LoadCloudCostAggregate(window Window, cloudCostAggregate *CloudCostAggregate) {
	if window.IsOpen() {
		log.Errorf("CloudCostItemSetRange: LoadCloudCostItem: invalid window %s", window.String())
		return
	}

	totalPct := 0.0

	// Distribute cost of the current item across one or more CloudCostAggregates in
	// across each relevant CloudCostAggregateSet. Stop when the end of the current
	// block reaches the item's end time or the end of the range.
	for _, ccas := range ccasr.CloudCostAggregateSets {
		pct := ccas.GetWindow().GetPercentInWindow(window)

		cca := cloudCostAggregate
		// If the current set Window only contains a portion of the CloudCostItem Window, insert costs relative to that portion
		cca = &CloudCostAggregate{
			Properties:        cloudCostAggregate.Properties,
			KubernetesPercent: cloudCostAggregate.KubernetesPercent * pct,
			Cost:              cloudCostAggregate.Cost * pct,
			NetCost:           cloudCostAggregate.NetCost * pct,
		}
		err := ccas.insertByProperty(cca, nil)
		if err != nil {
			log.Errorf("LoadCloudCostAggregateSets: failed to load CloudCostAggregate with key %s and window %s", cca.Key(nil), ccas.GetWindow().String())
		}

		// If all cost has been inserted then finish
		totalPct += pct
		if totalPct >= 1.0 {
			return
		}
	}
}

func (ccasr *CloudCostAggregateSetRange) Clone() *CloudCostAggregateSetRange {
	ccasSlice := make([]*CloudCostAggregateSet, len(ccasr.CloudCostAggregateSets))
	ccasSlice = append(ccasSlice, ccasr.CloudCostAggregateSets...)
	return &CloudCostAggregateSetRange{
		Window:                 ccasr.Window.Clone(),
		CloudCostAggregateSets: ccasSlice,
	}
}

func (ccasr *CloudCostAggregateSetRange) IsEmpty() bool {
	for _, ccas := range ccasr.CloudCostAggregateSets {
		if !ccas.IsEmpty() {
			return false
		}
	}
	return true
}

func (ccasr *CloudCostAggregateSetRange) Accumulate() (*CloudCostAggregateSet, error) {
	if ccasr == nil {
		return nil, errors.New("cannot accumulate a nil CloudCostAggregateSetRange")
	}

	if ccasr.Window.IsOpen() {
		return nil, fmt.Errorf("cannot accumulate a CloudCostAggregateSetRange with an open window: %s", ccasr.Window)
	}

	result := NewCloudCostAggregateSet(*ccasr.Window.Start(), *ccasr.Window.End())

	for _, ccas := range ccasr.CloudCostAggregateSets {
		for name, cca := range ccas.CloudCostAggregates {
			err := result.insertByProperty(cca.Clone(), ccas.AggregationProperties)
			if err != nil {
				return nil, fmt.Errorf("error accumulating CloudCostAggregateSetRange[%s][%s]: %s", ccas.Window.String(), name, err)
			}
		}
	}

	return result, nil
}
