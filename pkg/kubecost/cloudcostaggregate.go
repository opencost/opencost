package kubecost

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/log"
)

// CloudCostAggregateProperties unique property set for CloudCostAggregate within a window
type CloudCostAggregateProperties struct {
	Provider   string `json:"provider"`
	Account    string `json:"account"`
	Project    string `json:"project"`
	Service    string `json:"service"`
	LabelValue string `json:"labelValue"`
}

func (ccap CloudCostAggregateProperties) Equal(that CloudCostAggregateProperties) bool {
	return ccap.Provider == that.Provider &&
		ccap.Account == that.Account &&
		ccap.Project == that.Project &&
		ccap.Service == that.Service &&
		ccap.LabelValue == that.LabelValue
}

// TODO:cloudcost
func (ccap CloudCostAggregateProperties) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", ccap.Provider, ccap.Account, ccap.Project, ccap.Service, ccap.LabelValue)
}

// CloudCostAggregate represents an aggregation of Billing Integration data on the properties listed
type CloudCostAggregate struct {
	Properties        CloudCostAggregateProperties `json:"properties"`
	KubernetesPercent float64                      `json:"kubernetesPercent"`
	Window            Window                       `json:"window"`
	Cost              float64                      `json:"cost"`
	Credit            float64                      `json:"credit"`
}

func (cca *CloudCostAggregate) Clone() *CloudCostAggregate {
	return &CloudCostAggregate{
		Properties:        cca.Properties,
		KubernetesPercent: cca.KubernetesPercent,
		Window:            cca.Window.Clone(),
		Cost:              cca.Cost,
		Credit:            cca.Credit,
	}
}

func (cca *CloudCostAggregate) Equal(that *CloudCostAggregate) bool {
	if that == nil {
		return false
	}

	return cca.Cost == that.Cost &&
		cca.Credit == that.Credit &&
		cca.Window.Equal(that.Window) &&
		cca.Properties.Equal(that.Properties)
}

func (cca *CloudCostAggregate) Key() string {
	return cca.Properties.Key()
}

func (cca *CloudCostAggregate) add(that *CloudCostAggregate) {
	if cca == nil {
		log.Warnf("cannot add too nil CloudCostItem")
		return
	}

	// Compute KubernetesPercent for sum
	cca.KubernetesPercent = 0.0
	sumCost := cca.Cost + that.Cost
	if sumCost > 0.0 {
		thisK8sCost := cca.Cost * cca.KubernetesPercent
		thatK8sCost := that.Cost * that.KubernetesPercent
		cca.KubernetesPercent = (thisK8sCost + thatK8sCost) / sumCost
	}

	cca.Cost = sumCost
	cca.Credit += that.Credit
	cca.Window = cca.Window.Expand(that.Window)
}

// TODO Integration here or on Properties?
// TODO LabelName here or on Properties?
type CloudCostAggregateSet struct {
	CloudCostAggregates map[string]*CloudCostAggregate
	Integration         string
	LabelName           string
	Window              Window
}

func NewCloudCostAggregateSet(start, end time.Time, cloudCostAggregates ...*CloudCostAggregate) *CloudCostAggregateSet {
	ccas := &CloudCostAggregateSet{
		CloudCostAggregates: map[string]*CloudCostAggregate{},
		Window:              NewWindow(&start, &end),
	}

	for _, cca := range cloudCostAggregates {
		ccas.Insert(cca)
	}

	return ccas
}

func (ccas *CloudCostAggregateSet) Filter(filters filter.Filter[*CloudCostAggregate]) *CloudCostAggregateSet {
	if ccas == nil {
		return nil
	}

	if filters == nil {
		return ccas.Clone()
	}

	result := NewCloudCostAggregateSet(*ccas.Window.start, *ccas.Window.end)

	for _, cca := range ccas.CloudCostAggregates {
		if filters.Matches(cca) {
			// TODO:cloudcost ideally... this would be a Clone, but performance?
			result.Insert(cca)
		}
	}

	return result
}

func (ccas *CloudCostAggregateSet) Insert(that *CloudCostAggregate) error {
	if ccas == nil {
		return fmt.Errorf("cannot insert into nil CloudCostAggregateSet")
	}

	if ccas.CloudCostAggregates == nil {
		ccas.CloudCostAggregates = map[string]*CloudCostAggregate{}
	}

	// Add the given CloudCostAggregate to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := ccas.CloudCostAggregates[that.Key()]; !ok {
		ccas.CloudCostAggregates[that.Key()] = that
	} else {
		ccas.CloudCostAggregates[that.Key()].add(that)
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
		return nil, fmt.Errorf("cannot merge CloudCostAggregateSets with different label names")
	}

	start, end := *ccas.Window.Start(), *ccas.Window.End()
	result := NewCloudCostAggregateSet(start, end)
	result.LabelName = ccas.LabelName

	for _, cca := range ccas.CloudCostAggregates {
		result.Insert(cca)
	}

	for _, cca := range that.CloudCostAggregates {
		result.Insert(cca)
	}

	return result, nil
}

func GetCloudCostAggregateSets(start, end time.Time, windowDuration time.Duration, integration string, labelName string) ([]*CloudCostAggregateSet, error) {
	windows, err := GetWindows(start, end, windowDuration)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostAggregateSet to cover the range
	CloudCostAggregateSets := []*CloudCostAggregateSet{}
	for _, w := range windows {
		ccas := NewCloudCostAggregateSet(*w.Start(), *w.End())
		ccas.Integration = integration
		ccas.LabelName = labelName
		CloudCostAggregateSets = append(CloudCostAggregateSets, ccas)
	}
	return CloudCostAggregateSets, nil
}

// LoadCloudCostAggregateSets creates and loads CloudCostAggregates into provided CloudCostAggregateSets. This method makes it so
// that the input windows do not have to match the one day frame of the Athena queries. CloudCostAggregates being generated from a
// CUR which may be the identical except for the pricing model used (default, RI or savings plan)
// are accumulated here so that the resulting CloudCostAggregate with the 1d window has the correct price for the entire day.
func LoadCloudCostAggregateSets(itemStart time.Time, itemEnd time.Time, properties CloudCostAggregateProperties, K8sPercent, cost, credit float64, CloudCostAggregateSets []*CloudCostAggregateSet) {
	totalMins := itemEnd.Sub(itemStart).Minutes()
	// Disperse cost of the current item across one or more CloudCostAggregates in
	// across each relevant CloudCostAggregateSet. Stop when the end of the current
	// block reaches the item's end time or the end of the range.
	for _, ccas := range CloudCostAggregateSets {
		// Determine pct of total cost attributable to this window by
		// determining the start/end of the overlap with the current
		// window, which will be negative if there is no overlap. If
		// there is positive overlap, compare it with the total mins.
		//
		// e.g. here are the two possible scenarios as simplidied
		// 10m windows with dashes representing item's time running:
		//
		// 1. item falls entirely within one CloudCostAggregateSet window
		//    |     ---- |          |          |
		//    totalMins = 4.0
		//    pct := 4.0 / 4.0 = 1.0 for window 1
		//    pct := 0.0 / 4.0 = 0.0 for window 2
		//    pct := 0.0 / 4.0 = 0.0 for window 3
		//
		// 2. item overlaps multiple CloudCostAggregateSet windows
		//    |      ----|----------|--        |
		//    totalMins = 16.0
		//    pct :=  4.0 / 16.0 = 0.250 for window 1
		//    pct := 10.0 / 16.0 = 0.625 for window 2
		//    pct :=  2.0 / 16.0 = 0.125 for window 3
		window := ccas.Window

		s := itemStart
		if s.Before(*window.Start()) {
			s = *window.Start()
		}

		e := itemEnd
		if e.After(*window.End()) {
			e = *window.End()
		}

		mins := e.Sub(s).Minutes()
		if mins <= 0.0 {
			continue
		}

		pct := mins / totalMins

		// Insert an CloudCostAggregate with that cost into the CloudCostAggregateSet at the given index
		cca := &CloudCostAggregate{
			Properties:        properties,
			Window:            window.Clone(),
			KubernetesPercent: K8sPercent * pct,
			Cost:              cost * pct,
			Credit:            credit * pct,
		}
		err := ccas.Insert(cca)
		if err != nil {
			log.Errorf("LoadCloudCostAggregateSets: failed to load CloudCostAggregate with key %s and window %s", cca.Key(), window.String())
		}
	}
}

// TODO:cloudcost bingen for time.Duration
type CloudCostAggregateSetRange struct {
	CloudCostAggregateSets []*CloudCostAggregateSet
	// Step                   time.Duration
	Window Window
}
