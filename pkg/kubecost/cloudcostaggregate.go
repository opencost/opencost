package kubecost

import (
	"fmt"
	"github.com/opencost/opencost/pkg/log"
	"time"
)

// CloudCostAggregateProperties unique property set for CloudCostAggregate within a window
type CloudCostAggregateProperties struct {
	Provider   string
	Account    string
	Project    string
	Service    string
	LabelValue string
}

// CloudCostAggregate represents an aggregation of Billing Integration data on the properties listed
type CloudCostAggregate struct {
	Properties        CloudCostAggregateProperties
	KubernetesPercent float64
	Window            Window
	Cost              float64
	Credit            float64
}

func (cca *CloudCostAggregate) Key() string {
	return "" // todo
}
func (cca *CloudCostAggregate) add(that *CloudCostAggregate) {
	if cca == nil {
		log.Warnf("cannot add too nil CloudCostItem")
		return
	}

	// Normalize k8s percentages
	sumCost := cca.Cost + that.Cost
	thisK8sCost := cca.Cost * cca.KubernetesPercent
	thisK8sPercent := 0.0
	if thisK8sCost != 0 {
		thisK8sPercent = thisK8sCost / sumCost
	}
	thatK8sCost := that.Cost * that.KubernetesPercent
	thatK8sPercent := 0.0
	if thatK8sCost != 0 {
		thatK8sPercent = thatK8sCost / sumCost
	}
	cca.KubernetesPercent = thisK8sPercent + thatK8sPercent

	cca.Cost = sumCost
	cca.Credit += that.Credit
	cca.Window = cca.Window.Expand(that.Window)
}

func (cca *CloudCostAggregate) kubecostCost(that *CloudCostAggregate) {

}

// TODO Integration here or on Properties?
// TODO LabelName here or on Properties?
type CloudCostAggregateSet struct {
	CloudCostAggregates map[string]*CloudCostAggregate
	Integration         string
	LabelName           string
	Window              Window
}

func NewCloudCostAggregateSet(start, end time.Time, integration string, labelName string, cloudCostAggregates ...*CloudCostAggregate) *CloudCostAggregateSet {
	ccas := &CloudCostAggregateSet{
		Window:      NewWindow(&start, &end),
		Integration: integration,
		LabelName:   labelName}
	for _, cca := range cloudCostAggregates {
		ccas.Insert(cca)
	}
	return ccas
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
	// TODO
	return nil
}

func (ccas *CloudCostAggregateSet) IsEmpty() bool {
	// TODO
	return true
}

func (ccas *CloudCostAggregateSet) GetWindow() Window {
	return ccas.Window
}

func GetCloudCostAggregateSets(start time.Time, end time.Time, window time.Duration, integration string, labelName string) ([]*CloudCostAggregateSet, error) {
	windows, err := GetWindows(start, end, window)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostAggregateSet to cover the range
	CloudCostAggregateSets := []*CloudCostAggregateSet{}
	for _, w := range windows {
		CloudCostAggregateSets = append(CloudCostAggregateSets, NewCloudCostAggregateSet(*w.Start(), *w.End(), integration, labelName))
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

type CloudCostAggregateSetRange struct {
	CloudCostAggregateSets []*CloudCostAggregateSet
	Step                   time.Duration
	Window                 Window
}
