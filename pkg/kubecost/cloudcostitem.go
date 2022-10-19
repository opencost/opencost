package kubecost

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/pkg/log"
)

type CloudCostLabels map[string]string

// TODO:cloudcost Category?
type CloudCostItemProperties struct {
	ProviderID string          `json:"providerID,omitempty"`
	Provider   string          `json:"provider,omitempty"`
	Account    string          `json:"account,omitempty"`
	Project    string          `json:"project,omitempty"`
	Service    string          `json:"service,omitempty"`
	Category   string          `json:"category,omitempty"`
	Labels     CloudCostLabels `json:"labels,omitempty"`
}

// TODO:cloudcost
func (ccip CloudCostItemProperties) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", ccip.Provider, ccip.Account, ccip.Project, ccip.Category, ccip.Service, ccip.ProviderID)
}

// CloudCostItem represents a CUR line item, identifying a cloud resource and
// its cost over some period of time.
type CloudCostItem struct {
	Properties   CloudCostItemProperties
	IsKubernetes bool
	Window       Window
	Cost         float64
	Credit       float64
}

func (cci *CloudCostItem) Key() string {
	// TODO:cloudcost
	return cci.Properties.Key()
}

type CloudCostItemSet struct {
	CloudCostItems map[string]*CloudCostItem
	Window         Window
}

// NewAssetSet instantiates a new AssetSet and, optionally, inserts
// the given list of Assets
func NewCloudCostItemSet(start, end time.Time, cloudCostItems ...*CloudCostItem) *CloudCostItemSet {
	ccis := &CloudCostItemSet{
		CloudCostItems: map[string]*CloudCostItem{},
		Window:         NewWindow(&start, &end),
	}

	for _, cci := range cloudCostItems {
		ccis.Insert(cci)
	}

	return ccis
}

func (ccis *CloudCostItemSet) Insert(that *CloudCostItem) error {
	if ccis == nil {
		return fmt.Errorf("cannot insert into nil CloudCostItemSet")
	}

	if ccis.CloudCostItems == nil {
		ccis.CloudCostItems = map[string]*CloudCostItem{}
	}

	// Add the given CloudCostItem to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := ccis.CloudCostItems[that.Key()]; !ok {
		ccis.CloudCostItems[that.Key()] = that
	} else {
		// ccis.CloudCostItems[that.Key()].add(that)
		return fmt.Errorf("cannot re-insert %s", that.Key())
	}

	return nil
}

func (ccis *CloudCostItemSet) Clone() *CloudCostItemSet {
	// TODO
	return nil
}

func (ccis *CloudCostItemSet) IsEmpty() bool {
	// TODO
	return true
}

func (ccis *CloudCostItemSet) GetWindow() Window {
	return ccis.Window
}

// GetCloudCostItemSets
func GetCloudCostItemSets(start time.Time, end time.Time, window time.Duration) ([]*CloudCostItemSet, error) {
	windows, err := GetWindows(start, end, window)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostItemSet to cover the range
	CloudCostItemSets := []*CloudCostItemSet{}
	for _, w := range windows {
		CloudCostItemSets = append(CloudCostItemSets, NewCloudCostItemSet(*w.Start(), *w.End()))
	}
	return CloudCostItemSets, nil
}

// LoadCloudCostItemSets creates and loads CloudCostItems into provided CloudCostItemSets. This method makes it so
// that the input windows do not have to match the one day frame of the Athena queries. CloudCostItems being generated from a
// CUR which may be the identical except for the pricing model used (default, RI or savings plan)
// are accumulated here so that the resulting CloudCostItem with the 1d window has the correct price for the entire day.
func LoadCloudCostItemSets(itemStart time.Time, itemEnd time.Time, properties CloudCostItemProperties, isK8s bool, cost, credit float64, CloudCostItemSets []*CloudCostItemSet) {
	totalMins := itemEnd.Sub(itemStart).Minutes()
	// Disperse cost of the current item across one or more CloudCostItems in
	// across each relevant CloudCostItemSet. Stop when the end of the current
	// block reaches the item's end time or the end of the range.
	for _, ccis := range CloudCostItemSets {
		// Determine pct of total cost attributable to this window by
		// determining the start/end of the overlap with the current
		// window, which will be negative if there is no overlap. If
		// there is positive overlap, compare it with the total mins.
		//
		// e.g. here are the two possible scenarios as simplidied
		// 10m windows with dashes representing item's time running:
		//
		// 1. item falls entirely within one CloudCostItemSet window
		//    |     ---- |          |          |
		//    totalMins = 4.0
		//    pct := 4.0 / 4.0 = 1.0 for window 1
		//    pct := 0.0 / 4.0 = 0.0 for window 2
		//    pct := 0.0 / 4.0 = 0.0 for window 3
		//
		// 2. item overlaps multiple CloudCostItemSet windows
		//    |      ----|----------|--        |
		//    totalMins = 16.0
		//    pct :=  4.0 / 16.0 = 0.250 for window 1
		//    pct := 10.0 / 16.0 = 0.625 for window 2
		//    pct :=  2.0 / 16.0 = 0.125 for window 3
		window := ccis.Window

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

		// Insert an CloudCostItem with that cost into the CloudCostItemSet at the given index
		cci := &CloudCostItem{
			Properties:   properties,
			IsKubernetes: isK8s,
			Window:       window.Clone(),
			Cost:         cost * pct,
			Credit:       credit * pct,
		}
		err := ccis.Insert(cci)
		if err != nil {
			log.Errorf("LoadCloudCostItemSets: failed to load CloudCostItem with key %s and window %s", cci.Key(), window.String())
		}
	}
}

type CloudCostItemSetRange struct {
	CloudCostItemSets []*CloudCostItemSet
	Step              time.Duration
	Window            Window
}
