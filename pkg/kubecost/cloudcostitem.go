package kubecost

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/log"
)

type CloudCostItemLabels map[string]string

func (ccil CloudCostItemLabels) Clone() CloudCostItemLabels {
	result := make(map[string]string, len(ccil))
	for k, v := range ccil {
		result[k] = v
	}
	return result
}

func (ccil CloudCostItemLabels) Equal(that CloudCostItemLabels) bool {
	if len(ccil) != len(that) {
		return false
	}

	// Maps are of equal length, so if all keys are in both maps, we don't
	// have to check the keys of the other map.
	for k, v := range ccil {
		if tv, ok := that[k]; !ok || v != tv {
			return false
		}
	}

	return true
}

type CloudCostItemProperties struct {
	ProviderID string              `json:"providerID,omitempty"`
	Provider   string              `json:"provider,omitempty"`
	Account    string              `json:"account,omitempty"`
	Project    string              `json:"project,omitempty"`
	Service    string              `json:"service,omitempty"`
	Category   string              `json:"category,omitempty"`
	Labels     CloudCostItemLabels `json:"labels,omitempty"`
}

func (ccip CloudCostItemProperties) Equal(that CloudCostItemProperties) bool {
	return ccip.ProviderID == that.ProviderID &&
		ccip.Provider == that.Provider &&
		ccip.Account == that.Account &&
		ccip.Project == that.Project &&
		ccip.Service == that.Service &&
		ccip.Category == that.Category &&
		ccip.Labels.Equal(that.Labels)
}

func (ccip CloudCostItemProperties) Clone() CloudCostItemProperties {
	return CloudCostItemProperties{
		ProviderID: ccip.ProviderID,
		Provider:   ccip.Provider,
		Account:    ccip.Account,
		Project:    ccip.Project,
		Service:    ccip.Service,
		Category:   ccip.Category,
		Labels:     ccip.Labels.Clone(),
	}
}

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

func (cci *CloudCostItem) Clone() *CloudCostItem {
	return &CloudCostItem{
		Properties:   cci.Properties.Clone(),
		IsKubernetes: cci.IsKubernetes,
		Window:       cci.Window.Clone(),
		Cost:         cci.Cost,
		Credit:       cci.Credit,
	}
}

func (cci *CloudCostItem) Equal(that *CloudCostItem) bool {
	if that == nil {
		return false
	}

	return cci.Properties.Equal(that.Properties) &&
		cci.IsKubernetes == that.IsKubernetes &&
		cci.Window.Equal(that.Window) &&
		cci.Cost == that.Cost &&
		cci.Credit == that.Credit
}

func (cci *CloudCostItem) Key() string {
	return cci.Properties.Key()
}

func (cci *CloudCostItem) add(that *CloudCostItem) {
	if cci == nil {
		log.Warnf("cannot add to nil CloudCostItem")
		return
	}

	cci.Cost += that.Cost
	cci.Credit += that.Credit
	cci.Window = cci.Window.Expand(that.Window)
}

type CloudCostItemSet struct {
	CloudCostItems map[string]*CloudCostItem
	Window         Window
	Integration    string
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

func (ccis *CloudCostItemSet) Equal(that *CloudCostItemSet) bool {
	if ccis.Integration != that.Integration {
		return false
	}

	if !ccis.Window.Equal(that.Window) {
		return false
	}

	if len(ccis.CloudCostItems) != len(that.CloudCostItems) {
		return false
	}

	for k, cci := range ccis.CloudCostItems {
		tcci, ok := that.CloudCostItems[k]
		if !ok {
			return false
		}
		if !cci.Equal(tcci) {
			return false
		}
	}

	return true
}

func (ccis *CloudCostItemSet) Filter(filters filter.Filter[*CloudCostItem]) *CloudCostItemSet {
	if ccis == nil {
		return nil
	}

	if filters == nil {
		return ccis.Clone()
	}

	result := NewCloudCostItemSet(*ccis.Window.start, *ccis.Window.end)

	for _, cci := range ccis.CloudCostItems {
		if filters.Matches(cci) {
			result.Insert(cci.Clone())
		}
	}

	return result
}

func (ccis *CloudCostItemSet) Insert(that *CloudCostItem) error {
	if ccis == nil {
		return fmt.Errorf("cannot insert into nil CloudCostItemSet")
	}

	if that == nil {
		return fmt.Errorf("cannot insert nil CloudCostItem into CloudCostItemSet")
	}

	if ccis.CloudCostItems == nil {
		ccis.CloudCostItems = map[string]*CloudCostItem{}
	}

	// Add the given CloudCostItem to the existing entry, if there is one;
	// otherwise just set directly into allocations
	if _, ok := ccis.CloudCostItems[that.Key()]; !ok {
		ccis.CloudCostItems[that.Key()] = that.Clone()
	} else {
		ccis.CloudCostItems[that.Key()].add(that)
	}

	return nil
}

func (ccis *CloudCostItemSet) Clone() *CloudCostItemSet {
	items := make(map[string]*CloudCostItem, len(ccis.CloudCostItems))
	for k, v := range ccis.CloudCostItems {
		items[k] = v.Clone()
	}

	return &CloudCostItemSet{
		CloudCostItems: items,
		Integration:    ccis.Integration,
		Window:         ccis.Window.Clone(),
	}
}

func (ccis *CloudCostItemSet) IsEmpty() bool {
	if ccis == nil {
		return true
	}

	if len(ccis.CloudCostItems) == 0 {
		return true
	}

	return false
}

func (ccis *CloudCostItemSet) Length() int {
	if ccis == nil {
		return 0
	}
	return len(ccis.CloudCostItems)
}

func (ccis *CloudCostItemSet) GetWindow() Window {
	return ccis.Window
}

func (ccis *CloudCostItemSet) Merge(that *CloudCostItemSet) (*CloudCostItemSet, error) {
	if ccis == nil {
		return nil, fmt.Errorf("cannot merge nil CloudCostItemSets")
	}

	if that.IsEmpty() {
		return ccis.Clone(), nil
	}

	if !ccis.Window.Equal(that.Window) {
		return nil, fmt.Errorf("cannot merge CloudCostItemSets with different windows")
	}

	start, end := *ccis.Window.Start(), *ccis.Window.End()
	result := NewCloudCostItemSet(start, end)

	for _, cci := range ccis.CloudCostItems {
		result.Insert(cci)
	}

	for _, cci := range that.CloudCostItems {
		result.Insert(cci)
	}

	return result, nil
}

// GetCloudCostItemSets
func GetCloudCostItemSets(start time.Time, end time.Time, window time.Duration, integration string) ([]*CloudCostItemSet, error) {
	windows, err := GetWindows(start, end, window)
	if err != nil {
		return nil, err
	}

	// Build slice of CloudCostItemSet to cover the range
	CloudCostItemSets := []*CloudCostItemSet{}
	for _, w := range windows {
		ccis := NewCloudCostItemSet(*w.Start(), *w.End())
		ccis.Integration = integration
		CloudCostItemSets = append(CloudCostItemSets, ccis)
	}
	return CloudCostItemSets, nil
}

// LoadCloudCostItemSets creates and loads CloudCostItems into provided CloudCostItemSets. This method makes it so
// that the input windows do not have to match the one day frame of the Athena queries. CloudCostItems being generated from a
// CUR which may be the identical except for the pricing model used (default, RI or savings plan)
// are accumulated here so that the resulting CloudCostItem with the 1d window has the correct price for the entire day.
func LoadCloudCostItemSets(itemStart time.Time, itemEnd time.Time, properties CloudCostItemProperties, isK8s bool, cost, credit float64, CloudCostItemSets []*CloudCostItemSet) {

	// Disperse cost of the current item across one or more CloudCostItems in
	// across each relevant CloudCostItemSet. Stop when the end of the current
	// block reaches the item's end time or the end of the range.
	for _, ccis := range CloudCostItemSets {
		pct := ccis.GetWindow().GetPercentInWindow(itemStart, itemEnd)

		// Insert an CloudCostItem with that cost into the CloudCostItemSet at the given index
		cci := &CloudCostItem{
			Properties:   properties,
			IsKubernetes: isK8s,
			Window:       ccis.GetWindow(),
			Cost:         cost * pct,
			Credit:       credit * pct,
		}
		err := ccis.Insert(cci)
		if err != nil {
			log.Errorf("LoadCloudCostItemSets: failed to load CloudCostItem with key %s and window %s: %s", cci.Key(), ccis.GetWindow().String(), err.Error())
		}
	}
}

type CloudCostItemSetRange struct {
	CloudCostItemSets []*CloudCostItemSet `json:"sets"`
	Window            Window              `json:"window"`
}
