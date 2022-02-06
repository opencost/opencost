package kubecost

import (
	"bytes"
	"fmt"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"
	"sync"
	"time"
)

//// CloudUsage is temporarily aliased as the Cloud Asset type until further infrastructure and pages can be built to support its usage
//type CloudUsage = Cloud
//
//// CloudUsageSet is temporarily aliased as the AssetSet until further infrastructure and pages can be built to support its usage
//type CloudUsageSet = AssetSet
//
//// CloudUsageSetRange is temporarily aliased as the AssetSetRange until further infrastructure and pages can be built to support its usage
//type CloudUsageSetRange = AssetSetRange
//
//// CloudUsageAggregationOptions is temporarily aliased as the AssetAggregationOptions until further infrastructure and pages can be built to support its usage
//type CloudUsageAggregationOptions = AssetAggregationOptions
//
//// CloudUsageMatchFunc is temporarily aliased as the AssetMatchFunc until further infrastructure and pages can be built to support its usage
//type CloudUsageMatchFunc = AssetMatchFunc

// CloudUsage is a billing unit of usage of a service provided by a cloud service provider.
type CloudUsage struct {
	Labels     CloudLabels           `json:"labels,omitempty"`
	Properties *CloudUsageProperties `json:"properties,omitempty"`
	Start      time.Time             `json:"start"`
	End        time.Time             `json:"end"`
	Window     Window                `json:"window"`
	Cost       float64               `json:"cost"`
	Credit     float64               `json:"credit"`
}

// CloudLabels is a schema-free mapping of key/value pairs that can be
// attributed to a CloudUsage as a flexible a
type CloudLabels map[string]string

// Add returns the result of summing the two given CloudUsage, which sums the
// Cost and Credit
func (cu *CloudUsage) Add(that *CloudUsage) (*CloudUsage, error) {
	if cu == nil {
		return that.Clone(), nil
	}

	if that == nil {
		return cu.Clone(), nil
	}

	agg := cu.Clone()
	agg.add(that)

	return agg, nil
}

// Clone returns a deep copy of the given CloudUsage
func (cu *CloudUsage) Clone() *CloudUsage {
	if cu == nil {
		return nil
	}

	labels := make(map[string]string, len(cu.Labels))
	for k, v := range cu.Labels {
		labels[k] = v
	}

	return &CloudUsage{
		Labels:     labels,
		Properties: cu.Properties,
		Start:      cu.Start,
		End:        cu.End,
		Window:     cu.Window,
		Cost:       cu.Cost,
		Credit:     cu.Credit,
	}
}

// Equal returns true if the values held in the given CloudUsage precisely
// match those of the receiving CloudUsage. nil does not match nil. Floating
// point values need to match according to util.IsApproximately, which accounts
// for small, reasonable floating point error margins.
func (cu *CloudUsage) Equal(that *CloudUsage) bool {
	if cu == nil || that == nil {
		return false
	}

	if !cu.Properties.Equal(that.Properties) {
		return false
	}
	if !cu.Window.Equal(that.Window) {
		return false
	}
	if !cu.Start.Equal(that.Start) {
		return false
	}
	if !cu.End.Equal(that.End) {
		return false
	}
	if !util.IsApproximately(cu.Cost, that.Cost) {
		return false
	}
	if !util.IsApproximately(cu.Credit, that.Credit) {
		return false
	}
	return true
}

// TotalCost is the total cost of the CloudUsage including adjustments
func (cu *CloudUsage) TotalCost() float64 {
	if cu == nil {
		return 0.0
	}

	return cu.Cost + cu.Credit
}

// MarshalJSON implements json.Marshaler interface
func (cu *CloudUsage) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncode(buffer, "labels", cu.Labels, ",")
	jsonEncode(buffer, "properties", cu.Properties, ",")
	jsonEncode(buffer, "window", cu.Window, ",")
	jsonEncodeString(buffer, "start", cu.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", cu.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", cu.Minutes(), ",")
	jsonEncodeFloat64(buffer, "cost", cu.Cost, ",")
	jsonEncodeFloat64(buffer, "credit", cu.Credit, ",")
	jsonEncodeFloat64(buffer, "totalCost", cu.TotalCost(), ",")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// Minutes returns the number of minutes the CloudUsage represents, as defined
// by the difference between the end and start times.
func (cu *CloudUsage) Minutes() float64 {
	if cu == nil {
		return 0.0
	}

	return cu.End.Sub(cu.Start).Minutes()
}

// String represents the given CloudUsage as a string
func (cu *CloudUsage) String() string {
	if cu == nil {
		return "<nil>"
	}
	providerID := ""
	if cu.Properties != nil {
		providerID = cu.Properties.ProviderID
	}
	return fmt.Sprintf("%s%s=%.2f", providerID, NewWindow(&cu.Start, &cu.End), cu.TotalCost())
}

func (cu *CloudUsage) add(that *CloudUsage) {
	if cu == nil {
		log.Warningf("CloudUsage: trying to add a nil receiver")
		return
	}

	// Expand the window to encompass both CloudUsages
	cu.Window = cu.Window.Expand(that.Window)

	// Expand Start and End to be the "max" of among the given CloudUsages
	if that.Start.Before(cu.Start) {
		cu.Start = that.Start
	}
	if that.End.After(cu.End) {
		cu.End = that.End
	}

	cu.Cost += that.Cost
	cu.Credit += that.Credit


}

func (cu *CloudUsage) generateKey(aggregateBy []string) string {
	if cu == nil {
		return ""
	}

	return cu.Properties.GenerateKey(aggregateBy)
}

// CloudUsageSet stores a set of CloudUsages, each with a unique name, that share
// a window. An CloudUsageSet is mutable, so treat it like a threadsafe map.
type CloudUsageSet struct {
	sync.RWMutex
	aggregateBy []string
	cloudUsages  map[string]*CloudUsage
	Window       Window
	Warnings     []string
	Errors       []string
}

// NewCloudUsageSet instantiates a new CloudUsageSet and, optionally, inserts
// the given list of CloudUsages
func NewCloudUsageSet(start, end time.Time, usages ...*CloudUsage) *CloudUsageSet {
	cus := &CloudUsageSet{
		cloudUsages: map[string]*CloudUsage{},
		Window:       NewWindow(&start, &end),
	}

	for _, cu := range usages {
		cus.Insert(cu)
	}

	return cus
}

// CloudUsageMatchFunc is a function that can be used to match CloudUsages by
// returning true for any given CloudUsage if a condition is met.
type CloudUsageMatchFunc func(usage *CloudUsage) bool

// CloudUsageAggregationOptions provides parameters for CloudUsageSet AggregateBy
type CloudUsageAggregationOptions struct {
	FilterFuncs       []CloudUsageMatchFunc
}

// AggregateBy aggregates the CloudUsages in the CloudUsageSet by the given list of
// CloudUsageProperties, such that each cloudUsage is binned by a key determined by its
// relevant property values.
func (cus *CloudUsageSet) AggregateBy(aggregateBy []string, opts *CloudUsageAggregationOptions) error {
	if opts == nil {
		opts = &CloudUsageAggregationOptions{}
	}

	if cus.IsEmpty()  {
		return nil
	}

	cus.Lock()
	defer cus.Unlock()

	aggSet := NewCloudUsageSet(cus.Start(), cus.End())
	aggSet.aggregateBy = aggregateBy

	// Compute hours of the given CloudUsageSet, and if it ends in the future,
	// adjust the hours accordingly
	hours := cus.Window.Minutes() / 60.0
	diff := time.Since(cus.End())
	if diff < 0.0 {
		hours += diff.Hours()
	}

	// Move CloudUsages into aggregated cloudUsageSet
	for _, cloudUsage := range cus.cloudUsages {
		addCloudUsage := true

		// Check cloudUsage against all filter functions, break on failure
		for _, ff := range opts.FilterFuncs {
			if !ff(cloudUsage) {
				addCloudUsage = false
				break
			}
		}

		// Insert cloud usage into aggSet if it passed as filters
		if addCloudUsage {
			err := aggSet.Insert(cloudUsage)
			if err != nil {
				return err
			}
		}
	}

	// Assign the aggregated values back to the original set
	cus.cloudUsages = aggSet.cloudUsages
	cus.aggregateBy = aggregateBy

	return nil
}

// Clone returns a new CloudUsageSet with a deep copy of the given
// CloudUsageSet's CloudUsages.
func (cus *CloudUsageSet) Clone() *CloudUsageSet {
	if cus == nil {
		return nil
	}

	cus.RLock()
	defer cus.RUnlock()

	var aggregateBy []string
	if cus.aggregateBy != nil {
		aggregateBy = append([]string{}, cus.aggregateBy...)
	}

	cloudUsages := make(map[string]*CloudUsage, len(cus.cloudUsages))
	for k, v := range cus.cloudUsages {
		cloudUsages[k] = v.Clone()
	}

	s := cus.Start()
	e := cus.End()

	return &CloudUsageSet{
		Window:      NewWindow(&s, &e),
		aggregateBy: aggregateBy,
		cloudUsages:      cloudUsages,
	}
}

// Get returns the CloudUsage in the CloudUsageSet at the given key, or nil and false
// if no CloudUsage exists for the given key
func (cus *CloudUsageSet) Get(key string) (*CloudUsage, bool) {
	cus.RLock()
	defer cus.RUnlock()

	if a, ok := cus.cloudUsages[key]; ok {
		return a, true
	}
	return nil, false
}

// Insert inserts the given CloudUsage into the CloudUsageSet, using the CloudUsageSet's
// configured properties to determine the key under which the CloudUsage will
// be inserted.
func (cus *CloudUsageSet) Insert(cloudUsage *CloudUsage) error {
	if cus == nil {
		return fmt.Errorf("cannot Insert into nil CloudUsageSet")
	}

	cus.Lock()
	defer cus.Unlock()

	if cus.cloudUsages == nil {
		cus.cloudUsages = map[string]*CloudUsage{}
	}

	// Determine key into which to Insert the CloudUsage.
	k := cloudUsage.generateKey(cus.aggregateBy)

	// Add the given CloudUsage to the existing entry, if there is one;
	// otherwise just set directly into cloudUsages
	if _, ok := cus.cloudUsages[k]; !ok {
		cus.cloudUsages[k] = cloudUsage
	} else {
		cus.cloudUsages[k].add(cloudUsage)
	}

	// Expand the window, just to be safe. It's possible that the cloudUsage will
	// be set into the map without expanding it to the CloudUsageSet's window.
	cus.cloudUsages[k].Window.Expand(cus.Window)

	return nil
}

// IsEmpty returns true if the CloudUsageSet is nil, or if it contains
// zero cloudUsages.
func (cus *CloudUsageSet) IsEmpty() bool {
	if cus == nil {
		return true
	}

	cus.RLock()
	defer cus.RUnlock()
	return cus.cloudUsages == nil || len(cus.cloudUsages) == 0
}

// Length returns the length of the cloudUsages slice in the CloudUsageSet
func (cus *CloudUsageSet) Length() int {
	if cus == nil {
		return 0
	}

	cus.RLock()
	defer cus.RUnlock()
	return len(cus.cloudUsages)
}

// Start returns the start of the CloudUsageSet window
func (cus *CloudUsageSet) Start() time.Time {
	return *cus.Window.Start()
}

// End returns the end of the CloudUsageSet window
func (cus *CloudUsageSet) End() time.Time {
	return *cus.Window.End()
}

// TotalCost accumulates that total costs of all cloudUsages in the calling CloudUsageSet
func (cus *CloudUsageSet) TotalCost() float64 {
	tc := 0.0

	cus.Lock()
	defer cus.Unlock()

	for _, a := range cus.cloudUsages {
		tc += a.TotalCost()
	}

	return tc
}

func (cus *CloudUsageSet) accumulate(that *CloudUsageSet) (*CloudUsageSet, error) {
	if cus.IsEmpty() {
		return that.Clone(), nil
	}

	if that.IsEmpty() {
		return cus.Clone(), nil
	}

	// In the case of an CloudUsageSetRange with empty entries, we may end up with
	// an incoming `as` without an `aggregateBy`, even though we are tring to
	// aggregate here. This handles that case by assigning the correct `aggregateBy`.
	if !sameContents(cus.aggregateBy, that.aggregateBy) {
		if len(cus.aggregateBy) == 0 {
			cus.aggregateBy = that.aggregateBy
		}
	}

	// Set start, end to min(start), max(end)
	start := cus.Start()
	end := cus.End()

	if that.Start().Before(start) {
		start = that.Start()
	}

	if that.End().After(end) {
		end = that.End()
	}


	acc := NewCloudUsageSet(start, end)
	acc.aggregateBy = cus.aggregateBy

	cus.RLock()
	defer cus.RUnlock()

	that.RLock()
	defer that.RUnlock()

	for _, cu := range cus.cloudUsages {
		err := acc.Insert(cu)
		if err != nil {
			return nil, err
		}
	}

	for _, cu := range that.cloudUsages {
		err := acc.Insert(cu)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// CloudUsageSetRange is a thread-safe slice of CloudUsageSets. It is meant to
// be used such that the CloudUsageSets held are consecutive and coherent with
// respect to using the same aggregation properties, and
// resolution. However these rules are not necessarily enforced, so use wisely.
type CloudUsageSetRange struct {
	sync.RWMutex
	cloudUsages []*CloudUsageSet
}

// NewCloudUsageSetRange instantiates a new range composed of the given
// CloudUsageSet in the order provided.
func NewCloudUsageSetRange(cloudUsages ...*CloudUsageSet) *CloudUsageSetRange {
	return &CloudUsageSetRange{
		cloudUsages: cloudUsages,
	}
}

// Accumulate sums each CloudUsageSet in the given range, returning a single cumulative
// CloudUsageSet for the entire range.
func (cusr *CloudUsageSetRange) Accumulate() (*CloudUsageSet, error) {
	var cloudUsageSet *CloudUsageSet
	var err error

	cusr.RLock()
	defer cusr.RUnlock()

	for _, cus := range cusr.cloudUsages {
		cloudUsageSet, err = cloudUsageSet.accumulate(cus)
		if err != nil {
			return nil, err
		}
	}

	return cloudUsageSet, nil
}

// AggregateBy aggregates each CloudUsageSet in the range by the given
// properties and options.
func (cusr *CloudUsageSetRange) AggregateBy(aggregateBy []string, options *CloudUsageAggregationOptions) error {
	aggRange := &CloudUsageSetRange{cloudUsages: []*CloudUsageSet{}}

	cusr.Lock()
	defer cusr.Unlock()

	for _, cus := range cusr.cloudUsages {
		err := cus.AggregateBy(aggregateBy, options)
		if err != nil {
			return err
		}
		aggRange.Append(cus)
	}

	cusr.cloudUsages = aggRange.cloudUsages

	return nil
}

// Append appends the given CloudUsageSet to the end of the range. It does not
// validate whether or not that violates window continuity.
func (cusr *CloudUsageSetRange) Append(that *CloudUsageSet) {
	cusr.Lock()
	defer cusr.Unlock()
	cusr.cloudUsages = append(cusr.cloudUsages, that)
}
