package opencost

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/matcher"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

const (
	NetworkInsightsServiceUnknown = "unknownService"
	NetworkInsightsNamespace      = "namespace"
	NetworkInsightsCluster        = "cluster"
	NetworkInsightsPod            = "pod"
)

type NetworkInsightProperty string

func ConvertNetworkInsightPropertiesToString(nips []NetworkInsightProperty) string {
	aggString := make([]string, len(nips))
	for i, agg := range nips {
		aggString[i] = string(agg)
	}
	return strings.Join(aggString, "/")
}

// Alias for network traffic direction string
type NetworkTrafficDirection string

const (
	NetworkTrafficDirectionNone    NetworkTrafficDirection = ""
	NetworkTrafficDirectionEgress  NetworkTrafficDirection = "Egress"
	NetworkTrafficDirectionIngress NetworkTrafficDirection = "Ingress"
)

// Alias for network traffic type string
type NetworkTrafficType string

const (
	NetworkTrafficTypeNone        NetworkTrafficType = ""
	NetworkTrafficTypeCrossZone   NetworkTrafficType = "CrossZone"
	NetworkTrafficTypeCrossRegion NetworkTrafficType = "CrossRegion"
	NetworkTrafficTypeInternet    NetworkTrafficType = "Internet"
)

// Struct to store the filter options applied on network
// interaction in networkDetails of network insight
type NetworkDetailsOptions struct {
	ShowZeroCost         bool
	FilterNetworkDetails filter.Filter
}

// struct to lowest level Ingress and Egress details, interaction
// with the  endPoint, which is a source in case of Ingress and
// destination in case of Egress and also stores Traffic type property,
// which describes the traffic as either Internet, Cross Region or Cross Zone
type NetworkDetail struct {
	Cost             float64                 `json:"cost"`
	Bytes            float64                 `json:"bytes"`
	EndPoint         string                  `json:"endPoint"`
	TrafficDirection NetworkTrafficDirection `json:"trafficDirection"`
	TrafficType      NetworkTrafficType      `json:"trafficType"`
}

func NewNetworkDetail(cost float64,
	bytes float64, endPoint string,
	trafficDirection NetworkTrafficDirection, trafficType NetworkTrafficType) *NetworkDetail {
	return &NetworkDetail{
		Cost:             cost,
		Bytes:            bytes,
		EndPoint:         endPoint,
		TrafficDirection: trafficDirection,
		TrafficType:      trafficType,
	}
}

func (nd *NetworkDetail) Clone() *NetworkDetail {
	return &NetworkDetail{
		Cost:             nd.Cost,
		Bytes:            nd.Bytes,
		EndPoint:         nd.EndPoint,
		TrafficDirection: nd.TrafficDirection,
		TrafficType:      nd.TrafficType,
	}
}

func (nd *NetworkDetail) Key() string {
	if nd == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", nd.EndPoint, nd.TrafficDirection, nd.TrafficType)
}

func (nd *NetworkDetail) Add(that *NetworkDetail) {
	if nd == nil {
		return
	}

	if nd.Key() != that.Key() {
		log.Warnf("adding two NetworkDetail that dont match with key %s: %s", nd.Key(), that.Key())
	}
	nd.Cost += that.Cost
	nd.Bytes += that.Bytes
}

func (nd *NetworkDetail) SanitizeNaN() {
	if math.IsNaN(nd.Cost) {
		log.DedupedWarningf(5, "NetworkDetail: Unexpected NaN found for Cost: name: %s", nd.Key())
		nd.Cost = 0
	}

	if math.IsNaN(nd.Bytes) {
		log.DedupedWarningf(5, "NetworkDetail: Unexpected NaN found for Bytes: name: %s", nd.Key())
		nd.Bytes = 0
	}
}

func (nd *NetworkDetail) IsZeroCost() bool {
	if nd == nil {
		log.DedupedWarningf(5, "nd.IsZeroCost called on a nil network detail")
		return false
	}
	return nd.Cost == 0.0
}

type NetworkDetailsSet map[string]*NetworkDetail

func (nds NetworkDetailsSet) Clone() NetworkDetailsSet {
	retnids := make(NetworkDetailsSet, 0)
	for name, nid := range nds {
		retnids[name] = nid.Clone()
	}
	return retnids
}

func (nds NetworkDetailsSet) Add(nd *NetworkDetail) {
	key := nd.Key()
	if _, ok := nds[key]; ok {
		nds[key].Add(nd)
	} else {
		nds[key] = nd.Clone()
	}
}

// GetTotalInternetCost Gets the total internet cost in egress details i.e has internet flag true
func (nds NetworkDetailsSet) GetTotalInternetCost() float64 {
	totalCost := 0.0
	for _, nd := range nds {
		if nd.TrafficType == NetworkTrafficTypeInternet {
			totalCost += nd.Cost
		}
	}
	return totalCost
}

// Gets the total cross zone cost in egress details i.e has sameZone flag false
func (nds NetworkDetailsSet) GetCrossZoneCost() float64 {
	totalCost := 0.0
	for _, nd := range nds {
		if nd.TrafficType == NetworkTrafficTypeCrossZone {
			totalCost += nd.Cost
		}
	}
	return totalCost
}

// Gets the total cross region cost in egress details i.e has sameRegion flag false
func (nds NetworkDetailsSet) GetCrossRegionCost() float64 {
	totalCost := 0.0
	for _, nd := range nds {
		if nd.TrafficType == NetworkTrafficTypeCrossRegion {
			totalCost += nd.Cost
		}
	}
	return totalCost
}

func (nds NetworkDetailsSet) Combine(that NetworkDetailsSet) {
	for _, nd := range that {
		nds.Add(nd)
	}
}

func (nds NetworkDetailsSet) SanitizeNaN() {
	for _, nd := range nds {
		nd.SanitizeNaN()
	}
}

// filterZeroCost returns a new NetworkDetailsSet with all zero-cost details removed
func (nds NetworkDetailsSet) filterZeroCost() NetworkDetailsSet {
	newNds := make(map[string]*NetworkDetail, 0)
	for key, nd := range nds {
		if nd.IsZeroCost() {
			continue
		}
		newNds[key] = nd.Clone()
	}
	return newNds
}

// NetworkInsight struct that stores pod interactions both egress and ingress
// Currently only Cluster, namespace and pod will be populated from promsource.go
// Rest are placeholders if we need to support single cluster via cost-model
// using the same prometheus source.In aggregator we will join allocation of
// same time window to get the controller, node, labels, region and zone data.
type NetworkInsight struct {
	Cluster                string            `json:"cluster"`
	Namespace              string            `json:"namespace"`
	Controller             string            `json:"controller"`
	Pod                    string            `json:"pod"`
	Node                   string            `json:"node"`
	Labels                 map[string]string `json:"labels"`
	Region                 string            `json:"region"`
	Zone                   string            `json:"zone"`
	NetworkTotalCost       float64           `json:"networkCost"`
	NetworkCrossZoneCost   float64           `json:"networkCrossZoneCost"`
	NetworkCrossRegionCost float64           `json:"networkCrossRegionCost"`
	NetworkInternetCost    float64           `json:"networkInternetCost"`
	NetworkDetails         NetworkDetailsSet `json:"networkDetails"`
}

func NewNetworkInsight(cluster string,
	namespace string, controller string, pod string, node string,
	labels map[string]string, region string, zone string,
	networkTotalCost, networkCrossZoneCost, networkCrossRegionCost, networkInternetCost float64,
	networkDetails map[string]*NetworkDetail) *NetworkInsight {

	if networkDetails == nil {
		networkDetails = make(map[string]*NetworkDetail, 0)
	}

	return &NetworkInsight{
		Cluster:                cluster,
		Namespace:              namespace,
		Controller:             controller,
		Pod:                    pod,
		Node:                   node,
		Labels:                 labels,
		Region:                 region,
		Zone:                   zone,
		NetworkTotalCost:       networkTotalCost,
		NetworkCrossZoneCost:   networkCrossZoneCost,
		NetworkCrossRegionCost: networkCrossRegionCost,
		NetworkInternetCost:    networkInternetCost,
		NetworkDetails:         networkDetails,
	}

}

func (ni *NetworkInsight) Clone() *NetworkInsight {
	if ni == nil {
		return nil
	}
	return &NetworkInsight{
		Cluster:                ni.Cluster,
		Namespace:              ni.Namespace,
		Pod:                    ni.Pod,
		Node:                   ni.Node,
		Labels:                 ni.Labels,
		Region:                 ni.Region,
		Zone:                   ni.Zone,
		NetworkTotalCost:       ni.NetworkTotalCost,
		NetworkCrossZoneCost:   ni.NetworkCrossZoneCost,
		NetworkCrossRegionCost: ni.NetworkCrossRegionCost,
		NetworkInternetCost:    ni.NetworkInternetCost,
		NetworkDetails:         ni.NetworkDetails.Clone(),
	}
}

func (ni *NetworkInsight) add(that *NetworkInsight) {
	if ni == nil {
		log.Warnf("NetworkInsight.Add: trying to add a nil receiver")
		return
	}

	if ni.Cluster != that.Cluster {
		ni.Cluster = ""
	}

	if ni.Namespace != that.Namespace {
		ni.Namespace = ""
	}

	if ni.Pod != that.Pod {
		ni.Pod = ""
	}

	if ni.Controller != that.Controller {
		ni.Controller = ""
	}

	if ni.Node != that.Node {
		ni.Node = ""
	}

	if ni.Region != that.Region {
		ni.Region = ""
	}

	if ni.Zone != that.Zone {
		ni.Zone = ""
	}

	// TO-DO: Check for labels match if we support label in single cluster!

	ni.NetworkTotalCost += that.NetworkTotalCost
	ni.NetworkCrossZoneCost += that.NetworkCrossZoneCost
	ni.NetworkCrossRegionCost += that.NetworkCrossRegionCost
	ni.NetworkInternetCost += that.NetworkInternetCost
	ni.NetworkDetails.Combine(that.NetworkDetails)
}

// Key takes a list of NetworkInsightProperty and creates a "/"
// seperated key based on the values of the requested properties.
// Invalid values and empty slice are set to default key.
func (ni *NetworkInsight) Key(props []NetworkInsightProperty) (string, error) {
	defaultString := fmt.Sprintf("%s/%s/%s", ni.Cluster, ni.Namespace, ni.Pod)
	if len(props) == 0 {
		return defaultString, nil
	}
	values := make([]string, len(props))
	for i, prop := range props {
		switch prop {
		case NetworkInsightsNamespace:
			values[i] = ni.Namespace
		case NetworkInsightsPod:
			values[i] = ni.Pod
		case NetworkInsightsCluster:
			values[i] = ni.Cluster
		default:
			return defaultString, nil
		}
	}
	return strings.Join(values, "/"), nil
}

func (ni *NetworkInsight) GetTotalEgressByte() float64 {
	totalByte := 0.0
	for _, nd := range ni.NetworkDetails {
		if nd == nil || nd.TrafficDirection != NetworkTrafficDirectionEgress {
			continue
		}
		totalByte += nd.Bytes
	}
	return totalByte
}

func (ni *NetworkInsight) GetTotalIngressByte() float64 {
	totalByte := 0.0
	for _, nd := range ni.NetworkDetails {
		if nd == nil || nd.TrafficDirection != NetworkTrafficDirectionIngress {
			continue
		}
		totalByte += nd.Bytes
	}
	return totalByte
}

func (ni *NetworkInsight) SanitizeNaN() {
	if ni == nil {
		return
	}

	key, err := ni.Key([]NetworkInsightProperty{})
	if err != nil {
		log.DedupedWarningf(5, "NetworkInsight: unable to perform santization of network insight for cluster: %s, namespace: %s, pod: %s", ni.Cluster, ni.Namespace, ni.Pod)
	}

	if math.IsNaN(ni.NetworkTotalCost) {
		log.DedupedWarningf(5, "NetworkInsight: Unexpected NaN found for NetworkTotalCost: name: %s", key)
		ni.NetworkTotalCost = 0
	}

	if math.IsNaN(ni.NetworkCrossZoneCost) {
		log.DedupedWarningf(5, "NetworkInsight: Unexpected NaN found for NetworkCrossZoneCost: name: %s", key)
		ni.NetworkCrossZoneCost = 0
	}

	if math.IsNaN(ni.NetworkCrossRegionCost) {
		log.DedupedWarningf(5, "NetworkInsight: Unexpected NaN found for NetworkCrossRegionCost: name: %s", key)
		ni.NetworkCrossRegionCost = 0
	}

	if math.IsNaN(ni.NetworkInternetCost) {
		log.DedupedWarningf(5, "NetworkInsight: Unexpected NaN found for NetworkInternetCost: name: %s", key)
		ni.NetworkInternetCost = 0
	}
	ni.NetworkDetails.SanitizeNaN()
}

func (ni *NetworkInsight) filterZeroCost() {
	if ni == nil {
		return
	}
	ni.NetworkDetails = ni.NetworkDetails.filterZeroCost()
}

func (ni *NetworkInsight) filterNetworkDetails(networkDetailFilter NetworkInsightDetailMatcher) {
	if ni == nil {
		log.DedupedWarningf(5, "NetworkInsight:filterNetworkDetails called on nil network insight")
		return
	}
	newNds := make(NetworkDetailsSet, 0)
	for key, nd := range ni.NetworkDetails {
		if networkDetailFilter.Matches(nd) {
			newNds[key] = nd
		}
	}
	ni.NetworkDetails = newNds
}

// SetWithNetworkInsightProperty sets the corresponding property
// variable in the struct with the value passed to the function.
func (ni *NetworkInsight) SetWithNetworkInsightProperty(property NetworkInsightProperty, value interface{}) error {
	switch property {
	case NetworkInsightsCluster:
		ni.Cluster = value.(string)
	case NetworkInsightsNamespace:
		ni.Namespace = value.(string)
	case NetworkInsightsPod:
		ni.Pod = value.(string)
	}
	return fmt.Errorf("unsupported property: %s", string(property))
}

type NetworkInsightSet struct {
	NetworkInsights map[string]*NetworkInsight `json:"networkInsights"`
	Window          Window                     `json:"window"`
}

// NewNetworkInsightSet instantiates a new NetworkInsights set and, optionally, inserts
// the given list of NetworkInsight
func NewNetworkInsightSet(start, end time.Time, networkInsight ...*NetworkInsight) *NetworkInsightSet {
	nis := &NetworkInsightSet{
		NetworkInsights: make(map[string]*NetworkInsight, 0),
		Window:          NewWindow(&start, &end),
	}

	for _, ni := range networkInsight {
		nis.Insert(ni, []NetworkInsightProperty{})
	}

	return nis
}

func (nis *NetworkInsightSet) Add(that *NetworkInsightSet, keyProperties []NetworkInsightProperty) (*NetworkInsightSet, error) {
	if (nis == nil || len(nis.NetworkInsights) == 0) && (that == nil || len(that.NetworkInsights) == 0) {
		return nis, nil
	}

	if nis == nil || len(nis.NetworkInsights) == 0 {
		return that, nil
	}

	if that == nil || len(that.NetworkInsights) == 0 {
		return that, nil
	}

	start := *nis.Window.Start()
	end := *nis.Window.End()

	if that.Window.Start().Before(start) {
		start = *that.Window.Start()
	}

	if that.Window.End().After(end) {
		end = *that.Window.End()
	}

	acc := &NetworkInsightSet{
		NetworkInsights: make(map[string]*NetworkInsight, len(nis.NetworkInsights)),
		Window:          NewClosedWindow(start, end),
	}

	for _, ni := range nis.NetworkInsights {
		err := acc.Insert(ni, keyProperties)
		if err != nil {
			return nil, err
		}
	}

	for _, ni := range that.NetworkInsights {
		err := acc.Insert(ni, keyProperties)
		if err != nil {
			return nil, err
		}
	}

	return acc, nil
}

func (nis *NetworkInsightSet) Insert(that *NetworkInsight, aggregateBy []NetworkInsightProperty) error {
	if nis == nil {
		return fmt.Errorf("cannot insert into nil networkInsightSet")
	}

	if nis.NetworkInsights == nil {
		nis.NetworkInsights = map[string]*NetworkInsight{}
	}

	key, err := that.Key(aggregateBy)
	if err != nil {
		return fmt.Errorf("unable to generate key for aggregation: %v", err)
	}
	if _, ok := nis.NetworkInsights[key]; !ok {
		nis.NetworkInsights[key] = that
	} else {
		nis.NetworkInsights[key].add(that)
	}
	return nil
}

func (nis *NetworkInsightSet) Clone() *NetworkInsightSet {
	if nis == nil {
		return nil
	}

	networkInsights := make(map[string]*NetworkInsight, len(nis.NetworkInsights))
	for k, v := range nis.NetworkInsights {
		networkInsights[k] = v.Clone()
	}
	return &NetworkInsightSet{
		NetworkInsights: networkInsights,
		Window:          nis.Window.Clone(),
	}
}

func (nis *NetworkInsightSet) GetWindow() Window {
	return nis.Window
}

func (nis *NetworkInsightSet) IsValid() bool {
	if !nis.IsEmpty() {
		return false
	}

	if nis.Window.IsOpen() {
		return false
	}

	return true
}

func (nis *NetworkInsightSet) IsEmpty() bool {
	if nis == nil || len(nis.NetworkInsights) == 0 {
		return true
	}
	return false
}

func (nis *NetworkInsightSet) AggregateBy(aggregateBy []NetworkInsightProperty) error {
	if nis.IsEmpty() {
		return nil
	}

	aggSet := &NetworkInsightSet{}

	for _, ni := range nis.NetworkInsights {
		err := aggSet.Insert(ni, aggregateBy)
		if err != nil {
			return fmt.Errorf("NetworkInsightSet:AggregateBy failed with err: %v", err)
		}
	}

	nis.NetworkInsights = aggSet.NetworkInsights

	return nil
}

func (nis *NetworkInsightSet) Accumulate(that *NetworkInsightSet, keyProperties []NetworkInsightProperty) (*NetworkInsightSet, error) {
	if nis.IsEmpty() {
		return that.Clone(), nil
	}

	if that.IsEmpty() {
		return nis.Clone(), nil
	}

	start := nis.Window.Start()
	end := nis.Window.End()
	if start.After(*that.Window.Start()) {
		start = that.Window.Start()
	}

	if end.Before(*that.Window.End()) {
		end = that.Window.End()
	}
	newNis := nis.Clone()
	newNis.Window = NewClosedWindow(*start, *end)
	for _, ni := range that.NetworkInsights {
		err := newNis.Insert(ni, keyProperties)
		if err != nil {
			return nil, err
		}
	}
	return newNis, nil
}

func (nis *NetworkInsightSet) Length() int {
	if nis == nil {
		return 0
	}

	return len(nis.NetworkInsights)
}

func (nis *NetworkInsightSet) FilterOn(filter filter.Filter) error {
	if nis.IsEmpty() {
		return fmt.Errorf("NetworkInsightSet:FilterOn called on empty network insight set")
	}
	var networkInsightFilter NetworkInsightMatcher
	if filter == nil {
		networkInsightFilter = &matcher.AllPass[*NetworkInsight]{}
	} else {
		compiler := NewNetworkInsightMatchCompiler()
		var err error
		networkInsightFilter, err = compiler.Compile(filter)
		if err != nil {
			return fmt.Errorf("compiling filter '%s': %w", ast.ToPreOrderShortString(filter), err)
		}
	}

	if networkInsightFilter == nil {
		return fmt.Errorf("unexpected nil filter")
	}

	for key, ni := range nis.NetworkInsights {
		if ni == nil {
			continue
		}
		if !networkInsightFilter.Matches(ni) {
			delete(nis.NetworkInsights, key)
		}
	}

	return nil
}

// Resolution returns the NetworkInsightSet's window duration
func (nis *NetworkInsightSet) Resolution() time.Duration {
	if nis == nil {
		return time.Duration(0)
	}
	return nis.Window.Duration()
}

func (nis *NetworkInsightSet) FilterNetworkDetails(opts *NetworkDetailsOptions) error {
	if nis == nil {
		return fmt.Errorf("filterNetworkDetails called on nil network insight set")
	}
	if opts == nil {
		return nil
	}
	var networkDetailFilter NetworkInsightDetailMatcher
	if opts.FilterNetworkDetails == nil {
		networkDetailFilter = &matcher.AllPass[*NetworkDetail]{}
	} else {
		compiler := NewNetworkInsightDetailMatchCompiler()
		var err error
		networkDetailFilter, err = compiler.Compile(opts.FilterNetworkDetails)
		if err != nil {
			return fmt.Errorf("compiling filter '%s': %w", ast.ToPreOrderShortString(opts.FilterNetworkDetails), err)
		}
	}

	if networkDetailFilter == nil {
		return fmt.Errorf("unexpected nil filter")
	}

	for _, ni := range nis.NetworkInsights {
		// filter network details that satisfy the
		// network detail filter
		ni.filterNetworkDetails(networkDetailFilter)
		// filter zero cost network details
		if !opts.ShowZeroCost {
			ni.filterZeroCost()
		}
	}

	return nil
}

func (nis *NetworkInsightSet) SanitizeNaN() {
	if nis == nil {
		return
	}
	for _, ni := range nis.NetworkInsights {
		ni.SanitizeNaN()
	}
}

type NetworkInsightSetRange struct {
	sync.RWMutex
	NetworkInsightsSet []*NetworkInsightSet `json:"networkInsightSet"`
	Window             Window               `json:"window"`
}

func NewNetworkInsightSetRange(window Window, nis ...*NetworkInsightSet) *NetworkInsightSetRange {
	return &NetworkInsightSetRange{
		NetworkInsightsSet: nis,
		Window:             window,
	}
}

func (nisr *NetworkInsightSetRange) AggregateBy(aggregateBy []NetworkInsightProperty) error {
	if nisr == nil || len(nisr.NetworkInsightsSet) == 0 {
		return nil
	}

	if nisr.Window.IsOpen() {
		return fmt.Errorf("cannot aggregate a NetworkInsightSetRange with an open window")
	}

	tempNis := &NetworkInsightSetRange{NetworkInsightsSet: []*NetworkInsightSet{}}

	for _, ni := range nisr.NetworkInsightsSet {
		err := ni.AggregateBy(aggregateBy)
		if err != nil {
			return err
		}
		tempNis.NetworkInsightsSet = append(tempNis.NetworkInsightsSet, ni)
	}

	nisr.NetworkInsightsSet = tempNis.NetworkInsightsSet
	return nil
}

func (nisr *NetworkInsightSetRange) Append(that *NetworkInsightSet) {
	if nisr == nil {
		log.DedupedWarningf(5, "NetworkInsightSetRange:Append called on nil Network Insight Set Range")
		return
	}
	nisr.Lock()
	defer nisr.Unlock()

	nisr.NetworkInsightsSet = append(nisr.NetworkInsightsSet, that)

	// Adjust window
	start := nisr.Window.Start()
	end := nisr.Window.End()
	if nisr.Window.Start() == nil || (that.Window.Start() != nil && that.Window.Start().Before(*nisr.Window.Start())) {
		start = that.Window.Start()
	}
	if nisr.Window.End() == nil || (that.Window.End() != nil && that.Window.End().After(*nisr.Window.End())) {
		end = that.Window.End()
	}
	nisr.Window = NewClosedWindow(*start, *end)
}

func (nisr *NetworkInsightSetRange) Clone() *NetworkInsightSetRange {
	if nisr == nil {
		return nil
	}
	nisrClone := NewNetworkInsightSetRange(nisr.Window)

	for _, nis := range nisr.NetworkInsightsSet {
		nisClone := nis.Clone()
		nisrClone.Append(nisClone)
	}

	return nisrClone
}

func (nisr *NetworkInsightSetRange) accumulateByNone(keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	return nisr.Clone(), nil
}

func (nisr *NetworkInsightSetRange) accumulateByAll(keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	nis, err := nisr.newAccumulation(keyProperties)
	if err != nil {
		return nil, fmt.Errorf("error accumulating NetworkInsightSetRange:%w", err)
	}
	accumulated := NewNetworkInsightSetRange(nisr.Window, nis)
	return accumulated, nil
}

func (nisr *NetworkInsightSetRange) accumulateByHour(keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	if nisr == nil {
		return nil, fmt.Errorf("NetworkInsightSetRange:accumulateByHour called on nil set range")
	}
	// ensure that the network insight set have a 1-hour window and if a set exists
	duration := nisr.Window.Duration()
	if len(nisr.NetworkInsightsSet) > 0 && duration != time.Hour {
		return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", duration.String())
	}

	return nisr.Clone(), nil
}

func (nisr *NetworkInsightSetRange) accumulate(keyProperties []NetworkInsightProperty) (*NetworkInsightSet, error) {
	if nisr == nil {
		return nil, fmt.Errorf("NetworkInsightSetRange:accumulate called on nil set range")
	}
	var result *NetworkInsightSet
	var err error

	nisr.RLock()
	defer nisr.RUnlock()

	for _, ni := range nisr.NetworkInsightsSet {
		result, err = result.Add(ni, keyProperties)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (nisr *NetworkInsightSetRange) accumulateByDay(keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	if nisr == nil {
		return nil, fmt.Errorf("NetworkInsightSetRange:accumulateByDay called on nil set range")
	}
	// if the network insight set window is 1-day, just return the existing allocation set range
	duration := nisr.Window.Duration()
	if len(nisr.NetworkInsightsSet) > 0 && duration == timeutil.Day {
		return nisr, nil
	}

	var toAccumulate *NetworkInsightSetRange
	result := NewNetworkInsightSetRange(NewWindow(nil, nil))
	for i, nis := range nisr.NetworkInsightsSet {

		if nis.Window.Duration() != time.Hour {
			return nil, fmt.Errorf("window duration must equal 1 hour; got:%s", nis.Window.Duration())
		}

		hour := nis.Window.Start().Hour()

		if toAccumulate == nil {
			toAccumulate = NewNetworkInsightSetRange(NewWindow(nil, nil))
			nis = nis.Clone()
		}
		toAccumulate.Append(nis)
		nis, err := toAccumulate.accumulate(keyProperties)
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		if nis == nil {
			continue
		}
		toAccumulate = NewNetworkInsightSetRange(nis.Window, nis)

		if hour == 23 || i == len(nisr.NetworkInsightsSet)-1 {
			if length := len(toAccumulate.NetworkInsightsSet); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.NetworkInsightsSet[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (nisr *NetworkInsightSetRange) accumulateByWeek(keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	if nisr == nil {
		return nil, fmt.Errorf("NetworkInsightSetRange:accumulateByWeek called on nil set range")
	}
	var toAccumulate *NetworkInsightSetRange
	result := NewNetworkInsightSetRange(NewWindow(nil, nil))
	for i, nis := range nisr.NetworkInsightsSet {

		if nis.Window.Duration() != timeutil.Day {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", nis.Window.Duration())
		}

		dayOfWeek := nis.Window.Start().Weekday()

		if toAccumulate == nil {
			toAccumulate = NewNetworkInsightSetRange(NewWindow(nil, nil))
			nis = nis.Clone()
		}

		toAccumulate.Append(nis)
		nis, err := toAccumulate.accumulate(keyProperties)
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}

		if nis == nil {
			continue
		}
		toAccumulate = NewNetworkInsightSetRange(nis.Window, nis)

		if dayOfWeek == time.Saturday || i == len(nisr.NetworkInsightsSet)-1 {
			if length := len(toAccumulate.NetworkInsightsSet); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.NetworkInsightsSet[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (nisr *NetworkInsightSetRange) accumulateByMonth(keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	if nisr == nil {
		return nil, fmt.Errorf("NetworkInsightSetRange:accumulateByMonth called on nil set range")
	}
	var toAccumulate *NetworkInsightSetRange
	result := NewNetworkInsightSetRange(NewWindow(nil, nil))
	for i, nis := range nisr.NetworkInsightsSet {

		if nis.Window.Duration() != timeutil.Day {
			return nil, fmt.Errorf("window duration must equal 24 hours; got:%s", nis.Window.Duration())
		}

		_, month, _ := nis.Window.Start().Date()
		_, nextDayMonth, _ := nis.Window.Start().Add(time.Hour * 24).Date()

		if toAccumulate == nil {
			toAccumulate = NewNetworkInsightSetRange(NewWindow(nil, nil))
			nis = nis.Clone()
		}

		toAccumulate.Append(nis)
		nis, err := toAccumulate.accumulate(keyProperties)
		if err != nil {
			return nil, fmt.Errorf("error accumulating result: %s", err)
		}
		if nis == nil {
			continue
		}
		toAccumulate = NewNetworkInsightSetRange(nis.Window, nis)

		if month != nextDayMonth || i == len(nisr.NetworkInsightsSet)-1 {
			if length := len(toAccumulate.NetworkInsightsSet); length != 1 {
				return nil, fmt.Errorf("failed accumulation, detected %d sets instead of 1", length)
			}
			result.Append(toAccumulate.NetworkInsightsSet[0])
			toAccumulate = nil
		}
	}
	return result, nil
}

func (nisr *NetworkInsightSetRange) Accumulate(accumulateBy AccumulateOption, keyProperties []NetworkInsightProperty) (*NetworkInsightSetRange, error) {
	if nisr == nil {
		return nil, fmt.Errorf("NetworkInsightSetRange:Accumulate called on nil set range")
	}
	switch accumulateBy {
	case AccumulateOptionNone:
		return nisr.accumulateByNone(keyProperties)
	case AccumulateOptionAll:
		return nisr.accumulateByAll(keyProperties)
	case AccumulateOptionHour:
		return nisr.accumulateByHour(keyProperties)
	case AccumulateOptionDay:
		return nisr.accumulateByDay(keyProperties)
	case AccumulateOptionWeek:
		return nisr.accumulateByWeek(keyProperties)
	case AccumulateOptionMonth:
		return nisr.accumulateByMonth(keyProperties)
	default:
		// ideally, this should never happen
		return nil, fmt.Errorf("unexpected error, invalid accumulateByType: %s", accumulateBy)
	}
}

func (nisr *NetworkInsightSetRange) newAccumulation(keyProperties []NetworkInsightProperty) (*NetworkInsightSet, error) {
	if nisr == nil {
		return nil, fmt.Errorf("nil NetworkInsightSetRange in accumulation")
	}

	var networkInsigthSet *NetworkInsightSet
	var err error
	if len(nisr.NetworkInsightsSet) == 0 {
		return nil, fmt.Errorf("NetworkInsightSetRange has empty NetworkInsightSet in accumulation")
	}

	for _, nis := range nisr.NetworkInsightsSet {
		if networkInsigthSet == nil {
			networkInsigthSet = nis.Clone()
			continue
		}

		networkInsigthSet, err = networkInsigthSet.Accumulate(nis, keyProperties)
		if err != nil {
			return nil, err
		}
	}

	return networkInsigthSet, nil
}

func (nisr *NetworkInsightSetRange) FilterOn(filter filter.Filter) error {
	if nisr == nil {
		return fmt.Errorf("filter called on nil networkInsightSetRange")
	}

	for _, nis := range nisr.NetworkInsightsSet {
		err := nis.FilterOn(filter)
		if err != nil {
			return fmt.Errorf("unable to filter nis for window: %s with err: %v", nis.Window.String(), err)
		}
	}
	return nil
}

// FilterNetworkDetails for a given network insight set with the options applied.
// When ShowZeroCost is set to false, all the network detail interactions with
// zero cost are dropped and based on the applied filter only.
func (nisr *NetworkInsightSetRange) FilterNetworkDetails(opts *NetworkDetailsOptions) error {
	if opts == nil {
		return nil
	}
	if nisr == nil {
		return fmt.Errorf("filter called on nil networkInsightSetRange")
	}
	for _, nis := range nisr.NetworkInsightsSet {
		err := nis.FilterNetworkDetails(opts)
		if err != nil {
			return fmt.Errorf("unable to filter network details in nis for window: %s with err: %v", nis.Window.String(), err)
		}
	}
	return nil
}
