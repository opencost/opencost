package costmodel

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/kubecost/cost-model/cloud"
	prometheusClient "github.com/prometheus/client_golang/api"
	"k8s.io/klog"
)

const (
	hoursPerDay   = 24.0
	hoursPerMonth = 730.0
)

type Aggregation struct {
	Aggregator           string    `json:"aggregation"`
	Subfields            []string  `json:"subfields,omitempty"`
	Environment          string    `json:"environment"`
	Cluster              string    `json:"cluster,omitempty"`
	CPUAllocationAverage float64   `json:"cpuAllocationAverage"`
	CPUAllocationVectors []*Vector `json:"-"`
	CPUCost              float64   `json:"cpuCost"`
	CPUCostVector        []*Vector `json:"cpuCostVector,omitempty"`
	CPUEfficiency        float64   `json:"cpuEfficiency"`
	CPURequestedVectors  []*Vector `json:"-"`
	CPUUsedVectors       []*Vector `json:"-"`
	Efficiency           float64   `json:"efficiency"`
	GPUAllocationAverage float64   `json:"gpuAllocationAverage"`
	GPUAllocationVectors []*Vector `json:"-"`
	GPUCost              float64   `json:"gpuCost"`
	GPUCostVector        []*Vector `json:"gpuCostVector,omitempty"`
	RAMAllocationAverage float64   `json:"ramAllocationAverage"`
	RAMAllocationVectors []*Vector `json:"-"`
	RAMCost              float64   `json:"ramCost"`
	RAMCostVector        []*Vector `json:"ramCostVector,omitempty"`
	RAMEfficiency        float64   `json:"ramEfficiency"`
	RAMRequestedVectors  []*Vector `json:"-"`
	RAMUsedVectors       []*Vector `json:"-"`
	PVAllocationAverage  float64   `json:"pvAllocationAverage"`
	PVAllocationVectors  []*Vector `json:"-"`
	PVCost               float64   `json:"pvCost"`
	PVCostVector         []*Vector `json:"pvCostVector,omitempty"`
	NetworkCost          float64   `json:"networkCost"`
	NetworkCostVector    []*Vector `json:"networkCostVector,omitempty"`
	SharedCost           float64   `json:"sharedCost"`
	TotalCost            float64   `json:"totalCost"`
}

func (a *Aggregation) GetDataCount() int {
	length := 0

	if length < len(a.CPUCostVector) {
		length = len(a.CPUCostVector)
	}
	if length < len(a.RAMCostVector) {
		length = len(a.RAMCostVector)
	}
	if length < len(a.PVCostVector) {
		length = len(a.PVCostVector)
	}
	if length < len(a.GPUCostVector) {
		length = len(a.GPUCostVector)
	}
	if length < len(a.NetworkCostVector) {
		length = len(a.NetworkCostVector)
	}

	return length
}

type SharedResourceInfo struct {
	ShareResources  bool
	SharedNamespace map[string]bool
	LabelSelectors  map[string]string
}

func (s *SharedResourceInfo) IsSharedResource(costDatum *CostData) bool {
	if _, ok := s.SharedNamespace[costDatum.Namespace]; ok {
		return true
	}
	for labelName, labelValue := range s.LabelSelectors {
		if val, ok := costDatum.Labels[labelName]; ok {
			if val == labelValue {
				return true
			}
		}
	}
	return false
}

func NewSharedResourceInfo(shareResources bool, sharedNamespaces []string, labelnames []string, labelvalues []string) *SharedResourceInfo {
	sr := &SharedResourceInfo{
		ShareResources:  shareResources,
		SharedNamespace: make(map[string]bool),
		LabelSelectors:  make(map[string]string),
	}
	for _, ns := range sharedNamespaces {
		sr.SharedNamespace[ns] = true
	}
	sr.SharedNamespace["kube-system"] = true // kube-system should be split by default
	for i := range labelnames {
		sr.LabelSelectors[labelnames[i]] = labelvalues[i]
	}
	return sr
}

func ComputeIdleCoefficient(costData map[string]*CostData, cli prometheusClient.Client, cp cloud.Provider, discount float64, windowString, offset, resolution string) (map[string]float64, error) {
	coefficients := make(map[string]float64)

	windowDuration, err := time.ParseDuration(windowString)
	if err != nil {
		return nil, err
	}

	resolutionDuration, err := parseDuration(resolution)
	resolutionCoefficient := resolutionDuration.Hours()

	allTotals, err := ClusterCostsForAllClusters(cli, cp, windowString, offset)
	if err != nil {
		return nil, err
	}
	for cid, totals := range allTotals {
		klog.Infof("%s: %+v", cid, totals)
		if !(len(totals.CPUCost) > 0 && len(totals.MemCost) > 0 && len(totals.StorageCost) > 0) {
			klog.V(1).Infof("WARNING: NO DATA FOR CLUSTER %s. Is it emitting data?", cid)
			coefficients[cid] = 1.0
			continue
		}
		cpuCost, err := strconv.ParseFloat(totals.CPUCost[0][1], 64)
		if err != nil {
			return nil, err
		}
		memCost, err := strconv.ParseFloat(totals.MemCost[0][1], 64)
		if err != nil {
			return nil, err
		}
		storageCost, err := strconv.ParseFloat(totals.StorageCost[0][1], 64)
		if err != nil {
			return nil, err
		}
		totalClusterCost := (cpuCost * (1 - discount)) + (memCost * (1 - discount)) + storageCost
		if err != nil || totalClusterCost == 0.0 {
			return nil, err
		}
		totalClusterCostOverWindow := (totalClusterCost / 730) * windowDuration.Hours()
		totalContainerCost := 0.0
		for _, costDatum := range costData {
			if costDatum.ClusterID == cid {
				cpuv, ramv, gpuv, pvvs, _ := getPriceVectors(cp, costDatum, "", discount, 1)
				totalContainerCost += totalVectors(cpuv) * resolutionCoefficient
				totalContainerCost += totalVectors(ramv) * resolutionCoefficient
				totalContainerCost += totalVectors(gpuv) * resolutionCoefficient
				for _, pv := range pvvs {
					totalContainerCost += totalVectors(pv) * resolutionCoefficient
				}
			}

		}

		coefficients[cid] = totalContainerCost / totalClusterCostOverWindow
	}

	return coefficients, nil
}

// AggregationOptions provides optional parameters to AggregateCostData, allowing callers to perform more complex operations
type AggregationOptions struct {
	DataCount             int                // number of cost data points expected; ensures proper rate calculation if data is incomplete
	Discount              float64            // percent by which to discount CPU, RAM, and GPU cost
	IdleCoefficients      map[string]float64 // scales costs by amount of idle resources on a per-cluster basis
	IncludeEfficiency     bool               // set to true to receive efficiency/usage data
	IncludeTimeSeries     bool               // set to true to receive time series data
	Rate                  string             // set to "hourly", "daily", or "monthly" to receive cost rate, rather than cumulative cost
	ResolutionCoefficient float64            // coefficient for converting hourly costs to per-resolution cost; e.g. 6 for a 6h resolution
	SharedResourceInfo    *SharedResourceInfo
}

// AggregateCostData aggregates raw cost data by field; e.g. namespace, cluster, service, or label. In the case of label, callers
// must pass a slice of subfields indicating the labels by which to group. Provider is used to define custom resource pricing.
// See AggregationOptions for optional parameters.
func AggregateCostData(costData map[string]*CostData, field string, subfields []string, cp cloud.Provider, opts *AggregationOptions) map[string]*Aggregation {
	dataCount := opts.DataCount
	discount := opts.Discount
	idleCoefficients := opts.IdleCoefficients
	includeTimeSeries := opts.IncludeTimeSeries
	includeEfficiency := opts.IncludeEfficiency
	rate := opts.Rate
	sr := opts.SharedResourceInfo

	if idleCoefficients == nil {
		idleCoefficients = make(map[string]float64)
	}

	// resolution coefficient compensates for less-frequent-than-hourly samples by multiplying
	// cumulative values by the hours between samples. does not apply to rate data and defaults
	// to 1.0, which matches hourly sampling of hourly data.
	resolutionCoefficient := opts.ResolutionCoefficient
	if resolutionCoefficient == 0.0 || rate != "" {
		resolutionCoefficient = 1.0
	}

	// aggregations collects key-value pairs of resource group-to-aggregated data
	// e.g. namespace-to-data or label-value-to-data
	aggregations := make(map[string]*Aggregation)

	// sharedResourceCost is the running total cost of resources that should be reported
	// as shared across all other resources, rather than reported as a stand-alone category
	sharedResourceCost := 0.0

	for _, costDatum := range costData {
		idleCoefficient, ok := idleCoefficients[costDatum.ClusterID]
		if !ok {
			idleCoefficient = 1.0
		}
		if sr != nil && sr.ShareResources && sr.IsSharedResource(costDatum) {
			cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, idleCoefficient)
			sharedResourceCost += totalVectors(cpuv) * resolutionCoefficient
			sharedResourceCost += totalVectors(ramv) * resolutionCoefficient
			sharedResourceCost += totalVectors(gpuv) * resolutionCoefficient
			sharedResourceCost += totalVectors(netv)
			for _, pv := range pvvs {
				sharedResourceCost += totalVectors(pv) * resolutionCoefficient
			}
		} else {
			if field == "cluster" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.ClusterID, discount, idleCoefficient)
			} else if field == "namespace" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace, discount, idleCoefficient)
			} else if field == "service" {
				if len(costDatum.Services) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Services[0], discount, idleCoefficient)
				}
			} else if field == "deployment" {
				if len(costDatum.Deployments) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Deployments[0], discount, idleCoefficient)
				}
			} else if field == "daemonset" {
				if len(costDatum.Daemonsets) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Daemonsets[0], discount, idleCoefficient)
				}
			} else if field == "label" {
				if costDatum.Labels != nil {
					for _, sf := range subfields {
						if subfieldName, ok := costDatum.Labels[sf]; ok {
							aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, subfieldName, discount, idleCoefficient)
							break
						}
					}
				}
			} else if field == "pod" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.PodName, discount, idleCoefficient)
			}
		}
	}

	for _, agg := range aggregations {
		agg.CPUCost = totalVectors(agg.CPUCostVector) * resolutionCoefficient
		agg.RAMCost = totalVectors(agg.RAMCostVector) * resolutionCoefficient
		agg.GPUCost = totalVectors(agg.GPUCostVector) * resolutionCoefficient
		agg.PVCost = totalVectors(agg.PVCostVector) * resolutionCoefficient
		agg.NetworkCost = totalVectors(agg.NetworkCostVector)
		agg.SharedCost = sharedResourceCost / float64(len(aggregations))

		if dataCount == 0 {
			dataCount = agg.GetDataCount()
		}

		if rate != "" && dataCount > 0 {
			agg.CPUCost /= float64(dataCount)
			agg.RAMCost /= float64(dataCount)
			agg.GPUCost /= float64(dataCount)
			agg.PVCost /= float64(dataCount)
			agg.NetworkCost /= float64(dataCount)
			agg.SharedCost /= float64(dataCount)
		}

		agg.TotalCost = agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost + agg.NetworkCost + agg.SharedCost

		agg.CPUAllocationAverage = averageVectors(agg.CPUAllocationVectors)
		agg.GPUAllocationAverage = averageVectors(agg.GPUAllocationVectors)
		// convert RAM from bytes to GiB
		agg.RAMAllocationAverage = averageVectors(agg.RAMAllocationVectors) / 1024 / 1024 / 1024
		// convert storage from bytes to GiB
		agg.PVAllocationAverage = averageVectors(agg.PVAllocationVectors) / 1024 / 1024 / 1024

		if includeEfficiency {
			// Default both RAM and CPU to 100% efficiency so that a 0-requested, 0-allocated, 0-used situation
			// returns 100% efficiency, which should be a red-flag.
			//
			// If non-zero numbers are available, then efficiency is defined as:
			//   idlePercentage =  (requested - used) / allocated
			//   efficiency = (1.0 - idlePercentage)
			//
			// It is possible to score > 100% efficiency, which is meant to be interpreted as a red flag.
			// It is not possible to score < 0% efficiency.

			agg.CPUEfficiency = 1.0
			CPUIdle := 0.0
			if agg.CPUAllocationAverage > 0.0 {
				avgCPURequested := averageVectors(agg.CPURequestedVectors)
				avgCPUUsed := averageVectors(agg.CPUUsedVectors)
				CPUIdle = ((avgCPURequested - avgCPUUsed) / agg.CPUAllocationAverage)
				agg.CPUEfficiency = 1.0 - CPUIdle
			}

			agg.RAMEfficiency = 1.0
			RAMIdle := 0.0
			if agg.RAMAllocationAverage > 0.0 {
				avgRAMRequested := averageVectors(agg.RAMRequestedVectors)
				avgRAMUsed := averageVectors(agg.RAMUsedVectors)
				RAMIdle = ((avgRAMRequested - avgRAMUsed) / agg.RAMAllocationAverage)
				agg.RAMEfficiency = 1.0 - RAMIdle
			}

			// Score total efficiency by the sum of CPU and RAM efficiency, weighted by their
			// respective total costs.
			agg.Efficiency = 1.0
			if (agg.CPUCost + agg.RAMCost) > 0 {
				agg.Efficiency = 1.0 - ((agg.CPUCost*CPUIdle)+(agg.RAMCost*RAMIdle))/(agg.CPUCost+agg.RAMCost)
			}
		}

		// remove time series data if it is not explicitly requested
		if !includeTimeSeries {
			agg.CPUCostVector = nil
			agg.RAMCostVector = nil
			agg.GPUCostVector = nil
			agg.PVCostVector = nil
			agg.NetworkCostVector = nil
		}
	}

	return aggregations
}

func aggregateDatum(cp cloud.Provider, aggregations map[string]*Aggregation, costDatum *CostData, field string, subfields []string, rate string, key string, discount float64, idleCoefficient float64) {
	// add new entry to aggregation results if a new key is encountered
	if _, ok := aggregations[key]; !ok {
		agg := &Aggregation{}
		agg.Aggregator = field
		if len(subfields) > 0 {
			agg.Subfields = subfields
		}
		agg.Environment = key
		aggregations[key] = agg
	}

	mergeVectors(cp, costDatum, aggregations[key], rate, discount, idleCoefficient)
}

func mergeVectors(cp cloud.Provider, costDatum *CostData, aggregation *Aggregation, rate string, discount float64, idleCoefficient float64) {
	aggregation.CPUAllocationVectors = addVectors(costDatum.CPUAllocation, aggregation.CPUAllocationVectors)
	aggregation.CPURequestedVectors = addVectors(costDatum.CPUReq, aggregation.CPURequestedVectors)
	aggregation.CPUUsedVectors = addVectors(costDatum.CPUUsed, aggregation.CPUUsedVectors)

	aggregation.RAMAllocationVectors = addVectors(costDatum.RAMAllocation, aggregation.RAMAllocationVectors)
	aggregation.RAMRequestedVectors = addVectors(costDatum.RAMReq, aggregation.RAMRequestedVectors)
	aggregation.RAMUsedVectors = addVectors(costDatum.RAMUsed, aggregation.RAMUsedVectors)

	aggregation.GPUAllocationVectors = addVectors(costDatum.GPUReq, aggregation.GPUAllocationVectors)

	for _, pvcd := range costDatum.PVCData {
		aggregation.PVAllocationVectors = addVectors(pvcd.Values, aggregation.PVAllocationVectors)
	}

	cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, idleCoefficient)
	aggregation.CPUCostVector = addVectors(cpuv, aggregation.CPUCostVector)
	aggregation.RAMCostVector = addVectors(ramv, aggregation.RAMCostVector)
	aggregation.GPUCostVector = addVectors(gpuv, aggregation.GPUCostVector)
	aggregation.NetworkCostVector = addVectors(netv, aggregation.NetworkCostVector)
	for _, vectorList := range pvvs {
		aggregation.PVCostVector = addVectors(aggregation.PVCostVector, vectorList)
	}
}

func getPriceVectors(cp cloud.Provider, costDatum *CostData, rate string, discount float64, idleCoefficient float64) ([]*Vector, []*Vector, []*Vector, [][]*Vector, []*Vector) {
	cpuCostStr := costDatum.NodeData.VCPUCost
	ramCostStr := costDatum.NodeData.RAMCost
	gpuCostStr := costDatum.NodeData.GPUCost
	pvCostStr := costDatum.NodeData.StorageCost

	// If custom pricing is enabled and can be retrieved, replace
	// default cost values with custom values
	customPricing, err := cp.GetConfig()
	if err != nil {
		klog.Errorf("failed to load custom pricing: %s", err)
	}
	if cloud.CustomPricesEnabled(cp) && err == nil {
		if costDatum.NodeData.IsSpot() {
			cpuCostStr = customPricing.SpotCPU
			ramCostStr = customPricing.SpotRAM
			gpuCostStr = customPricing.SpotGPU
		} else {
			cpuCostStr = customPricing.CPU
			ramCostStr = customPricing.RAM
			gpuCostStr = customPricing.GPU
		}
		pvCostStr = customPricing.Storage
	}

	cpuCost, _ := strconv.ParseFloat(cpuCostStr, 64)
	ramCost, _ := strconv.ParseFloat(ramCostStr, 64)
	gpuCost, _ := strconv.ParseFloat(gpuCostStr, 64)
	pvCost, _ := strconv.ParseFloat(pvCostStr, 64)

	// rateCoeff scales the individual time series data values by the appropriate
	// number. Each value is, by default, the daily value, so the scales convert
	// from daily to the target rate.
	rateCoeff := 1.0
	switch rate {
	case "daily":
		rateCoeff = hoursPerDay
	case "monthly":
		rateCoeff = hoursPerMonth
	case "hourly":
	default:
	}

	cpuv := make([]*Vector, 0, len(costDatum.CPUAllocation))
	for _, val := range costDatum.CPUAllocation {
		cpuv = append(cpuv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value * cpuCost * (1 - discount) / idleCoefficient) * rateCoeff,
		})
	}

	ramv := make([]*Vector, 0, len(costDatum.RAMAllocation))
	for _, val := range costDatum.RAMAllocation {
		ramv = append(ramv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     ((val.Value / 1024 / 1024 / 1024) * ramCost * (1 - discount) / idleCoefficient) * rateCoeff,
		})
	}

	gpuv := make([]*Vector, 0, len(costDatum.GPUReq))
	for _, val := range costDatum.GPUReq {
		gpuv = append(gpuv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value * gpuCost * (1 - discount) / idleCoefficient) * rateCoeff,
		})
	}

	pvvs := make([][]*Vector, 0, len(costDatum.PVCData))
	for _, pvcData := range costDatum.PVCData {
		pvv := make([]*Vector, 0, len(pvcData.Values))
		if pvcData.Volume != nil {
			cost, _ := strconv.ParseFloat(pvcData.Volume.Cost, 64)

			// override with custom pricing if enabled
			if cloud.CustomPricesEnabled(cp) {
				cost = pvCost
			}

			for _, val := range pvcData.Values {
				pvv = append(pvv, &Vector{
					Timestamp: math.Round(val.Timestamp/10) * 10,
					Value:     ((val.Value / 1024 / 1024 / 1024) * cost / idleCoefficient) * rateCoeff,
				})
			}
			pvvs = append(pvvs, pvv)
		}
	}

	netv := make([]*Vector, 0, len(costDatum.NetworkData))
	for _, val := range costDatum.NetworkData {
		netv = append(netv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     val.Value,
		})
	}

	return cpuv, ramv, gpuv, pvvs, netv
}

func averageVectors(vectors []*Vector) float64 {
	if len(vectors) == 0 {
		return 0.0
	}
	return totalVectors(vectors) / float64(len(vectors))
}

func totalVectors(vectors []*Vector) float64 {
	total := 0.0
	for _, vector := range vectors {
		total += vector.Value
	}
	return total
}

// roundTimestamp rounds the given timestamp to the given precision; e.g. a
// timestamp given in seconds, rounded to precision 10, will be rounded
// to the nearest value dividible by 10 (24 goes to 20, but 25 goes to 30).
func roundTimestamp(ts float64, precision float64) float64 {
	return math.Round(ts/precision) * precision
}

// NormalizeVectorByVector produces a version of xvs (a slice of Vectors)
// which has had its timestamps rounded and its values divided by the values
// of the Vectors of yvs, such that yvs is the "unit" Vector slice.
func NormalizeVectorByVector(xvs []*Vector, yvs []*Vector) []*Vector {
	// round all non-zero timestamps to the nearest 10 second mark
	for _, yv := range yvs {
		if yv.Timestamp != 0 {
			yv.Timestamp = roundTimestamp(yv.Timestamp, 10.0)
		}
	}
	for _, xv := range xvs {
		if xv.Timestamp != 0 {
			xv.Timestamp = roundTimestamp(xv.Timestamp, 10.0)
		}
	}

	// if xvs is empty, return yvs
	if xvs == nil || len(xvs) == 0 {
		return yvs
	}

	// if yvs is empty, return xvs
	if yvs == nil || len(yvs) == 0 {
		return xvs
	}

	// sum stores the sum of the vector slices xvs and yvs
	var sum []*Vector

	// timestamps stores all timestamps present in both vector slices
	// without duplicates
	var timestamps []float64

	// turn each vector slice into a map of timestamp-to-value so that
	// values at equal timestamps can be lined-up and summed
	xMap := make(map[float64]float64)
	for _, xv := range xvs {
		if xv.Timestamp == 0 {
			continue
		}
		xMap[xv.Timestamp] = xv.Value
		timestamps = append(timestamps, xv.Timestamp)
	}
	yMap := make(map[float64]float64)
	for _, yv := range yvs {
		if yv.Timestamp == 0 {
			continue
		}
		yMap[yv.Timestamp] = yv.Value
		if _, ok := xMap[yv.Timestamp]; !ok {
			// no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, yv.Timestamp)
		}
	}

	// iterate over each timestamp to produce a final normalized vector slice
	sort.Float64s(timestamps)
	for _, t := range timestamps {
		x, okX := xMap[t]
		y, okY := yMap[t]
		sv := &Vector{Timestamp: t}
		if okX && okY && y != 0 {
			sv.Value = x / y
		} else if okX {
			sv.Value = x
		} else if okY {
			sv.Value = 0
		}
		sum = append(sum, sv)
	}

	return sum
}

// addVectors adds two slices of Vectors. Vector timestamps are rounded to the
// nearest ten seconds to allow matching of Vectors within a delta allowance.
// Matching Vectors are summed, while unmatched Vectors are passed through.
// e.g. [(t=1, 1), (t=2, 2)] + [(t=2, 2), (t=3, 3)] = [(t=1, 1), (t=2, 4), (t=3, 3)]
func addVectors(xvs []*Vector, yvs []*Vector) []*Vector {
	// round all non-zero timestamps to the nearest 10 second mark
	for _, yv := range yvs {
		if yv.Timestamp != 0 {
			yv.Timestamp = roundTimestamp(yv.Timestamp, 10.0)
		}
	}
	for _, xv := range xvs {
		if xv.Timestamp != 0 {
			xv.Timestamp = roundTimestamp(xv.Timestamp, 10.0)
		}
	}

	// if xvs is empty, return yvs
	if xvs == nil || len(xvs) == 0 {
		return yvs
	}

	// if yvs is empty, return xvs
	if yvs == nil || len(yvs) == 0 {
		return xvs
	}

	// sum stores the sum of the vector slices xvs and yvs
	var sum []*Vector

	// timestamps stores all timestamps present in both vector slices
	// without duplicates
	var timestamps []float64

	// turn each vector slice into a map of timestamp-to-value so that
	// values at equal timestamps can be lined-up and summed
	xMap := make(map[float64]float64)
	for _, xv := range xvs {
		if xv.Timestamp == 0 {
			continue
		}
		xMap[xv.Timestamp] = xv.Value
		timestamps = append(timestamps, xv.Timestamp)
	}
	yMap := make(map[float64]float64)
	for _, yv := range yvs {
		if yv.Timestamp == 0 {
			continue
		}
		yMap[yv.Timestamp] = yv.Value
		if _, ok := xMap[yv.Timestamp]; !ok {
			// no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, yv.Timestamp)
		}
	}

	// iterate over each timestamp to produce a final summed vector slice
	sort.Float64s(timestamps)
	for _, t := range timestamps {
		x, okX := xMap[t]
		y, okY := yMap[t]
		sv := &Vector{Timestamp: t}
		if okX && okY {
			sv.Value = x + y
		} else if okX {
			sv.Value = x
		} else if okY {
			sv.Value = y
		}
		sum = append(sum, sv)
	}

	return sum
}
