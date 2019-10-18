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

type Aggregation struct {
	Aggregator        string    `json:"aggregation"`
	Subfields         []string  `json:"subfields"`
	Environment       string    `json:"environment"`
	Cluster           string    `json:"cluster,omitempty"`
	CPUAllocation     []*Vector `json:"-"`
	CPUCostVector     []*Vector `json:"cpuCostVector,omitempty"`
	RAMAllocation     []*Vector `json:"-"`
	RAMCostVector     []*Vector `json:"ramCostVector,omitempty"`
	PVCostVector      []*Vector `json:"pvCostVector,omitempty"`
	GPUAllocation     []*Vector `json:"-"`
	GPUCostVector     []*Vector `json:"gpuCostVector,omitempty"`
	NetworkCostVector []*Vector `json:"networkCostVector,omitempty"`
	CPUCost           float64   `json:"cpuCost"`
	RAMCost           float64   `json:"ramCost"`
	GPUCost           float64   `json:"gpuCost"`
	PVCost            float64   `json:"pvCost"`
	NetworkCost       float64   `json:"networkCost"`
	SharedCost        float64   `json:"sharedCost"`
	TotalCost         float64   `json:"totalCost"`
}

const (
	hoursPerDay   = 24.0
	hoursPerMonth = 730.0
)

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

func ComputeIdleCoefficient(costData map[string]*CostData, cli prometheusClient.Client, cp cloud.Provider, discount float64, windowString, offset string) (map[string]float64, error) {

	coefficients := make(map[string]float64)

	windowDuration, err := time.ParseDuration(windowString)
	if err != nil {
		return nil, err
	}
	aggregateContainerCosts := AggregateCostData(cp, costData, 0, "cluster", []string{}, "", false, discount, 1, nil)
	allTotals, err := ClusterCostsForAllClusters(cli, cp, windowString, offset)
	if err != nil {
		return nil, err
	}
	for cid, totals := range allTotals {

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
			cpuv, ramv, gpuv, pvvs, _ := getPriceVectors(cp, costDatum, "", discount, 1)
			totalContainerCost += totalVector(cpuv)
			totalContainerCost += totalVector(ramv)
			totalContainerCost += totalVector(gpuv)
			for _, pv := range pvvs {
				totalContainerCost += totalVector(pv)
			}
		}

		coefficients[cid] = aggregateContainerCosts[cid].TotalCost / totalClusterCostOverWindow
	}

	return coefficients, nil
}

// AggregateCostData reduces the dimensions of raw cost data by field and, optionally, by time. The field parameter determines the field
// by which to group data, with an optional subfield, e.g. for groupings like field="label" and subfield="app" for grouping by "label.app".
func AggregateCostData(cp cloud.Provider, costData map[string]*CostData, dataCount int64, field string, subfields []string, rate string, timeSeries bool, discount float64, idleCoefficient float64, sr *SharedResourceInfo) map[string]*Aggregation {
	// aggregations collects key-value pairs of resource group-to-aggregated data
	// e.g. namespace-to-data or label-value-to-data
	aggregations := make(map[string]*Aggregation)

	// sharedResourceCost is the running total cost of resources that should be reported
	// as shared across all other resources, rather than reported as a stand-alone category
	sharedResourceCost := 0.0

	for _, costDatum := range costData {
		if sr != nil && sr.ShareResources && sr.IsSharedResource(costDatum) {
			cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, idleCoefficient)
			sharedResourceCost += totalVector(cpuv)
			sharedResourceCost += totalVector(ramv)
			sharedResourceCost += totalVector(gpuv)
			sharedResourceCost += totalVector(netv)
			for _, pv := range pvvs {
				sharedResourceCost += totalVector(pv)
			}
		} else {
			if field == "cluster" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.ClusterID, discount, idleCoefficient)
			} else if field == "namespace" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace, discount, idleCoefficient)
			} else if field == "service" {
				if len(costDatum.Services) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Services[0], discount, idleCoefficient)
				}
			} else if field == "deployment" {
				if len(costDatum.Deployments) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Deployments[0], discount, idleCoefficient)
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
			}
		}
	}

	for _, agg := range aggregations {
		agg.CPUCost = totalVector(agg.CPUCostVector)
		agg.RAMCost = totalVector(agg.RAMCostVector)
		agg.GPUCost = totalVector(agg.GPUCostVector)
		agg.PVCost = totalVector(agg.PVCostVector)
		agg.NetworkCost = totalVector(agg.NetworkCostVector)
		agg.SharedCost = sharedResourceCost / float64(len(aggregations))

		if rate != "" {
			klog.V(1).Infof("scaling '%s' costs to '%s' rate by %d", agg.Environment, rate, dataCount)

			if dataCount > 0 {
				agg.CPUCost /= float64(dataCount)
				agg.RAMCost /= float64(dataCount)
				agg.GPUCost /= float64(dataCount)
				agg.PVCost /= float64(dataCount)
				agg.NetworkCost /= float64(dataCount)
				agg.SharedCost /= float64(dataCount)
			}
		}

		agg.TotalCost = agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost + agg.NetworkCost + agg.SharedCost

		// remove time series data if it is not explicitly requested
		if !timeSeries {
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
	// add new entry to aggregation results if a new
	if _, ok := aggregations[key]; !ok {
		agg := &Aggregation{}
		agg.Aggregator = field
		agg.Subfields = subfields
		agg.Environment = key
		aggregations[key] = agg
	}

	mergeVectors(cp, costDatum, aggregations[key], rate, discount, idleCoefficient)
}

func mergeVectors(cp cloud.Provider, costDatum *CostData, aggregation *Aggregation, rate string, discount float64, idleCoefficient float64) {
	aggregation.CPUAllocation = addVectors(costDatum.CPUAllocation, aggregation.CPUAllocation)
	aggregation.RAMAllocation = addVectors(costDatum.RAMAllocation, aggregation.RAMAllocation)
	aggregation.GPUAllocation = addVectors(costDatum.GPUReq, aggregation.GPUAllocation)

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

	netv := costDatum.NetworkData

	return cpuv, ramv, gpuv, pvvs, netv
}

func totalVector(vectors []*Vector) float64 {
	total := 0.0
	for _, vector := range vectors {
		total += vector.Value
	}
	return total
}

func addVectors(req []*Vector, used []*Vector) []*Vector {
	if req == nil || len(req) == 0 {
		for _, usedV := range used {
			if usedV.Timestamp == 0 {
				continue
			}
			usedV.Timestamp = math.Round(usedV.Timestamp/10) * 10
		}
		return used
	}
	if used == nil || len(used) == 0 {
		for _, reqV := range req {
			if reqV.Timestamp == 0 {
				continue
			}
			reqV.Timestamp = math.Round(reqV.Timestamp/10) * 10
		}
		return req
	}
	var allocation []*Vector

	var timestamps []float64
	reqMap := make(map[float64]float64)
	for _, reqV := range req {
		if reqV.Timestamp == 0 {
			continue
		}
		reqV.Timestamp = math.Round(reqV.Timestamp/10) * 10
		reqMap[reqV.Timestamp] = reqV.Value
		timestamps = append(timestamps, reqV.Timestamp)
	}
	usedMap := make(map[float64]float64)
	for _, usedV := range used {
		if usedV.Timestamp == 0 {
			continue
		}
		usedV.Timestamp = math.Round(usedV.Timestamp/10) * 10
		usedMap[usedV.Timestamp] = usedV.Value
		if _, ok := reqMap[usedV.Timestamp]; !ok { // no need to double add, since we'll range over sorted timestamps and check.
			timestamps = append(timestamps, usedV.Timestamp)
		}
	}

	sort.Float64s(timestamps)
	for _, t := range timestamps {
		rv, okR := reqMap[t]
		uv, okU := usedMap[t]
		allocationVector := &Vector{
			Timestamp: t,
		}
		if okR && okU {
			allocationVector.Value = rv + uv
		} else if okR {
			allocationVector.Value = rv
		} else if okU {
			allocationVector.Value = uv
		}
		allocation = append(allocation, allocationVector)
	}

	return allocation
}
