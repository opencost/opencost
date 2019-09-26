package costmodel

import (
	"math"
	"sort"
	"strconv"
)

type Aggregation struct {
	Aggregator         string    `json:"aggregator"`
	AggregatorSubField string    `json:"aggregatorSubField"`
	Environment        string    `json:"environment"`
	Cluster            string    `json:"cluster"`
	CPUAllocation      []*Vector `json:"-"`
	CPUCostVector      []*Vector `json:"-"`
	RAMAllocation      []*Vector `json:"-"`
	RAMCostVector      []*Vector `json:"-"`
	PVCostVector       []*Vector `json:"-"`
	GPUAllocation      []*Vector `json:"-"`
	GPUCostVector      []*Vector `json:"-"`
	CPUCost            float64   `json:"cpuCost"`
	RAMCost            float64   `json:"ramCost"`
	GPUCost            float64   `json:"gpuCost"`
	PVCost             float64   `json:"pvCost"`
	NetworkCost        float64   `json:"networkCost"`
	TotalCost          float64   `json:"totalCost"`
}

func AggregateCostModel(costData map[string]*CostData, aggregationField string, aggregationSubField string) map[string]*Aggregation {
	aggregations := make(map[string]*Aggregation)
	for _, costDatum := range costData {
		if aggregationField == "cluster" {
			aggregationHelper(costDatum, aggregationField, aggregationSubField, costDatum.ClusterID, aggregations)
		} else if aggregationField == "namespace" {
			aggregationHelper(costDatum, aggregationField, aggregationSubField, costDatum.Namespace, aggregations)
		} else if aggregationField == "service" {
			if len(costDatum.Services) > 0 {
				aggregationHelper(costDatum, aggregationField, aggregationSubField, costDatum.Services[0], aggregations)
			}
		} else if aggregationField == "deployment" {
			if len(costDatum.Deployments) > 0 {
				aggregationHelper(costDatum, aggregationField, aggregationSubField, costDatum.Deployments[0], aggregations)
			}
		} else if aggregationField == "label" {
			if costDatum.Labels != nil {
				if subfieldName, ok := costDatum.Labels[aggregationSubField]; ok {
					aggregationHelper(costDatum, aggregationField, aggregationSubField, subfieldName, aggregations)
				}
			}
		}
	}
	for _, agg := range aggregations {
		agg.CPUCost = totalVector(agg.CPUCostVector)
		agg.RAMCost = totalVector(agg.RAMCostVector)
		agg.GPUCost = totalVector(agg.GPUCostVector)
		agg.PVCost = totalVector(agg.PVCostVector)
		agg.TotalCost = agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost
	}
	return aggregations
}

func aggregationHelper(costDatum *CostData, aggregator string, aggregatorSubField string, key string, aggregations map[string]*Aggregation) {
	if _, ok := aggregations[key]; !ok {
		agg := &Aggregation{}
		agg.Aggregator = aggregator
		agg.AggregatorSubField = aggregatorSubField
		agg.Environment = key
		agg.Cluster = costDatum.ClusterID
		aggregations[key] = agg
	}
	mergeVectors(costDatum, aggregations[key])
}

func mergeVectors(costDatum *CostData, aggregation *Aggregation) {
	aggregation.CPUAllocation = addVectors(costDatum.CPUAllocation, aggregation.CPUAllocation)
	aggregation.RAMAllocation = addVectors(costDatum.RAMAllocation, aggregation.RAMAllocation)
	aggregation.GPUAllocation = addVectors(costDatum.GPUReq, aggregation.GPUAllocation)

	cpuv, ramv, gpuv, pvvs := getPriceVectors(costDatum)
	aggregation.CPUCostVector = addVectors(cpuv, aggregation.CPUCostVector)
	aggregation.RAMCostVector = addVectors(ramv, aggregation.RAMCostVector)
	aggregation.GPUCostVector = addVectors(gpuv, aggregation.GPUCostVector)
	for _, vectorList := range pvvs {
		aggregation.PVCostVector = addVectors(aggregation.PVCostVector, vectorList)
	}
}

func getPriceVectors(costDatum *CostData) ([]*Vector, []*Vector, []*Vector, [][]*Vector) {
	cpuv := make([]*Vector, 0, len(costDatum.CPUAllocation))
	for _, val := range costDatum.CPUAllocation {
		cost, _ := strconv.ParseFloat(costDatum.NodeData.VCPUCost, 64)
		cpuv = append(cpuv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     val.Value * cost,
		})
	}
	ramv := make([]*Vector, 0, len(costDatum.RAMAllocation))
	for _, val := range costDatum.RAMAllocation {
		cost, _ := strconv.ParseFloat(costDatum.NodeData.RAMCost, 64)
		ramv = append(ramv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value / 1024 / 1024 / 1024) * cost,
		})
	}
	gpuv := make([]*Vector, 0, len(costDatum.GPUReq))
	for _, val := range costDatum.GPUReq {
		cost, _ := strconv.ParseFloat(costDatum.NodeData.GPUCost, 64)
		gpuv = append(gpuv, &Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     val.Value * cost,
		})
	}
	pvvs := make([][]*Vector, 0, len(costDatum.PVCData))
	for _, pvcData := range costDatum.PVCData {
		pvv := make([]*Vector, 0, len(pvcData.Values))
		if pvcData.Volume != nil {
			cost, _ := strconv.ParseFloat(pvcData.Volume.Cost, 64)
			for _, val := range pvcData.Values {
				pvv = append(pvv, &Vector{
					Timestamp: math.Round(val.Timestamp/10) * 10,
					Value:     (val.Value / 1024 / 1024 / 1024) * cost,
				})
			}
			pvvs = append(pvvs, pvv)
		}
	}
	return cpuv, ramv, gpuv, pvvs
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
