package costmodel

import (
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"
	"github.com/kubecost/cost-model/pkg/util"
	"github.com/patrickmn/go-cache"
	prometheusClient "github.com/prometheus/client_golang/api"
	"k8s.io/klog"
)

const (
	// SplitTypeWeighted signals that shared costs should be shared
	// proportionally, rather than evenly
	SplitTypeWeighted = "weighted"

	// UnallocatedSubfield indicates an allocation datum that does not have the
	// chosen Aggregator; e.g. during aggregation by some label, there may be
	// cost data that do not have the given label.
	UnallocatedSubfield = "__unallocated__"
)

// Aggregation describes aggregated cost data, containing cumulative cost and
// allocation data per resource, vectors of rate data per resource, efficiency
// data, and metadata describing the type of aggregation operation.
type Aggregation struct {
	Aggregator                 string               `json:"aggregation"`
	Subfields                  []string             `json:"subfields,omitempty"`
	Environment                string               `json:"environment"`
	Cluster                    string               `json:"cluster,omitempty"`
	Properties                 *kubecost.Properties `json:"-"`
	CPUAllocationHourlyAverage float64              `json:"cpuAllocationAverage"`
	CPUAllocationVectors       []*util.Vector       `json:"-"`
	CPUAllocationTotal         float64              `json:"-"`
	CPUCost                    float64              `json:"cpuCost"`
	CPUCostVector              []*util.Vector       `json:"cpuCostVector,omitempty"`
	CPUEfficiency              float64              `json:"cpuEfficiency"`
	CPURequestedVectors        []*util.Vector       `json:"-"`
	CPUUsedVectors             []*util.Vector       `json:"-"`
	Efficiency                 float64              `json:"efficiency"`
	GPUAllocationHourlyAverage float64              `json:"gpuAllocationAverage"`
	GPUAllocationVectors       []*util.Vector       `json:"-"`
	GPUCost                    float64              `json:"gpuCost"`
	GPUCostVector              []*util.Vector       `json:"gpuCostVector,omitempty"`
	GPUAllocationTotal         float64              `json:"-"`
	RAMAllocationHourlyAverage float64              `json:"ramAllocationAverage"`
	RAMAllocationVectors       []*util.Vector       `json:"-"`
	RAMAllocationTotal         float64              `json:"-"`
	RAMCost                    float64              `json:"ramCost"`
	RAMCostVector              []*util.Vector       `json:"ramCostVector,omitempty"`
	RAMEfficiency              float64              `json:"ramEfficiency"`
	RAMRequestedVectors        []*util.Vector       `json:"-"`
	RAMUsedVectors             []*util.Vector       `json:"-"`
	PVAllocationHourlyAverage  float64              `json:"pvAllocationAverage"`
	PVAllocationVectors        []*util.Vector       `json:"-"`
	PVAllocationTotal          float64              `json:"-"`
	PVCost                     float64              `json:"pvCost"`
	PVCostVector               []*util.Vector       `json:"pvCostVector,omitempty"`
	NetworkCost                float64              `json:"networkCost"`
	NetworkCostVector          []*util.Vector       `json:"networkCostVector,omitempty"`
	SharedCost                 float64              `json:"sharedCost"`
	TotalCost                  float64              `json:"totalCost"`
	TotalCostVector            []*util.Vector       `json:"totalCostVector,omitempty"`
}

// TotalHours determines the amount of hours the Aggregation covers, as a
// function of the cost vectors and the resolution of those vectors' data
func (a *Aggregation) TotalHours(resolutionHours float64) float64 {
	length := 1

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

	return float64(length) * resolutionHours
}

// RateCoefficient computes the coefficient by which the total cost needs to be
// multiplied in order to convert totals costs into per-rate costs.
func (a *Aggregation) RateCoefficient(rateStr string, resolutionHours float64) float64 {
	// monthly rate = (730.0)*(total cost)/(total hours)
	// daily rate = (24.0)*(total cost)/(total hours)
	// hourly rate = (1.0)*(total cost)/(total hours)

	// default to hourly rate
	coeff := 1.0
	switch rateStr {
	case "daily":
		coeff = util.HoursPerDay
	case "monthly":
		coeff = util.HoursPerMonth
	}

	return coeff / a.TotalHours(resolutionHours)
}

type SharedResourceInfo struct {
	ShareResources  bool
	SharedNamespace map[string]bool
	LabelSelectors  map[string]map[string]bool
}

type SharedCostInfo struct {
	Name      string
	Cost      float64
	ShareType string
}

func (s *SharedResourceInfo) IsSharedResource(costDatum *CostData) bool {
	// exists in a shared namespace
	if _, ok := s.SharedNamespace[costDatum.Namespace]; ok {
		return true
	}
	// has at least one shared label (OR, not AND in the case of multiple labels)
	for labelName, labelValues := range s.LabelSelectors {
		if val, ok := costDatum.Labels[labelName]; ok && labelValues[val] {
			return true
		}
	}
	return false
}

func NewSharedResourceInfo(shareResources bool, sharedNamespaces []string, labelNames []string, labelValues []string) *SharedResourceInfo {
	sr := &SharedResourceInfo{
		ShareResources:  shareResources,
		SharedNamespace: make(map[string]bool),
		LabelSelectors:  make(map[string]map[string]bool),
	}

	for _, ns := range sharedNamespaces {
		sr.SharedNamespace[strings.Trim(ns, " ")] = true
	}

	// Creating a map of label name to label value, but only if
	// the cardinality matches
	if len(labelNames) == len(labelValues) {
		for i := range labelNames {
			cleanedLname := prom.SanitizeLabelName(strings.Trim(labelNames[i], " "))
			if values, ok := sr.LabelSelectors[cleanedLname]; ok {
				values[strings.Trim(labelValues[i], " ")] = true
			} else {
				sr.LabelSelectors[cleanedLname] = map[string]bool{strings.Trim(labelValues[i], " "): true}
			}
		}
	}

	return sr
}

func GetTotalContainerCost(costData map[string]*CostData, rate string, cp cloud.Provider, discount float64, customDiscount float64, idleCoefficients map[string]float64) float64 {
	totalContainerCost := 0.0
	for _, costDatum := range costData {
		clusterID := costDatum.ClusterID
		cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, customDiscount, idleCoefficients[clusterID])
		totalContainerCost += totalVectors(cpuv)
		totalContainerCost += totalVectors(ramv)
		totalContainerCost += totalVectors(gpuv)
		for _, pv := range pvvs {
			totalContainerCost += totalVectors(pv)
		}
		totalContainerCost += totalVectors(netv)
	}
	return totalContainerCost
}

func (a *Accesses) ComputeIdleCoefficient(costData map[string]*CostData, cli prometheusClient.Client, cp cloud.Provider, discount float64, customDiscount float64, windowString, offset string) (map[string]float64, error) {
	coefficients := make(map[string]float64)

	profileName := "ComputeIdleCoefficient: ComputeClusterCosts"
	profileStart := time.Now()

	var clusterCosts map[string]*ClusterCosts
	var err error

	key := fmt.Sprintf("%s:%s", windowString, offset)
	if data, valid := a.ClusterCostsCache.Get(key); valid {
		clusterCosts = data.(map[string]*ClusterCosts)
	} else {
		clusterCosts, err = a.ComputeClusterCosts(cli, cp, windowString, offset, false)
		if err != nil {
			return nil, err
		}
	}

	measureTime(profileStart, profileThreshold, profileName)

	for cid, costs := range clusterCosts {
		if costs.CPUCumulative == 0 && costs.RAMCumulative == 0 && costs.StorageCumulative == 0 {
			klog.V(1).Infof("[Warning] No ClusterCosts data for cluster '%s'. Is it emitting data?", cid)
			coefficients[cid] = 1.0
			continue
		}

		if costs.TotalCumulative == 0 {
			return nil, fmt.Errorf("TotalCumulative cluster cost for cluster '%s' returned 0 over window '%s' offset '%s'", cid, windowString, offset)
		}

		totalContainerCost := 0.0
		for _, costDatum := range costData {
			if costDatum.ClusterID == cid {
				cpuv, ramv, gpuv, pvvs, _ := getPriceVectors(cp, costDatum, "", discount, customDiscount, 1)
				totalContainerCost += totalVectors(cpuv)
				totalContainerCost += totalVectors(ramv)
				totalContainerCost += totalVectors(gpuv)
				for _, pv := range pvvs {
					totalContainerCost += totalVectors(pv)
				}
			}
		}

		coeff := totalContainerCost / costs.TotalCumulative
		coefficients[cid] = coeff
	}

	return coefficients, nil
}

// AggregationOptions provides optional parameters to AggregateCostData, allowing callers to perform more complex operations
type AggregationOptions struct {
	Discount               float64            // percent by which to discount CPU, RAM, and GPU cost
	CustomDiscount         float64            // additional custom discount applied to all prices
	IdleCoefficients       map[string]float64 // scales costs by amount of idle resources on a per-cluster basis
	IncludeEfficiency      bool               // set to true to receive efficiency/usage data
	IncludeTimeSeries      bool               // set to true to receive time series data
	Rate                   string             // set to "hourly", "daily", or "monthly" to receive cost rate, rather than cumulative cost
	ResolutionHours        float64
	SharedResourceInfo     *SharedResourceInfo
	SharedCosts            map[string]*SharedCostInfo
	FilteredContainerCount int
	FilteredEnvironments   map[string]int
	SharedSplit            string
	TotalContainerCost     float64
}

// Helper method to test request/usgae values against allocation averages for efficiency scores. Generate a warning log if
// clamp is required
func clampAverage(requestsAvg float64, usedAverage float64, allocationAvg float64, resource string) (float64, float64) {
	rAvg := requestsAvg
	if rAvg > allocationAvg {
		klog.V(4).Infof("[Warning] Average %s Requested (%f) > Average %s Allocated (%f). Clamping.", resource, rAvg, resource, allocationAvg)
		rAvg = allocationAvg
	}

	uAvg := usedAverage
	if uAvg > allocationAvg {
		klog.V(4).Infof("[Warning]: Average %s Used (%f) > Average %s Allocated (%f). Clamping.", resource, uAvg, resource, allocationAvg)
		uAvg = allocationAvg
	}

	return rAvg, uAvg
}

// AggregateCostData aggregates raw cost data by field; e.g. namespace, cluster, service, or label. In the case of label, callers
// must pass a slice of subfields indicating the labels by which to group. Provider is used to define custom resource pricing.
// See AggregationOptions for optional parameters.
func AggregateCostData(costData map[string]*CostData, field string, subfields []string, cp cloud.Provider, opts *AggregationOptions) map[string]*Aggregation {
	discount := opts.Discount
	customDiscount := opts.CustomDiscount
	idleCoefficients := opts.IdleCoefficients
	includeTimeSeries := opts.IncludeTimeSeries
	includeEfficiency := opts.IncludeEfficiency
	rate := opts.Rate
	sr := opts.SharedResourceInfo

	resolutionHours := 1.0
	if opts.ResolutionHours > 0.0 {
		resolutionHours = opts.ResolutionHours
	}

	if idleCoefficients == nil {
		idleCoefficients = make(map[string]float64)
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
			cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, customDiscount, idleCoefficient)
			sharedResourceCost += totalVectors(cpuv)
			sharedResourceCost += totalVectors(ramv)
			sharedResourceCost += totalVectors(gpuv)
			sharedResourceCost += totalVectors(netv)
			for _, pv := range pvvs {
				sharedResourceCost += totalVectors(pv)
			}
		} else {
			if field == "cluster" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.ClusterID, discount, customDiscount, idleCoefficient, false)
			} else if field == "node" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.NodeName, discount, customDiscount, idleCoefficient, false)
			} else if field == "namespace" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace, discount, customDiscount, idleCoefficient, false)
			} else if field == "service" {
				if len(costDatum.Services) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Services[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "deployment" {
				if len(costDatum.Deployments) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Deployments[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "statefulset" {
				if len(costDatum.Statefulsets) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Statefulsets[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "daemonset" {
				if len(costDatum.Daemonsets) > 0 {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.Daemonsets[0], discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "controller" {
				if controller, kind, hasController := costDatum.GetController(); hasController {
					key := fmt.Sprintf("%s/%s:%s", costDatum.Namespace, kind, controller)
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, key, discount, customDiscount, idleCoefficient, false)
				} else {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "label" {
				found := false
				if costDatum.Labels != nil {
					for _, sf := range subfields {
						if subfieldName, ok := costDatum.Labels[sf]; ok {
							aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, subfieldName, discount, customDiscount, idleCoefficient, false)
							found = true
							break
						}
					}
				}
				if !found {
					aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, UnallocatedSubfield, discount, customDiscount, idleCoefficient, false)
				}
			} else if field == "pod" {
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, costDatum.Namespace+"/"+costDatum.PodName, discount, customDiscount, idleCoefficient, false)
			} else if field == "container" {
				key := fmt.Sprintf("%s/%s/%s/%s", costDatum.ClusterID, costDatum.Namespace, costDatum.PodName, costDatum.Name)
				aggregateDatum(cp, aggregations, costDatum, field, subfields, rate, key, discount, customDiscount, idleCoefficient, true)
			}
		}
	}

	for key, agg := range aggregations {
		sharedCoefficient := 1 / float64(len(opts.FilteredEnvironments)+len(aggregations))

		agg.CPUCost = totalVectors(agg.CPUCostVector)
		agg.RAMCost = totalVectors(agg.RAMCostVector)
		agg.GPUCost = totalVectors(agg.GPUCostVector)
		agg.PVCost = totalVectors(agg.PVCostVector)
		agg.NetworkCost = totalVectors(agg.NetworkCostVector)
		if opts.SharedSplit == SplitTypeWeighted {
			d := opts.TotalContainerCost - sharedResourceCost
			if d == 0 {
				klog.V(1).Infof("[Warning] Total container cost '%f' and shared resource cost '%f are the same'. Setting sharedCoefficient to 1", opts.TotalContainerCost, sharedResourceCost)
				sharedCoefficient = 1.0
			} else {
				sharedCoefficient = (agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost + agg.NetworkCost) / d
			}
		}
		agg.SharedCost = sharedResourceCost * sharedCoefficient

		for _, v := range opts.SharedCosts {
			agg.SharedCost += v.Cost * sharedCoefficient
		}

		if rate != "" {
			rateCoeff := agg.RateCoefficient(rate, resolutionHours)
			agg.CPUCost *= rateCoeff
			agg.RAMCost *= rateCoeff
			agg.GPUCost *= rateCoeff
			agg.PVCost *= rateCoeff
			agg.NetworkCost *= rateCoeff
			agg.SharedCost *= rateCoeff
		}

		agg.TotalCost = agg.CPUCost + agg.RAMCost + agg.GPUCost + agg.PVCost + agg.NetworkCost + agg.SharedCost

		// Evicted and Completed Pods can still show up here, but have 0 cost.
		// Filter these by default. Any reason to keep them?
		if agg.TotalCost == 0 {
			delete(aggregations, key)
			continue
		}

		// CPU, RAM, and PV allocation are cumulative per-datum, whereas GPU is rate per-datum
		agg.CPUAllocationHourlyAverage = totalVectors(agg.CPUAllocationVectors) / agg.TotalHours(resolutionHours)
		agg.RAMAllocationHourlyAverage = totalVectors(agg.RAMAllocationVectors) / agg.TotalHours(resolutionHours)
		agg.GPUAllocationHourlyAverage = averageVectors(agg.GPUAllocationVectors)
		agg.PVAllocationHourlyAverage = totalVectors(agg.PVAllocationVectors) / agg.TotalHours(resolutionHours)

		// TODO niko/etl does this check out for GPU data? Do we need to rewrite GPU queries to be
		// culumative?
		agg.CPUAllocationTotal = totalVectors(agg.CPUAllocationVectors)
		agg.GPUAllocationTotal = totalVectors(agg.GPUAllocationVectors)
		agg.PVAllocationTotal = totalVectors(agg.PVAllocationVectors)
		agg.RAMAllocationTotal = totalVectors(agg.RAMAllocationVectors)

		if includeEfficiency {
			// Default both RAM and CPU to 0% efficiency so that a 0-requested, 0-allocated, 0-used situation
			// returns 0% efficiency, which should be a red-flag.
			//
			// If non-zero numbers are available, then efficiency is defined as:
			//   idlePercentage =  (requested - used) / allocated
			//   efficiency = (1.0 - idlePercentage)
			//
			// It is possible to score > 100% efficiency, which is meant to be interpreted as a red flag.
			// It is not possible to score < 0% efficiency.

			agg.CPUEfficiency = 0.0
			CPUIdle := 0.0
			if agg.CPUAllocationHourlyAverage > 0.0 {
				avgCPURequested := averageVectors(agg.CPURequestedVectors)
				avgCPUUsed := averageVectors(agg.CPUUsedVectors)

				// Clamp averages, log range violations
				avgCPURequested, avgCPUUsed = clampAverage(avgCPURequested, avgCPUUsed, agg.CPUAllocationHourlyAverage, "CPU")

				CPUIdle = ((avgCPURequested - avgCPUUsed) / agg.CPUAllocationHourlyAverage)
				agg.CPUEfficiency = 1.0 - CPUIdle
			}

			agg.RAMEfficiency = 0.0
			RAMIdle := 0.0
			if agg.RAMAllocationHourlyAverage > 0.0 {
				avgRAMRequested := averageVectors(agg.RAMRequestedVectors)
				avgRAMUsed := averageVectors(agg.RAMUsedVectors)

				// Clamp averages, log range violations
				avgRAMRequested, avgRAMUsed = clampAverage(avgRAMRequested, avgRAMUsed, agg.RAMAllocationHourlyAverage, "RAM")

				RAMIdle = ((avgRAMRequested - avgRAMUsed) / agg.RAMAllocationHourlyAverage)
				agg.RAMEfficiency = 1.0 - RAMIdle
			}

			// Score total efficiency by the sum of CPU and RAM efficiency, weighted by their
			// respective total costs.
			agg.Efficiency = 0.0
			if (agg.CPUCost + agg.RAMCost) > 0 {
				agg.Efficiency = ((agg.CPUCost * agg.CPUEfficiency) + (agg.RAMCost * agg.RAMEfficiency)) / (agg.CPUCost + agg.RAMCost)
			}
		}

		// convert RAM from bytes to GiB
		agg.RAMAllocationHourlyAverage = agg.RAMAllocationHourlyAverage / 1024 / 1024 / 1024
		// convert storage from bytes to GiB
		agg.PVAllocationHourlyAverage = agg.PVAllocationHourlyAverage / 1024 / 1024 / 1024

		// remove time series data if it is not explicitly requested
		if !includeTimeSeries {
			agg.CPUCostVector = nil
			agg.RAMCostVector = nil
			agg.GPUCostVector = nil
			agg.PVCostVector = nil
			agg.NetworkCostVector = nil
			agg.TotalCostVector = nil
		} else { // otherwise compute a totalcostvector
			v1 := addVectors(agg.CPUCostVector, agg.RAMCostVector)
			v2 := addVectors(v1, agg.GPUCostVector)
			v3 := addVectors(v2, agg.PVCostVector)
			v4 := addVectors(v3, agg.NetworkCostVector)
			agg.TotalCostVector = v4
		}
		// Typesafety checks
		if math.IsNaN(agg.CPUAllocationHourlyAverage) || math.IsInf(agg.CPUAllocationHourlyAverage, 0) {
			klog.V(1).Infof("[Warning] CPUAllocationHourlyAverage is %f for '%s: %s/%s'", agg.CPUAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.CPUAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.CPUCost) || math.IsInf(agg.CPUCost, 0) {
			klog.V(1).Infof("[Warning] CPUCost is %f for '%s: %s/%s'", agg.CPUCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.CPUCost = 0
		}
		if math.IsNaN(agg.CPUEfficiency) || math.IsInf(agg.CPUEfficiency, 0) {
			klog.V(1).Infof("[Warning] CPUEfficiency is %f for '%s: %s/%s'", agg.CPUEfficiency, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.CPUEfficiency = 0
		}
		if math.IsNaN(agg.Efficiency) || math.IsInf(agg.Efficiency, 0) {
			klog.V(1).Infof("[Warning] Efficiency is %f for '%s: %s/%s'", agg.Efficiency, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.Efficiency = 0
		}
		if math.IsNaN(agg.GPUAllocationHourlyAverage) || math.IsInf(agg.GPUAllocationHourlyAverage, 0) {
			klog.V(1).Infof("[Warning] GPUAllocationHourlyAverage is %f for '%s: %s/%s'", agg.GPUAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.GPUAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.GPUCost) || math.IsInf(agg.GPUCost, 0) {
			klog.V(1).Infof("[Warning] GPUCost is %f for '%s: %s/%s'", agg.GPUCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.GPUCost = 0
		}
		if math.IsNaN(agg.RAMAllocationHourlyAverage) || math.IsInf(agg.RAMAllocationHourlyAverage, 0) {
			klog.V(1).Infof("[Warning] RAMAllocationHourlyAverage is %f for '%s: %s/%s'", agg.RAMAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.RAMAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.RAMCost) || math.IsInf(agg.RAMCost, 0) {
			klog.V(1).Infof("[Warning] RAMCost is %f for '%s: %s/%s'", agg.RAMCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.RAMCost = 0
		}
		if math.IsNaN(agg.RAMEfficiency) || math.IsInf(agg.RAMEfficiency, 0) {
			klog.V(1).Infof("[Warning] RAMEfficiency is %f for '%s: %s/%s'", agg.RAMEfficiency, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.RAMEfficiency = 0
		}
		if math.IsNaN(agg.PVAllocationHourlyAverage) || math.IsInf(agg.PVAllocationHourlyAverage, 0) {
			klog.V(1).Infof("[Warning] PVAllocationHourlyAverage is %f for '%s: %s/%s'", agg.PVAllocationHourlyAverage, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.PVAllocationHourlyAverage = 0
		}
		if math.IsNaN(agg.PVCost) || math.IsInf(agg.PVCost, 0) {
			klog.V(1).Infof("[Warning] PVCost is %f for '%s: %s/%s'", agg.PVCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.PVCost = 0
		}
		if math.IsNaN(agg.NetworkCost) || math.IsInf(agg.NetworkCost, 0) {
			klog.V(1).Infof("[Warning] NetworkCost is %f for '%s: %s/%s'", agg.NetworkCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.NetworkCost = 0
		}
		if math.IsNaN(agg.SharedCost) || math.IsInf(agg.SharedCost, 0) {
			klog.V(1).Infof("[Warning] SharedCost is %f for '%s: %s/%s'", agg.SharedCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.SharedCost = 0
		}
		if math.IsNaN(agg.TotalCost) || math.IsInf(agg.TotalCost, 0) {
			klog.V(1).Infof("[Warning] TotalCost is %f for '%s: %s/%s'", agg.TotalCost, agg.Cluster, agg.Aggregator, agg.Environment)
			agg.TotalCost = 0
		}
	}

	return aggregations
}

func aggregateDatum(cp cloud.Provider, aggregations map[string]*Aggregation, costDatum *CostData, field string, subfields []string, rate string, key string, discount float64, customDiscount float64, idleCoefficient float64, includeProperties bool) {
	// add new entry to aggregation results if a new key is encountered
	if _, ok := aggregations[key]; !ok {
		agg := &Aggregation{
			Aggregator:  field,
			Environment: key,
		}
		if len(subfields) > 0 {
			agg.Subfields = subfields
		}
		if includeProperties {
			props := &kubecost.Properties{}
			props.SetCluster(costDatum.ClusterID)
			props.SetNode(costDatum.NodeName)
			if controller, kind, hasController := costDatum.GetController(); hasController {
				props.SetController(controller)
				props.SetControllerKind(kind)
			}
			props.SetLabels(costDatum.Labels)
			props.SetNamespace(costDatum.Namespace)
			props.SetPod(costDatum.PodName)
			props.SetServices(costDatum.Services)
			props.SetContainer(costDatum.Name)
			agg.Properties = props
		}

		aggregations[key] = agg
	}

	mergeVectors(cp, costDatum, aggregations[key], rate, discount, customDiscount, idleCoefficient)
}

func mergeVectors(cp cloud.Provider, costDatum *CostData, aggregation *Aggregation, rate string, discount float64, customDiscount float64, idleCoefficient float64) {
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

	cpuv, ramv, gpuv, pvvs, netv := getPriceVectors(cp, costDatum, rate, discount, customDiscount, idleCoefficient)
	aggregation.CPUCostVector = addVectors(cpuv, aggregation.CPUCostVector)
	aggregation.RAMCostVector = addVectors(ramv, aggregation.RAMCostVector)
	aggregation.GPUCostVector = addVectors(gpuv, aggregation.GPUCostVector)
	aggregation.NetworkCostVector = addVectors(netv, aggregation.NetworkCostVector)
	for _, vectorList := range pvvs {
		aggregation.PVCostVector = addVectors(aggregation.PVCostVector, vectorList)
	}
}

// Returns the blended discounts applied to the node as a result of global discounts and reserved instance
// discounts
func getDiscounts(costDatum *CostData, cpuCost float64, ramCost float64, discount float64) (float64, float64) {
	if costDatum.NodeData == nil {
		return discount, discount
	}
	if costDatum.NodeData.IsSpot() {
		return 0, 0
	}

	reserved := costDatum.NodeData.Reserved

	// blended discounts
	blendedCPUDiscount := discount
	blendedRAMDiscount := discount

	if reserved != nil && reserved.CPUCost > 0 && reserved.RAMCost > 0 {
		reservedCPUDiscount := 0.0
		if cpuCost == 0 {
			klog.V(1).Infof("[Warning] No cpu cost found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
		} else {
			reservedCPUDiscount = 1.0 - (reserved.CPUCost / cpuCost)
		}
		reservedRAMDiscount := 0.0
		if ramCost == 0 {
			klog.V(1).Infof("[Warning] No ram cost found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
		} else {
			reservedRAMDiscount = 1.0 - (reserved.RAMCost / ramCost)
		}

		// AWS passes the # of reserved CPU and RAM as -1 to represent "All"
		if reserved.ReservedCPU < 0 && reserved.ReservedRAM < 0 {
			blendedCPUDiscount = reservedCPUDiscount
			blendedRAMDiscount = reservedRAMDiscount
		} else {
			nodeCPU, ierr := strconv.ParseInt(costDatum.NodeData.VCPU, 10, 64)
			nodeRAM, ferr := strconv.ParseFloat(costDatum.NodeData.RAMBytes, 64)
			if ierr == nil && ferr == nil {
				nodeRAMGB := nodeRAM / 1024 / 1024 / 1024
				reservedRAMGB := float64(reserved.ReservedRAM) / 1024 / 1024 / 1024
				nonReservedCPU := nodeCPU - reserved.ReservedCPU
				nonReservedRAM := nodeRAMGB - reservedRAMGB

				if nonReservedCPU == 0 {
					blendedCPUDiscount = reservedCPUDiscount
				} else {
					if nodeCPU == 0 {
						klog.V(1).Infof("[Warning] No ram found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
					} else {
						blendedCPUDiscount = (float64(reserved.ReservedCPU) * reservedCPUDiscount) + (float64(nonReservedCPU)*discount)/float64(nodeCPU)
					}
				}

				if nonReservedRAM == 0 {
					blendedRAMDiscount = reservedRAMDiscount
				} else {
					if nodeRAMGB == 0 {
						klog.V(1).Infof("[Warning] No ram found for cluster '%s' node '%s'", costDatum.ClusterID, costDatum.NodeName)
					} else {
						blendedRAMDiscount = (reservedRAMGB * reservedRAMDiscount) + (nonReservedRAM*discount)/nodeRAMGB
					}
				}
			}
		}
	}

	return blendedCPUDiscount, blendedRAMDiscount
}

func parseVectorPricing(cfg *cloud.CustomPricing, costDatum *CostData, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr string) (float64, float64, float64, float64, bool) {
	usesCustom := false
	cpuCost, err := strconv.ParseFloat(cpuCostStr, 64)
	if err != nil || math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) || cpuCost == 0 {
		cpuCost, err = strconv.ParseFloat(cfg.CPU, 64)
		usesCustom = true
		if err != nil || math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) {
			cpuCost = 0
		}
	}
	ramCost, err := strconv.ParseFloat(ramCostStr, 64)
	if err != nil || math.IsNaN(ramCost) || math.IsInf(ramCost, 0) || ramCost == 0 {
		ramCost, err = strconv.ParseFloat(cfg.RAM, 64)
		usesCustom = true
		if err != nil || math.IsNaN(ramCost) || math.IsInf(ramCost, 0) {
			ramCost = 0
		}
	}
	gpuCost, err := strconv.ParseFloat(gpuCostStr, 64)
	if err != nil || math.IsNaN(gpuCost) || math.IsInf(gpuCost, 0) {
		gpuCost, err = strconv.ParseFloat(cfg.GPU, 64)
		if err != nil || math.IsNaN(gpuCost) || math.IsInf(gpuCost, 0) {
			gpuCost = 0
		}
	}
	pvCost, err := strconv.ParseFloat(pvCostStr, 64)
	if err != nil || math.IsNaN(cpuCost) || math.IsInf(cpuCost, 0) {
		pvCost, err = strconv.ParseFloat(cfg.Storage, 64)
		if err != nil || math.IsNaN(pvCost) || math.IsInf(pvCost, 0) {
			pvCost = 0
		}
	}
	return cpuCost, ramCost, gpuCost, pvCost, usesCustom
}

func getPriceVectors(cp cloud.Provider, costDatum *CostData, rate string, discount float64, customDiscount float64, idleCoefficient float64) ([]*util.Vector, []*util.Vector, []*util.Vector, [][]*util.Vector, []*util.Vector) {

	var cpuCost float64
	var ramCost float64
	var gpuCost float64
	var pvCost float64
	var usesCustom bool

	// If custom pricing is enabled and can be retrieved, replace
	// default cost values with custom values
	customPricing, err := cp.GetConfig()
	if err != nil {
		klog.Errorf("failed to load custom pricing: %s", err)
	}
	if cloud.CustomPricesEnabled(cp) && err == nil {
		var cpuCostStr string
		var ramCostStr string
		var gpuCostStr string
		var pvCostStr string
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
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, costDatum, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	} else if costDatum.NodeData == nil && err == nil {
		cpuCostStr := customPricing.CPU
		ramCostStr := customPricing.RAM
		gpuCostStr := customPricing.GPU
		pvCostStr := customPricing.Storage
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, costDatum, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	} else {
		cpuCostStr := costDatum.NodeData.VCPUCost
		ramCostStr := costDatum.NodeData.RAMCost
		gpuCostStr := costDatum.NodeData.GPUCost
		pvCostStr := costDatum.NodeData.StorageCost
		cpuCost, ramCost, gpuCost, pvCost, usesCustom = parseVectorPricing(customPricing, costDatum, cpuCostStr, ramCostStr, gpuCostStr, pvCostStr)
	}

	if usesCustom {
		log.DedupedWarningf(5, "No pricing data found for node `%s` , using custom pricing", costDatum.NodeName)
	}

	cpuDiscount, ramDiscount := getDiscounts(costDatum, cpuCost, ramCost, discount)

	klog.V(4).Infof("Node Name: %s", costDatum.NodeName)
	klog.V(4).Infof("Blended CPU Discount: %f", cpuDiscount)
	klog.V(4).Infof("Blended RAM Discount: %f", ramDiscount)

	// TODO should we try to apply the rate coefficient here or leave it as a totals-only metric?
	rateCoeff := 1.0

	if idleCoefficient == 0 {
		idleCoefficient = 1.0
	}

	cpuv := make([]*util.Vector, 0, len(costDatum.CPUAllocation))
	for _, val := range costDatum.CPUAllocation {
		cpuv = append(cpuv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value * cpuCost * (1 - cpuDiscount) * (1 - customDiscount) / idleCoefficient) * rateCoeff,
		})
	}

	ramv := make([]*util.Vector, 0, len(costDatum.RAMAllocation))
	for _, val := range costDatum.RAMAllocation {
		ramv = append(ramv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     ((val.Value / 1024 / 1024 / 1024) * ramCost * (1 - ramDiscount) * (1 - customDiscount) / idleCoefficient) * rateCoeff,
		})
	}

	gpuv := make([]*util.Vector, 0, len(costDatum.GPUReq))
	for _, val := range costDatum.GPUReq {
		gpuv = append(gpuv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     (val.Value * gpuCost * (1 - discount) * (1 - customDiscount) / idleCoefficient) * rateCoeff,
		})
	}

	pvvs := make([][]*util.Vector, 0, len(costDatum.PVCData))
	for _, pvcData := range costDatum.PVCData {
		pvv := make([]*util.Vector, 0, len(pvcData.Values))
		if pvcData.Volume != nil {
			cost, _ := strconv.ParseFloat(pvcData.Volume.Cost, 64)

			// override with custom pricing if enabled
			if cloud.CustomPricesEnabled(cp) {
				cost = pvCost
			}

			for _, val := range pvcData.Values {
				pvv = append(pvv, &util.Vector{
					Timestamp: math.Round(val.Timestamp/10) * 10,
					Value:     ((val.Value / 1024 / 1024 / 1024) * cost * (1 - customDiscount) / idleCoefficient) * rateCoeff,
				})
			}
			pvvs = append(pvvs, pvv)
		}
	}

	netv := make([]*util.Vector, 0, len(costDatum.NetworkData))
	for _, val := range costDatum.NetworkData {
		netv = append(netv, &util.Vector{
			Timestamp: math.Round(val.Timestamp/10) * 10,
			Value:     val.Value,
		})
	}

	return cpuv, ramv, gpuv, pvvs, netv
}

func averageVectors(vectors []*util.Vector) float64 {
	if len(vectors) == 0 {
		return 0.0
	}
	return totalVectors(vectors) / float64(len(vectors))
}

func totalVectors(vectors []*util.Vector) float64 {
	total := 0.0
	for _, vector := range vectors {
		total += vector.Value
	}
	return total
}

// addVectors adds two slices of Vectors. Vector timestamps are rounded to the
// nearest ten seconds to allow matching of Vectors within a delta allowance.
// Matching Vectors are summed, while unmatched Vectors are passed through.
// e.g. [(t=1, 1), (t=2, 2)] + [(t=2, 2), (t=3, 3)] = [(t=1, 1), (t=2, 4), (t=3, 3)]
func addVectors(xvs []*util.Vector, yvs []*util.Vector) []*util.Vector {
	sumOp := func(result *util.Vector, x *float64, y *float64) bool {
		if x != nil && y != nil {
			result.Value = *x + *y
		} else if y != nil {
			result.Value = *y
		} else if x != nil {
			result.Value = *x
		}

		return true
	}

	return util.ApplyVectorOp(xvs, yvs, sumOp)
}

// minCostDataLength sets the minimum number of time series data required to
// cache both raw and aggregated cost data
const minCostDataLength = 2

// EmptyDataError describes an error caused by empty cost data for some
// defined interval
type EmptyDataError struct {
	err      error
	duration string
	offset   string
}

// Error implements the error interface
func (ede *EmptyDataError) Error() string {
	err := fmt.Sprintf("empty data for range: %s", ede.duration)
	if ede.offset != "" {
		err += fmt.Sprintf(" offset %s", ede.offset)
	}
	if ede.err != nil {
		err += fmt.Sprintf(": %s", ede.err)
	}
	return err
}

func costDataTimeSeriesLength(costData map[string]*CostData) int {
	l := 0
	for _, cd := range costData {
		if l < len(cd.RAMAllocation) {
			l = len(cd.RAMAllocation)
		}
		if l < len(cd.CPUAllocation) {
			l = len(cd.CPUAllocation)
		}
	}
	return l
}

// ScaleHourlyCostData converts per-hour cost data to per-resolution data. If the target resolution is higher (i.e. < 1.0h)
// then we can do simple multiplication by the fraction-of-an-hour and retain accuracy. If the target resolution is
// lower (i.e. > 1.0h) then we sum groups of hourly data by resolution to maintain fidelity.
// e.g. (100 hours of per-hour hourly data, resolutionHours=10) => 10 data points, grouped and summed by 10-hour window
// e.g. (20 minutes of per-minute hourly data, resolutionHours=1/60) => 20 data points, scaled down by a factor of 60
func ScaleHourlyCostData(data map[string]*CostData, resolutionHours float64) map[string]*CostData {
	scaled := map[string]*CostData{}

	for key, datum := range data {
		datum.RAMReq = scaleVectorSeries(datum.RAMReq, resolutionHours)
		datum.RAMUsed = scaleVectorSeries(datum.RAMUsed, resolutionHours)
		datum.RAMAllocation = scaleVectorSeries(datum.RAMAllocation, resolutionHours)
		datum.CPUReq = scaleVectorSeries(datum.CPUReq, resolutionHours)
		datum.CPUUsed = scaleVectorSeries(datum.CPUUsed, resolutionHours)
		datum.CPUAllocation = scaleVectorSeries(datum.CPUAllocation, resolutionHours)
		datum.GPUReq = scaleVectorSeries(datum.GPUReq, resolutionHours)
		datum.NetworkData = scaleVectorSeries(datum.NetworkData, resolutionHours)

		for _, pvcDatum := range datum.PVCData {
			pvcDatum.Values = scaleVectorSeries(pvcDatum.Values, resolutionHours)
		}

		scaled[key] = datum
	}

	return scaled
}

func scaleVectorSeries(vs []*util.Vector, resolutionHours float64) []*util.Vector {
	// if scaling to a lower resolution, compress the hourly data for maximum accuracy
	if resolutionHours > 1.0 {
		return compressVectorSeries(vs, resolutionHours)
	}

	// if scaling to a higher resolution, simply scale each value down by the fraction of an hour
	for _, v := range vs {
		v.Value *= resolutionHours
	}
	return vs
}

func compressVectorSeries(vs []*util.Vector, resolutionHours float64) []*util.Vector {
	if len(vs) == 0 {
		return vs
	}

	compressed := []*util.Vector{}

	threshold := float64(60 * 60 * resolutionHours)
	var acc *util.Vector

	for i, v := range vs {
		if acc == nil {
			// start a new accumulation from current datum
			acc = &util.Vector{
				Value:     vs[i].Value,
				Timestamp: vs[i].Timestamp,
			}
			continue
		}
		if v.Timestamp-acc.Timestamp < threshold {
			// v should be accumulated in current datum
			acc.Value += v.Value
		} else {
			// v falls outside current datum's threshold; append and start a new one
			compressed = append(compressed, acc)
			acc = &util.Vector{
				Value:     vs[i].Value,
				Timestamp: vs[i].Timestamp,
			}
		}
	}
	// append any remaining, incomplete accumulation
	if acc != nil {
		compressed = append(compressed, acc)
	}

	return compressed
}

// ComputeAggregateCostModel computes cost data for the given window, then aggregates it by the given fields.
// Data is cached on two levels: the aggregation is cached as well as the underlying cost data.
func (a *Accesses) ComputeAggregateCostModel(promClient prometheusClient.Client, duration, offset, field string, subfields []string, rate string, filters map[string]string,
	sri *SharedResourceInfo, shared string, allocateIdle, includeTimeSeries, includeEfficiency, disableCache, clearCache, noCache, noExpireCache, remoteEnabled, disableSharedOverhead bool) (map[string]*Aggregation, string, error) {

	profileBaseName := fmt.Sprintf("ComputeAggregateCostModel(duration=%s, offet=%s, field=%s)", duration, offset, field)
	defer measureTime(time.Now(), profileThreshold, profileBaseName)

	// parse cost data filters into FilterFuncs
	filterFuncs := []FilterFunc{}
	retainFuncs := []FilterFunc{}
	retainFuncs = append(retainFuncs, func(cd *CostData) (bool, string) {
		if sri != nil {
			return sri.IsSharedResource(cd), ""
		}
		return false, ""
	})
	aggregateEnvironment := func(costDatum *CostData) string {
		if field == "cluster" {
			return costDatum.ClusterID
		} else if field == "node" {
			return costDatum.NodeName
		} else if field == "namespace" {
			return costDatum.Namespace
		} else if field == "service" {
			if len(costDatum.Services) > 0 {
				return costDatum.Namespace + "/" + costDatum.Services[0]
			}
		} else if field == "deployment" {
			if len(costDatum.Deployments) > 0 {
				return costDatum.Namespace + "/" + costDatum.Deployments[0]
			}
		} else if field == "daemonset" {
			if len(costDatum.Daemonsets) > 0 {
				return costDatum.Namespace + "/" + costDatum.Daemonsets[0]
			}
		} else if field == "statefulset" {
			if len(costDatum.Statefulsets) > 0 {
				return costDatum.Namespace + "/" + costDatum.Statefulsets[0]
			}
		} else if field == "label" {
			if costDatum.Labels != nil {
				for _, sf := range subfields {
					if subfieldName, ok := costDatum.Labels[sf]; ok {
						return fmt.Sprintf("%s=%s", sf, subfieldName)
					}
				}
			}
		} else if field == "pod" {
			return costDatum.Namespace + "/" + costDatum.PodName
		} else if field == "container" {
			return costDatum.Namespace + "/" + costDatum.PodName + "/" + costDatum.Name
		}
		return ""
	}

	if filters["podprefix"] != "" {
		pps := []string{}
		for _, fp := range strings.Split(filters["podprefix"], ",") {
			if fp != "" {
				cleanedFilter := strings.TrimSpace(fp)
				pps = append(pps, cleanedFilter)
			}
		}
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, pp := range pps {
				cleanedFilter := strings.TrimSpace(pp)
				if strings.HasPrefix(cd.PodName, cleanedFilter) {
					return true, aggEnv
				}
			}
			return false, aggEnv
		})
	}

	if filters["namespace"] != "" {
		// namespaces may be comma-separated, e.g. kubecost,default
		// multiple namespaces are evaluated as an OR relationship
		nss := strings.Split(filters["namespace"], ",")
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, ns := range nss {
				nsTrim := strings.TrimSpace(ns)
				if cd.Namespace == nsTrim {
					return true, aggEnv
				} else if strings.HasSuffix(nsTrim, "*") { // trigger wildcard prefix filtering
					nsTrimAsterisk := strings.TrimSuffix(nsTrim, "*")
					if strings.HasPrefix(cd.Namespace, nsTrimAsterisk) {
						return true, aggEnv
					}
				}
			}
			return false, aggEnv
		})
	}
	if filters["node"] != "" {
		// nodes may be comma-separated, e.g. aws-node-1,aws-node-2
		// multiple nodes are evaluated as an OR relationship
		nodes := strings.Split(filters["node"], ",")
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, node := range nodes {
				nodeTrim := strings.TrimSpace(node)
				if cd.NodeName == nodeTrim {
					return true, aggEnv
				} else if strings.HasSuffix(nodeTrim, "*") { // trigger wildcard prefix filtering
					nodeTrimAsterisk := strings.TrimSuffix(nodeTrim, "*")
					if strings.HasPrefix(cd.NodeName, nodeTrimAsterisk) {
						return true, aggEnv
					}
				}
			}
			return false, aggEnv
		})
	}
	if filters["cluster"] != "" {
		// clusters may be comma-separated, e.g. cluster-one,cluster-two
		// multiple clusters are evaluated as an OR relationship
		cs := strings.Split(filters["cluster"], ",")
		filterFuncs = append(filterFuncs, func(cd *CostData) (bool, string) {
			aggEnv := aggregateEnvironment(cd)
			for _, c := range cs {
				cTrim := strings.TrimSpace(c)
				id, name := cd.ClusterID, cd.ClusterName
				if id == cTrim || name == cTrim {
					return true, aggEnv
				} else if strings.HasSuffix(cTrim, "*") { // trigger wildcard prefix filtering
					cTrimAsterisk := strings.TrimSuffix(cTrim, "*")
					if strings.HasPrefix(id, cTrimAsterisk) || strings.HasPrefix(name, cTrimAsterisk) {
						return true, aggEnv
					}
				}
			}
			return false, aggEnv
		})
	}
	if filters["labels"] != "" {
		// labels are expected to be comma-separated and to take the form key=value
		// e.g. app=cost-analyzer,app.kubernetes.io/instance=kubecost
		// each different label will be applied as an AND
		// multiple values for a single label will be evaluated as an OR
		labelValues := map[string][]string{}
		ls := strings.Split(filters["labels"], ",")
		for _, l := range ls {
			lTrim := strings.TrimSpace(l)
			label := strings.Split(lTrim, "=")
			if len(label) == 2 {
				ln := prom.SanitizeLabelName(strings.TrimSpace(label[0]))
				lv := strings.TrimSpace(label[1])
				labelValues[ln] = append(labelValues[ln], lv)
			} else {
				// label is not of the form name=value, so log it and move on
				klog.V(2).Infof("[Warning] aggregate cost model: skipping illegal label filter: %s", l)
			}
		}

		// Generate FilterFunc for each set of label filters by invoking a function instead of accessing
		// values by closure to prevent reference-type looping bug.
		// (see https://github.com/golang/go/wiki/CommonMistakes#using-reference-to-loop-iterator-variable)
		for label, values := range labelValues {
			ff := (func(l string, vs []string) FilterFunc {
				return func(cd *CostData) (bool, string) {
					ae := aggregateEnvironment(cd)
					for _, v := range vs {
						if v == "__unallocated__" { // Special case. __unallocated__ means return all pods without the attached label
							if _, ok := cd.Labels[label]; !ok {
								return true, ae
							}
						}
						if cd.Labels[label] == v {
							return true, ae
						} else if strings.HasSuffix(v, "*") { // trigger wildcard prefix filtering
							vTrim := strings.TrimSuffix(v, "*")
							if strings.HasPrefix(cd.Labels[label], vTrim) {
								return true, ae
							}
						}
					}
					return false, ae
				}
			})(label, values)
			filterFuncs = append(filterFuncs, ff)
		}
	}

	// clear cache prior to checking the cache so that a clearCache=true
	// request always returns a freshly computed value
	if clearCache {
		a.AggregateCache.Flush()
		a.CostDataCache.Flush()
	}

	cacheExpiry := a.GetCacheExpiration(duration)
	if noExpireCache {
		cacheExpiry = cache.NoExpiration
	}

	// parametrize cache key by all request parameters
	aggKey := GenerateAggKey(aggKeyParams{
		duration:   duration,
		offset:     offset,
		filters:    filters,
		field:      field,
		subfields:  subfields,
		rate:       rate,
		sri:        sri,
		shareType:  shared,
		idle:       allocateIdle,
		timeSeries: includeTimeSeries,
		efficiency: includeEfficiency,
	})

	// convert duration and offset to start and end times
	startTime, endTime, err := ParseTimeRange(duration, offset)
	if err != nil {
		return nil, "", fmt.Errorf("Error parsing duration (%s) and offset (%s): %s", duration, offset, err)
	}
	durationHours := endTime.Sub(*startTime).Hours()

	thanosOffset := time.Now().Add(-thanos.OffsetDuration())
	if a.ThanosClient != nil && endTime.After(thanosOffset) {
		klog.V(4).Infof("Setting end time backwards to first present data")

		// Apply offsets to both end and start times to maintain correct time range
		deltaDuration := endTime.Sub(thanosOffset)
		*startTime = startTime.Add(-1 * deltaDuration)
		*endTime = time.Now().Add(-thanos.OffsetDuration())
	}

	// determine resolution by size of duration
	resolutionHours := durationHours
	if durationHours >= 2160 {
		// 90 days
		resolutionHours = 72.0
	} else if durationHours >= 720 {
		// 30 days
		resolutionHours = 24.0
	} else if durationHours >= 168 {
		// 7 days
		resolutionHours = 24.0
	} else if durationHours >= 48 {
		// 2 days
		resolutionHours = 2.0
	} else if durationHours >= 1 {
		resolutionHours = 1.0
	}

	key := fmt.Sprintf(`%s:%s:%fh:%t`, duration, offset, resolutionHours, remoteEnabled)

	// report message about which of the two caches hit. by default report a miss
	cacheMessage := fmt.Sprintf("L1 cache miss: %s L2 cache miss: %s", aggKey, key)

	// check the cache for aggregated response; if cache is hit and not disabled, return response
	if value, found := a.AggregateCache.Get(aggKey); found && !disableCache && !noCache {
		result, ok := value.(map[string]*Aggregation)
		if !ok {
			// disable cache and recompute if type cast fails
			klog.Errorf("caching error: failed to cast aggregate data to struct: %s", aggKey)
			return a.ComputeAggregateCostModel(promClient, duration, offset, field, subfields, rate, filters,
				sri, shared, allocateIdle, includeTimeSeries, includeEfficiency, true, false, noExpireCache, noCache, remoteEnabled, disableSharedOverhead)
		}
		return result, fmt.Sprintf("aggregate cache hit: %s", aggKey), nil
	}

	profileStart := time.Now()
	profileName := profileBaseName + ": "

	window := duration
	if durationHours >= 1 {
		window = fmt.Sprintf("%dh", int(resolutionHours))
		// exclude the last window of the time frame to match Prometheus definitions of range, offset, and resolution
		*startTime = startTime.Add(time.Duration(resolutionHours) * time.Hour)
	} else {
		// don't cache requests for durations of less than one hour
		klog.Infof("key %s has durationhours %f", key, durationHours)
		disableCache = true
	}

	profileBaseName = fmt.Sprintf("ComputeAggregateCostModel(duration=%s, offset=%s, field=%s, window=%s)", duration, offset, field, window)

	// attempt to retrieve cost data from cache
	var costData map[string]*CostData
	cacheData, found := a.CostDataCache.Get(key)
	if found && !disableCache && !noCache {
		profileName += "get cost data from cache"

		ok := false
		costData, ok = cacheData.(map[string]*CostData)
		cacheMessage = fmt.Sprintf("L1 cache miss: %s, L2 cost data cache hit: %s", aggKey, key)
		if !ok {
			klog.Errorf("caching error: failed to cast cost data to struct: %s", key)
		}
	} else {
		klog.Infof("key %s missed cache. found %t, disableCache %t, noCache %t ", key, found, disableCache, noCache)

		cv := a.CostDataCache.Items()
		klog.V(3).Infof("Logging cache items...")
		for k := range cv {
			klog.V(3).Infof("Cache item: %s", k)
		}

		profileName += "compute cost data"

		start := startTime.Format(RFC3339Milli)
		end := endTime.Format(RFC3339Milli)

		costData, err = a.Model.ComputeCostDataRange(promClient, a.KubeClientSet, a.CloudProvider, start, end, window, resolutionHours, "", "", remoteEnabled, offset)
		if err != nil {
			if prom.IsErrorCollection(err) {
				return nil, "", err
			}
			if pce, ok := err.(prom.CommError); ok {
				return nil, "", pce
			}
			if strings.Contains(err.Error(), "data is empty") {
				return nil, "", &EmptyDataError{err: err, duration: duration, offset: offset}
			}
			return nil, "", err
		}

		// compute length of the time series in the cost data and only compute
		// aggregates and cache if the length is sufficiently high
		costDataLen := costDataTimeSeriesLength(costData)

		if durationHours < 1.0 {
			// scale hourly cost data down to fractional hour
			costData = ScaleHourlyCostData(costData, resolutionHours)
		}

		if costDataLen == 0 {
			return nil, "", &EmptyDataError{duration: duration, offset: offset}
		}
		if costDataLen >= minCostDataLength && !noCache {
			klog.Infof("Setting L2 cache: %s", key)
			a.CostDataCache.Set(key, costData, cacheExpiry)
		}
	}

	measureTime(profileStart, profileThreshold, profileName)

	c, err := a.CloudProvider.GetConfig()
	if err != nil {
		return nil, "", err
	}
	discount, err := ParsePercentString(c.Discount)
	if err != nil {
		return nil, "", err
	}
	customDiscount, err := ParsePercentString(c.NegotiatedDiscount)
	if err != nil {
		return nil, "", err
	}

	sc := make(map[string]*SharedCostInfo)
	if !disableSharedOverhead {
		for key, val := range c.SharedCosts {
			cost, err := strconv.ParseFloat(val, 64)
			durationCoefficient := durationHours / util.HoursPerMonth
			if err != nil {
				return nil, "", fmt.Errorf("Unable to parse shared cost %s: %s", val, err.Error())
			}
			sc[key] = &SharedCostInfo{
				Name: key,
				Cost: cost * durationCoefficient,
			}
		}
	}

	profileStart = time.Now()
	profileName = profileBaseName + ": compute idle coefficient"

	idleCoefficients := make(map[string]float64)
	if allocateIdle {
		idleDurationCalcHours := durationHours
		if durationHours < 1 {
			idleDurationCalcHours = 1
		}
		windowStr := fmt.Sprintf("%dh", int(idleDurationCalcHours))
		if a.ThanosClient != nil {
			offset = thanos.Offset()
			klog.Infof("Setting offset to %s", offset)
		}
		idleCoefficients, err = a.ComputeIdleCoefficient(costData, promClient, a.CloudProvider, discount, customDiscount, windowStr, offset)
		if err != nil {
			klog.Errorf("error computing idle coefficient: windowString=%s, offset=%s, err=%s", windowStr, offset, err)
			return nil, "", err
		}
	}
	for cid, idleCoefficient := range idleCoefficients {
		klog.Infof("Idle Coeff: %s: %f", cid, idleCoefficient)
	}

	totalContainerCost := 0.0
	if shared == SplitTypeWeighted {
		totalContainerCost = GetTotalContainerCost(costData, rate, a.CloudProvider, discount, customDiscount, idleCoefficients)
	}

	measureTime(profileStart, profileThreshold, profileName)

	profileStart = time.Now()
	profileName = profileBaseName + ": filter cost data"

	// filter cost data by namespace and cluster after caching for maximal cache hits
	costData, filteredContainerCount, filteredEnvironments := FilterCostData(costData, retainFuncs, filterFuncs)

	measureTime(profileStart, profileThreshold, profileName)

	profileStart = time.Now()
	profileName = profileBaseName + ": aggregate cost data"

	// aggregate cost model data by given fields and cache the result for the default expiration
	opts := &AggregationOptions{
		Discount:               discount,
		CustomDiscount:         customDiscount,
		IdleCoefficients:       idleCoefficients,
		IncludeEfficiency:      includeEfficiency,
		IncludeTimeSeries:      includeTimeSeries,
		Rate:                   rate,
		ResolutionHours:        resolutionHours,
		SharedResourceInfo:     sri,
		SharedCosts:            sc,
		FilteredContainerCount: filteredContainerCount,
		FilteredEnvironments:   filteredEnvironments,
		TotalContainerCost:     totalContainerCost,
		SharedSplit:            shared,
	}
	result := AggregateCostData(costData, field, subfields, a.CloudProvider, opts)

	// If sending time series data back, switch scale back to hourly data. At this point,
	// resolutionHours may have converted our hourly data to more- or less-than hourly data.
	if includeTimeSeries {
		for _, aggs := range result {
			ScaleAggregationTimeSeries(aggs, resolutionHours)
		}
	}

	// compute length of the time series in the cost data and only cache
	// aggregation results if the length is sufficiently high
	costDataLen := costDataTimeSeriesLength(costData)
	if costDataLen >= minCostDataLength && durationHours > 1 && !noCache {
		// Set the result map (rather than a pointer to it) because map is a reference type
		klog.Infof("Caching key in aggregate cache: %s", key)
		a.AggregateCache.Set(aggKey, result, cacheExpiry)
	} else {
		klog.Infof("Not caching for key %s. Not enough data: %t, Duration less than 1h: %t, noCache: %t", key, costDataLen < minCostDataLength, durationHours < 1, noCache)
	}

	measureTime(profileStart, profileThreshold, profileName)

	return result, cacheMessage, nil
}

// ScaleAggregationTimeSeries reverses the scaling done by ScaleHourlyCostData, returning
// the aggregation's time series to hourly data.
func ScaleAggregationTimeSeries(aggregation *Aggregation, resolutionHours float64) {
	for _, v := range aggregation.CPUCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.GPUCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.RAMCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.PVCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.NetworkCostVector {
		v.Value /= resolutionHours
	}

	for _, v := range aggregation.TotalCostVector {
		v.Value /= resolutionHours
	}

	return
}

// String returns a string representation of the encapsulated shared resources, which
// can be used to uniquely identify a set of shared resources. Sorting sets of shared
// resources ensures that strings representing permutations of the same combination match.
func (s *SharedResourceInfo) String() string {
	if s == nil {
		return ""
	}

	nss := []string{}
	for ns := range s.SharedNamespace {
		nss = append(nss, ns)
	}
	sort.Strings(nss)
	nsStr := strings.Join(nss, ",")

	labels := []string{}
	for lbl, vals := range s.LabelSelectors {
		for val := range vals {
			if lbl != "" && val != "" {
				labels = append(labels, fmt.Sprintf("%s=%s", lbl, val))
			}
		}
	}
	sort.Strings(labels)
	labelStr := strings.Join(labels, ",")

	return fmt.Sprintf("%s:%s", nsStr, labelStr)
}

type aggKeyParams struct {
	duration   string
	offset     string
	filters    map[string]string
	field      string
	subfields  []string
	rate       string
	sri        *SharedResourceInfo
	shareType  string
	idle       bool
	timeSeries bool
	efficiency bool
}

// GenerateAggKey generates a parameter-unique key for caching the aggregate cost model
func GenerateAggKey(ps aggKeyParams) string {
	// parse, trim, and sort podprefix filters
	podPrefixFilters := []string{}
	if ppfs, ok := ps.filters["podprefix"]; ok && ppfs != "" {
		for _, psf := range strings.Split(ppfs, ",") {
			podPrefixFilters = append(podPrefixFilters, strings.TrimSpace(psf))
		}
	}
	sort.Strings(podPrefixFilters)
	podPrefixFiltersStr := strings.Join(podPrefixFilters, ",")

	// parse, trim, and sort namespace filters
	nsFilters := []string{}
	if nsfs, ok := ps.filters["namespace"]; ok && nsfs != "" {
		for _, nsf := range strings.Split(nsfs, ",") {
			nsFilters = append(nsFilters, strings.TrimSpace(nsf))
		}
	}
	sort.Strings(nsFilters)
	nsFilterStr := strings.Join(nsFilters, ",")

	// parse, trim, and sort node filters
	nodeFilters := []string{}
	if nodefs, ok := ps.filters["node"]; ok && nodefs != "" {
		for _, nodef := range strings.Split(nodefs, ",") {
			nodeFilters = append(nodeFilters, strings.TrimSpace(nodef))
		}
	}
	sort.Strings(nodeFilters)
	nodeFilterStr := strings.Join(nodeFilters, ",")

	// parse, trim, and sort cluster filters
	cFilters := []string{}
	if cfs, ok := ps.filters["cluster"]; ok && cfs != "" {
		for _, cf := range strings.Split(cfs, ",") {
			cFilters = append(cFilters, strings.TrimSpace(cf))
		}
	}
	sort.Strings(cFilters)
	cFilterStr := strings.Join(cFilters, ",")

	// parse, trim, and sort label filters
	lFilters := []string{}
	if lfs, ok := ps.filters["labels"]; ok && lfs != "" {
		for _, lf := range strings.Split(lfs, ",") {
			// trim whitespace from the label name and the label value
			// of each label name/value pair, then reconstruct
			// e.g. "tier = frontend, app = kubecost" == "app=kubecost,tier=frontend"
			lfa := strings.Split(lf, "=")
			if len(lfa) == 2 {
				lfn := strings.TrimSpace(lfa[0])
				lfv := strings.TrimSpace(lfa[1])
				lFilters = append(lFilters, fmt.Sprintf("%s=%s", lfn, lfv))
			} else {
				// label is not of the form name=value, so log it and move on
				klog.V(2).Infof("[Warning] GenerateAggKey: skipping illegal label filter: %s", lf)
			}
		}
	}
	sort.Strings(lFilters)
	lFilterStr := strings.Join(lFilters, ",")

	filterStr := fmt.Sprintf("%s:%s:%s:%s:%s", nsFilterStr, nodeFilterStr, cFilterStr, lFilterStr, podPrefixFiltersStr)

	sort.Strings(ps.subfields)
	fieldStr := fmt.Sprintf("%s:%s", ps.field, strings.Join(ps.subfields, ","))

	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s:%t:%t:%t", ps.duration, ps.offset, filterStr, fieldStr, ps.rate,
		ps.sri, ps.shareType, ps.idle, ps.timeSeries, ps.efficiency)
}

// AggregateCostModelHandler handles requests to the aggregated cost model API. See
// ComputeAggregateCostModel for details.
func (a *Accesses) AggregateCostModelHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	windowStr := r.URL.Query().Get("window")

	// Convert UTC-RFC3339 pairs to configured UTC offset
	// e.g. with UTC offset of -0600, 2020-07-01T00:00:00Z becomes
	// 2020-07-01T06:00:00Z == 2020-07-01T00:00:00-0600
	// TODO niko/etl fix the frontend because this is confusing if you're
	// actually asking for UTC time (...Z) and we swap that "Z" out for the
	// configured UTC offset without asking
	rfc3339 := `\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ`
	regex := regexp.MustCompile(fmt.Sprintf(`(%s),(%s)`, rfc3339, rfc3339))
	match := regex.FindStringSubmatch(windowStr)
	if match != nil {
		start, _ := time.Parse(time.RFC3339, match[1])
		start = start.Add(-env.GetParsedUTCOffset()).In(time.UTC)
		end, _ := time.Parse(time.RFC3339, match[2])
		end = end.Add(-env.GetParsedUTCOffset()).In(time.UTC)
		windowStr = fmt.Sprintf("%sZ,%sZ", start.Format("2006-01-02T15:04:05"), end.Format("2006-01-02T15:04:05Z"))
	}

	// determine duration and offset from query parameters
	window, err := kubecost.ParseWindowWithOffset(windowStr, env.GetParsedUTCOffset())
	if err != nil || window.Start() == nil {
		http.Error(w, fmt.Sprintf("invalid window: %s", err), http.StatusBadRequest)
		return
	}
	duration, offset := window.ToDurationOffset()

	durRegex := regexp.MustCompile(`^(\d+)(m|h|d|s)$`)
	isDurationStr := durRegex.MatchString(windowStr)

	// legacy offset option should override window offset
	if r.URL.Query().Get("offset") != "" {
		offset = r.URL.Query().Get("offset")
		// Shift window by offset, but only when manually set with separate
		// parameter and window was provided as a duration string. Otherwise,
		// do not alter the (duration, offset) from ParseWindowWithOffset.
		if offset != "1m" && isDurationStr {
			match := durRegex.FindStringSubmatch(offset)
			if match != nil && len(match) == 3 {
				dur := time.Minute
				if match[2] == "h" {
					dur = time.Hour
				}
				if match[2] == "d" {
					dur = 24 * time.Hour
				}
				if match[2] == "s" {
					dur = time.Second
				}

				num, _ := strconv.ParseInt(match[1], 10, 64)
				window = window.Shift(-time.Duration(num) * dur)
			}
		}
	}

	// redirect requests with no offset to a 1m offset to improve cache hits
	if offset == "" {
		offset = "1m"
	}

	// parse remaining query parameters
	namespace := r.URL.Query().Get("namespace")
	cluster := r.URL.Query().Get("cluster")
	labels := r.URL.Query().Get("labels")
	podprefix := r.URL.Query().Get("podprefix")
	labelArray := strings.Split(labels, "=")
	labelArray[0] = strings.ReplaceAll(labelArray[0], "-", "_")
	labels = strings.Join(labelArray, "=")
	field := r.URL.Query().Get("aggregation")
	subfieldStr := r.URL.Query().Get("aggregationSubfield")
	rate := r.URL.Query().Get("rate")
	idleFlag := r.URL.Query().Get("allocateIdle")
	sharedNamespaces := r.URL.Query().Get("sharedNamespaces")
	sharedLabelNames := r.URL.Query().Get("sharedLabelNames")
	sharedLabelValues := r.URL.Query().Get("sharedLabelValues")
	remote := r.URL.Query().Get("remote") != "false"
	shared := r.URL.Query().Get("sharedSplit")
	subfields := []string{}
	if len(subfieldStr) > 0 {
		s := strings.Split(r.URL.Query().Get("aggregationSubfield"), ",")
		for _, rawLabel := range s {
			subfields = append(subfields, prom.SanitizeLabelName(rawLabel))
		}
	}

	var allocateIdle bool
	if idleFlag == "default" {
		c, _ := a.CloudProvider.GetConfig()
		allocateIdle = (c.DefaultIdle == "true")
	} else {
		allocateIdle = (idleFlag == "true")
	}

	// timeSeries == true maintains the time series dimension of the data,
	// which by default gets summed over the entire interval
	includeTimeSeries := r.URL.Query().Get("timeSeries") == "true"

	// efficiency == true aggregates and returns usage and efficiency data
	// includeEfficiency := r.URL.Query().Get("efficiency") == "true"

	// efficiency has been deprecated in favor of a default to always send efficiency
	includeEfficiency := true

	// TODO niko/caching rename "recomputeCache"
	// disableCache, if set to "true", tells this function to recompute and
	// cache the requested data
	disableCache := r.URL.Query().Get("disableCache") == "true"

	// clearCache, if set to "true", tells this function to flush the cache,
	// then recompute and cache the requested data
	clearCache := r.URL.Query().Get("clearCache") == "true"

	// noCache avoids the cache altogether, both reading from and writing to
	noCache := r.URL.Query().Get("noCache") == "true"

	// noExpireCache should only be used by cache warming to set non-expiring caches
	noExpireCache := false

	// aggregation field is required
	if field == "" {
		http.Error(w, "Missing aggregation field parameter", http.StatusBadRequest)
		return
	}

	// aggregation subfield is required when aggregation field is "label"
	if field == "label" && len(subfields) == 0 {
		http.Error(w, "Missing aggregation subfield parameter for aggregation by label", http.StatusBadRequest)
		return
	}

	// enforce one of four available rate options
	if rate != "" && rate != "hourly" && rate != "daily" && rate != "monthly" {
		http.Error(w, "If set, rate parameter must be one of: 'hourly', 'daily', 'monthly'", http.StatusBadRequest)
		return
	}

	// parse cost data filters
	// namespace and cluster are exact-string-matches
	// labels are expected to be comma-separated and to take the form key=value
	// e.g. app=cost-analyzer,app.kubernetes.io/instance=kubecost
	filters := map[string]string{
		"namespace": namespace,
		"cluster":   cluster,
		"labels":    labels,
		"podprefix": podprefix,
	}

	// parse shared resources
	sn := []string{}
	sln := []string{}
	slv := []string{}
	if sharedNamespaces != "" {
		sn = strings.Split(sharedNamespaces, ",")
	}
	if sharedLabelNames != "" {
		sln = strings.Split(sharedLabelNames, ",")
		slv = strings.Split(sharedLabelValues, ",")
		if len(sln) != len(slv) || slv[0] == "" {
			http.Error(w, "Supply exacly one shared label value per shared label name", http.StatusBadRequest)
			return
		}
	}
	var sr *SharedResourceInfo
	if len(sn) > 0 || len(sln) > 0 {
		sr = NewSharedResourceInfo(true, sn, sln, slv)
	}

	// enable remote if it is available and not disabled
	remoteEnabled := remote && env.IsRemoteEnabled()

	// if custom pricing has changed, then clear the cache and recompute data
	if a.CustomPricingHasChanged() {
		clearCache = true
	}

	promClient := a.GetPrometheusClient(remote)

	var data map[string]*Aggregation
	var message string

	// etlEnabled := env.IsETLEnabled()
	// useETLAdapter := r.URL.Query().Get("etl") == "true"
	// if etlEnabled && useETLAdapter {
	// 	data, message, err = a.AdaptETLAggregateCostModel(window, field, subfields, rate, filters, sr, shared, allocateIdle, includeTimeSeries)
	// } else {
	// 	data, message, err = a.ComputeAggregateCostModel(promClient, duration, offset, field, subfields, rate, filters,
	// 		sr, shared, allocateIdle, includeTimeSeries, includeEfficiency, disableCache, clearCache, noCache, noExpireCache, remoteEnabled, false)
	// }
	data, message, err = a.ComputeAggregateCostModel(promClient, duration, offset, field, subfields, rate, filters,
		sr, shared, allocateIdle, includeTimeSeries, includeEfficiency, disableCache, clearCache, noCache, noExpireCache, remoteEnabled, false)

	// Find any warnings in http request context
	warning, _ := product.GetWarning(r)

	if err != nil {
		if emptyErr, ok := err.(*EmptyDataError); ok {
			if warning == "" {
				w.Write(WrapData(map[string]interface{}{}, emptyErr))
			} else {
				w.Write(WrapDataWithWarning(map[string]interface{}{}, emptyErr, warning))
			}
			return
		}
		if boundaryErr, ok := err.(*kubecost.BoundaryError); ok {
			if window.Start() != nil && window.Start().After(time.Now().Add(-90*24*time.Hour)) {
				// Asking for data within a 90 day period: it will be available
				// after the pipeline builds
				msg := "Data will be available after ETL is built"

				rex := regexp.MustCompile(`(\d+\.*\d*)%`)
				match := rex.FindStringSubmatch(boundaryErr.Message)
				if len(match) > 1 {
					completionPct, err := strconv.ParseFloat(match[1], 64)
					if err == nil {
						msg = fmt.Sprintf("%s (%.1f%% complete)", msg, completionPct)
					}
				}

				http.Error(w, msg, http.StatusInternalServerError)
			} else {
				// Boundary error outside of 90 day period; may not be available
				http.Error(w, boundaryErr.Error(), http.StatusInternalServerError)
			}
			return
		}
		errStr := fmt.Sprintf("error computing aggregate cost model: %s", err)
		http.Error(w, errStr, http.StatusInternalServerError)
		return
	}

	if warning == "" {
		w.Write(WrapDataWithMessage(data, nil, message))
	} else {
		w.Write(WrapDataWithMessageAndWarning(data, nil, message, warning))
	}
}
