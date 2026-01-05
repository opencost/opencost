package costmodel

import (
	"fmt"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
)

func (cm *CostModel) ComputeNetworkInsights(start, end time.Time) (*opencost.NetworkInsightSet, error) {
	log.Debugf("Network Insight compute called on CostModel for window  %s", opencost.NewClosedWindow(start, end).String())

	// If the duration is short enough, compute the network insight directly
	if end.Sub(start) <= cm.BatchDuration {
		return cm.GetNetworkInsightSet(start, end)
	}

	// Incase prometheus max query duration is less than the resolution

	// s and e track the coverage of the entire given window over multiple
	// internal queries.
	s, e := start, start

	window := opencost.NewClosedWindow(start, end)
	totalNis := opencost.NewNetworkInsightSet(start, end)

	for e.Before(end) {
		duration := end.Sub(e)
		if duration > cm.BatchDuration {
			duration = cm.BatchDuration
		}
		e = s.Add(duration)
		nis, err := cm.GetNetworkInsightSet(start, end)
		if err != nil {
			return &opencost.NetworkInsightSet{}, fmt.Errorf("error computing network insight for %s: %v", window.String(), err)
		}
		totalNis.Accumulate(nis, []opencost.NetworkInsightProperty{})
		s = e
	}
	return totalNis, fmt.Errorf("unable to query data source for large duration")
}

func (cm *CostModel) GetNetworkInsightSet(start, end time.Time) (*opencost.NetworkInsightSet, error) {
	resultingSet := &opencost.NetworkInsightSet{}
	resultingSet.Window = opencost.NewClosedWindow(start, end)

	querier := cm.DataSource.Metrics()
	grp := source.NewQueryGroup()

	// Egress Cross Zone
	resChNetZoneGiB := source.WithGroup(grp, querier.QueryNetZoneGiB(start, end))
	resNetZoneGiB, _ := resChNetZoneGiB.Await()

	resChNetZoneCostPerGiB := source.WithGroup(grp, querier.QueryNetZonePricePerGiB(start, end))
	resNetZoneCostPerGiB, _ := resChNetZoneCostPerGiB.Await()

	// Egress Cross Region
	resChNetRegionGiB := source.WithGroup(grp, querier.QueryNetRegionGiB(start, end))
	resNetRegionGiB, _ := resChNetRegionGiB.Await()

	resChNetRegionCostPerGiB := source.WithGroup(grp, querier.QueryNetRegionPricePerGiB(start, end))
	resNetRegionCostPerGiB, _ := resChNetRegionCostPerGiB.Await()

	// Egress Internet
	resChNetInternetGiB := source.WithGroup(grp, querier.QueryNetInternetServiceGiB(start, end))
	resNetInternetGiB, _ := resChNetInternetGiB.Await()

	resChNetInternetCostPerGiB := source.WithGroup(grp, querier.QueryNetInternetPricePerGiB(start, end))
	resNetInternetCostPerGiB, _ := resChNetInternetCostPerGiB.Await()

	// Ingress Cross Zone
	resChIngNetZoneGiB := source.WithGroup(grp, querier.QueryNetZoneIngressGiB(start, end))
	resIngNetZoneGiB, _ := resChIngNetZoneGiB.Await()

	// There's no prometheus cost at the moment for Ingress
	resIngNetZoneCostPerGiB := []*source.NetworkPricePerGiBResult{}

	// Ingress Cross Region
	resChIngNetRegionGiB := source.WithGroup(grp, querier.QueryNetRegionIngressGiB(start, end))
	resIngNetRegionGiB, _ := resChIngNetRegionGiB.Await()

	// There's no prometheus cost at the moment for Ingress
	resIngNetRegionCostPerGiB := []*source.NetworkPricePerGiBResult{}

	// Ingress Internet
	resChIngNetInternetGiB := source.WithGroup(grp, querier.QueryNetInternetServiceIngressGiB(start, end))
	resIngNetInternetGiB, _ := resChIngNetInternetGiB.Await()

	// There's no prometheus cost at the moment for Ingress
	resIngNetInternetCostPerGiB := []*source.NetworkPricePerGiBResult{}

	// apply Egress cross zone network details
	applyNetworkCosts(resultingSet, resNetZoneGiB, resNetZoneCostPerGiB, opencost.NetworkTrafficTypeCrossZone, opencost.NetworkTrafficDirectionEgress)

	// apply Egress cross region network details
	applyNetworkCosts(resultingSet, resNetRegionGiB, resNetRegionCostPerGiB, opencost.NetworkTrafficTypeCrossRegion, opencost.NetworkTrafficDirectionEgress)

	// apply Egress internet network details
	applyNetworkCosts(resultingSet, resNetInternetGiB, resNetInternetCostPerGiB, opencost.NetworkTrafficTypeInternet, opencost.NetworkTrafficDirectionEgress)

	// apply Ingress cross zone network details
	applyNetworkCosts(resultingSet, resIngNetZoneGiB, resIngNetZoneCostPerGiB, opencost.NetworkTrafficTypeCrossZone, opencost.NetworkTrafficDirectionIngress)

	// apply Ingress cross region network details
	applyNetworkCosts(resultingSet, resIngNetRegionGiB, resIngNetRegionCostPerGiB, opencost.NetworkTrafficTypeCrossRegion, opencost.NetworkTrafficDirectionIngress)

	// apply Ingress internet network details
	applyNetworkCosts(resultingSet, resIngNetInternetGiB, resIngNetInternetCostPerGiB, opencost.NetworkTrafficTypeInternet, opencost.NetworkTrafficDirectionIngress)

	return resultingSet, nil
}

func applyNetworkCosts(
	ns *opencost.NetworkInsightSet,
	resNetworkGiB []*source.NetworkGiBResult,
	resNetworkCostPerGiB []*source.NetworkPricePerGiBResult,
	networkType opencost.NetworkTrafficType,
	trafficType opencost.NetworkTrafficDirection,
) error {
	var cost float64
	// All ingress cost are comming out empty at the moment?
	// do we charge at all here?
	if len(resNetworkCostPerGiB) == 0 {
		cost = 0
	} else {
		cost = resNetworkCostPerGiB[0].Data[0].Value
	}

	for _, res := range resNetworkGiB {
		bytes := res.Data[0].Value
		// dont really care about bytes <=0
		if bytes <= 0 {
			continue
		}

		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}
		namespace := res.Namespace
		pod := res.Pod
		service := res.Service
		if service == "" {
			service = opencost.NetworkInsightsServiceUnknown
		}

		totalByteCost := bytes * cost
		// sameZone, sameRegion, internet := getNetworkBools(networkType)
		nds := make(opencost.NetworkDetailsSet, 1)
		nd := &opencost.NetworkDetail{
			Cost:             totalByteCost,
			Bytes:            bytes,
			EndPoint:         service,
			TrafficType:      networkType,
			TrafficDirection: trafficType,
		}

		nds.Add(nd)

		crossZoneCost, crossRegionCost, internetCost, totalCost := getNetworkCost(networkType, totalByteCost)

		ni := &opencost.NetworkInsight{
			Cluster:                cluster,
			Namespace:              namespace,
			Controller:             "",
			Pod:                    pod,
			Node:                   "",
			Labels:                 make(map[string]string),
			Region:                 "",
			Zone:                   "",
			NetworkTotalCost:       totalCost,
			NetworkCrossZoneCost:   crossZoneCost,
			NetworkCrossRegionCost: crossRegionCost,
			NetworkInternetCost:    internetCost,
			NetworkDetails:         nds,
		}

		ns.Insert(ni, []opencost.NetworkInsightProperty{})
	}
	return nil
}

func getNetworkCost(networkType opencost.NetworkTrafficType, cost float64) (crossZoneCost, crossRegionCost, internetCost, totalCost float64) {
	switch networkType {
	case opencost.NetworkTrafficTypeCrossZone:
		return cost, 0.0, 0.0, cost
	case opencost.NetworkTrafficTypeCrossRegion:
		return 0.0, cost, 0.0, cost
	case opencost.NetworkTrafficTypeInternet:
		return 0.0, 0.0, cost, cost
	default:
		log.Warnf("unknown string passed: %s", networkType)
		return 0.0, 0.0, 0.0, 0.0
	}
}
