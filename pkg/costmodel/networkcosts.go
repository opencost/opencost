package costmodel

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	costAnalyzerCloud "github.com/opencost/opencost/pkg/cloud/models"
)

// NetworkUsageData contains the network usage values for egress network traffic and nat gateway
type NetworkUsageData struct {
	ClusterID                string
	PodName                  string
	Namespace                string
	NetworkZoneEgress        []*util.Vector
	NetworkRegionEgress      []*util.Vector
	NetworkInternetEgress    []*util.Vector
	NetworkNatGatewayEgress  []*util.Vector
	NetworkNatGatewayIngress []*util.Vector
}

// NetworkUsageVector contains a network usage vector for egress network traffic
type NetworkUsageVector struct {
	ClusterID string
	PodName   string
	Namespace string
	Values    []*util.Vector
}

// GetNetworkUsageData performs a join of the the results of zone, region, and internet usage queries to return a single
// map containing network costs for each namespace+pod
func GetNetworkUsageData(
	zr []*source.NetZoneGiBResult,
	rr []*source.NetRegionGiBResult,
	ir []*source.NetInternetGiBResult,
	nge []*source.NetNatGatewayGiBResult,
	ngi []*source.NetNatGatewayIngressGiBResult,
	defaultClusterID string,
) (map[string]*NetworkUsageData, error) {
	zoneNetworkMap, err := getNetworkUsage(zr, defaultClusterID)
	if err != nil {
		return nil, err
	}

	regionNetworkMap, err := getNetworkUsage(rr, defaultClusterID)
	if err != nil {
		return nil, err
	}

	internetNetworkMap, err := getNetworkUsage(ir, defaultClusterID)
	if err != nil {
		return nil, err
	}

	natGatewayEgressNetMap, err := getNetworkUsage(nge, defaultClusterID)
	if err != nil {
		return nil, err
	}

	natGatewayIngressNetMap, err := getNetworkUsage(ngi, defaultClusterID)
	if err != nil {
		return nil, err
	}

	usageData := make(map[string]*NetworkUsageData)
	for k, v := range zoneNetworkMap {
		existing, ok := usageData[k]
		if !ok {
			usageData[k] = &NetworkUsageData{
				ClusterID:         v.ClusterID,
				PodName:           v.PodName,
				Namespace:         v.Namespace,
				NetworkZoneEgress: v.Values,
			}
			continue
		}

		existing.NetworkZoneEgress = v.Values
	}

	for k, v := range regionNetworkMap {
		existing, ok := usageData[k]
		if !ok {
			usageData[k] = &NetworkUsageData{
				ClusterID:           v.ClusterID,
				PodName:             v.PodName,
				Namespace:           v.Namespace,
				NetworkRegionEgress: v.Values,
			}
			continue
		}

		existing.NetworkRegionEgress = v.Values
	}

	for k, v := range internetNetworkMap {
		existing, ok := usageData[k]
		if !ok {
			usageData[k] = &NetworkUsageData{
				ClusterID:             v.ClusterID,
				PodName:               v.PodName,
				Namespace:             v.Namespace,
				NetworkInternetEgress: v.Values,
			}
			continue
		}

		existing.NetworkInternetEgress = v.Values
	}

	for k, v := range natGatewayEgressNetMap {
		existing, ok := usageData[k]
		if !ok {
			usageData[k] = &NetworkUsageData{
				ClusterID:               v.ClusterID,
				PodName:                 v.PodName,
				Namespace:               v.Namespace,
				NetworkNatGatewayEgress: v.Values,
			}
			continue
		}

		existing.NetworkNatGatewayEgress = v.Values
	}

	for k, v := range natGatewayIngressNetMap {
		existing, ok := usageData[k]
		if !ok {
			usageData[k] = &NetworkUsageData{
				ClusterID:                v.ClusterID,
				PodName:                  v.PodName,
				Namespace:                v.Namespace,
				NetworkNatGatewayIngress: v.Values,
			}
			continue
		}

		existing.NetworkNatGatewayIngress = v.Values
	}

	return usageData, nil
}

// GetNetworkCost computes the actual cost for NetworkUsageData based on data provided by the Provider.
func GetNetworkCost(usage *NetworkUsageData, cloud costAnalyzerCloud.Provider) ([]*util.Vector, error) {
	var results []*util.Vector

	pricing, err := cloud.NetworkPricing()
	if err != nil {
		return nil, err
	}
	zoneCost := pricing.ZoneNetworkEgressCost
	regionCost := pricing.RegionNetworkEgressCost
	internetCost := pricing.InternetNetworkEgressCost
	natGatewayEgressCost := pricing.NatGatewayEgressCost
	natGatewayIngressCost := pricing.NatGatewayIngressCost

	zlen := len(usage.NetworkZoneEgress)
	rlen := len(usage.NetworkRegionEgress)
	ilen := len(usage.NetworkInternetEgress)
	ngelen := len(usage.NetworkNatGatewayEgress)
	ngilen := len(usage.NetworkNatGatewayIngress)

	l := max(zlen, rlen, ilen, ngelen, ngilen)
	for i := 0; i < l; i++ {
		var cost float64 = 0
		var timestamp float64

		if i < zlen {
			cost += usage.NetworkZoneEgress[i].Value * zoneCost
			timestamp = usage.NetworkZoneEgress[i].Timestamp
		}

		if i < rlen {
			cost += usage.NetworkRegionEgress[i].Value * regionCost
			timestamp = usage.NetworkRegionEgress[i].Timestamp
		}

		if i < ilen {
			cost += usage.NetworkInternetEgress[i].Value * internetCost
			timestamp = usage.NetworkInternetEgress[i].Timestamp
		}

		if i < ngelen {
			cost += usage.NetworkNatGatewayEgress[i].Value * natGatewayEgressCost
			timestamp = usage.NetworkNatGatewayEgress[i].Timestamp
		}

		if i < ngilen {
			cost += usage.NetworkNatGatewayIngress[i].Value * natGatewayIngressCost
			timestamp = usage.NetworkNatGatewayIngress[i].Timestamp
		}

		results = append(results, &util.Vector{
			Value:     cost,
			Timestamp: timestamp,
		})
	}

	return results, nil
}

func getNetworkUsage(qrs []*source.NetworkGiBResult, defaultClusterID string) (map[string]*NetworkUsageVector, error) {
	ncdmap := make(map[string]*NetworkUsageVector)

	for _, val := range qrs {
		podName := val.Pod
		if podName == "" {
			return nil, fmt.Errorf("network vector does not contain 'pod' or 'pod_name' field")
		}

		namespace := val.Namespace
		if namespace == "" {
			return nil, fmt.Errorf("network vector does not contain 'namespace' field")
		}

		clusterID := val.Cluster
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		key := namespace + "," + podName + "," + clusterID
		ncdmap[key] = &NetworkUsageVector{
			ClusterID: clusterID,
			Namespace: namespace,
			PodName:   podName,
			Values:    val.Data,
		}
	}
	return ncdmap, nil
}

func max(x int, rest ...int) int {
	curr := x
	for _, v := range rest {
		if v > curr {
			curr = v
		}
	}
	return curr
}
