package costmodel

import (
	"fmt"
	"math"
	"strconv"

	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	"k8s.io/klog"
)

// NetworkUsageVNetworkUsageDataector contains the network usage values for egress network traffic
type NetworkUsageData struct {
	ClusterID             string
	PodName               string
	Namespace             string
	NetworkZoneEgress     []*Vector
	NetworkRegionEgress   []*Vector
	NetworkInternetEgress []*Vector
}

// NetworkUsageVector contains a network usage vector for egress network traffic
type NetworkUsageVector struct {
	ClusterID string
	PodName   string
	Namespace string
	Values    []*Vector
}

// GetNetworkUsageData performs a join of the the results of zone, region, and internet usage queries to return a single
// map containing network costs for each namespace+pod
func GetNetworkUsageData(zr interface{}, rr interface{}, ir interface{}, defaultClusterID string, isRange bool) (map[string]*NetworkUsageData, error) {
	var vectorFn func(interface{}, string) (map[string]*NetworkUsageVector, error)

	if isRange {
		vectorFn = getNetworkUsageVectors
	} else {
		vectorFn = getNetworkUsageVector
	}

	zoneNetworkMap, err := vectorFn(zr, defaultClusterID)
	if err != nil {
		return nil, err
	}

	regionNetworkMap, err := vectorFn(rr, defaultClusterID)
	if err != nil {
		return nil, err
	}

	internetNetworkMap, err := vectorFn(ir, defaultClusterID)
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

	return usageData, nil
}

// GetNetworkCost computes the actual cost for NetworkUsageData based on data provided by the Provider.
func GetNetworkCost(usage *NetworkUsageData, cloud costAnalyzerCloud.Provider) ([]*Vector, error) {
	var results []*Vector

	pricing, err := cloud.NetworkPricing()
	if err != nil {
		return nil, err
	}
	zoneCost := pricing.ZoneNetworkEgressCost
	regionCost := pricing.RegionNetworkEgressCost
	internetCost := pricing.InternetNetworkEgressCost

	zlen := len(usage.NetworkZoneEgress)
	rlen := len(usage.NetworkRegionEgress)
	ilen := len(usage.NetworkInternetEgress)

	l := max(zlen, rlen, ilen)
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

		results = append(results, &Vector{
			Value:     cost,
			Timestamp: timestamp,
		})
	}

	return results, nil
}

func getNetworkUsageVector(qr interface{}, defaultClusterID string) (map[string]*NetworkUsageVector, error) {
	ncdmap := make(map[string]*NetworkUsageVector)
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Data field improperly formatted in prometheus repsonse")
	}
	result, ok := d["result"]
	if !ok {
		return nil, fmt.Errorf("Result field not present in prometheus response")
	}
	results, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result field improperly formatted in prometheus response")
	}
	for _, val := range results {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		podName, ok := metricMap["pod_name"]
		if !ok {
			return nil, fmt.Errorf("Pod Name does not exist in data result vector")
		}
		podNameStr, ok := podName.(string)
		if !ok {
			return nil, fmt.Errorf("Pod Name field improperly formatted")
		}
		namespace, ok := metricMap["namespace"]
		if !ok {
			return nil, fmt.Errorf("Namespace field does not exist in data result vector")
		}
		namespaceStr, ok := namespace.(string)
		if !ok {
			return nil, fmt.Errorf("Namespace field improperly formatted")
		}
		cid, ok := metricMap["cluster_id"]
		if !ok {
			klog.V(4).Info("Prometheus vector does not have cluster id")
			cid = defaultClusterID
		}
		clusterID, ok := cid.(string)
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have string cluster_id")
		}
		dataPoint, ok := val.(map[string]interface{})["value"]
		if !ok {
			return nil, fmt.Errorf("Value field does not exist in data result vector")
		}
		value, ok := dataPoint.([]interface{})
		if !ok || len(value) != 2 {
			return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		var vectors []*Vector
		strVal := value[1].(string)
		v, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return nil, err
		}

		vectors = append(vectors, &Vector{
			Timestamp: value[0].(float64),
			Value:     v,
		})

		key := namespaceStr + "," + podNameStr + "," + clusterID
		ncdmap[key] = &NetworkUsageVector{
			ClusterID: clusterID,
			Namespace: namespaceStr,
			PodName:   podNameStr,
			Values:    vectors,
		}
	}
	return ncdmap, nil
}

func getNetworkUsageVectors(qr interface{}, defaultClusterID string) (map[string]*NetworkUsageVector, error) {
	ncdmap := make(map[string]*NetworkUsageVector)
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Data field improperly formatted in prometheus repsonse")
	}
	result, ok := d["result"]
	if !ok {
		return nil, fmt.Errorf("Result field not present in prometheus response")
	}
	results, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result field improperly formatted in prometheus response")
	}
	for _, val := range results {
		metricInterface, ok := val.(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}
		podName, ok := metricMap["pod_name"]
		if !ok {
			return nil, fmt.Errorf("Pod Name does not exist in data result vector")
		}
		podNameStr, ok := podName.(string)
		if !ok {
			return nil, fmt.Errorf("Pod Name field improperly formatted")
		}
		namespace, ok := metricMap["namespace"]
		if !ok {
			return nil, fmt.Errorf("Namespace field does not exist in data result vector")
		}
		namespaceStr, ok := namespace.(string)
		if !ok {
			return nil, fmt.Errorf("Namespace field improperly formatted")
		}
		cid, ok := metricMap["cluster_id"]
		if !ok {
			klog.V(4).Info("Prometheus vector does not have cluster id")
			cid = defaultClusterID
		}
		clusterID, ok := cid.(string)
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have string cluster_id")
		}

		values, ok := val.(map[string]interface{})["values"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("Values field is improperly formatted")
		}
		var vectors []*Vector
		for _, value := range values {
			dataPoint, ok := value.([]interface{})
			if !ok || len(dataPoint) != 2 {
				return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
			}

			strVal := dataPoint[1].(string)
			v, _ := strconv.ParseFloat(strVal, 64)
			vectors = append(vectors, &Vector{
				Timestamp: math.Round(dataPoint[0].(float64)/10) * 10,
				Value:     v,
			})
		}

		key := namespaceStr + "," + podNameStr + "," + clusterID
		ncdmap[key] = &NetworkUsageVector{
			ClusterID: clusterID,
			Namespace: namespaceStr,
			PodName:   podNameStr,
			Values:    vectors,
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
