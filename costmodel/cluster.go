package costmodel

import (
	"fmt"
	"os"
	"time"

	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	prometheusClient "github.com/prometheus/client_golang/api"

	"k8s.io/klog"
)

const (
	queryClusterCores = `sum(
		avg(kube_node_status_capacity_cpu_cores %s) by (node, cluster_id) * avg(node_cpu_hourly_cost %s) by (node, cluster_id) * 730 +
		avg(node_gpu_hourly_cost %s) by (node, cluster_id) * 730
	  ) by (cluster_id)`

	queryClusterRAM = `sum(
		avg(kube_node_status_capacity_memory_bytes %s) by (node, cluster_id) / 1024 / 1024 / 1024 * avg(node_ram_hourly_cost %s) by (node, cluster_id) * 730
	  ) by (cluster_id)`

	queryStorage = `sum(
		avg(avg_over_time(pv_hourly_cost[%s] %s)) by (persistentvolume, cluster_id) * 730 
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes[%s] %s)) by (persistentvolume, cluster_id) / 1024 / 1024 / 1024
	  ) by (cluster_id) %s`

	queryTotal = `sum(avg(node_total_hourly_cost) by (node, cluster_id)) * 730 +
	  sum(
		avg(avg_over_time(pv_hourly_cost[1h])) by (persistentvolume, cluster_id) * 730 
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes[1h])) by (persistentvolume, cluster_id) / 1024 / 1024 / 1024
	  ) by (cluster_id) %s`
)

type Totals struct {
	TotalCost   [][]string `json:"totalcost"`
	CPUCost     [][]string `json:"cpucost"`
	MemCost     [][]string `json:"memcost"`
	StorageCost [][]string `json:"storageCost"`
}

func resultToTotals(qr interface{}) ([][]string, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}
	r, ok := data.(map[string]interface{})["result"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted data from prometheus, data has no result field")
	}
	results, ok := r.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Improperly formatted results from prometheus, result field is not a slice")
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("Not enough data available in the selected time range")
	}
	res, ok := results[0].(map[string]interface{})["values"]
	totals := [][]string{}
	for _, val := range res.([]interface{}) {
		if !ok {
			return nil, fmt.Errorf("Improperly formatted results from prometheus, value is not a field in the vector")
		}
		dataPoint, ok := val.([]interface{})
		if !ok || len(dataPoint) != 2 {
			return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		d0 := fmt.Sprintf("%f", dataPoint[0].(float64))
		toAppend := []string{
			d0,
			dataPoint[1].(string),
		}
		totals = append(totals, toAppend)
	}
	return totals, nil
}

func resultToTotal(qr interface{}) (map[string][][]string, error) {
	defaultClusterID := os.Getenv(clusterIDKey)

	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(qr)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("Prometheus query error: %s", e)
	}
	r, ok := data.(map[string]interface{})["result"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted data from prometheus, data has no result field")
	}
	results, ok := r.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Improperly formatted results from prometheus, result field is not a slice")
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("Not enough data available in the selected time range")
	}
	toReturn := make(map[string][][]string)
	for i := range results {
		metrics, ok := results[i].(map[string]interface{})["metric"]
		if !ok {
			return nil, fmt.Errorf("Improperly formatted results from prometheus, metric is not a field in the vector")
		}
		metricMap, ok := metrics.(map[string]interface{})
		cid, ok := metricMap["cluster_id"]
		if !ok {
			klog.V(4).Info("Prometheus vector does not have cluster id")
			cid = defaultClusterID
		}
		clusterID, ok := cid.(string)
		if !ok {
			return nil, fmt.Errorf("Prometheus vector does not have string cluster_id")
		}

		val, ok := results[i].(map[string]interface{})["value"]
		if !ok {
			return nil, fmt.Errorf("Improperly formatted results from prometheus, value is not a field in the vector")
		}
		dataPoint, ok := val.([]interface{})
		if !ok || len(dataPoint) != 2 {
			return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
		}
		d0 := fmt.Sprintf("%f", dataPoint[0].(float64))
		toAppend := []string{
			d0,
			dataPoint[1].(string),
		}
		if t, ok := toReturn[clusterID]; ok {
			t = append(t, toAppend)
		} else {
			toReturn[clusterID] = [][]string{toAppend}
		}
	}
	return toReturn, nil
}

// ClusterCostsForAllClusters gives the cluster costs averaged over a window of time for all clusters.
func ClusterCostsForAllClusters(cli prometheusClient.Client, cloud costAnalyzerCloud.Provider, windowString, offset string) (map[string]*Totals, error) {

	if offset != "" {
		offset = fmt.Sprintf("offset 3h") // Set offset to 3h for block sync
	}

	qCores := fmt.Sprintf(queryClusterCores, offset, offset, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, offset, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, "")

	resultClusterCores, err := Query(cli, qCores)
	if err != nil {
		return nil, fmt.Errorf("Error for query %s: %s", qCores, err.Error())
	}
	resultClusterRAM, err := Query(cli, qRAM)
	if err != nil {
		return nil, fmt.Errorf("Error for query %s: %s", qRAM, err.Error())
	}

	resultStorage, err := Query(cli, qStorage)
	if err != nil {
		return nil, fmt.Errorf("Error for query %s: %s", qStorage, err.Error())
	}

	toReturn := make(map[string]*Totals)

	coreTotal, err := resultToTotal(resultClusterCores)
	if err != nil {
		return nil, fmt.Errorf("Error for query %s: %s", qCores, err.Error())
	}
	for clusterID, total := range coreTotal {
		if _, ok := toReturn[clusterID]; !ok {
			toReturn[clusterID] = &Totals{}
		}
		toReturn[clusterID].CPUCost = total
	}

	ramTotal, err := resultToTotal(resultClusterRAM)
	if err != nil {
		return nil, fmt.Errorf("Error for query %s: %s", qRAM, err.Error())
	}
	for clusterID, total := range ramTotal {
		if _, ok := toReturn[clusterID]; !ok {
			toReturn[clusterID] = &Totals{}
		}
		toReturn[clusterID].MemCost = total
	}

	storageTotal, err := resultToTotal(resultStorage)
	if err != nil {
		return nil, fmt.Errorf("Error for query %s: %s", qStorage, err.Error())
	}
	for clusterID, total := range storageTotal {
		if _, ok := toReturn[clusterID]; !ok {
			toReturn[clusterID] = &Totals{}
		}
		toReturn[clusterID].StorageCost = total
	}

	return toReturn, nil
}

// ClusterCosts gives the current full cluster costs averaged over a window of time.
func ClusterCosts(cli prometheusClient.Client, cloud costAnalyzerCloud.Provider, windowString, offset string) (*Totals, error) {

	localStorageQuery, err := cloud.GetLocalStorageQuery()
	if err != nil {
		return nil, err
	}
	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf("+ %s", localStorageQuery)
	}

	// turn offsets of the format "[0-9+]h" into the format "offset [0-9+]h" for use in query templatess
	if offset != "" {
		offset = fmt.Sprintf("offset %s", offset)
	}

	qCores := fmt.Sprintf(queryClusterCores, offset, offset, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, offset, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, localStorageQuery)
	qTotal := fmt.Sprintf(queryTotal, localStorageQuery)

	resultClusterCores, err := Query(cli, qCores)
	if err != nil {
		return nil, err
	}
	resultClusterRAM, err := Query(cli, qRAM)
	if err != nil {
		return nil, err
	}

	resultStorage, err := Query(cli, qStorage)
	if err != nil {
		return nil, err
	}

	resultTotal, err := Query(cli, qTotal)
	if err != nil {
		return nil, err
	}

	coreTotal, err := resultToTotal(resultClusterCores)
	if err != nil {
		return nil, err
	}

	ramTotal, err := resultToTotal(resultClusterRAM)
	if err != nil {
		return nil, err
	}

	storageTotal, err := resultToTotal(resultStorage)
	if err != nil {
		return nil, err
	}

	clusterTotal, err := resultToTotal(resultTotal)
	if err != nil {
		return nil, err
	}

	defaultClusterID := os.Getenv(clusterIDKey)

	return &Totals{
		TotalCost:   clusterTotal[defaultClusterID],
		CPUCost:     coreTotal[defaultClusterID],
		MemCost:     ramTotal[defaultClusterID],
		StorageCost: storageTotal[defaultClusterID],
	}, nil
}

// ClusterCostsOverTime gives the full cluster costs over time
func ClusterCostsOverTime(cli prometheusClient.Client, cloud costAnalyzerCloud.Provider, startString, endString, windowString, offset string) (*Totals, error) {

	localStorageQuery, err := cloud.GetLocalStorageQuery()
	if err != nil {
		return nil, err
	}
	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf("+ %s", localStorageQuery)
	}

	layout := "2006-01-02T15:04:05.000Z"

	start, err := time.Parse(layout, startString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + startString + ". Error: " + err.Error())
		return nil, err
	}
	end, err := time.Parse(layout, endString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + endString + ". Error: " + err.Error())
		return nil, err
	}
	window, err := time.ParseDuration(windowString)
	if err != nil {
		klog.V(1).Infof("Error parsing time " + windowString + ". Error: " + err.Error())
		return nil, err
	}

	// turn offsets of the format "[0-9+]h" into the format "offset [0-9+]h" for use in query templatess
	if offset != "" {
		offset = fmt.Sprintf("offset %s", offset)
	}

	qCores := fmt.Sprintf(queryClusterCores, offset, offset, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, offset, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, localStorageQuery)
	qTotal := fmt.Sprintf(queryTotal, localStorageQuery)

	resultClusterCores, err := QueryRange(cli, qCores, start, end, window)
	if err != nil {
		return nil, err
	}
	resultClusterRAM, err := QueryRange(cli, qRAM, start, end, window)
	if err != nil {
		return nil, err
	}

	resultStorage, err := QueryRange(cli, qStorage, start, end, window)
	if err != nil {
		return nil, err
	}

	resultTotal, err := QueryRange(cli, qTotal, start, end, window)
	if err != nil {
		return nil, err
	}

	coreTotal, err := resultToTotals(resultClusterCores)
	if err != nil {
		return nil, err
	}

	ramTotal, err := resultToTotals(resultClusterRAM)
	if err != nil {
		return nil, err
	}

	storageTotal, err := resultToTotals(resultStorage)
	if err != nil {
		return nil, err
	}

	clusterTotal, err := resultToTotals(resultTotal)
	if err != nil {
		return nil, err
	}

	return &Totals{
		TotalCost:   clusterTotal,
		CPUCost:     coreTotal,
		MemCost:     ramTotal,
		StorageCost: storageTotal,
	}, nil

}
