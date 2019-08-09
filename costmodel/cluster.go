package costmodel

import (
	"fmt"
	"log"
	"time"

	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	prometheusClient "github.com/prometheus/client_golang/api"

	"k8s.io/klog"
)

const (
	queryClusterCores = `sum(
		avg(kube_node_status_capacity_cpu_cores %s) by (node) * avg(node_cpu_hourly_cost %s) by (node) * 730 +
		avg(node_gpu_hourly_cost %s) by (node) * 730
	  )`

	queryClusterRAM = `sum(
		avg(kube_node_status_capacity_memory_bytes %s) by (node) / 1024 / 1024 / 1024 * avg(node_ram_hourly_cost %s) by (node) * 730
	  )`

	queryStorage = `sum(
		avg(avg_over_time(pv_hourly_cost[%s] %s)) by (persistentvolume) * 730 
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes[%s] %s)) by (persistentvolume) / 1024 / 1024 / 1024
	  ) %s`

	queryTotal = `sum(avg(node_total_hourly_cost) by (node)) * 730 +
	  sum(
		avg(avg_over_time(pv_hourly_cost[1h])) by (persistentvolume) * 730 
		* avg(avg_over_time(kube_persistentvolume_capacity_bytes[1h])) by (persistentvolume) / 1024 / 1024 / 1024
	  ) %s`
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
		return nil, fmt.Errorf("Improperly formatted response from prometheus, response %+v has no data field", data)
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
		//log.Printf("%+v", dataPoint)
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

func resultToTotal(qr interface{}) ([][]string, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		return nil, fmt.Errorf("Improperly formatted response from prometheus, response %+v has no data field", data)
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
	val, ok := results[0].(map[string]interface{})["value"]
	totals := [][]string{}
	if !ok {
		return nil, fmt.Errorf("Improperly formatted results from prometheus, value is not a field in the vector")
	}
	dataPoint, ok := val.([]interface{})
	//log.Printf("%+v", dataPoint)
	if !ok || len(dataPoint) != 2 {
		return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
	}
	d0 := fmt.Sprintf("%f", dataPoint[0].(float64))
	toAppend := []string{
		d0,
		dataPoint[1].(string),
	}
	totals = append(totals, toAppend)
	return totals, nil
}

// ClusterCostsOverTime gives the current full cluster costs averaged over a window of time.
func ClusterCosts(cli prometheusClient.Client, cloud costAnalyzerCloud.Provider, windowString, offset string) (*Totals, error) {

	localStorageQuery, err := cloud.GetLocalStorageQuery()
	if err != nil {
		return nil, err
	}
	if localStorageQuery != "" {
		localStorageQuery = fmt.Sprintf("+ %s", localStorageQuery)
	}

	qCores := fmt.Sprintf(queryClusterCores, offset, offset, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, offset, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, localStorageQuery)
	qTotal := fmt.Sprintf(queryTotal, localStorageQuery)
	log.Printf("%s", qTotal)

	resultClusterCores, err := query(cli, qCores)
	if err != nil {
		return nil, err
	}
	resultClusterRAM, err := query(cli, qRAM)
	if err != nil {
		return nil, err
	}

	resultStorage, err := query(cli, qStorage)
	if err != nil {
		return nil, err
	}

	resultTotal, err := query(cli, qTotal)
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

	return &Totals{
		TotalCost:   clusterTotal,
		CPUCost:     coreTotal,
		MemCost:     ramTotal,
		StorageCost: storageTotal,
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

	qCores := fmt.Sprintf(queryClusterCores, offset, offset, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, offset, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, localStorageQuery)
	qTotal := fmt.Sprintf(queryTotal, localStorageQuery)
	log.Printf("%s", qTotal)

	resultClusterCores, err := queryRange(cli, qCores, start, end, window)
	if err != nil {
		return nil, err
	}
	resultClusterRAM, err := queryRange(cli, qRAM, start, end, window)
	if err != nil {
		return nil, err
	}

	resultStorage, err := queryRange(cli, qStorage, start, end, window)
	if err != nil {
		return nil, err
	}

	resultTotal, err := queryRange(cli, qTotal, start, end, window)
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
