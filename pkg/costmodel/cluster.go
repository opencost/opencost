package costmodel

import (
	"fmt"
	"os"
	"time"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util"
	prometheus "github.com/prometheus/client_golang/api"
	"k8s.io/klog"
)

const (
	queryClusterCores = `sum(
		avg(avg_over_time(kube_node_status_capacity_cpu_cores[%s] %s)) by (node, cluster_id) * avg(avg_over_time(node_cpu_hourly_cost[%s] %s)) by (node, cluster_id) * 730 +
		avg(avg_over_time(node_gpu_hourly_cost[%s] %s)) by (node, cluster_id) * 730
	  ) by (cluster_id)`

	queryClusterRAM = `sum(
		avg(avg_over_time(kube_node_status_capacity_memory_bytes[%s] %s)) by (node, cluster_id) / 1024 / 1024 / 1024 * avg(avg_over_time(node_ram_hourly_cost[%s] %s)) by (node, cluster_id) * 730
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

	queryNodes = `sum(avg(node_total_hourly_cost) by (node, cluster_id)) * 730 %s`
)

// Costs represents cumulative and monthly cluster costs over a given duration. Costs
// are broken down by cores, memory, and storage.
type ClusterCosts struct {
	Start             *time.Time             `json:"startTime"`
	End               *time.Time             `json:"endTime"`
	CPUCumulative     float64                `json:"cpuCumulativeCost"`
	CPUMonthly        float64                `json:"cpuMonthlyCost"`
	CPUBreakdown      *ClusterCostsBreakdown `json:"cpuBreakdown"`
	GPUCumulative     float64                `json:"gpuCumulativeCost"`
	GPUMonthly        float64                `json:"gpuMonthlyCost"`
	RAMCumulative     float64                `json:"ramCumulativeCost"`
	RAMMonthly        float64                `json:"ramMonthlyCost"`
	RAMBreakdown      *ClusterCostsBreakdown `json:"ramBreakdown"`
	StorageCumulative float64                `json:"storageCumulativeCost"`
	StorageMonthly    float64                `json:"storageMonthlyCost"`
	StorageBreakdown  *ClusterCostsBreakdown `json:"storageBreakdown"`
	TotalCumulative   float64                `json:"totalCumulativeCost"`
	TotalMonthly      float64                `json:"totalMonthlyCost"`
	DataMinutes       float64
}

// ClusterCostsBreakdown provides percentage-based breakdown of a resource by
// categories: user for user-space (i.e. non-system) usage, system, and idle.
type ClusterCostsBreakdown struct {
	Idle   float64 `json:"idle"`
	Other  float64 `json:"other"`
	System float64 `json:"system"`
	User   float64 `json:"user"`
}

// NewClusterCostsFromCumulative takes cumulative cost data over a given time range, computes
// the associated monthly rate data, and returns the Costs.
func NewClusterCostsFromCumulative(cpu, gpu, ram, storage float64, window, offset string, dataHours float64) (*ClusterCosts, error) {
	start, end, err := util.ParseTimeRange(window, offset)
	if err != nil {
		return nil, err
	}

	// If the number of hours is not given (i.e. is zero) compute one from the window and offset
	if dataHours == 0 {
		dataHours = end.Sub(*start).Hours()
	}

	// Do not allow zero-length windows to prevent divide-by-zero issues
	if dataHours == 0 {
		return nil, fmt.Errorf("illegal time range: window %s, offset %s", window, offset)
	}

	cc := &ClusterCosts{
		Start:             start,
		End:               end,
		CPUCumulative:     cpu,
		GPUCumulative:     gpu,
		RAMCumulative:     ram,
		StorageCumulative: storage,
		TotalCumulative:   cpu + gpu + ram + storage,
		CPUMonthly:        cpu / dataHours * (util.HoursPerMonth),
		GPUMonthly:        gpu / dataHours * (util.HoursPerMonth),
		RAMMonthly:        ram / dataHours * (util.HoursPerMonth),
		StorageMonthly:    storage / dataHours * (util.HoursPerMonth),
	}
	cc.TotalMonthly = cc.CPUMonthly + cc.GPUMonthly + cc.RAMMonthly + cc.StorageMonthly

	return cc, nil
}

// ComputeClusterCosts gives the cumulative and monthly-rate cluster costs over a window of time for all clusters.
func ComputeClusterCosts(client prometheus.Client, provider cloud.Provider, window, offset string, withBreakdown bool) (map[string]*ClusterCosts, error) {
	// Compute number of minutes in the full interval, for use interpolating missed scrapes or scaling missing data
	start, end, err := util.ParseTimeRange(window, offset)
	if err != nil {
		return nil, err
	}
	mins := end.Sub(*start).Minutes()

	// minsPerResolution determines accuracy and resource use for the following
	// queries. Smaller values (higher resolution) result in better accuracy,
	// but more expensive queries, and vice-a-versa.
	minsPerResolution := 5

	// hourlyToCumulative is a scaling factor that, when multiplied by an hourly
	// value, converts it to a cumulative value; i.e.
	// [$/hr] * [min/res]*[hr/min] = [$/res]
	hourlyToCumulative := float64(minsPerResolution) * (1.0 / 60.0)

	const fmtQueryDataCount = `
		count_over_time(sum(kube_node_status_capacity_cpu_cores) by (cluster_id)[%s:%dm]%s) * %d
	`

	const fmtQueryTotalGPU = `
		sum(
			sum_over_time(node_gpu_hourly_cost[%s:%dm]%s) * %f
		) by (cluster_id)
	`

	const fmtQueryTotalCPU = `
		sum(
			sum_over_time(avg(kube_node_status_capacity_cpu_cores) by (node, cluster_id)[%s:%dm]%s) *
			avg(avg_over_time(node_cpu_hourly_cost[%s:%dm]%s)) by (node, cluster_id) * %f
		) by (cluster_id)
	`

	const fmtQueryTotalRAM = `
		sum(
			sum_over_time(avg(kube_node_status_capacity_memory_bytes) by (node, cluster_id)[%s:%dm]%s) / 1024 / 1024 / 1024 *
			avg(avg_over_time(node_ram_hourly_cost[%s:%dm]%s)) by (node, cluster_id) * %f
		) by (cluster_id)
	`

	const fmtQueryTotalStorage = `
		sum(
			sum_over_time(avg(kube_persistentvolume_capacity_bytes) by (persistentvolume, cluster_id)[%s:%dm]%s) / 1024 / 1024 / 1024 *
			avg(avg_over_time(pv_hourly_cost[%s:%dm]%s)) by (persistentvolume, cluster_id) * %f
		) by (cluster_id)
	`

	const fmtQueryCPUModePct = `
		sum(rate(node_cpu_seconds_total[%s]%s)) by (cluster_id, mode) / ignoring(mode)
		group_left sum(rate(node_cpu_seconds_total[%s]%s)) by (cluster_id)
	`

	const fmtQueryRAMSystemPct = `
		sum(sum_over_time(container_memory_usage_bytes{container_name!="",namespace="kube-system"}[%s:%dm]%s)) by (cluster_id)
		/ sum(sum_over_time(kube_node_status_capacity_memory_bytes[%s:%dm]%s)) by (cluster_id)
	`

	const fmtQueryRAMUserPct = `
		sum(sum_over_time(kubecost_cluster_memory_working_set_bytes[%s:%dm]%s)) by (cluster_id)
		/ sum(sum_over_time(kube_node_status_capacity_memory_bytes[%s:%dm]%s)) by (cluster_id)
	`

	// TODO niko/clustercost metric "kubelet_volume_stats_used_bytes" was deprecated in 1.12, then seems to have come back in 1.17
	// const fmtQueryPVStorageUsePct = `(sum(kube_persistentvolumeclaim_info) by (persistentvolumeclaim, storageclass,namespace) + on (persistentvolumeclaim,namespace)
	// group_right(storageclass) sum(kubelet_volume_stats_used_bytes) by (persistentvolumeclaim,namespace))`

	queryUsedLocalStorage := provider.GetLocalStorageQuery(window, offset, false, true)

	queryTotalLocalStorage := provider.GetLocalStorageQuery(window, offset, false, false)
	if queryTotalLocalStorage != "" {
		queryTotalLocalStorage = fmt.Sprintf(" + %s", queryTotalLocalStorage)
	}

	fmtOffset := ""
	if offset != "" {
		fmtOffset = fmt.Sprintf("offset %s", offset)
	}

	queryDataCount := fmt.Sprintf(fmtQueryDataCount, window, minsPerResolution, fmtOffset, minsPerResolution)
	queryTotalGPU := fmt.Sprintf(fmtQueryTotalGPU, window, minsPerResolution, fmtOffset, hourlyToCumulative)
	queryTotalCPU := fmt.Sprintf(fmtQueryTotalCPU, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset, hourlyToCumulative)
	queryTotalRAM := fmt.Sprintf(fmtQueryTotalRAM, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset, hourlyToCumulative)
	queryTotalStorage := fmt.Sprintf(fmtQueryTotalStorage, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset, hourlyToCumulative)

	ctx := prom.NewContext(client)

	resChs := ctx.QueryAll(
		queryDataCount,
		queryTotalGPU,
		queryTotalCPU,
		queryTotalRAM,
		queryTotalStorage,
		queryTotalLocalStorage,
	)

	if withBreakdown {
		queryCPUModePct := fmt.Sprintf(fmtQueryCPUModePct, window, fmtOffset, window, fmtOffset)
		queryRAMSystemPct := fmt.Sprintf(fmtQueryRAMSystemPct, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset)
		queryRAMUserPct := fmt.Sprintf(fmtQueryRAMUserPct, window, minsPerResolution, fmtOffset, window, minsPerResolution, fmtOffset)

		bdResChs := ctx.QueryAll(
			queryCPUModePct,
			queryRAMSystemPct,
			queryRAMUserPct,
			queryUsedLocalStorage,
		)

		resChs = append(resChs, bdResChs...)
	}

	defaultClusterID := os.Getenv(clusterIDKey)

	dataMinsByCluster := map[string]float64{}
	for _, result := range resChs[0].Await().Results {
		clusterID, _ := result.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}
		dataMins := mins
		if len(result.Values) > 0 {
			dataMins = result.Values[0].Value
		} else {
			klog.V(3).Infof("[Warning] cluster cost data count returned no results for cluster %s", clusterID)
		}
		dataMinsByCluster[clusterID] = dataMins
	}

	// Determine combined discount
	discount, customDiscount := 0.0, 0.0
	c, err := A.Cloud.GetConfig()
	if err == nil {
		discount, err = ParsePercentString(c.Discount)
		if err != nil {
			discount = 0.0
		}
		customDiscount, err = ParsePercentString(c.NegotiatedDiscount)
		if err != nil {
			customDiscount = 0.0
		}
	}

	// Intermediate structure storing mapping of [clusterID][type ∈ {cpu, ram, storage, total}]=cost
	costData := make(map[string]map[string]float64)

	// Helper function to iterate over Prom query results, parsing the raw values into
	// the intermediate costData structure.
	setCostsFromResults := func(costData map[string]map[string]float64, results []*prom.QueryResult, name string, discount float64, customDiscount float64) {
		for _, result := range results {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := costData[clusterID]; !ok {
				costData[clusterID] = map[string]float64{}
			}
			if len(result.Values) > 0 {
				costData[clusterID][name] += result.Values[0].Value * (1.0 - discount) * (1.0 - customDiscount)
				costData[clusterID]["total"] += result.Values[0].Value * (1.0 - discount) * (1.0 - customDiscount)
			}
		}
	}
	// Apply both sustained use and custom discounts to RAM and CPU
	setCostsFromResults(costData, resChs[2].Await().Results, "cpu", discount, customDiscount)
	setCostsFromResults(costData, resChs[3].Await().Results, "ram", discount, customDiscount)
	// Apply only custom discount to GPU and storage
	setCostsFromResults(costData, resChs[1].Await().Results, "gpu", 0.0, customDiscount)
	setCostsFromResults(costData, resChs[4].Await().Results, "storage", 0.0, customDiscount)
	setCostsFromResults(costData, resChs[5].Await().Results, "localstorage", 0.0, customDiscount)

	cpuBreakdownMap := map[string]*ClusterCostsBreakdown{}
	ramBreakdownMap := map[string]*ClusterCostsBreakdown{}
	pvUsedCostMap := map[string]float64{}
	if withBreakdown {
		for _, result := range resChs[6].Await().Results {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := cpuBreakdownMap[clusterID]; !ok {
				cpuBreakdownMap[clusterID] = &ClusterCostsBreakdown{}
			}
			cpuBD := cpuBreakdownMap[clusterID]

			mode, err := result.GetString("mode")
			if err != nil {
				klog.V(3).Infof("[Warning] ComputeClusterCosts: unable to read CPU mode: %s", err)
				mode = "other"
			}

			switch mode {
			case "idle":
				cpuBD.Idle += result.Values[0].Value
			case "system":
				cpuBD.System += result.Values[0].Value
			case "user":
				cpuBD.User += result.Values[0].Value
			default:
				cpuBD.Other += result.Values[0].Value
			}
		}

		for _, result := range resChs[7].Await().Results {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := ramBreakdownMap[clusterID]; !ok {
				ramBreakdownMap[clusterID] = &ClusterCostsBreakdown{}
			}
			ramBD := ramBreakdownMap[clusterID]
			ramBD.System += result.Values[0].Value
		}
		for _, result := range resChs[8].Await().Results {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			if _, ok := ramBreakdownMap[clusterID]; !ok {
				ramBreakdownMap[clusterID] = &ClusterCostsBreakdown{}
			}
			ramBD := ramBreakdownMap[clusterID]
			ramBD.User += result.Values[0].Value
		}
		for _, ramBD := range ramBreakdownMap {
			remaining := 1.0
			remaining -= ramBD.Other
			remaining -= ramBD.System
			remaining -= ramBD.User
			ramBD.Idle = remaining
		}

		for _, result := range resChs[9].Await().Results {
			clusterID, _ := result.GetString("cluster_id")
			if clusterID == "" {
				clusterID = defaultClusterID
			}
			pvUsedCostMap[clusterID] += result.Values[0].Value
		}
	}

	// Convert intermediate structure to Costs instances
	costsByCluster := map[string]*ClusterCosts{}
	for id, cd := range costData {
		dataMins, ok := dataMinsByCluster[id]
		if !ok {
			dataMins = mins
			klog.V(3).Infof("[Warning] cluster cost data count not found for cluster %s", id)
		}
		costs, err := NewClusterCostsFromCumulative(cd["cpu"], cd["gpu"], cd["ram"], cd["storage"]+cd["localstorage"], window, offset, dataMins/util.MinsPerHour)
		if err != nil {
			klog.V(3).Infof("[Warning] Failed to parse cluster costs on %s (%s) from cumulative data: %+v", window, offset, cd)
			return nil, err
		}

		if cpuBD, ok := cpuBreakdownMap[id]; ok {
			costs.CPUBreakdown = cpuBD
		}
		if ramBD, ok := ramBreakdownMap[id]; ok {
			costs.RAMBreakdown = ramBD
		}
		costs.StorageBreakdown = &ClusterCostsBreakdown{}
		if pvUC, ok := pvUsedCostMap[id]; ok {
			costs.StorageBreakdown.Idle = (costs.StorageCumulative - pvUC) / costs.StorageCumulative
			costs.StorageBreakdown.User = pvUC / costs.StorageCumulative
		}
		costs.DataMinutes = dataMins
		costsByCluster[id] = costs
	}

	return costsByCluster, nil
}

type Totals struct {
	TotalCost   [][]string `json:"totalcost"`
	CPUCost     [][]string `json:"cpucost"`
	MemCost     [][]string `json:"memcost"`
	StorageCost [][]string `json:"storageCost"`
}

func resultToTotals(qr interface{}) ([][]string, error) {
	// TODO: Provide an actual query instead of resultToTotals
	qResults, err := prom.NewQueryResults("resultToTotals", qr)
	if err != nil {
		return nil, err
	}

	results := qResults.Results

	if len(results) == 0 {
		return [][]string{}, fmt.Errorf("Not enough data available in the selected time range")
	}

	result := results[0]
	totals := [][]string{}
	for _, value := range result.Values {
		d0 := fmt.Sprintf("%f", value.Timestamp)
		d1 := fmt.Sprintf("%f", value.Value)
		toAppend := []string{
			d0,
			d1,
		}
		totals = append(totals, toAppend)
	}
	return totals, nil
}

// ClusterCostsOverTime gives the full cluster costs over time
func ClusterCostsOverTime(cli prometheus.Client, provider cloud.Provider, startString, endString, windowString, offset string) (*Totals, error) {
	localStorageQuery := provider.GetLocalStorageQuery(windowString, offset, true, false)
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

	qCores := fmt.Sprintf(queryClusterCores, windowString, offset, windowString, offset, windowString, offset)
	qRAM := fmt.Sprintf(queryClusterRAM, windowString, offset, windowString, offset)
	qStorage := fmt.Sprintf(queryStorage, windowString, offset, windowString, offset, localStorageQuery)
	qTotal := fmt.Sprintf(queryTotal, localStorageQuery)

	ctx := prom.NewContext(cli)

	resChClusterCores := ctx.QueryRange(qCores, start, end, window)
	resChClusterRAM := ctx.QueryRange(qRAM, start, end, window)
	resChStorage := ctx.QueryRange(qStorage, start, end, window)
	resChTotal := ctx.QueryRange(qTotal, start, end, window)

	resultClusterCores := resChClusterCores.Await()
	resultClusterRAM := resChClusterRAM.Await()
	resultStorage := resChStorage.Await()
	resultTotal := resChTotal.Await()

	if ctx.HasErrors() {
		return nil, ctx.Errors()[0]
	}

	coreTotal, err := resultToTotals(resultClusterCores)
	if err != nil {
		klog.Infof("[Warning] ClusterCostsOverTime: no cpu data: %s", err)
		return nil, err
	}

	ramTotal, err := resultToTotals(resultClusterRAM)
	if err != nil {
		klog.Infof("[Warning] ClusterCostsOverTime: no ram data: %s", err)
		return nil, err
	}

	storageTotal, err := resultToTotals(resultStorage)
	if err != nil {
		klog.Infof("[Warning] ClusterCostsOverTime: no storage data: %s", err)
	}

	clusterTotal, err := resultToTotals(resultTotal)
	if err != nil {
		// If clusterTotal query failed, it's likely because there are no PVs, which
		// causes the qTotal query to return no data. Instead, query only node costs.
		// If that fails, return an error because something is actually wrong.
		qNodes := fmt.Sprintf(queryNodes, localStorageQuery)

		resultNodes, err := ctx.QueryRangeSync(qNodes, start, end, window)
		if err != nil {
			return nil, err
		}

		clusterTotal, err = resultToTotals(resultNodes)
		if err != nil {
			klog.Infof("[Warning] ClusterCostsOverTime: no node data: %s", err)
			return nil, err
		}
	}

	return &Totals{
		TotalCost:   clusterTotal,
		CPUCost:     coreTotal,
		MemCost:     ramTotal,
		StorageCost: storageTotal,
	}, nil
}
