package costmodel

import (
	"fmt"
	"time"

	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"
)

// TODO niko/cdmr move to pkg/kubecost
// TODO niko/cdmr add PersistenVolumeClaims to type Allocation?
type PVC struct {
	Bytes     float64 `json:"bytes"`
	Count     int     `json:"count"`
	Name      string  `json:"name"`
	Namespace string  `json:"namespace"`
	Volume    *PV     `json:"persistentVolume"`
}

// TODO niko/cdmr move to pkg/kubecost
type PV struct {
	Bytes          float64 `json:"bytes"`
	CostPerGiBHour float64 `json:"costPerGiBHour"` // TODO niko/cdmr GiB or GB?
	Cluster        string  `json:"cluster"`
	Name           string  `json:"name"`
	StorageClass   string  `json:"storageClass"`
}

// ComputeAllocation uses the CostModel instance to compute an AllocationSet
// for the window defined by the given start and end times. The Allocations
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeAllocation(start, end time.Time) (*kubecost.AllocationSet, error) {
	// Create a window spanning the requested query
	s, e := start, end
	window := kubecost.NewWindow(&s, &e)

	// Create an empty AllocationSet. For safety, in the case of an error, we
	// should prefer to return this empty set with the error. (In the case of
	// no error, of course we populate the set and return it.)
	allocSet := kubecost.NewAllocationSet(start, end)

	// Convert window (start, end) to (duration, offset) for querying Prometheus
	timesToDurations := func(s, e time.Time) (dur, off time.Duration) {
		now := time.Now()
		off = now.Sub(e)
		dur = e.Sub(s)
		return dur, off
	}
	duration, offset := timesToDurations(start, end)

	// If using Thanos, increase offset to 3 hours, reducing the duration by
	// equal measure to maintain the same starting point.
	thanosDur := thanos.OffsetDuration()
	// TODO niko/cdmr confirm that this flag works interchangeably with ThanosClient != nil
	if offset < thanosDur && env.IsThanosEnabled() {
		diff := thanosDur - offset
		offset += diff
		duration -= diff
	}

	// If duration < 0, return an empty set
	if duration < 0 {
		return allocSet, nil
	}

	// Negative offset means that the end time is in the future. Prometheus
	// fails for non-positive offset values, so shrink the duration and
	// remove the offset altogether.
	if offset < 0 {
		duration = duration + offset
		offset = 0
	}

	durStr := fmt.Sprintf("%dm", int64(duration.Minutes()))
	offStr := fmt.Sprintf(" offset %dm", int64(offset.Minutes()))
	if offset < time.Minute {
		offStr = ""
	}

	// TODO niko/cdmr dynamic resolution? add to ComputeAllocation() in allocation.Source?
	resStr := "1m"
	// resPerHr := 60

	// TODO niko/cdmr remove after testing
	startQuerying := time.Now()

	ctx := prom.NewContext(cm.PrometheusClient)

	// TODO niko/cdmr retries? (That should probably go into the Store.)

	// TODO niko/cmdr check: will multiple Prometheus jobs multiply the totals?

	// TODO niko/cdmr should we try doing this without resolution? Could yield
	// more accurate results, but might also be more challenging in some
	// respects; e.g. "correcting" the start point by what amount?
	queryMinutes := fmt.Sprintf(`avg(kube_pod_container_status_running{}) by (container, pod, namespace, kubernetes_node, cluster_id)[%s:%s]%s`, durStr, resStr, offStr)
	resChMinutes := ctx.Query(queryMinutes)

	queryRAMBytesAllocated := fmt.Sprintf(`avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`, durStr, offStr)
	resChRAMBytesAllocated := ctx.Query(queryRAMBytesAllocated)

	queryRAMRequests := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`, durStr, offStr)
	resChRAMRequests := ctx.Query(queryRAMRequests)

	queryRAMUsage := fmt.Sprintf(`avg(avg_over_time(container_memory_working_set_bytes{container_name!="", container_name!="POD", instance!=""}[%s]%s)) by (container_name, pod_name, namespace, instance, cluster_id)`, durStr, offStr)
	resChRAMUsage := ctx.Query(queryRAMUsage)

	queryCPUCoresAllocated := fmt.Sprintf(`avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`, durStr, offStr)
	resChCPUCoresAllocated := ctx.Query(queryCPUCoresAllocated)

	queryCPURequests := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`, durStr, offStr)
	resChCPURequests := ctx.Query(queryCPURequests)

	queryCPUUsage := fmt.Sprintf(`avg(rate(container_cpu_usage_seconds_total{container_name!="", container_name!="POD", instance!=""}[%s]%s)) by (container_name, pod_name, namespace, instance, cluster_id)`, durStr, offStr)
	resChCPUUsage := ctx.Query(queryCPUUsage)

	// TODO niko/cdmr find an env with GPUs to test this (generate one?)
	queryGPUsRequested := fmt.Sprintf(`avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`, durStr, offStr)
	resChGPUsRequested := ctx.Query(queryGPUsRequested)

	queryNodeCostPerCPUHr := fmt.Sprintf(`avg(avg_over_time(node_cpu_hourly_cost[%s]%s)) by (node, cluster_id, instance_type)`, durStr, offStr)
	resChNodeCostPerCPUHr := ctx.Query(queryNodeCostPerCPUHr)

	queryNodeCostPerRAMGiBHr := fmt.Sprintf(`avg(avg_over_time(node_ram_hourly_cost[%s]%s)) by (node, cluster_id, instance_type)`, durStr, offStr)
	resChNodeCostPerRAMGiBHr := ctx.Query(queryNodeCostPerRAMGiBHr)

	queryNodeCostPerGPUHr := fmt.Sprintf(`avg(avg_over_time(node_gpu_hourly_cost[%s]%s)) by (node, cluster_id, instance_type)`, durStr, offStr)
	resChNodeCostPerGPUHr := ctx.Query(queryNodeCostPerGPUHr)

	queryNodeIsSpot := fmt.Sprintf(`avg_over_time(kubecost_node_is_spot[%s]%s)`, durStr, offStr)
	resChNodeIsSpot := ctx.Query(queryNodeIsSpot)

	queryPVCAllocation := fmt.Sprintf(`avg(avg_over_time(pod_pvc_allocation[%s]%s)) by (persistentvolume, persistentvolumeclaim, pod, namespace, cluster_id)`, durStr, offStr)
	resChPVCAllocation := ctx.Query(queryPVCAllocation)

	queryPVCBytesRequested := fmt.Sprintf(`avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{}[%s]%s)) by (persistentvolumeclaim, namespace, cluster_id)`, durStr, offStr)
	resChPVCBytesRequested := ctx.Query(queryPVCBytesRequested)

	queryPVCostPerGiBHour := fmt.Sprintf(`avg(avg_over_time(pv_hourly_cost[%s]%s)) by (volumename, cluster_id)`, durStr, offStr)
	resChPVCostPerGiBHour := ctx.Query(queryPVCostPerGiBHour)

	queryPVCInfo := fmt.Sprintf(`avg(avg_over_time(kube_persistentvolumeclaim_info{volumename != ""}[%s]%s)) by (persistentvolumeclaim, storageclass, volumename, namespace, cluster_id)`, durStr, offStr)
	resChPVCInfo := ctx.Query(queryPVCInfo)

	// TODO niko/cdmr
	// queryNetZoneRequests := fmt.Sprintf()
	// resChNetZoneRequests := ctx.Query(queryNetZoneRequests)

	// TODO niko/cdmr
	// queryNetRegionRequests := fmt.Sprintf()
	// resChNetRegionRequests := ctx.Query(queryNetRegionRequests)

	// TODO niko/cdmr
	// queryNetInternetRequests := fmt.Sprintf()
	// resChNetInternetRequests := ctx.Query(queryNetInternetRequests)

	// TODO niko/cdmr
	// queryNamespaceLabels := fmt.Sprintf()
	// resChNamespaceLabels := ctx.Query(queryNamespaceLabels)

	// TODO niko/cdmr
	// queryPodLabels := fmt.Sprintf()
	// resChPodLabels := ctx.Query(queryPodLabels)

	// TODO niko/cdmr
	// queryNamespaceAnnotations := fmt.Sprintf()
	// resChNamespaceAnnotations := ctx.Query(queryNamespaceAnnotations)

	// TODO niko/cdmr
	// queryPodAnnotations := fmt.Sprintf()
	// resChPodAnnotations := ctx.Query(queryPodAnnotations)

	// TODO niko/cdmr
	// queryServiceLabels := fmt.Sprintf()
	// resChServiceLabels := ctx.Query(queryServiceLabels)

	// TODO niko/cdmr
	// queryDeploymentLabels := fmt.Sprintf()
	// resChDeploymentLabels := ctx.Query(queryDeploymentLabels)

	// TODO niko/cdmr
	// queryStatefulSetLabels := fmt.Sprintf()
	// resChStatefulSetLabels := ctx.Query(queryStatefulSetLabels)

	// TODO niko/cdmr
	// queryDaemonSetLabels := fmt.Sprintf()
	// resChDaemonSetLabels := ctx.Query(queryDaemonSetLabels)

	// TODO niko/cdmr
	// queryJobLabels := fmt.Sprintf()
	// resChJobLabels := ctx.Query(queryJobLabels)

	resMinutes, _ := resChMinutes.Await()
	resCPUCoresAllocated, _ := resChCPUCoresAllocated.Await()
	resCPURequests, _ := resChCPURequests.Await()
	resCPUUsage, _ := resChCPUUsage.Await()
	resRAMBytesAllocated, _ := resChRAMBytesAllocated.Await()
	resRAMRequests, _ := resChRAMRequests.Await()
	resRAMUsage, _ := resChRAMUsage.Await()
	resGPUsRequested, _ := resChGPUsRequested.Await()

	resNodeCostPerCPUHr, _ := resChNodeCostPerCPUHr.Await()
	resNodeCostPerRAMGiBHr, _ := resChNodeCostPerRAMGiBHr.Await()
	resNodeCostPerGPUHr, _ := resChNodeCostPerGPUHr.Await()
	resNodeIsSpot, _ := resChNodeIsSpot.Await()

	resPVCAllocation, _ := resChPVCAllocation.Await()
	resPVCBytesRequested, _ := resChPVCBytesRequested.Await()
	resPVCostPerGiBHour, _ := resChPVCostPerGiBHour.Await()
	resPVCInfo, _ := resChPVCInfo.Await()

	// TODO niko/cdmr remove after testing
	log.Infof("CostModel.ComputeAllocation: minutes:   %s", queryMinutes)

	log.Infof("CostModel.ComputeAllocation: CPU cores: %s", queryCPUCoresAllocated)
	log.Infof("CostModel.ComputeAllocation: CPU req  : %s", queryCPURequests)
	log.Infof("CostModel.ComputeAllocation: CPU use  : %s", queryCPUUsage)
	log.Infof("CostModel.ComputeAllocation: $/CPU*Hr : %s", queryNodeCostPerCPUHr)

	log.Infof("CostModel.ComputeAllocation: RAM bytes: %s", queryRAMBytesAllocated)
	log.Infof("CostModel.ComputeAllocation: RAM req  : %s", queryRAMRequests)
	log.Infof("CostModel.ComputeAllocation: RAM use  : %s", queryRAMUsage)
	log.Infof("CostModel.ComputeAllocation: $/GiB*Hr : %s", queryNodeCostPerRAMGiBHr)

	log.Infof("CostModel.ComputeAllocation: PVC alloc: %s", queryPVCAllocation)
	log.Infof("CostModel.ComputeAllocation: PVC bytes: %s", queryPVCBytesRequested)
	log.Infof("CostModel.ComputeAllocation: PVC info : %s", queryPVCInfo)
	log.Infof("CostModel.ComputeAllocation: PV $/gbhr: %s", queryPVCostPerGiBHour)

	log.Profile(startQuerying, "CostModel.ComputeAllocation: queries complete")

	// Build out a map of Allocations, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	// TODO niko/cdmr can we start with a reasonable guess at map size?
	allocationMap := map[containerKey]*kubecost.Allocation{}

	// Keep track of the number of allocations per pod, for the sake of
	// splitting PVC allocation into per-Allocation from per-Pod.
	podAllocationCount := map[podKey]int{}

	// clusterStarts and clusterEnds record the earliest start and latest end
	// times, respectively, on a cluster-basis. These are used for unmounted
	// PVs and other "virtual" Allocations so that minutes are maximally
	// accurate during start-up or spin-down of a cluster
	clusterStart := map[string]time.Time{}
	clusterEnd := map[string]time.Time{}

	buildAllocationMap(window, allocationMap, podAllocationCount, clusterStart, clusterEnd, resMinutes)
	applyCPUCoresAllocated(allocationMap, resCPUCoresAllocated)
	applyCPUCoresRequested(allocationMap, resCPURequests)
	applyCPUCoresUsed(allocationMap, resCPUUsage)
	applyRAMBytesAllocated(allocationMap, resRAMBytesAllocated)
	applyRAMBytesRequested(allocationMap, resRAMRequests)
	applyRAMBytesUsed(allocationMap, resRAMUsage)
	applyGPUsRequested(allocationMap, resGPUsRequested)

	// Build out a map of Nodes with resource costs, discounts, and node types
	// for converting resource allocation data to cumulative costs.
	nodeMap := map[nodeKey]*Node{}

	applyNodeCostPerCPUHr(nodeMap, resNodeCostPerCPUHr)
	applyNodeCostPerRAMGiBHr(nodeMap, resNodeCostPerRAMGiBHr)
	applyNodeCostPerGPUHr(nodeMap, resNodeCostPerGPUHr)
	applyNodeSpot(nodeMap, resNodeIsSpot)
	applyNodeDiscount(nodeMap, cm)

	// TODO niko/cdmr comment
	pvMap := map[pvKey]*PV{}

	for _, res := range resPVCostPerGiBHour {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		name, err := res.GetString("volumename")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV cost without volumename")
			continue
		}

		key := newPVKey(cluster, name)

		pvMap[key] = &PV{
			Cluster:        cluster,
			Name:           name,
			CostPerGiBHour: res.Values[0].Value,
		}
	}

	// TODO niko/cdmr comment
	pvcMap := map[pvcKey]*PVC{}

	for _, res := range resPVCBytesRequested {
		key, err := resultPVCKey(res, "cluster_id", "namespace", "persistentvolumeclaim")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV bytes requested query result missing field: %s", err)
			continue
		}

		// TODO niko/cdmr double-check "persistentvolume" vs "volumename"
		values, err := res.GetStrings("persistentvolumeclaim", "namespace")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV bytes requested query result missing field: %s", err)
			continue
		}
		name := values["persistentvolumeclaim"]
		namespace := values["namespace"]

		log.Infof("CostModel.ComputeAllocation: PVC: %s %fGiB", key, res.Values[0].Value/1024/1024/1024)

		// TODO niko/cdmr
		pvcMap[key] = &PVC{
			Bytes:     res.Values[0].Value,
			Name:      name,
			Namespace: namespace,
		}
	}

	// TODO niko/cdmr comment
	podPVCMap := map[podKey][]*PVC{}

	for _, res := range resPVCAllocation {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		values, err := res.GetStrings("persistentvolume", "persistentvolumeclaim", "pod", "namespace")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PVC allocation query result missing field: %s", err)
			continue
		}

		namespace := values["namespace"]
		pod := values["pod"]
		name := values["persistentvolumeclaim"]
		volume := values["persistentvolume"]

		podKey := newPodKey(cluster, namespace, pod)
		pvKey := newPVKey(cluster, volume)

		if _, ok := pvMap[pvKey]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV missing for PVC allocation query result: %s", pvKey)
			continue
		}

		if _, ok := podPVCMap[podKey]; !ok {
			podPVCMap[podKey] = []*PVC{}
		}

		podPVCMap[podKey] = append(podPVCMap[podKey], &PVC{
			Bytes:  res.Values[0].Value,
			Count:  podAllocationCount[podKey],
			Name:   name,
			Volume: pvMap[pvKey],
		})
	}

	for _, res := range resPVCInfo {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		values, err := res.GetStrings("persistentvolumeclaim", "storageclass", "volumename", "namespace")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PVC info query result missing field: %s", err)
			continue
		}

		// TODO niko/cdmr ?
		// namespace := values["namespace"]
		// name := values["persistentvolumeclaim"]
		volume := values["volumename"]
		storageClass := values["storageclass"]

		pvKey := newPVKey(cluster, volume)

		if _, ok := pvMap[pvKey]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV missing for PVC info query result: %s", pvKey)
			continue
		}

		pvMap[pvKey].StorageClass = storageClass
	}

	log.Infof("CostModel.ComputeAllocation: %d allocations", len(allocationMap))
	log.Infof("CostModel.ComputeAllocation: %d nodes", len(nodeMap))
	log.Infof("CostModel.ComputeAllocation: %d PVs", len(pvMap))
	log.Infof("CostModel.ComputeAllocation: %d PVCs", len(pvcMap))
	log.Infof("CostModel.ComputeAllocation: %d pods with PVCs", len(podPVCMap))

	for _, node := range nodeMap {
		log.Infof("CostModel.ComputeAllocation: Node: %s: %f/CPUHr; %f/RAMHr; %f/GPUHr; %f discount", node.Name, node.CostPerCPUHr, node.CostPerRAMGiBHr, node.CostPerGPUHr, node.Discount)
	}

	for _, pv := range pvMap {
		log.Infof("CostModel.ComputeAllocation: PV: %v", pv)
	}

	for pod, pvcs := range podPVCMap {
		for _, pvc := range pvcs {
			log.Infof("CostModel.ComputeAllocation: Pod %s: PVC: %v", pod, pvc)
		}
	}

	for _, alloc := range allocationMap {
		// TODO niko/cdmr compute costs from resources and prices?

		cluster, _ := alloc.Properties.GetCluster()
		node, _ := alloc.Properties.GetNode()
		namespace, _ := alloc.Properties.GetNamespace()
		pod, _ := alloc.Properties.GetPod()

		podKey := newPodKey(cluster, namespace, pod)
		nodeKey := newNodeKey(cluster, node)

		if n, ok := nodeMap[nodeKey]; !ok {
			log.Warningf("CostModel.ComputeAllocation: failed to find node %s for %s", nodeKey, alloc.Name)
		} else {
			alloc.CPUCost = alloc.CPUCoreHours * n.CostPerCPUHr
			alloc.RAMCost = (alloc.RAMByteHours / 1024 / 1024 / 1024) * n.CostPerRAMGiBHr
			alloc.GPUCost = alloc.GPUHours * n.CostPerGPUHr
		}

		if pvcs, ok := podPVCMap[podKey]; ok {
			for _, pvc := range pvcs {
				// TODO niko/cdmr this isn't quite right... use PVC info query for PVC minutes?
				hrs := alloc.Minutes() / 60.0
				gib := pvc.Bytes / 1024 / 1024 / 1024

				alloc.PVByteHours += pvc.Bytes * hrs
				alloc.PVCost += pvc.Volume.CostPerGiBHour * gib * hrs
			}
		}

		// log.Infof("CostModel.ComputeAllocation: %s: %v", alloc.Name, alloc)

		alloc.TotalCost += alloc.CPUCost
		alloc.TotalCost += alloc.RAMCost
		alloc.TotalCost += alloc.GPUCost
		alloc.TotalCost += alloc.PVCost
		alloc.TotalCost += alloc.NetworkCost
		alloc.TotalCost += alloc.SharedCost
		alloc.TotalCost += alloc.ExternalCost

		allocSet.Set(alloc)
	}

	return allocSet, nil
}

func buildAllocationMap(window kubecost.Window, allocationMap map[containerKey]*kubecost.Allocation, podAllocationCount map[podKey]int, clusterStart, clusterEnd map[string]time.Time, resMinutes []*prom.QueryResult) {
	for _, res := range resMinutes {
		if len(res.Values) == 0 {
			log.Warningf("CostModel.ComputeAllocation: empty minutes result")
			continue
		}

		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		labels, err := res.GetStrings("kubernetes_node", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: minutes query result missing field: %s", err)
			continue
		}

		node := labels["kubernetes_node"]
		namespace := labels["namespace"]
		pod := labels["pod"]
		container := labels["container"]

		containerKey := newContainerKey(cluster, namespace, pod, container)
		podKey := newPodKey(cluster, namespace, pod)

		// allocStart and allocEnd are the timestamps of the first and last
		// minutes the allocation was running, respectively. We subtract 1m
		// from allocStart because this point will actually represent the end
		// of the first minute. We don't subtract from allocEnd because it
		// already represents the end of the last minute.
		var allocStart, allocEnd time.Time
		for _, datum := range res.Values {
			t := time.Unix(int64(datum.Timestamp), 0)
			if allocStart.IsZero() && datum.Value > 0 && window.Contains(t) {
				allocStart = t
			}
			if datum.Value > 0 && window.Contains(t) {
				allocEnd = t
			}
		}
		if allocStart.IsZero() || allocEnd.IsZero() {
			log.Warningf("CostModel.ComputeAllocation: allocation %s has no running time", containerKey)
		}
		allocStart = allocStart.Add(-time.Minute)

		// TODO niko/cdmr scan "minutes" results for 1s and 0s, and discard points
		// that fall outside the given window (why does that happen??)

		// TODO niko/cdmr "snap-to" start and end if within some epsilon of window start, end

		// Set start if unset or this datum's start time is earlier than the
		// current earliest time.
		if _, ok := clusterStart[cluster]; !ok || allocStart.Before(clusterStart[cluster]) {
			clusterStart[cluster] = allocStart
		}

		// Set end if unset or this datum's end time is later than the
		// current latest time.
		if _, ok := clusterEnd[cluster]; !ok || allocEnd.After(clusterEnd[cluster]) {
			clusterEnd[cluster] = allocEnd
		}

		name := fmt.Sprintf("%s/%s/%s/%s", cluster, namespace, pod, container)

		alloc := &kubecost.Allocation{
			Name:       name,
			Properties: kubecost.Properties{},
			Window:     window.Clone(),
			Start:      allocStart,
			End:        allocEnd,
		}
		alloc.Properties.SetContainer(container)
		alloc.Properties.SetPod(pod)
		alloc.Properties.SetNamespace(namespace)
		alloc.Properties.SetNode(node)
		alloc.Properties.SetCluster(cluster)

		allocationMap[containerKey] = alloc

		podAllocationCount[podKey]++
	}
}

func applyCPUCoresAllocated(allocationMap map[containerKey]*kubecost.Allocation, resCPUCoresAllocated []*prom.QueryResult) {
	for _, res := range resCPUCoresAllocated {
		// TODO niko/cdmr do we need node here?
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified CPU allocation query result: %s", key)
			continue
		}

		cpuCores := res.Values[0].Value
		hours := allocationMap[key].Minutes() / 60.0
		allocationMap[key].CPUCoreHours = cpuCores * hours
	}
}

func applyCPUCoresRequested(allocationMap map[containerKey]*kubecost.Allocation, resCPUCoresRequested []*prom.QueryResult) {
	for _, res := range resCPUCoresRequested {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU request query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified CPU request query result: %s", key)
			continue
		}

		allocationMap[key].CPUCoreRequestAverage = res.Values[0].Value
	}
}

func applyCPUCoresUsed(allocationMap map[containerKey]*kubecost.Allocation, resCPUCoresUsed []*prom.QueryResult) {
	for _, res := range resCPUCoresUsed {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU usage query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified CPU usage query result: %s", key)
			continue
		}

		allocationMap[key].CPUCoreUsageAverage = res.Values[0].Value
	}
}

func applyRAMBytesRequested(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesRequested []*prom.QueryResult) {
	for _, res := range resRAMBytesRequested {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU request query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified CPU request query result: %s", key)
			continue
		}

		allocationMap[key].RAMBytesRequestAverage = res.Values[0].Value
	}
}

func applyRAMBytesUsed(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesUsed []*prom.QueryResult) {
	for _, res := range resRAMBytesUsed {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU usage query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified CPU usage query result: %s", key)
			continue
		}

		allocationMap[key].RAMBytesUsageAverage = res.Values[0].Value
	}
}

func applyRAMBytesAllocated(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesAllocated []*prom.QueryResult) {
	for _, res := range resRAMBytesAllocated {
		// TODO niko/cdmr do we need node here?
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified RAM allocation query result: %s", key)
			continue
		}

		ramBytes := res.Values[0].Value
		hours := allocationMap[key].Minutes() / 60.0
		allocationMap[key].RAMByteHours = ramBytes * hours
	}
}

func applyGPUsRequested(allocationMap map[containerKey]*kubecost.Allocation, resGPUsRequested []*prom.QueryResult) {
	for _, res := range resGPUsRequested {
		// TODO niko/cdmr do we need node here?
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified RAM allocation query result: %s", key)
			continue
		}

		// TODO niko/cdmr complete
		log.Infof("CostModel.ComputeAllocation: GPU results: %s=%f", key, res.Values[0].Value)
	}
}

func applyNodeCostPerCPUHr(nodeMap map[nodeKey]*Node, resNodeCostPerCPUHr []*prom.QueryResult) {
	for _, res := range resNodeCostPerCPUHr {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node CPU cost query result missing field: %s", err)
			continue
		}

		instanceType, err := res.GetString("instance_type")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node CPU cost query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &Node{
				Name:     node,
				NodeType: instanceType,
			}
		}

		nodeMap[key].CostPerCPUHr = res.Values[0].Value
	}
}

func applyNodeCostPerRAMGiBHr(nodeMap map[nodeKey]*Node, resNodeCostPerRAMGiBHr []*prom.QueryResult) {
	for _, res := range resNodeCostPerRAMGiBHr {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node RAM cost query result missing field: %s", err)
			continue
		}

		instanceType, err := res.GetString("instance_type")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node RAM cost query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &Node{
				Name:     node,
				NodeType: instanceType,
			}
		}

		nodeMap[key].CostPerRAMGiBHr = res.Values[0].Value
	}
}

func applyNodeCostPerGPUHr(nodeMap map[nodeKey]*Node, resNodeCostPerGPUHr []*prom.QueryResult) {
	for _, res := range resNodeCostPerGPUHr {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node GPU cost query result missing field: %s", err)
			continue
		}

		instanceType, err := res.GetString("instance_type")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node GPU cost query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &Node{
				Name:     node,
				NodeType: instanceType,
			}
		}

		nodeMap[key].CostPerGPUHr = res.Values[0].Value
	}
}

func applyNodeSpot(nodeMap map[nodeKey]*Node, resNodeIsSpot []*prom.QueryResult) {
	for _, res := range resNodeIsSpot {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Node spot query result missing field: %s", err)
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			log.Warningf("CostModel.ComputeAllocation: Node spot  query result for missing node: %s", key)
			continue
		}

		nodeMap[key].Preemptible = res.Values[0].Value > 0
	}
}

func applyNodeDiscount(nodeMap map[nodeKey]*Node, cm *CostModel) {
	if cm == nil {
		return
	}

	c, err := cm.Provider.GetConfig()
	if err != nil {
		log.Errorf("CostModel.ComputeAllocation: applyNodeDiscount: %s", err)
		return
	}

	discount, err := ParsePercentString(c.Discount)
	if err != nil {
		log.Errorf("CostModel.ComputeAllocation: applyNodeDiscount: %s", err)
		return
	}

	negotiatedDiscount, err := ParsePercentString(c.NegotiatedDiscount)
	if err != nil {
		log.Errorf("CostModel.ComputeAllocation: applyNodeDiscount: %s", err)
		return
	}

	for _, node := range nodeMap {
		// TODO niko/cdmr take RI into account?
		node.Discount = cm.Provider.CombinedDiscountForNode(node.NodeType, node.Preemptible, discount, negotiatedDiscount)
		node.CostPerCPUHr *= (1.0 - node.Discount)
		node.CostPerRAMGiBHr *= (1.0 - node.Discount)
	}
}

type containerKey struct {
	Cluster   string
	Namespace string
	Pod       string
	Container string
}

func (k containerKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.Cluster, k.Namespace, k.Pod, k.Container)
}

func newContainerKey(cluster, namespace, pod, container string) containerKey {
	return containerKey{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
		Container: container,
	}
}

func resultContainerKey(res *prom.QueryResult, clusterLabel, namespaceLabel, podLabel, containerLabel string) (containerKey, error) {
	key := containerKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	pod, err := res.GetString(podLabel)
	if err != nil {
		return key, err
	}
	key.Pod = pod

	container, err := res.GetString(containerLabel)
	if err != nil {
		return key, err
	}
	key.Container = container

	return key, nil
}

type podKey struct {
	Cluster   string
	Namespace string
	Pod       string
}

func (k podKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.Pod)
}

func newPodKey(cluster, namespace, pod string) podKey {
	return podKey{
		Cluster:   cluster,
		Namespace: namespace,
		Pod:       pod,
	}
}

func resultPodKey(res *prom.QueryResult, clusterLabel, namespaceLabel, podLabel string) (podKey, error) {
	key := podKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	pod, err := res.GetString(podLabel)
	if err != nil {
		return key, err
	}
	key.Pod = pod

	return key, nil
}

type nodeKey struct {
	Cluster string
	Node    string
}

func (k nodeKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.Node)
}

func newNodeKey(cluster, node string) nodeKey {
	return nodeKey{
		Cluster: cluster,
		Node:    node,
	}
}

func resultNodeKey(res *prom.QueryResult, clusterLabel, nodeLabel string) (nodeKey, error) {
	key := nodeKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	node, err := res.GetString(nodeLabel)
	if err != nil {
		return key, err
	}
	key.Node = node

	return key, nil
}

type pvcKey struct {
	Cluster               string
	Namespace             string
	PersistentVolumeClaim string
}

func (k pvcKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Cluster, k.Namespace, k.PersistentVolumeClaim)
}

func newPVCKey(cluster, namespace, persistentVolumeClaim string) pvcKey {
	return pvcKey{
		Cluster:               cluster,
		Namespace:             namespace,
		PersistentVolumeClaim: persistentVolumeClaim,
	}
}

func resultPVCKey(res *prom.QueryResult, clusterLabel, namespaceLabel, pvcLabel string) (pvcKey, error) {
	key := pvcKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	namespace, err := res.GetString(namespaceLabel)
	if err != nil {
		return key, err
	}
	key.Namespace = namespace

	pvc, err := res.GetString(pvcLabel)
	if err != nil {
		return key, err
	}
	key.PersistentVolumeClaim = pvc

	return key, nil
}

type pvKey struct {
	Cluster          string
	PersistentVolume string
}

func (k pvKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.PersistentVolume)
}

func newPVKey(cluster, persistentVolume string) pvKey {
	return pvKey{
		Cluster:          cluster,
		PersistentVolume: persistentVolume,
	}
}

func resultPVKey(res *prom.QueryResult, clusterLabel, persistentVolumeLabel string) (pvKey, error) {
	key := pvKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	persistentVolume, err := res.GetString(persistentVolumeLabel)
	if err != nil {
		return key, err
	}
	key.PersistentVolume = persistentVolume

	return key, nil
}
