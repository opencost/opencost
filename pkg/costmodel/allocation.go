package costmodel

import (
	"fmt"
	"time"

	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	queryFmtMinutes               = `avg(kube_pod_container_status_running{}) by (container, pod, namespace, cluster_id)[%s:%s]%s`
	queryFmtRAMBytesAllocated     = `avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`
	queryFmtRAMRequests           = `avg(avg_over_time(kube_pod_container_resource_requests_memory_bytes{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`
	queryFmtRAMUsage              = `avg(avg_over_time(container_memory_working_set_bytes{container_name!="", container_name!="POD", instance!=""}[%s]%s)) by (container_name, pod_name, namespace, instance, cluster_id)`
	queryFmtCPUCoresAllocated     = `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`
	queryFmtCPURequests           = `avg(avg_over_time(kube_pod_container_resource_requests_cpu_cores{container!="", container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`
	queryFmtCPUUsage              = `avg(rate(container_cpu_usage_seconds_total{container_name!="", container_name!="POD", instance!=""}[%s]%s)) by (container_name, pod_name, namespace, instance, cluster_id)`
	queryFmtGPUsRequested         = `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[%s]%s)) by (container, pod, namespace, node, cluster_id)`
	queryFmtNodeCostPerCPUHr      = `avg(avg_over_time(node_cpu_hourly_cost[%s]%s)) by (node, cluster_id, instance_type)`
	queryFmtNodeCostPerRAMGiBHr   = `avg(avg_over_time(node_ram_hourly_cost[%s]%s)) by (node, cluster_id, instance_type)`
	queryFmtNodeCostPerGPUHr      = `avg(avg_over_time(node_gpu_hourly_cost[%s]%s)) by (node, cluster_id, instance_type)`
	queryFmtNodeIsSpot            = `avg_over_time(kubecost_node_is_spot[%s]%s)`
	queryFmtPVCInfo               = `avg(kube_persistentvolumeclaim_info{volumename != ""}) by (persistentvolumeclaim, storageclass, volumename, namespace, cluster_id)[%s:%s]%s`
	queryFmtPVBytes               = `avg(avg_over_time(kube_persistentvolume_capacity_bytes[%s]%s)) by (persistentvolume, cluster_id)`
	queryFmtPodPVCAllocation      = `avg(avg_over_time(pod_pvc_allocation[%s]%s)) by (persistentvolume, persistentvolumeclaim, pod, namespace, cluster_id)`
	queryFmtPVCBytesRequested     = `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{}[%s]%s)) by (persistentvolumeclaim, namespace, cluster_id)`
	queryFmtPVCostPerGiBHour      = `avg(avg_over_time(pv_hourly_cost[%s]%s)) by (volumename, cluster_id)`
	queryFmtNetZoneGiB            = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="true"}[%s]%s)) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024`
	queryFmtNetZoneCostPerGiB     = `avg(avg_over_time(kubecost_network_zone_egress_cost{}[%s]%s)) by (cluster_id)`
	queryFmtNetRegionGiB          = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="false"}[%s]%s)) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024`
	queryFmtNetRegionCostPerGiB   = `avg(avg_over_time(kubecost_network_region_egress_cost{}[%s]%s)) by (cluster_id)`
	queryFmtNetInternetGiB        = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true"}[%s]%s)) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024`
	queryFmtNetInternetCostPerGiB = `avg(avg_over_time(kubecost_network_internet_egress_cost{}[%s]%s)) by (cluster_id)`
	queryFmtNamespaceLabels       = `avg_over_time(kube_namespace_labels[%s]%s)`
	queryFmtNamespaceAnnotations  = `avg_over_time(kube_namespace_annotations[%s]%s)`
	queryFmtPodLabels             = `avg_over_time(kube_pod_labels[%s]%s)`
	queryFmtPodAnnotations        = `avg_over_time(kube_pod_annotations[%s]%s)`
	queryFmtServiceLabels         = `avg_over_time(service_selector_labels[%s]%s)`
	queryFmtDeploymentLabels      = `avg_over_time(deployment_match_labels[%s]%s)`
	queryFmtStatefulSetLabels     = `avg_over_time(statefulSet_match_labels[%s]%s)`
	queryFmtDaemonSetLabels       = `sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet"}[%s]%s)) by (pod, owner_name, namespace, cluster_id)`
	queryFmtJobLabels             = `sum(avg_over_time(kube_pod_owner{owner_kind="Job"}[%s]%s)) by (pod, owner_name, namespace ,cluster_id)`
)

// TODO niko/computeallocation idle minutes = 1?

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

	// TODO niko/computeallocation dynamic resolution? add to ComputeAllocation() in allocation.Source?
	resStr := "1m"
	// resPerHr := 60

	startQuerying := time.Now()

	ctx := prom.NewContext(cm.PrometheusClient)

	// TODO niko/computeallocation retries? (That should probably go into the Store.)

	// TODO niko/computeallocation split into required and optional queries?

	queryMinutes := fmt.Sprintf(queryFmtMinutes, durStr, resStr, offStr)
	resChMinutes := ctx.Query(queryMinutes)

	queryRAMBytesAllocated := fmt.Sprintf(queryFmtRAMBytesAllocated, durStr, offStr)
	resChRAMBytesAllocated := ctx.Query(queryRAMBytesAllocated)

	queryRAMRequests := fmt.Sprintf(queryFmtRAMRequests, durStr, offStr)
	resChRAMRequests := ctx.Query(queryRAMRequests)

	queryRAMUsage := fmt.Sprintf(queryFmtRAMUsage, durStr, offStr)
	resChRAMUsage := ctx.Query(queryRAMUsage)

	queryCPUCoresAllocated := fmt.Sprintf(queryFmtCPUCoresAllocated, durStr, offStr)
	resChCPUCoresAllocated := ctx.Query(queryCPUCoresAllocated)

	queryCPURequests := fmt.Sprintf(queryFmtCPURequests, durStr, offStr)
	resChCPURequests := ctx.Query(queryCPURequests)

	queryCPUUsage := fmt.Sprintf(queryFmtCPUUsage, durStr, offStr)
	resChCPUUsage := ctx.Query(queryCPUUsage)

	queryGPUsRequested := fmt.Sprintf(queryFmtGPUsRequested, durStr, offStr)
	resChGPUsRequested := ctx.Query(queryGPUsRequested)

	queryNodeCostPerCPUHr := fmt.Sprintf(queryFmtNodeCostPerCPUHr, durStr, offStr)
	resChNodeCostPerCPUHr := ctx.Query(queryNodeCostPerCPUHr)

	queryNodeCostPerRAMGiBHr := fmt.Sprintf(queryFmtNodeCostPerRAMGiBHr, durStr, offStr)
	resChNodeCostPerRAMGiBHr := ctx.Query(queryNodeCostPerRAMGiBHr)

	queryNodeCostPerGPUHr := fmt.Sprintf(queryFmtNodeCostPerGPUHr, durStr, offStr)
	resChNodeCostPerGPUHr := ctx.Query(queryNodeCostPerGPUHr)

	queryNodeIsSpot := fmt.Sprintf(queryFmtNodeIsSpot, durStr, offStr)
	resChNodeIsSpot := ctx.Query(queryNodeIsSpot)

	queryPVCInfo := fmt.Sprintf(queryFmtPVCInfo, durStr, resStr, offStr)
	resChPVCInfo := ctx.Query(queryPVCInfo)

	queryPVBytes := fmt.Sprintf(queryFmtPVBytes, durStr, offStr)
	resChPVBytes := ctx.Query(queryPVBytes)

	queryPodPVCAllocation := fmt.Sprintf(queryFmtPodPVCAllocation, durStr, offStr)
	resChPodPVCAllocation := ctx.Query(queryPodPVCAllocation)

	queryPVCBytesRequested := fmt.Sprintf(queryFmtPVCBytesRequested, durStr, offStr)
	resChPVCBytesRequested := ctx.Query(queryPVCBytesRequested)

	queryPVCostPerGiBHour := fmt.Sprintf(queryFmtPVCostPerGiBHour, durStr, offStr)
	resChPVCostPerGiBHour := ctx.Query(queryPVCostPerGiBHour)

	queryNetZoneGiB := fmt.Sprintf(queryFmtNetZoneGiB, durStr, offStr)
	resChNetZoneGiB := ctx.Query(queryNetZoneGiB)

	queryNetZoneCostPerGiB := fmt.Sprintf(queryFmtNetZoneCostPerGiB, durStr, offStr)
	resChNetZoneCostPerGiB := ctx.Query(queryNetZoneCostPerGiB)

	queryNetRegionGiB := fmt.Sprintf(queryFmtNetRegionGiB, durStr, offStr)
	resChNetRegionGiB := ctx.Query(queryNetRegionGiB)

	queryNetRegionCostPerGiB := fmt.Sprintf(queryFmtNetRegionCostPerGiB, durStr, offStr)
	resChNetRegionCostPerGiB := ctx.Query(queryNetRegionCostPerGiB)

	queryNetInternetGiB := fmt.Sprintf(queryFmtNetInternetGiB, durStr, offStr)
	resChNetInternetGiB := ctx.Query(queryNetInternetGiB)

	queryNetInternetCostPerGiB := fmt.Sprintf(queryFmtNetInternetCostPerGiB, durStr, offStr)
	resChNetInternetCostPerGiB := ctx.Query(queryNetInternetCostPerGiB)

	queryNamespaceLabels := fmt.Sprintf(queryFmtNamespaceLabels, durStr, offStr)
	resChNamespaceLabels := ctx.Query(queryNamespaceLabels)

	queryNamespaceAnnotations := fmt.Sprintf(queryFmtNamespaceAnnotations, durStr, offStr)
	resChNamespaceAnnotations := ctx.Query(queryNamespaceAnnotations)

	queryPodLabels := fmt.Sprintf(queryFmtPodLabels, durStr, offStr)
	resChPodLabels := ctx.Query(queryPodLabels)

	queryPodAnnotations := fmt.Sprintf(queryFmtPodAnnotations, durStr, offStr)
	resChPodAnnotations := ctx.Query(queryPodAnnotations)

	queryServiceLabels := fmt.Sprintf(queryFmtServiceLabels, durStr, offStr)
	resChServiceLabels := ctx.Query(queryServiceLabels)

	queryDeploymentLabels := fmt.Sprintf(queryFmtDeploymentLabels, durStr, offStr)
	resChDeploymentLabels := ctx.Query(queryDeploymentLabels)

	queryStatefulSetLabels := fmt.Sprintf(queryFmtStatefulSetLabels, durStr, offStr)
	resChStatefulSetLabels := ctx.Query(queryStatefulSetLabels)

	queryDaemonSetLabels := fmt.Sprintf(queryFmtDaemonSetLabels, durStr, offStr)
	resChDaemonSetLabels := ctx.Query(queryDaemonSetLabels)

	queryJobLabels := fmt.Sprintf(queryFmtJobLabels, durStr, offStr)
	resChJobLabels := ctx.Query(queryJobLabels)

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

	resPVBytes, _ := resChPVBytes.Await()
	resPVCostPerGiBHour, _ := resChPVCostPerGiBHour.Await()

	resPVCInfo, _ := resChPVCInfo.Await()
	resPVCBytesRequested, _ := resChPVCBytesRequested.Await()
	resPodPVCAllocation, _ := resChPodPVCAllocation.Await()

	resNetZoneGiB, _ := resChNetZoneGiB.Await()
	resNetZoneCostPerGiB, _ := resChNetZoneCostPerGiB.Await()
	resNetRegionGiB, _ := resChNetRegionGiB.Await()
	resNetRegionCostPerGiB, _ := resChNetRegionCostPerGiB.Await()
	resNetInternetGiB, _ := resChNetInternetGiB.Await()
	resNetInternetCostPerGiB, _ := resChNetInternetCostPerGiB.Await()

	resNamespaceLabels, _ := resChNamespaceLabels.Await()
	resNamespaceAnnotations, _ := resChNamespaceAnnotations.Await()
	resPodLabels, _ := resChPodLabels.Await()
	resPodAnnotations, _ := resChPodAnnotations.Await()
	resServiceLabels, _ := resChServiceLabels.Await()
	resDeploymentLabels, _ := resChDeploymentLabels.Await()
	resStatefulSetLabels, _ := resChStatefulSetLabels.Await()
	resDaemonSetLabels, _ := resChDaemonSetLabels.Await()
	resJobLabels, _ := resChJobLabels.Await()

	// ---------------------------------------
	// TODO niko/computeallocation remove logs
	log.Infof("CostModel.ComputeAllocation: %s", queryMinutes)
	log.Infof("CostModel.ComputeAllocation: %s", queryRAMBytesAllocated)
	log.Infof("CostModel.ComputeAllocation: %s", queryCPUCoresAllocated)
	log.Infof("CostModel.ComputeAllocation: %s", queryGPUsRequested)
	// ---------------------------------------

	log.Profile(startQuerying, "CostModel.ComputeAllocation: queries complete")
	defer log.Profile(time.Now(), "CostModel.ComputeAllocation: processing complete")

	// Build out a map of Allocations, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	allocationMap := map[containerKey]*kubecost.Allocation{}

	// Keep track of the allocations per pod, for the sake of splitting PVC and
	// Network allocation into per-Allocation from per-Pod.
	podAllocation := map[podKey][]*kubecost.Allocation{}

	// clusterStarts and clusterEnds record the earliest start and latest end
	// times, respectively, on a cluster-basis. These are used for unmounted
	// PVs and other "virtual" Allocations so that minutes are maximally
	// accurate during start-up or spin-down of a cluster
	clusterStart := map[string]time.Time{}
	clusterEnd := map[string]time.Time{}

	buildAllocationMap(window, allocationMap, podAllocation, clusterStart, clusterEnd, resMinutes)
	applyCPUCoresAllocated(allocationMap, resCPUCoresAllocated)
	applyCPUCoresRequested(allocationMap, resCPURequests)
	applyCPUCoresUsed(allocationMap, resCPUUsage)
	applyRAMBytesAllocated(allocationMap, resRAMBytesAllocated)
	applyRAMBytesRequested(allocationMap, resRAMRequests)
	applyRAMBytesUsed(allocationMap, resRAMUsage)
	applyGPUsRequested(allocationMap, resGPUsRequested)
	applyNetworkAllocation(allocationMap, podAllocation, resNetZoneGiB, resNetZoneCostPerGiB)
	applyNetworkAllocation(allocationMap, podAllocation, resNetRegionGiB, resNetRegionCostPerGiB)
	applyNetworkAllocation(allocationMap, podAllocation, resNetInternetGiB, resNetInternetCostPerGiB)

	// TODO niko/computeallocation pruneDuplicateData? (see costmodel.go)

	namespaceLabels := resToNamespaceLabels(resNamespaceLabels)
	podLabels := resToPodLabels(resPodLabels)
	namespaceAnnotations := resToNamespaceAnnotations(resNamespaceAnnotations)
	podAnnotations := resToPodAnnotations(resPodAnnotations)
	applyLabels(allocationMap, namespaceLabels, podLabels)
	applyAnnotations(allocationMap, namespaceAnnotations, podAnnotations)

	serviceLabels := getServiceLabels(resServiceLabels)
	applyServicesToPods(allocationMap, podLabels, serviceLabels)

	podDeploymentMap := labelsToPodControllerMap(podLabels, resToDeploymentLabels(resDeploymentLabels))
	podStatefulSetMap := labelsToPodControllerMap(podLabels, resToStatefulSetLabels(resStatefulSetLabels))
	podDaemonSetMap := resToPodDaemonSetMap(resDaemonSetLabels)
	podJobMap := resToPodJobMap(resJobLabels)
	applyControllersToPods(allocationMap, podDeploymentMap)
	applyControllersToPods(allocationMap, podStatefulSetMap)
	applyControllersToPods(allocationMap, podDaemonSetMap)
	applyControllersToPods(allocationMap, podJobMap)

	// TODO breakdown network costs?

	// Build out a map of Nodes with resource costs, discounts, and node types
	// for converting resource allocation data to cumulative costs.
	nodeMap := map[nodeKey]*Node{}

	applyNodeCostPerCPUHr(nodeMap, resNodeCostPerCPUHr)
	applyNodeCostPerRAMGiBHr(nodeMap, resNodeCostPerRAMGiBHr)
	applyNodeCostPerGPUHr(nodeMap, resNodeCostPerGPUHr)
	applyNodeSpot(nodeMap, resNodeIsSpot)
	applyNodeDiscount(nodeMap, cm)

	// Build out the map of all PVs with class, size and cost-per-hour.
	// Note: this does not record time running, which we may want to
	// include later for increased PV precision. (As long as the PV has
	// a PVC, we get time running there, so this is only inaccurate
	// for short-lived, unmounted PVs.)
	pvMap := map[pvKey]*PV{}
	buildPVMap(pvMap, resPVCostPerGiBHour)
	applyPVBytes(pvMap, resPVBytes)

	// Build out the map of all PVCs with time running, bytes requested,
	// and connect to the correct PV from pvMap. (If no PV exists, that
	// is noted, but does not result in any allocation/cost.)
	pvcMap := map[pvcKey]*PVC{}
	buildPVCMap(window, pvcMap, pvMap, resPVCInfo)
	applyPVCBytesRequested(pvcMap, resPVCBytesRequested)

	// Build out the relationships of pods to their PVCs. This step
	// populates the PVC.Count field so that PVC allocation can be
	// split appropriately among each pod's container allocation.
	podPVCMap := map[podKey][]*PVC{}
	buildPodPVCMap(podPVCMap, pvMap, pvcMap, podAllocation, resPodPVCAllocation)

	// Identify unmounted PVs (PVs without PVCs) and add one Allocation per
	// cluster representing each cluster's unmounted PVs (if necessary).
	applyUnmountedPVs(window, allocationMap, pvMap, pvcMap)

	for _, alloc := range allocationMap {
		cluster, _ := alloc.Properties.GetCluster()
		node, _ := alloc.Properties.GetNode()
		namespace, _ := alloc.Properties.GetNamespace()
		pod, _ := alloc.Properties.GetPod()
		container, _ := alloc.Properties.GetContainer()

		podKey := newPodKey(cluster, namespace, pod)
		nodeKey := newNodeKey(cluster, node)

		if n, ok := nodeMap[nodeKey]; !ok {
			if pod != "unmounted-pvs" {
				log.Warningf("CostModel.ComputeAllocation: failed to find node %s for %s", nodeKey, alloc.Name)
			}
		} else {
			alloc.CPUCost = alloc.CPUCoreHours * n.CostPerCPUHr
			alloc.RAMCost = (alloc.RAMByteHours / 1024 / 1024 / 1024) * n.CostPerRAMGiBHr
			alloc.GPUCost = alloc.GPUHours * n.CostPerGPUHr
		}

		if pvcs, ok := podPVCMap[podKey]; ok {
			for _, pvc := range pvcs {
				// Determine the (start, end) of the relationship between the
				// given PVC and the associated Allocation so that a precise
				// number of hours can be used to compute cumulative cost.
				s, e := alloc.Start, alloc.End
				if pvc.Start.After(alloc.Start) {
					s = pvc.Start
				}
				if pvc.End.Before(alloc.End) {
					e = pvc.End
				}
				minutes := e.Sub(s).Minutes()
				hrs := minutes / 60.0

				count := float64(pvc.Count)
				if pvc.Count < 1 {
					count = 1
				}

				gib := pvc.Bytes / 1024 / 1024 / 1024
				cost := pvc.Volume.CostPerGiBHour * gib * hrs

				alloc.PVByteHours += pvc.Bytes * hrs / count
				alloc.PVCost += cost / count

				// TODO niko/computeallocation remove log
				log.Warningf("CostModel.ComputeAllocation: PVC %s: count=%d, %f GiB, %f hrs, $%f ($%f after split)", pvc.Name, pvc.Count, gib, hrs, cost, cost/count)
			}
		}

		alloc.TotalCost = 0.0
		alloc.TotalCost += alloc.CPUCost
		alloc.TotalCost += alloc.RAMCost
		alloc.TotalCost += alloc.GPUCost
		alloc.TotalCost += alloc.PVCost
		alloc.TotalCost += alloc.NetworkCost
		alloc.TotalCost += alloc.SharedCost
		alloc.TotalCost += alloc.ExternalCost

		if alloc.RAMBytesRequestAverage > 0 {
			alloc.RAMEfficiency = alloc.RAMBytesUsageAverage / alloc.RAMBytesRequestAverage
		}

		if alloc.CPUCoreRequestAverage > 0 {
			alloc.CPUEfficiency = alloc.CPUCoreUsageAverage / alloc.CPUCoreRequestAverage
		}

		if alloc.CPUCost+alloc.RAMCost > 0 {
			ramCostEff := alloc.RAMEfficiency * alloc.RAMCost
			cpuCostEff := alloc.CPUEfficiency * alloc.CPUCost
			alloc.TotalEfficiency = (ramCostEff + cpuCostEff) / (alloc.CPUCost + alloc.RAMCost)
		}

		// Make sure that the name is correct (node may not be present at this
		// point due to it missing from queryMinutes) then insert.
		alloc.Name = fmt.Sprintf("%s/%s/%s/%s/%s", cluster, node, namespace, pod, container)
		allocSet.Set(alloc)
	}

	return allocSet, nil
}

func buildAllocationMap(window kubecost.Window, allocationMap map[containerKey]*kubecost.Allocation, podAllocation map[podKey][]*kubecost.Allocation, clusterStart, clusterEnd map[string]time.Time, resMinutes []*prom.QueryResult) {
	for _, res := range resMinutes {
		if len(res.Values) == 0 {
			log.Warningf("CostModel.ComputeAllocation: empty minutes result")
			continue
		}

		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		labels, err := res.GetStrings("namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: minutes query result missing field: %s", err)
			continue
		}

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
			continue
		}
		allocStart = allocStart.Add(-time.Minute)

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
		alloc.Properties.SetCluster(cluster)

		allocationMap[containerKey] = alloc

		if _, ok := podAllocation[podKey]; !ok {
			podAllocation[podKey] = []*kubecost.Allocation{}
		}
		podAllocation[podKey] = append(podAllocation[podKey], alloc)
	}
}

func applyCPUCoresAllocated(allocationMap map[containerKey]*kubecost.Allocation, resCPUCoresAllocated []*prom.QueryResult) {
	for _, res := range resCPUCoresAllocated {
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

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing 'node': %s", key)
			continue
		}
		allocationMap[key].Properties.SetNode(node)
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
			continue
		}

		allocationMap[key].CPUCoreRequestAverage = res.Values[0].Value

		// If CPU allocation is less than requests, set CPUCoreHours to
		// request level.
		if allocationMap[key].CPUCores() < res.Values[0].Value {
			allocationMap[key].CPUCoreHours = res.Values[0].Value * (allocationMap[key].Minutes() / 60.0)
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU request query result missing 'node': %s", key)
			continue
		}
		allocationMap[key].Properties.SetNode(node)
	}
}

func applyCPUCoresUsed(allocationMap map[containerKey]*kubecost.Allocation, resCPUCoresUsed []*prom.QueryResult) {
	for _, res := range resCPUCoresUsed {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod_name", "container_name")
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

func applyRAMBytesAllocated(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesAllocated []*prom.QueryResult) {
	for _, res := range resRAMBytesAllocated {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM allocation query result missing field: %s", err)
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

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM allocation query result missing 'node': %s", key)
			continue
		}
		allocationMap[key].Properties.SetNode(node)
	}
}

func applyRAMBytesRequested(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesRequested []*prom.QueryResult) {
	for _, res := range resRAMBytesRequested {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM request query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			continue
		}

		allocationMap[key].RAMBytesRequestAverage = res.Values[0].Value

		// If RAM allocation is less than requests, set RAMByteHours to
		// request level.
		if allocationMap[key].RAMBytes() < res.Values[0].Value {
			allocationMap[key].RAMByteHours = res.Values[0].Value * (allocationMap[key].Minutes() / 60.0)
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM request query result missing 'node': %s", key)
			continue
		}
		allocationMap[key].Properties.SetNode(node)
	}
}

func applyRAMBytesUsed(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesUsed []*prom.QueryResult) {
	for _, res := range resRAMBytesUsed {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod_name", "container_name")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM usage query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified RAM usage query result: %s", key)
			continue
		}

		allocationMap[key].RAMBytesUsageAverage = res.Values[0].Value
	}
}

func applyGPUsRequested(allocationMap map[containerKey]*kubecost.Allocation, resGPUsRequested []*prom.QueryResult) {
	for _, res := range resGPUsRequested {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: GPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified GPU allocation query result: %s", key)
			continue
		}

		// TODO niko/computeallocation remove log
		log.Infof("CostModel.ComputeAllocation: GPU results: %s=%f", key, res.Values[0].Value)

		hrs := allocationMap[key].Minutes() / 60.0
		allocationMap[key].GPUHours = res.Values[0].Value * hrs
	}
}

func applyNetworkAllocation(allocationMap map[containerKey]*kubecost.Allocation, podAllocation map[podKey][]*kubecost.Allocation, resNetworkGiB []*prom.QueryResult, resNetworkCostPerGiB []*prom.QueryResult) {
	costPerGiBByCluster := map[string]float64{}

	for _, res := range resNetworkCostPerGiB {
		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		costPerGiBByCluster[cluster] = res.Values[0].Value
	}

	for _, res := range resNetworkGiB {
		podKey, err := resultPodKey(res, "cluster_id", "namespace", "pod_name")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: Network allocation query result missing field: %s", err)
			continue
		}

		allocs, ok := podAllocation[podKey]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: Network allocation query result for unidentified pod allocations: %s", podKey)
			continue
		}

		for _, alloc := range allocs {
			gib := res.Values[0].Value
			costPerGiB := costPerGiBByCluster[podKey.Cluster]
			alloc.NetworkCost = gib * costPerGiB
		}
	}
}

func resToNamespaceLabels(resNamespaceLabels []*prom.QueryResult) map[string]map[string]string {
	namespaceLabels := map[string]map[string]string{}

	for _, res := range resNamespaceLabels {
		namespace, err := res.GetString("namespace")
		if err != nil {
			continue
		}

		if _, ok := namespaceLabels[namespace]; !ok {
			namespaceLabels[namespace] = map[string]string{}
		}

		for k, l := range res.GetLabels() {
			namespaceLabels[namespace][k] = l
		}
	}

	return namespaceLabels
}

func resToPodLabels(resPodLabels []*prom.QueryResult) map[podKey]map[string]string {
	podLabels := map[podKey]map[string]string{}

	for _, res := range resPodLabels {
		podKey, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			continue
		}

		if _, ok := podLabels[podKey]; !ok {
			podLabels[podKey] = map[string]string{}
		}

		for k, l := range res.GetLabels() {
			podLabels[podKey][k] = l
		}
	}

	return podLabels
}

func resToNamespaceAnnotations(resNamespaceAnnotations []*prom.QueryResult) map[string]map[string]string {
	namespaceAnnotations := map[string]map[string]string{}

	for _, res := range resNamespaceAnnotations {
		namespace, err := res.GetString("namespace")
		if err != nil {
			continue
		}

		if _, ok := namespaceAnnotations[namespace]; !ok {
			namespaceAnnotations[namespace] = map[string]string{}
		}

		for k, l := range res.GetAnnotations() {
			namespaceAnnotations[namespace][k] = l
		}
	}

	return namespaceAnnotations
}

func resToPodAnnotations(resPodAnnotations []*prom.QueryResult) map[podKey]map[string]string {
	podAnnotations := map[podKey]map[string]string{}

	for _, res := range resPodAnnotations {
		podKey, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			continue
		}

		if _, ok := podAnnotations[podKey]; !ok {
			podAnnotations[podKey] = map[string]string{}
		}

		for k, l := range res.GetAnnotations() {
			podAnnotations[podKey][k] = l
		}
	}

	return podAnnotations
}

func applyLabels(allocationMap map[containerKey]*kubecost.Allocation, namespaceLabels map[string]map[string]string, podLabels map[podKey]map[string]string) {
	for key, alloc := range allocationMap {
		allocLabels, err := alloc.Properties.GetLabels()
		if err != nil {
			allocLabels = map[string]string{}
		}

		// Apply namespace labels first, then pod labels so that pod labels
		// overwrite namespace labels.
		if labels, ok := namespaceLabels[key.Namespace]; ok {
			for k, v := range labels {
				allocLabels[k] = v
			}
		}
		podKey := newPodKey(key.Cluster, key.Namespace, key.Pod)
		if labels, ok := podLabels[podKey]; ok {
			for k, v := range labels {
				allocLabels[k] = v
			}
		}

		alloc.Properties.SetLabels(allocLabels)
	}
}

func applyAnnotations(allocationMap map[containerKey]*kubecost.Allocation, namespaceAnnotations map[string]map[string]string, podAnnotations map[podKey]map[string]string) {
	for key, alloc := range allocationMap {
		allocAnnotations, err := alloc.Properties.GetAnnotations()
		if err != nil {
			allocAnnotations = map[string]string{}
		}

		// Apply namespace annotations first, then pod annotations so that
		// pod labels overwrite namespace labels.
		if labels, ok := namespaceAnnotations[key.Namespace]; ok {
			for k, v := range labels {
				allocAnnotations[k] = v
			}
		}
		podKey := newPodKey(key.Cluster, key.Namespace, key.Pod)
		if labels, ok := podAnnotations[podKey]; ok {
			for k, v := range labels {
				allocAnnotations[k] = v
			}
		}

		alloc.Properties.SetAnnotations(allocAnnotations)
	}
}

func getServiceLabels(resServiceLabels []*prom.QueryResult) map[serviceKey]map[string]string {
	serviceLabels := map[serviceKey]map[string]string{}

	for _, res := range resServiceLabels {
		serviceKey, err := resultServiceKey(res, "cluster_id", "namespace", "service")
		if err != nil {
			continue
		}

		if _, ok := serviceLabels[serviceKey]; !ok {
			serviceLabels[serviceKey] = map[string]string{}
		}

		for k, l := range res.GetLabels() {
			serviceLabels[serviceKey][k] = l
		}
	}

	return serviceLabels
}

func resToDeploymentLabels(resDeploymentLabels []*prom.QueryResult) map[controllerKey]map[string]string {
	deploymentLabels := map[controllerKey]map[string]string{}

	for _, res := range resDeploymentLabels {
		controllerKey, err := resultDeploymentKey(res, "cluster_id", "namespace", "deployment")
		if err != nil {
			continue
		}

		if _, ok := deploymentLabels[controllerKey]; !ok {
			deploymentLabels[controllerKey] = map[string]string{}
		}

		for k, l := range res.GetLabels() {
			deploymentLabels[controllerKey][k] = l
		}
	}

	return deploymentLabels
}

func resToStatefulSetLabels(resStatefulSetLabels []*prom.QueryResult) map[controllerKey]map[string]string {
	statefulSetLabels := map[controllerKey]map[string]string{}

	for _, res := range resStatefulSetLabels {
		controllerKey, err := resultStatefulSetKey(res, "cluster_id", "namespace", "statefulSet")
		if err != nil {
			continue
		}

		if _, ok := statefulSetLabels[controllerKey]; !ok {
			statefulSetLabels[controllerKey] = map[string]string{}
		}

		for k, l := range res.GetLabels() {
			statefulSetLabels[controllerKey][k] = l
		}
	}

	return statefulSetLabels
}

func labelsToPodControllerMap(podLabels map[podKey]map[string]string, controllerLabels map[controllerKey]map[string]string) map[podKey]controllerKey {
	podControllerMap := map[podKey]controllerKey{}

	// For each controller, turn the labels into a selector and attempt to
	// match it with each set of pod labels. A match indicates that the pod
	// belongs to the controller.
	for cKey, cLabels := range controllerLabels {
		selector := labels.Set(cLabels).AsSelectorPreValidated()

		for pKey, pLabels := range podLabels {
			// If the pod is in a different cluster or namespace, there is
			// no need to compare the labels.
			if cKey.Cluster != pKey.Cluster || cKey.Namespace != pKey.Namespace {
				continue
			}

			podLabelSet := labels.Set(pLabels)
			if selector.Matches(podLabelSet) {
				if _, ok := podControllerMap[pKey]; ok {
					log.Warningf("CostModel.ComputeAllocation: PodControllerMap match already exists: %s matches %s and %s", pKey, podControllerMap[pKey], cKey)
				}
				podControllerMap[pKey] = cKey
			}
		}
	}

	return podControllerMap
}

func resToPodDaemonSetMap(resDaemonSetLabels []*prom.QueryResult) map[podKey]controllerKey {
	daemonSetLabels := map[podKey]controllerKey{}

	for _, res := range resDaemonSetLabels {
		controllerKey, err := resultDaemonSetKey(res, "cluster_id", "namespace", "owner_name")
		if err != nil {
			continue
		}

		pod, err := res.GetString("pod")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: DaemonSetLabel result without pod: %s", controllerKey)
		}

		podKey := newPodKey(controllerKey.Cluster, controllerKey.Namespace, pod)

		daemonSetLabels[podKey] = controllerKey
	}

	return daemonSetLabels
}

func resToPodJobMap(resJobLabels []*prom.QueryResult) map[podKey]controllerKey {
	jobLabels := map[podKey]controllerKey{}

	for _, res := range resJobLabels {
		controllerKey, err := resultJobKey(res, "cluster_id", "namespace", "owner_name")
		if err != nil {
			continue
		}

		pod, err := res.GetString("pod")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: JobLabel result without pod: %s", controllerKey)
		}

		podKey := newPodKey(controllerKey.Cluster, controllerKey.Namespace, pod)

		jobLabels[podKey] = controllerKey
	}

	return jobLabels
}

func applyServicesToPods(allocationMap map[containerKey]*kubecost.Allocation, podLabels map[podKey]map[string]string, serviceLabels map[serviceKey]map[string]string) {
	podServicesMap := map[podKey][]serviceKey{}

	// For each service, turn the labels into a selector and attempt to
	// match it with each set of pod labels. A match indicates that the pod
	// belongs to the service.
	for sKey, sLabels := range serviceLabels {
		selector := labels.Set(sLabels).AsSelectorPreValidated()

		for pKey, pLabels := range podLabels {
			// If the pod is in a different cluster or namespace, there is
			// no need to compare the labels.
			if sKey.Cluster != pKey.Cluster || sKey.Namespace != pKey.Namespace {
				continue
			}

			podLabelSet := labels.Set(pLabels)
			if selector.Matches(podLabelSet) {
				if _, ok := podServicesMap[pKey]; !ok {
					podServicesMap[pKey] = []serviceKey{}
				}
				podServicesMap[pKey] = append(podServicesMap[pKey], sKey)
			}
		}
	}

	// For each allocation, attempt to find and apply the list of services
	// associated with the allocation's pod.
	for key, alloc := range allocationMap {
		pKey := newPodKey(key.Cluster, key.Namespace, key.Pod)
		if sKeys, ok := podServicesMap[pKey]; ok {
			services := []string{}
			for _, sKey := range sKeys {
				services = append(services, sKey.Service)
			}
			alloc.Properties.SetServices(services)
		}
	}
}

func applyControllersToPods(allocationMap map[containerKey]*kubecost.Allocation, podControllerMap map[podKey]controllerKey) {
	for key, alloc := range allocationMap {
		podKey := newPodKey(key.Cluster, key.Namespace, key.Pod)
		if controllerKey, ok := podControllerMap[podKey]; ok {
			alloc.Properties.SetControllerKind(controllerKey.ControllerKind)
			alloc.Properties.SetController(controllerKey.Controller)
		}
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
		// TODO niko/computeallocation GKE Reserved Instances into account
		node.Discount = cm.Provider.CombinedDiscountForNode(node.NodeType, node.Preemptible, discount, negotiatedDiscount)
		node.CostPerCPUHr *= (1.0 - node.Discount)
		node.CostPerRAMGiBHr *= (1.0 - node.Discount)
	}
}

func buildPVMap(pvMap map[pvKey]*PV, resPVCostPerGiBHour []*prom.QueryResult) {
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
}

func applyPVBytes(pvMap map[pvKey]*PV, resPVBytes []*prom.QueryResult) {
	for _, res := range resPVBytes {
		key, err := resultPVKey(res, "cluster_id", "persistentvolume")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV bytes query result missing field: %s", err)
			continue
		}

		if _, ok := pvMap[key]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV bytes result for missing PV: %s", err)
			continue
		}

		pvMap[key].Bytes = res.Values[0].Value
	}
}

func buildPVCMap(window kubecost.Window, pvcMap map[pvcKey]*PVC, pvMap map[pvKey]*PV, resPVCInfo []*prom.QueryResult) {
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

		namespace := values["namespace"]
		name := values["persistentvolumeclaim"]
		volume := values["volumename"]
		storageClass := values["storageclass"]

		pvKey := newPVKey(cluster, volume)
		pvcKey := newPVCKey(cluster, namespace, name)

		// pvcStart and pvcEnd are the timestamps of the first and last minutes
		// the PVC was running, respectively. We subtract 1m from pvcStart
		// because this point will actually represent the end of the first
		// minute. We don't subtract from pvcEnd because it already represents
		// the end of the last minute.
		var pvcStart, pvcEnd time.Time
		for _, datum := range res.Values {
			t := time.Unix(int64(datum.Timestamp), 0)
			if pvcStart.IsZero() && datum.Value > 0 && window.Contains(t) {
				pvcStart = t
			}
			if datum.Value > 0 && window.Contains(t) {
				pvcEnd = t
			}
		}
		if pvcStart.IsZero() || pvcEnd.IsZero() {
			log.Warningf("CostModel.ComputeAllocation: PVC %s has no running time", pvcKey)
		}
		pvcStart = pvcStart.Add(-time.Minute)

		if _, ok := pvMap[pvKey]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV missing for PVC info query result: %s", pvKey)
			continue
		}

		pvMap[pvKey].StorageClass = storageClass

		if _, ok := pvcMap[pvcKey]; !ok {
			pvcMap[pvcKey] = &PVC{}
		}

		pvcMap[pvcKey].Name = name
		pvcMap[pvcKey].Namespace = namespace
		pvcMap[pvcKey].Volume = pvMap[pvKey]
		pvcMap[pvcKey].Start = pvcStart
		pvcMap[pvcKey].End = pvcEnd
	}
}

func applyPVCBytesRequested(pvcMap map[pvcKey]*PVC, resPVCBytesRequested []*prom.QueryResult) {
	for _, res := range resPVCBytesRequested {
		key, err := resultPVCKey(res, "cluster_id", "namespace", "persistentvolumeclaim")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PVC bytes requested query result missing field: %s", err)
			continue
		}

		if _, ok := pvcMap[key]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PVC bytes requested result for missing PVC: %s", key)
			continue
		}

		pvcMap[key].Bytes = res.Values[0].Value
	}
}

func buildPodPVCMap(podPVCMap map[podKey][]*PVC, pvMap map[pvKey]*PV, pvcMap map[pvcKey]*PVC, podAllocation map[podKey][]*kubecost.Allocation, resPodPVCAllocation []*prom.QueryResult) {
	for _, res := range resPodPVCAllocation {
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
		pvcKey := newPVCKey(cluster, namespace, name)

		if _, ok := pvMap[pvKey]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV missing for PVC allocation query result: %s", pvKey)
			continue
		}

		if _, ok := podPVCMap[podKey]; !ok {
			podPVCMap[podKey] = []*PVC{}
		}

		pvc, ok := pvcMap[pvcKey]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: PVC missing for PVC allocation query: %s", pvcKey)
			continue
		}

		pvc.Count = len(podAllocation[podKey])
		pvc.Mounted = true

		podPVCMap[podKey] = append(podPVCMap[podKey], pvc)
	}
}

func applyUnmountedPVs(window kubecost.Window, allocationMap map[containerKey]*kubecost.Allocation, pvMap map[pvKey]*PV, pvcMap map[pvcKey]*PVC) {
	unmountedPVBytes := map[string]float64{}
	unmountedPVCost := map[string]float64{}

	for _, pv := range pvMap {
		mounted := false
		for _, pvc := range pvcMap {
			if pvc.Volume == nil {
				continue
			}
			if pvc.Volume == pv {
				mounted = true
				break
			}
		}

		if !mounted {
			gib := pv.Bytes / 1024 / 1024 / 1024
			hrs := window.Minutes() / 60.0 // TODO niko/computeallocation PV hours, not window hours?
			cost := pv.CostPerGiBHour * gib * hrs
			unmountedPVCost[pv.Cluster] += cost
			unmountedPVBytes[pv.Cluster] += pv.Bytes
		}
	}

	for cluster, amount := range unmountedPVCost {
		container := "unmounted-pvs"
		pod := "unmounted-pvs"
		namespace := ""
		node := ""

		containerKey := newContainerKey(cluster, namespace, pod, container)
		allocationMap[containerKey] = &kubecost.Allocation{
			Name: fmt.Sprintf("%s/%s/%s/%s/%s", cluster, node, namespace, pod, container),
			Properties: kubecost.Properties{
				kubecost.ClusterProp:   cluster,
				kubecost.NodeProp:      node,
				kubecost.NamespaceProp: namespace,
				kubecost.PodProp:       pod,
				kubecost.ContainerProp: container,
			},
			Window:      window.Clone(),
			Start:       *window.Start(),
			End:         *window.End(),
			PVByteHours: unmountedPVBytes[cluster] * window.Minutes() / 60.0,
			PVCost:      amount,
			TotalCost:   amount,
		}
	}
}

func applyUnmountedPVCs(window kubecost.Window, allocationMap map[containerKey]*kubecost.Allocation, pvcMap map[pvcKey]*PVC) {
	unmountedPVCBytes := map[namespaceKey]float64{}
	unmountedPVCCost := map[namespaceKey]float64{}

	for _, pvc := range pvcMap {
		if !pvc.Mounted && pvc.Volume != nil {
			key := newNamespaceKey(pvc.Cluster, pvc.Namespace)

			gib := pvc.Volume.Bytes / 1024 / 1024 / 1024
			hrs := pvc.Minutes() / 60.0
			cost := pvc.Volume.CostPerGiBHour * gib * hrs
			unmountedPVCCost[key] += cost
			unmountedPVCBytes[key] += pvc.Volume.Bytes
		}
	}

	for key, amount := range unmountedPVCCost {
		container := "unmounted-pvs"
		pod := "unmounted-pvs"
		namespace := key.Namespace
		node := ""
		cluster := key.Cluster

		containerKey := newContainerKey(cluster, namespace, pod, container)
		allocationMap[containerKey] = &kubecost.Allocation{
			Name: fmt.Sprintf("%s/%s/%s/%s/%s", cluster, node, namespace, pod, container),
			Properties: kubecost.Properties{
				kubecost.ClusterProp:   cluster,
				kubecost.NodeProp:      node,
				kubecost.NamespaceProp: namespace,
				kubecost.PodProp:       pod,
				kubecost.ContainerProp: container,
			},
			Window:      window.Clone(),
			Start:       *window.Start(),
			End:         *window.End(),
			PVByteHours: unmountedPVCBytes[key] * window.Minutes() / 60.0,
			PVCost:      amount,
			TotalCost:   amount,
		}
	}
}

// PVC describes a PersistentVolumeClaim
// TODO move to pkg/kubecost?  [TODO:CLEANUP]
// TODO add PersistentVolumeClaims field to type Allocation?  [TODO:CLEANUP]
type PVC struct {
	Bytes     float64   `json:"bytes"`
	Count     int       `json:"count"`
	Name      string    `json:"name"`
	Cluster   string    `json:"cluster"`
	Namespace string    `json:"namespace"`
	Volume    *PV       `json:"persistentVolume"`
	Mounted   bool      `json:"mounted"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
}

// Cost computes the cumulative cost of the PVC
func (pvc *PVC) Cost() float64 {
	if pvc == nil || pvc.Volume == nil {
		return 0.0
	}

	gib := pvc.Bytes / 1024 / 1024 / 1024
	hrs := pvc.Minutes() / 60.0

	return pvc.Volume.CostPerGiBHour * gib * hrs
}

// Minutes computes the number of minutes over which the PVC is defined
func (pvc *PVC) Minutes() float64 {
	if pvc == nil {
		return 0.0
	}

	return pvc.End.Sub(pvc.Start).Minutes()
}

// String returns a string representation of the PVC
func (pvc *PVC) String() string {
	if pvc == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s/%s{Bytes:%.2f, Cost:%.6f, Start,End:%s}", pvc.Cluster, pvc.Namespace, pvc.Name, pvc.Bytes, pvc.Cost(), kubecost.NewWindow(&pvc.Start, &pvc.End))
}

// PV describes a PersistentVolume
// TODO move to pkg/kubecost? [TODO:CLEANUP]
type PV struct {
	Bytes          float64 `json:"bytes"`
	CostPerGiBHour float64 `json:"costPerGiBHour"` // TODO niko/computeallocation GiB or GB?
	Cluster        string  `json:"cluster"`
	Name           string  `json:"name"`
	StorageClass   string  `json:"storageClass"`
}

// String returns a string representation of the PV
func (pv *PV) String() string {
	if pv == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s{Bytes:%.2f, Cost/GiB*Hr:%.6f, StorageClass:%s}", pv.Cluster, pv.Name, pv.Bytes, pv.CostPerGiBHour, pv.StorageClass)
}
