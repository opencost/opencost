package costmodel

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/util"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	queryFmtPods                  = `avg(kube_pod_container_status_running{}) by (pod, namespace, cluster_id)[%s:%s]%s`
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

// ComputeAllocation uses the CostModel instance to compute an AllocationSet
// for the window defined by the given start and end times. The Allocations
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeAllocation(start, end time.Time, resolution time.Duration) (*kubecost.AllocationSet, error) {
	// 1. Build out Pod map from resolution-tuned, batched Pod start/end query
	// 2. Run and apply the results of the remaining queries to
	// 3. Build out AllocationSet from completed Pod map

	// Create a window spanning the requested query
	window := kubecost.NewWindow(&start, &end)

	// TODO niko/computeallocation remove log
	defer log.Profile(time.Now(), fmt.Sprintf("CostModel.ComputeAllocation: completed %s", window))

	// Create an empty AllocationSet. For safety, in the case of an error, we
	// should prefer to return this empty set with the error. (In the case of
	// no error, of course we populate the set and return it.)
	allocSet := kubecost.NewAllocationSet(start, end)

	// (1) Build out Pod map

	// Build out a map of Allocations as a mapping from pod-to-container-to-
	// underlying-Allocation instance, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	podMap := map[podKey]*Pod{}

	// clusterStarts and clusterEnds record the earliest start and latest end
	// times, respectively, on a cluster-basis. These are used for unmounted
	// PVs and other "virtual" Allocations so that minutes are maximally
	// accurate during start-up or spin-down of a cluster
	clusterStart := map[string]time.Time{}
	clusterEnd := map[string]time.Time{}

	// TODO niko/computeallocation make this configurable?
	batchSize := 6 * time.Hour

	cm.buildPodMap(window, resolution, batchSize, podMap, clusterStart, clusterEnd)

	// (2) Run and apply remaining queries

	// Convert window (start, end) to (duration, offset) for querying Prometheus,
	// including handling Thanos offset
	durStr, offStr, err := window.DurationOffsetForPrometheus()
	if err != nil {
		// Negative duration, so return empty set
		return allocSet, nil
	}

	// Convert resolution duration to a query-ready string
	resStr := util.DurationString(resolution)

	ctx := prom.NewContext(cm.PrometheusClient)
	startQuerying := time.Now()

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

	log.Profile(startQuerying, "CostModel.ComputeAllocation: queries complete")

	if ctx.HasErrors() {
		for _, err := range ctx.Errors() {
			log.Errorf("CostModel.ComputeAllocation: %s", err)
		}

		return allocSet, ctx.ErrorCollection()
	}

	defer log.Profile(time.Now(), "CostModel.ComputeAllocation: processing complete")

	applyCPUCoresAllocated(podMap, resCPUCoresAllocated)
	applyCPUCoresRequested(podMap, resCPURequests)
	applyCPUCoresUsed(podMap, resCPUUsage)
	applyRAMBytesAllocated(podMap, resRAMBytesAllocated)
	applyRAMBytesRequested(podMap, resRAMRequests)
	applyRAMBytesUsed(podMap, resRAMUsage)
	applyGPUsRequested(podMap, resGPUsRequested)
	applyNetworkAllocation(podMap, resNetZoneGiB, resNetZoneCostPerGiB)
	applyNetworkAllocation(podMap, resNetRegionGiB, resNetRegionCostPerGiB)
	applyNetworkAllocation(podMap, resNetInternetGiB, resNetInternetCostPerGiB)

	// TODO niko/computeallocation pruneDuplicateData? (see costmodel.go)

	namespaceLabels := resToNamespaceLabels(resNamespaceLabels)
	podLabels := resToPodLabels(resPodLabels)
	namespaceAnnotations := resToNamespaceAnnotations(resNamespaceAnnotations)
	podAnnotations := resToPodAnnotations(resPodAnnotations)
	applyLabels(podMap, namespaceLabels, podLabels)
	applyAnnotations(podMap, namespaceAnnotations, podAnnotations)

	serviceLabels := getServiceLabels(resServiceLabels)
	applyServicesToPods(podMap, podLabels, serviceLabels)

	podDeploymentMap := labelsToPodControllerMap(podLabels, resToDeploymentLabels(resDeploymentLabels))
	podStatefulSetMap := labelsToPodControllerMap(podLabels, resToStatefulSetLabels(resStatefulSetLabels))
	podDaemonSetMap := resToPodDaemonSetMap(resDaemonSetLabels)
	podJobMap := resToPodJobMap(resJobLabels)
	applyControllersToPods(podMap, podDeploymentMap)
	applyControllersToPods(podMap, podStatefulSetMap)
	applyControllersToPods(podMap, podDaemonSetMap)
	applyControllersToPods(podMap, podJobMap)

	// TODO breakdown network costs?

	// Build out a map of Nodes with resource costs, discounts, and node types
	// for converting resource allocation data to cumulative costs.
	nodeMap := map[nodeKey]*NodePricing{}

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
	buildPodPVCMap(podPVCMap, pvMap, pvcMap, podMap, resPodPVCAllocation)

	// Identify unmounted PVs (PVs without PVCs) and add one Allocation per
	// cluster representing each cluster's unmounted PVs (if necessary).
	applyUnmountedPVs(window, podMap, pvMap, pvcMap)

	// (3) Build out AllocationSet from Pod map

	for _, pod := range podMap {
		for _, alloc := range pod.Allocations {
			cluster, _ := alloc.Properties.GetCluster()
			nodeName, _ := alloc.Properties.GetNode()
			namespace, _ := alloc.Properties.GetNamespace()
			pod, _ := alloc.Properties.GetPod()
			container, _ := alloc.Properties.GetContainer()

			podKey := newPodKey(cluster, namespace, pod)
			nodeKey := newNodeKey(cluster, nodeName)

			node := cm.getNodePricing(nodeMap, nodeKey)
			alloc.CPUCost = alloc.CPUCoreHours * node.CostPerCPUHr
			alloc.RAMCost = (alloc.RAMByteHours / 1024 / 1024 / 1024) * node.CostPerRAMGiBHr
			alloc.GPUCost = alloc.GPUHours * node.CostPerGPUHr

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

					// Apply the size and cost of the PV to the allocation, each
					// weighted by count (i.e. the number of containers in the pod)
					alloc.PVByteHours += pvc.Bytes * hrs / count
					alloc.PVCost += cost / count
				}
			}

			// Make sure that the name is correct (node may not be present at this
			// point due to it missing from queryMinutes) then insert.
			alloc.Name = fmt.Sprintf("%s/%s/%s/%s/%s", cluster, nodeName, namespace, pod, container)
			allocSet.Set(alloc)
		}
	}

	return allocSet, nil
}

func (cm *CostModel) buildPodMap(window kubecost.Window, resolution, maxBatchSize time.Duration, podMap map[podKey]*Pod, clusterStart, clusterEnd map[string]time.Time) error {
	// Assumes that window is positive and closed
	start, end := *window.Start(), *window.End()

	// Convert resolution duration to a query-ready string
	resStr := util.DurationString(resolution)

	ctx := prom.NewContext(cm.PrometheusClient)
	profile := time.Now()

	// Query for (start, end) by (pod, namespace, cluster) over the given
	// window, using the given resolution, and if necessary in batches no
	// larger than the given maximum batch size. If working in batches, track
	// overall progress by starting with (window.start, window.start) and
	// querying in batches no larger than maxBatchSize from start-to-end,
	// folding each result set into podMap as the results come back.
	coverage := kubecost.NewWindow(&start, &start)

	numQuery := 1
	for coverage.End().Before(end) {
		// Determine the (start, end) of the current batch
		batchStart := *coverage.End()
		batchEnd := coverage.End().Add(maxBatchSize)
		if batchEnd.After(end) {
			batchEnd = end
		}
		batchWindow := kubecost.NewWindow(&batchStart, &batchEnd)

		var resPods []*prom.QueryResult
		var err error
		maxTries := 3
		numTries := 0
		for resPods == nil && numTries < maxTries {
			numTries++

			// Convert window (start, end) to (duration, offset) for querying Prometheus,
			// including handling Thanos offset
			durStr, offStr, err := batchWindow.DurationOffsetForPrometheus()
			if err != nil || durStr == "" {
				// Negative duration, so set empty results and don't query
				resPods = []*prom.QueryResult{}
				err = nil
				break
			}

			// Submit and profile query
			queryPods := fmt.Sprintf(queryFmtPods, durStr, resStr, offStr)
			queryProfile := time.Now()
			resPods, err = ctx.Query(queryPods).Await()
			if err != nil {
				// TODO niko/computeallocation remove log
				log.Profile(queryProfile, fmt.Sprintf("CostModel.ComputeAllocation: pod query batch %d try %d failed: %s", numQuery, numTries, queryPods))
				resPods = nil
			} else {
				// TODO niko/computeallocation remove log
				log.Profile(queryProfile, fmt.Sprintf("CostModel.ComputeAllocation: pod query batch %d try %d succeeded: %s", numQuery, numTries, queryPods))
			}
		}

		if err != nil {
			return err
		}

		applyPodResults(window, resolution, podMap, clusterStart, clusterEnd, resPods)

		coverage = coverage.ExpandEnd(batchEnd)
		numQuery++
	}

	log.Profile(profile, "CostModel.ComputeAllocation: pod map built")

	return nil
}

func applyPodResults(window kubecost.Window, resolution time.Duration, podMap map[podKey]*Pod, clusterStart, clusterEnd map[string]time.Time, resPods []*prom.QueryResult) {
	for _, res := range resPods {
		if len(res.Values) == 0 {
			log.Warningf("CostModel.ComputeAllocation: empty minutes result")
			continue
		}

		cluster, err := res.GetString("cluster_id")
		if err != nil {
			cluster = env.GetClusterID()
		}

		labels, err := res.GetStrings("namespace", "pod")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: minutes query result missing field: %s", err)
			continue
		}

		namespace := labels["namespace"]
		pod := labels["pod"]
		key := newPodKey(cluster, namespace, pod)

		// allocStart and allocEnd are the timestamps of the first and last
		// minutes the pod was running, respectively. We subtract one resolution
		// from allocStart because this point will actually represent the end
		// of the first minute. We don't subtract from allocEnd because it
		// already represents the end of the last minute.
		var allocStart, allocEnd time.Time
		startAdjustmentCoeff, endAdjustmentCoeff := 1.0, 1.0
		for _, datum := range res.Values {
			t := time.Unix(int64(datum.Timestamp), 0)

			if allocStart.IsZero() && datum.Value > 0 && window.Contains(t) {
				// Set the start timestamp to the earliest non-zero timestamp
				allocStart = t

				// Record adjustment coefficient, i.e. the portion of the start
				// timestamp to "ignore". That is, sometimes the value will be
				// 0.5, meaning that we should discount the time running by
				// half of the resolution the timestamp stands for.
				startAdjustmentCoeff = (1.0 - datum.Value)
			}

			if datum.Value > 0 && window.Contains(t) {
				// Set the end timestamp to the latest non-zero timestamp
				allocEnd = t

				// If the end timestamp differs from the start, then record the
				// adjustment coefficient, i.e. the portion of the end
				// timestamp to "ignore". That is, sometimes the value will be
				// 0.5, meaning that we should discount the time running by
				// half of the resolution the timestamp stands for.
				if !allocStart.Equal(t) {
					endAdjustmentCoeff = (1.0 - datum.Value)
				}
			}
		}

		if allocStart.IsZero() || allocEnd.IsZero() {
			continue
		}

		// Adjust timestamps accorind to the resolution and the adjustment
		// coefficients, as described above. That is, count the start timestamp
		// from the beginning of the resolution, not the end. Then "reduce" the
		// start and end by the correct amount, in the case that the "running"
		// value of the first or last timestamp was not a full 1.0.
		allocStart = allocStart.Add(-resolution)
		allocStart = allocStart.Add(time.Duration(startAdjustmentCoeff) * resolution)
		allocEnd = allocEnd.Add(-time.Duration(endAdjustmentCoeff) * resolution)

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

		if pod, ok := podMap[key]; ok {
			// Pod has already been recorded, so update it accordingly
			if allocStart.Before(pod.Start) {
				pod.Start = allocStart
			}
			if allocEnd.After(pod.End) {
				pod.End = allocEnd
			}
		} else {
			// Pod has not been recorded yet, so insert it
			podMap[key] = &Pod{
				Window:      window.Clone(),
				Start:       allocStart,
				End:         allocEnd,
				Key:         key,
				Allocations: map[string]*kubecost.Allocation{},
			}
		}
	}
}

func applyCPUCoresAllocated(podMap map[podKey]*Pod, resCPUCoresAllocated []*prom.QueryResult) {
	for _, res := range resCPUCoresAllocated {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU allocation result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU allocation result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU allocation query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		cpuCores := res.Values[0].Value
		hours := pod.Allocations[container].Minutes() / 60.0
		pod.Allocations[container].CPUCoreHours = cpuCores * hours

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing 'node': %s", key)
			continue
		}
		pod.Allocations[container].Properties.SetNode(node)
	}
}

func applyCPUCoresRequested(podMap map[podKey]*Pod, resCPUCoresRequested []*prom.QueryResult) {
	for _, res := range resCPUCoresRequested {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU request result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU request result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU request query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		pod.Allocations[container].CPUCoreRequestAverage = res.Values[0].Value

		// If CPU allocation is less than requests, set CPUCoreHours to
		// request level.
		if pod.Allocations[container].CPUCores() < res.Values[0].Value {
			pod.Allocations[container].CPUCoreHours = res.Values[0].Value * (pod.Allocations[container].Minutes() / 60.0)
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU request query result missing 'node': %s", key)
			continue
		}
		pod.Allocations[container].Properties.SetNode(node)
	}
}

func applyCPUCoresUsed(podMap map[podKey]*Pod, resCPUCoresUsed []*prom.QueryResult) {
	for _, res := range resCPUCoresUsed {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod_name")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container_name")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		pod.Allocations[container].CPUCoreUsageAverage = res.Values[0].Value
	}
}

func applyRAMBytesAllocated(podMap map[podKey]*Pod, resRAMBytesAllocated []*prom.QueryResult) {
	for _, res := range resRAMBytesAllocated {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM allocation result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM allocation result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM allocation query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		ramBytes := res.Values[0].Value
		hours := pod.Allocations[container].Minutes() / 60.0
		pod.Allocations[container].RAMByteHours = ramBytes * hours

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM allocation query result missing 'node': %s", key)
			continue
		}
		pod.Allocations[container].Properties.SetNode(node)
	}
}

func applyRAMBytesRequested(podMap map[podKey]*Pod, resRAMBytesRequested []*prom.QueryResult) {
	for _, res := range resRAMBytesRequested {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM request result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM request result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM request query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		pod.Allocations[container].RAMBytesRequestAverage = res.Values[0].Value

		// If RAM allocation is less than requests, set RAMByteHours to
		// request level.
		if pod.Allocations[container].RAMBytes() < res.Values[0].Value {
			pod.Allocations[container].RAMByteHours = res.Values[0].Value * (pod.Allocations[container].Minutes() / 60.0)
		}

		node, err := res.GetString("node")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM request query result missing 'node': %s", key)
			continue
		}
		pod.Allocations[container].Properties.SetNode(node)
	}
}

func applyRAMBytesUsed(podMap map[podKey]*Pod, resRAMBytesUsed []*prom.QueryResult) {
	for _, res := range resRAMBytesUsed {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod_name")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM usage result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM usage result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container_name")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM usage query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		pod.Allocations[container].RAMBytesUsageAverage = res.Values[0].Value
	}
}

func applyGPUsRequested(podMap map[podKey]*Pod, resGPUsRequested []*prom.QueryResult) {
	for _, res := range resGPUsRequested {
		key, err := resultPodKey(res, "cluster_id", "namespace", "pod")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU request result missing field: %s", err)
			continue
		}

		pod, ok := podMap[key]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU request result for unidentified pod: %s", key)
			continue
		}

		container, err := res.GetString("container")
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU request query result missing 'container': %s", key)
			continue
		}

		if _, ok := pod.Allocations[container]; !ok {
			pod.AppendContainer(container)
		}

		// TODO niko/computeallocation remove log
		log.Infof("CostModel.ComputeAllocation: GPU results: %s=%f", key, res.Values[0].Value)

		hrs := pod.Allocations[container].Minutes() / 60.0
		pod.Allocations[container].GPUHours = res.Values[0].Value * hrs
	}
}

func applyNetworkAllocation(podMap map[podKey]*Pod, resNetworkGiB []*prom.QueryResult, resNetworkCostPerGiB []*prom.QueryResult) {
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
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: Network allocation query result missing field: %s", err)
			continue
		}

		pod, ok := podMap[podKey]
		if !ok {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: Network allocation query result for unidentified pod: %s", podKey)
			continue
		}

		for _, alloc := range pod.Allocations {
			gib := res.Values[0].Value / float64(len(pod.Allocations))
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

func applyLabels(podMap map[podKey]*Pod, namespaceLabels map[string]map[string]string, podLabels map[podKey]map[string]string) {
	for key, pod := range podMap {
		for _, alloc := range pod.Allocations {
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
			if labels, ok := podLabels[key]; ok {
				for k, v := range labels {
					allocLabels[k] = v
				}
			}

			alloc.Properties.SetLabels(allocLabels)
		}
	}
}

func applyAnnotations(podMap map[podKey]*Pod, namespaceAnnotations map[string]map[string]string, podAnnotations map[podKey]map[string]string) {
	for key, pod := range podMap {
		for _, alloc := range pod.Allocations {
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
			if labels, ok := podAnnotations[key]; ok {
				for k, v := range labels {
					allocAnnotations[k] = v
				}
			}

			alloc.Properties.SetAnnotations(allocAnnotations)
		}
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

		// Convert the name of Jobs generated by CronJobs to the name of the
		// CronJob by stripping the timestamp off the end.
		match := isCron.FindStringSubmatch(controllerKey.Controller)
		if match != nil {
			controllerKey.Controller = match[1]
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

func applyServicesToPods(podMap map[podKey]*Pod, podLabels map[podKey]map[string]string, serviceLabels map[serviceKey]map[string]string) {
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

	// For each allocation in each pod, attempt to find and apply the list of
	// services associated with the allocation's pod.
	for key, pod := range podMap {
		for _, alloc := range pod.Allocations {
			if sKeys, ok := podServicesMap[key]; ok {
				services := []string{}
				for _, sKey := range sKeys {
					services = append(services, sKey.Service)
				}
				alloc.Properties.SetServices(services)
			}
		}
	}
}

func applyControllersToPods(podMap map[podKey]*Pod, podControllerMap map[podKey]controllerKey) {
	for key, pod := range podMap {
		for _, alloc := range pod.Allocations {
			if controllerKey, ok := podControllerMap[key]; ok {
				alloc.Properties.SetControllerKind(controllerKey.ControllerKind)
				alloc.Properties.SetController(controllerKey.Controller)
			}
		}
	}
}

func applyNodeCostPerCPUHr(nodeMap map[nodeKey]*NodePricing, resNodeCostPerCPUHr []*prom.QueryResult) {
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
			nodeMap[key] = &NodePricing{
				Name:     node,
				NodeType: instanceType,
			}
		}

		nodeMap[key].CostPerCPUHr = res.Values[0].Value
	}
}

func applyNodeCostPerRAMGiBHr(nodeMap map[nodeKey]*NodePricing, resNodeCostPerRAMGiBHr []*prom.QueryResult) {
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
			nodeMap[key] = &NodePricing{
				Name:     node,
				NodeType: instanceType,
			}
		}

		nodeMap[key].CostPerRAMGiBHr = res.Values[0].Value
	}
}

func applyNodeCostPerGPUHr(nodeMap map[nodeKey]*NodePricing, resNodeCostPerGPUHr []*prom.QueryResult) {
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
			nodeMap[key] = &NodePricing{
				Name:     node,
				NodeType: instanceType,
			}
		}

		nodeMap[key].CostPerGPUHr = res.Values[0].Value
	}
}

func applyNodeSpot(nodeMap map[nodeKey]*NodePricing, resNodeIsSpot []*prom.QueryResult) {
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

func applyNodeDiscount(nodeMap map[nodeKey]*NodePricing, cm *CostModel) {
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

func buildPodPVCMap(podPVCMap map[podKey][]*PVC, pvMap map[pvKey]*PV, pvcMap map[pvcKey]*PVC, podMap map[podKey]*Pod, resPodPVCAllocation []*prom.QueryResult) {
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

		count := 1
		if pod, ok := podMap[podKey]; ok && len(pod.Allocations) > 0 {
			count = len(pod.Allocations)
		} else {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: PVC %s for missing pod %s", pvcKey, podKey)
		}

		pvc.Count = count
		pvc.Mounted = true

		podPVCMap[podKey] = append(podPVCMap[podKey], pvc)
	}
}

func applyUnmountedPVs(window kubecost.Window, podMap map[podKey]*Pod, pvMap map[pvKey]*PV, pvcMap map[pvcKey]*PVC) {
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
		container := kubecost.UnmountedSuffix
		pod := kubecost.UnmountedSuffix
		namespace := kubecost.UnmountedSuffix
		node := ""

		key := newPodKey(cluster, namespace, pod)
		podMap[key] = &Pod{
			Window:      window.Clone(),
			Start:       *window.Start(),
			End:         *window.End(),
			Key:         key,
			Allocations: map[string]*kubecost.Allocation{},
		}

		podMap[key].AppendContainer(container)
		podMap[key].Allocations[container].Properties.SetCluster(cluster)
		podMap[key].Allocations[container].Properties.SetNode(node)
		podMap[key].Allocations[container].Properties.SetNamespace(namespace)
		podMap[key].Allocations[container].Properties.SetPod(pod)
		podMap[key].Allocations[container].Properties.SetContainer(container)
		podMap[key].Allocations[container].PVByteHours = unmountedPVBytes[cluster] * window.Minutes() / 60.0
		podMap[key].Allocations[container].PVCost = amount
	}
}

func applyUnmountedPVCs(window kubecost.Window, podMap map[podKey]*Pod, pvcMap map[pvcKey]*PVC) {
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
		container := kubecost.UnmountedSuffix
		pod := kubecost.UnmountedSuffix
		namespace := key.Namespace
		node := ""
		cluster := key.Cluster

		podKey := newPodKey(cluster, namespace, pod)
		podMap[podKey] = &Pod{
			Window:      window.Clone(),
			Start:       *window.Start(),
			End:         *window.End(),
			Key:         podKey,
			Allocations: map[string]*kubecost.Allocation{},
		}

		podMap[podKey].AppendContainer(container)
		podMap[podKey].Allocations[container].Properties.SetCluster(cluster)
		podMap[podKey].Allocations[container].Properties.SetNode(node)
		podMap[podKey].Allocations[container].Properties.SetNamespace(namespace)
		podMap[podKey].Allocations[container].Properties.SetPod(pod)
		podMap[podKey].Allocations[container].Properties.SetContainer(container)
		podMap[podKey].Allocations[container].PVByteHours = unmountedPVCBytes[key] * window.Minutes() / 60.0
		podMap[podKey].Allocations[container].PVCost = amount
	}
}

// getNodePricing determines node pricing, given a key and a mapping from keys
// to their NodePricing instances, as well as the custom pricing configuration
// inherent to the CostModel instance. If custom pricing is set, use that. If
// not, use the pricing defined by the given key. If that doesn't exist, fall
// back on custom pricing as a default.
func (cm *CostModel) getNodePricing(nodeMap map[nodeKey]*NodePricing, nodeKey nodeKey) *NodePricing {
	// Find the relevant NodePricing, if it exists. If not, substitute the
	// custom NodePricing as a default.
	node, ok := nodeMap[nodeKey]
	if !ok || node == nil {
		if nodeKey.Node != "" {
			log.Warningf("CostModel: failed to find node for %s", nodeKey)
		}
		return cm.getCustomNodePricing(false)
	}

	// If custom pricing is enabled and can be retrieved, override detected
	// node pricing with the custom values.
	customPricingConfig, err := cm.Provider.GetConfig()
	if err != nil {
		log.Warningf("CostModel: failed to load custom pricing: %s", err)
	}
	if cloud.CustomPricesEnabled(cm.Provider) && customPricingConfig != nil {
		return cm.getCustomNodePricing(node.Preemptible)
	}

	// If any of the values are NaN or zero, replace them with the custom
	// values as default.
	// TODO:CLEANUP can't we parse these custom prices once? why do we store
	// them as strings like this?

	if node.CostPerCPUHr == 0 || math.IsNaN(node.CostPerCPUHr) {
		log.Warningf("CostModel: node pricing has illegal CostPerCPUHr; replacing with custom pricing: %s", nodeKey)
		cpuCostStr := customPricingConfig.CPU
		if node.Preemptible {
			cpuCostStr = customPricingConfig.SpotCPU
		}
		costPerCPUHr, err := strconv.ParseFloat(cpuCostStr, 64)
		if err != nil {
			log.Warningf("CostModel: custom pricing has illegal CPU cost: %s", cpuCostStr)
		}
		node.CostPerCPUHr = costPerCPUHr
	}

	if math.IsNaN(node.CostPerGPUHr) {
		log.Warningf("CostModel: node pricing has illegal CostPerGPUHr; replacing with custom pricing: %s", nodeKey)
		gpuCostStr := customPricingConfig.GPU
		if node.Preemptible {
			gpuCostStr = customPricingConfig.SpotGPU
		}
		costPerGPUHr, err := strconv.ParseFloat(gpuCostStr, 64)
		if err != nil {
			log.Warningf("CostModel: custom pricing has illegal GPU cost: %s", gpuCostStr)
		}
		node.CostPerGPUHr = costPerGPUHr
	}

	if node.CostPerRAMGiBHr == 0 || math.IsNaN(node.CostPerRAMGiBHr) {
		log.Warningf("CostModel: node pricing has illegal CostPerRAMHr; replacing with custom pricing: %s", nodeKey)
		ramCostStr := customPricingConfig.RAM
		if node.Preemptible {
			ramCostStr = customPricingConfig.SpotRAM
		}
		costPerRAMHr, err := strconv.ParseFloat(ramCostStr, 64)
		if err != nil {
			log.Warningf("CostModel: custom pricing has illegal RAM cost: %s", ramCostStr)
		}
		node.CostPerRAMGiBHr = costPerRAMHr
	}

	return node
}

// getCustomNodePricing converts the CostModel's configured custom pricing
// values into a NodePricing instance.
func (cm *CostModel) getCustomNodePricing(spot bool) *NodePricing {
	customPricingConfig, err := cm.Provider.GetConfig()
	if err != nil {
		return nil
	}

	cpuCostStr := customPricingConfig.CPU
	gpuCostStr := customPricingConfig.GPU
	ramCostStr := customPricingConfig.RAM
	if spot {
		cpuCostStr = customPricingConfig.SpotCPU
		gpuCostStr = customPricingConfig.SpotGPU
		ramCostStr = customPricingConfig.SpotRAM
	}

	node := &NodePricing{}

	costPerCPUHr, err := strconv.ParseFloat(cpuCostStr, 64)
	if err != nil {
		log.Warningf("CostModel: custom pricing has illegal CPU cost: %s", cpuCostStr)
	}
	node.CostPerCPUHr = costPerCPUHr

	costPerGPUHr, err := strconv.ParseFloat(gpuCostStr, 64)
	if err != nil {
		log.Warningf("CostModel: custom pricing has illegal GPU cost: %s", gpuCostStr)
	}
	node.CostPerGPUHr = costPerGPUHr

	costPerRAMHr, err := strconv.ParseFloat(ramCostStr, 64)
	if err != nil {
		log.Warningf("CostModel: custom pricing has illegal RAM cost: %s", ramCostStr)
	}
	node.CostPerRAMGiBHr = costPerRAMHr

	return node
}

type NodePricing struct {
	Name            string
	NodeType        string
	Preemptible     bool
	CostPerCPUHr    float64
	CostPerRAMGiBHr float64
	CostPerGPUHr    float64
	Discount        float64
	Source          string
}

// TODO niko/computealloction comment
type Pod struct {
	Window      kubecost.Window
	Start       time.Time
	End         time.Time
	Key         podKey
	Allocations map[string]*kubecost.Allocation
}

// TODO niko/computealloction comment
func (p Pod) AppendContainer(container string) {
	name := fmt.Sprintf("%s/%s/%s/%s", p.Key.Cluster, p.Key.Namespace, p.Key.Pod, container)

	alloc := &kubecost.Allocation{
		Name:       name,
		Properties: kubecost.Properties{},
		Window:     p.Window.Clone(),
		Start:      p.Start,
		End:        p.End,
	}
	alloc.Properties.SetContainer(container)
	alloc.Properties.SetPod(p.Key.Pod)
	alloc.Properties.SetNamespace(p.Key.Namespace)
	alloc.Properties.SetCluster(p.Key.Cluster)

	p.Allocations[container] = alloc
}

// PVC describes a PersistentVolumeClaim
// TODO:CLEANUP move to pkg/kubecost?
// TODO:CLEANUP add PersistentVolumeClaims field to type Allocation?
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
// TODO move to pkg/kubecost? TODO:CLEANUP
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
