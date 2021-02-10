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
	Bytes     float64   `json:"bytes"`
	Count     int       `json:"count"`
	Name      string    `json:"name"`
	Cluster   string    `json:"cluster"`
	Namespace string    `json:"namespace"`
	Volume    *PV       `json:"persistentVolume"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
}

func (pvc *PVC) Cost() float64 {
	if pvc == nil || pvc.Volume == nil {
		return 0.0
	}

	gib := pvc.Bytes / 1024 / 1024 / 1024
	hrs := pvc.Minutes() / 60.0

	return pvc.Volume.CostPerGiBHour * gib * hrs
}

func (pvc *PVC) Minutes() float64 {
	if pvc == nil {
		return 0.0
	}

	return pvc.End.Sub(pvc.Start).Minutes()
}

func (pvc *PVC) String() string {
	if pvc == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s/%s{Bytes:%.2f, Cost:%.6f, Start,End:%s}", pvc.Cluster, pvc.Namespace, pvc.Name, pvc.Bytes, pvc.Cost(), kubecost.NewWindow(&pvc.Start, &pvc.End))
}

// TODO niko/cdmr move to pkg/kubecost
type PV struct {
	Bytes          float64 `json:"bytes"`
	CostPerGiBHour float64 `json:"costPerGiBHour"` // TODO niko/cdmr GiB or GB?
	Cluster        string  `json:"cluster"`
	Name           string  `json:"name"`
	StorageClass   string  `json:"storageClass"`
}

func (pv *PV) String() string {
	if pv == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%s{Bytes:%.2f, Cost/GiB*Hr:%.6f, StorageClass:%s}", pv.Cluster, pv.Name, pv.Bytes, pv.CostPerGiBHour, pv.StorageClass)
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

	queryPVCInfo := fmt.Sprintf(`avg(kube_persistentvolumeclaim_info{volumename != ""}) by (persistentvolumeclaim, storageclass, volumename, namespace, cluster_id)[%s:%s]%s`, durStr, resStr, offStr)
	resChPVCInfo := ctx.Query(queryPVCInfo)

	queryPVBytes := fmt.Sprintf(`avg(avg_over_time(kube_persistentvolume_capacity_bytes[%s]%s)) by (persistentvolume, cluster_id)`, durStr, offStr)
	resChPVBytes := ctx.Query(queryPVBytes)

	queryPodPVCAllocation := fmt.Sprintf(`avg(avg_over_time(pod_pvc_allocation[%s]%s)) by (persistentvolume, persistentvolumeclaim, pod, namespace, cluster_id)`, durStr, offStr)
	resChPodPVCAllocation := ctx.Query(queryPodPVCAllocation)

	queryPVCBytesRequested := fmt.Sprintf(`avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{}[%s]%s)) by (persistentvolumeclaim, namespace, cluster_id)`, durStr, offStr)
	resChPVCBytesRequested := ctx.Query(queryPVCBytesRequested)

	queryPVCostPerGiBHour := fmt.Sprintf(`avg(avg_over_time(pv_hourly_cost[%s]%s)) by (volumename, cluster_id)`, durStr, offStr)
	resChPVCostPerGiBHour := ctx.Query(queryPVCostPerGiBHour)

	queryNetZoneGiB := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="true"}[%s]%s)) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024`, durStr, offStr)
	resChNetZoneGiB := ctx.Query(queryNetZoneGiB)

	queryNetZoneCostPerGiB := fmt.Sprintf(`avg(avg_over_time(kubecost_network_zone_egress_cost{}[%s]%s)) by (cluster_id)`, durStr, offStr)
	resChNetZoneCostPerGiB := ctx.Query(queryNetZoneCostPerGiB)

	queryNetRegionGiB := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", sameZone="false", sameRegion="false"}[%s]%s)) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024`, durStr, offStr)
	resChNetRegionGiB := ctx.Query(queryNetRegionGiB)

	queryNetRegionCostPerGiB := fmt.Sprintf(`avg(avg_over_time(kubecost_network_region_egress_cost{}[%s]%s)) by (cluster_id)`, durStr, offStr)
	resChNetRegionCostPerGiB := ctx.Query(queryNetRegionCostPerGiB)

	queryNetInternetGiB := fmt.Sprintf(`sum(increase(kubecost_pod_network_egress_bytes_total{internet="true"}[%s]%s)) by (pod_name, namespace, cluster_id) / 1024 / 1024 / 1024`, durStr, offStr)
	resChNetInternetGiB := ctx.Query(queryNetInternetGiB)

	queryNetInternetCostPerGiB := fmt.Sprintf(`avg(avg_over_time(kubecost_network_internet_egress_cost{}[%s]%s)) by (cluster_id)`, durStr, offStr)
	resChNetInternetCostPerGiB := ctx.Query(queryNetInternetCostPerGiB)

	queryNamespaceLabels := fmt.Sprintf(`avg_over_time(kube_namespace_labels[%s]%s)`, durStr, offStr)
	resChNamespaceLabels := ctx.Query(queryNamespaceLabels)

	queryNamespaceAnnotations := fmt.Sprintf(`avg_over_time(kube_namespace_annotations[%s]%s)`, durStr, offStr)
	resChNamespaceAnnotations := ctx.Query(queryNamespaceAnnotations)

	queryPodLabels := fmt.Sprintf(`avg_over_time(kube_pod_labels[%s]%s)`, durStr, offStr)
	resChPodLabels := ctx.Query(queryPodLabels)

	queryPodAnnotations := fmt.Sprintf(`avg_over_time(kube_pod_annotations[%s]%s)`, durStr, offStr)
	resChPodAnnotations := ctx.Query(queryPodAnnotations)

	queryServiceLabels := fmt.Sprintf(`avg_over_time(service_selector_labels[%s]%s)`, durStr, offStr)
	resChServiceLabels := ctx.Query(queryServiceLabels)

	queryDeploymentLabels := fmt.Sprintf(`avg_over_time(deployment_match_labels[%s]%s)`, durStr, offStr)
	resChDeploymentLabels := ctx.Query(queryDeploymentLabels)

	queryStatefulSetLabels := fmt.Sprintf(`avg_over_time(statefulSet_match_labels[%s]%s)`, durStr, offStr)
	resChStatefulSetLabels := ctx.Query(queryStatefulSetLabels)

	queryDaemonSetLabels := fmt.Sprintf(`sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet"}[%s]%s)) by (pod, owner_name, namespace, cluster_id)`, durStr, offStr)
	resChDaemonSetLabels := ctx.Query(queryDaemonSetLabels)

	queryJobLabels := fmt.Sprintf(`sum(avg_over_time(kube_pod_owner{owner_kind="Job"}[%s]%s)) by (pod, owner_name, namespace ,cluster_id)`, durStr, offStr)
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

	// ----------------------------------------------------------------------//
	// TODO niko/cdmr remove all logs after testing

	log.Infof("CostModel.ComputeAllocation: minutes  : %s", queryMinutes)

	log.Infof("CostModel.ComputeAllocation: CPU cores: %s", queryCPUCoresAllocated)
	log.Infof("CostModel.ComputeAllocation: CPU req  : %s", queryCPURequests)
	log.Infof("CostModel.ComputeAllocation: CPU use  : %s", queryCPUUsage)
	log.Infof("CostModel.ComputeAllocation: $/CPU*Hr : %s", queryNodeCostPerCPUHr)

	log.Infof("CostModel.ComputeAllocation: RAM bytes: %s", queryRAMBytesAllocated)
	log.Infof("CostModel.ComputeAllocation: RAM req  : %s", queryRAMRequests)
	log.Infof("CostModel.ComputeAllocation: RAM use  : %s", queryRAMUsage)
	log.Infof("CostModel.ComputeAllocation: $/GiB*Hr : %s", queryNodeCostPerRAMGiBHr)

	log.Infof("CostModel.ComputeAllocation: PV $/gbhr: %s", queryPVCostPerGiBHour)
	log.Infof("CostModel.ComputeAllocation: PV bytes : %s", queryPVBytes)

	log.Infof("CostModel.ComputeAllocation: PVC alloc: %s", queryPodPVCAllocation)
	log.Infof("CostModel.ComputeAllocation: PVC bytes: %s", queryPVCBytesRequested)
	log.Infof("CostModel.ComputeAllocation: PVC info : %s", queryPVCInfo)

	log.Infof("CostModel.ComputeAllocation: Net Z GiB: %s", queryNetZoneGiB)
	log.Infof("CostModel.ComputeAllocation: Net Z $  : %s", queryNetZoneCostPerGiB)
	log.Infof("CostModel.ComputeAllocation: Net R GiB: %s", queryNetRegionGiB)
	log.Infof("CostModel.ComputeAllocation: Net R $  : %s", queryNetRegionCostPerGiB)
	log.Infof("CostModel.ComputeAllocation: Net I GiB: %s", queryNetInternetGiB)
	log.Infof("CostModel.ComputeAllocation: Net I $  : %s", queryNetInternetCostPerGiB)

	log.Infof("CostModel.ComputeAllocation: NamespaceLabels: %s", queryNamespaceLabels)
	log.Infof("CostModel.ComputeAllocation: NamespaceAnnotations: %s", queryNamespaceAnnotations)
	log.Infof("CostModel.ComputeAllocation: PodLabels: %s", queryPodLabels)
	log.Infof("CostModel.ComputeAllocation: PodAnnotations: %s", queryPodAnnotations)
	log.Infof("CostModel.ComputeAllocation: ServiceLabels: %s", queryServiceLabels)
	log.Infof("CostModel.ComputeAllocation: DeploymentLabels: %s", queryDeploymentLabels)
	log.Infof("CostModel.ComputeAllocation: StatefulSetLabels: %s", queryStatefulSetLabels)
	log.Infof("CostModel.ComputeAllocation: DaemonSetLabels: %s", queryDaemonSetLabels)
	log.Infof("CostModel.ComputeAllocation: JobLabels: %s", queryJobLabels)

	log.Profile(startQuerying, "CostModel.ComputeAllocation: queries complete")
	defer log.Profile(time.Now(), "CostModel.ComputeAllocation: processing complete")

	// ----------------------------------------------------------------------//

	// Build out a map of Allocations, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	// TODO niko/cdmr can we start with a reasonable guess at map size?
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

	applyLabels := func(name string, res []*prom.QueryResult) {
		log.Infof("CostModel.ComputeAllocation: %s: %d results", name, len(res))
	}

	applyLabels("NamespaceLabels", resNamespaceLabels)
	applyLabels("NamespaceAnnotations", resNamespaceAnnotations)
	applyLabels("PodLabels", resPodLabels)
	applyLabels("PodAnnotations", resPodAnnotations)
	applyLabels("ServiceLabels", resServiceLabels)
	applyLabels("DeploymentLabels", resDeploymentLabels)
	applyLabels("StatefulSetLabels", resStatefulSetLabels)
	applyLabels("DaemonSetLabels", resDaemonSetLabels)
	applyLabels("JobLabels", resJobLabels)

	// TODO niko/cdmr breakdown network costs?

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
	buildPVMap(pvMap, resPVCostPerGiBHour)
	applyPVBytes(pvMap, resPVBytes)
	// TODO niko/cdmr apply PV bytes?

	// TODO niko/cdmr comment
	pvcMap := map[pvcKey]*PVC{}
	buildPVCMap(window, pvcMap, pvMap, resPVCInfo)
	applyPVCBytesRequested(pvcMap, resPVCBytesRequested)

	// TODO niko/cdmr comment
	podPVCMap := map[podKey][]*PVC{}
	buildPodPVCMap(podPVCMap, pvMap, pvcMap, podAllocation, resPodPVCAllocation)

	// Identify unmounted PVs (PVs without PVCs) and add one Allocation per
	// cluster representing each cluster's unmounted PVs (if necessary).
	applyUnmountedPVs(window, allocationMap, pvMap, pvcMap)

	// TODO niko/cdmr remove logs
	log.Infof("CostModel.ComputeAllocation: %d allocations", len(allocationMap))
	log.Infof("CostModel.ComputeAllocation: %d nodes", len(nodeMap))
	log.Infof("CostModel.ComputeAllocation: %d PVs", len(pvMap))
	log.Infof("CostModel.ComputeAllocation: %d PVCs", len(pvcMap))
	log.Infof("CostModel.ComputeAllocation: %d pods with PVCs", len(podPVCMap))
	for _, node := range nodeMap {
		log.Infof("CostModel.ComputeAllocation: Node: %s: %f/CPUHr; %f/RAMHr; %f/GPUHr; %f discount", node.Name, node.CostPerCPUHr, node.CostPerRAMGiBHr, node.CostPerGPUHr, node.Discount)
	}
	for _, pv := range pvMap {
		log.Infof("CostModel.ComputeAllocation: PV: %s", pv)
	}
	for pod, pvcs := range podPVCMap {
		for _, pvc := range pvcs {
			log.Infof("CostModel.ComputeAllocation: Pod %s: PVC: %s", pod, pvc)
		}
	}

	for _, alloc := range allocationMap {
		cluster, _ := alloc.Properties.GetCluster()
		node, _ := alloc.Properties.GetNode()
		namespace, _ := alloc.Properties.GetNamespace()
		pod, _ := alloc.Properties.GetPod()

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
				gib := pvc.Bytes / 1024 / 1024 / 1024

				alloc.PVByteHours += pvc.Bytes * hrs
				alloc.PVCost += pvc.Volume.CostPerGiBHour * gib * hrs / float64(pvc.Count)
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
			// TODO niko/cdmr remove log?
			// log.Warningf("CostModel.ComputeAllocation: allocation %s has no running time, skipping", containerKey)
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
		alloc.Properties.SetNode(node)
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
			// TODO niko/cdmr remove log?
			// log.Warningf("CostModel.ComputeAllocation: unidentified CPU request query result: %s", key)
			continue
		}

		allocationMap[key].CPUCoreRequestAverage = res.Values[0].Value

		// CPU allocation is less than requests, so set CPUCoreHours to
		// request level.
		// TODO niko/cdmr why is this happening?
		if allocationMap[key].CPUCores() < res.Values[0].Value {
			allocationMap[key].CPUCoreHours = res.Values[0].Value * (allocationMap[key].Minutes() / 60.0)
		}
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

func applyRAMBytesRequested(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesRequested []*prom.QueryResult) {
	for _, res := range resRAMBytesRequested {
		key, err := resultContainerKey(res, "cluster_id", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: RAM request query result missing field: %s", err)
			continue
		}

		_, ok := allocationMap[key]
		if !ok {
			// TODO niko/cdmr remove log?
			// log.Warningf("CostModel.ComputeAllocation: unidentified RAM request query result: %s", key)
			continue
		}

		allocationMap[key].RAMBytesRequestAverage = res.Values[0].Value

		// RAM allocation is less than requests, so set RAMByteHours to
		// request level.
		// TODO niko/cdmr why is this happening?
		if allocationMap[key].RAMBytes() < res.Values[0].Value {
			allocationMap[key].RAMByteHours = res.Values[0].Value * (allocationMap[key].Minutes() / 60.0)
		}
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

func applyRAMBytesAllocated(allocationMap map[containerKey]*kubecost.Allocation, resRAMBytesAllocated []*prom.QueryResult) {
	for _, res := range resRAMBytesAllocated {
		// TODO niko/cdmr do we need node here?
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
	}
}

func applyGPUsRequested(allocationMap map[containerKey]*kubecost.Allocation, resGPUsRequested []*prom.QueryResult) {
	for _, res := range resGPUsRequested {
		// TODO niko/cdmr do we need node here?
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

		// TODO niko/cdmr complete
		log.Infof("CostModel.ComputeAllocation: GPU results: %s=%f", key, res.Values[0].Value)
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

		// TODO niko/cdmr ?
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
			log.Warningf("CostModel.ComputeAllocation: PVC bytes requested result for missing PVC: %s", err)
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

		log.Infof("CostModel.ComputeAllocation: PV %s is mounted? %t", pv.Name, mounted)

		if !mounted {
			gib := pv.Bytes / 1024 / 1024 / 1024
			hrs := window.Minutes() / 60.0
			cost := pv.CostPerGiBHour * gib * hrs
			unmountedPVCost[pv.Cluster] += cost
			unmountedPVBytes[pv.Cluster] += pv.Bytes
		}
	}

	for cluster, amount := range unmountedPVCost {
		container := "unmounted-pvs"
		pod := "unmounted-pvs"
		namespace := "" // TODO niko/cdmr what about this?

		containerKey := newContainerKey(cluster, namespace, pod, container)
		allocationMap[containerKey] = &kubecost.Allocation{
			Name: fmt.Sprintf("%s/%s/%s/%s", cluster, namespace, pod, container),
			Properties: kubecost.Properties{
				kubecost.ClusterProp:   cluster,
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
