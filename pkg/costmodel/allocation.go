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
	offStr := fmt.Sprintf("%dm", int64(offset.Minutes()))
	if offset < time.Minute {
		offStr = ""
	}

	// TODO niko/cdmr dynamic resolution? add to ComputeAllocation() in allocation.Source?
	resStr := "1m"
	// resPerHr := 60

	ctx := prom.NewContext(cm.PrometheusClient)

	// TODO niko/cdmr retries? (That should probably go into the Store.)

	// TODO niko/cmdr check: will multiple Prometheus jobs multiply the totals?

	// TODO niko/cdmr should we try doing this without resolution? Could yield
	// more accurate results, but might also be more challenging in some
	// respects; e.g. "correcting" the start point by what amount?
	queryMinutes := fmt.Sprintf(`
		avg(kube_pod_container_status_running{}) by (container, pod, namespace, kubernetes_node, cluster_id)[%s:%s]%s
	`, durStr, resStr, offStr)
	resChMinutes := ctx.Query(queryMinutes)

	queryRAMBytesAllocated := fmt.Sprintf(`
		avg(
			avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!=""}[%s]%s)
		) by (container, pod, namespace, node, cluster_id)
	`, durStr, offStr)
	resChRAMBytesAllocated := ctx.Query(queryRAMBytesAllocated)

	// TODO niko/cdmr
	// queryRAMRequests := fmt.Sprintf()
	// resChRAMRequests := ctx.Query(queryRAMRequests)

	// TODO niko/cdmr
	// queryRAMUsage := fmt.Sprintf()
	// resChRAMUsage := ctx.Query(queryRAMUsage)

	queryCPUCoresAllocated := fmt.Sprintf(`
		avg(
			avg_over_time(container_cpu_allocation{container!="", container!="POD", node!=""}[%s]%s)
		) by (container, pod, namespace, node, cluster_id)
	`, durStr, offStr)
	resChCPUCoresAllocated := ctx.Query(queryCPUCoresAllocated)

	// TODO niko/cdmr
	// queryCPURequests := fmt.Sprintf()
	// resChCPURequests := ctx.Query(queryCPURequests)

	// TODO niko/cdmr
	// queryCPUUsage := fmt.Sprintf()
	// resChCPUUsage := ctx.Query(queryCPUUsage)

	// TODO niko/cdmr find an env with GPUs to test this (generate one?)
	queryGPUsRequested := fmt.Sprintf(`
		avg(
			avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!=""}[%s]%s)
		) by (container, pod, namespace, node, cluster_id)
	`, durStr, offStr)
	resChGPUsRequested := ctx.Query(queryGPUsRequested)

	queryPVCAllocation := fmt.Sprintf(`
		avg(
			avg_over_time(pod_pvc_allocation[%s]%s)
		) by (persistentvolume, persistentvolumeclaim, pod, namespace, cluster_id)
	`, durStr, offStr)
	resChPVCAllocation := ctx.Query(queryPVCAllocation)

	queryPVBytesRequested := fmt.Sprintf(`
		avg(
			avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{}[%s]%s)
		) by (persistentvolumeclaim, namespace, cluster_id)
	`, durStr, offStr)
	resChPVBytesRequested := ctx.Query(queryPVBytesRequested)

	queryPVCostPerGiBHour := fmt.Sprintf(`
		avg(
			avg_over_time(pv_hourly_cost[%s]%s)
		) by (volumename, cluster_id)
	`, durStr, offStr)
	resChPVCostPerGiBHour := ctx.Query(queryPVCostPerGiBHour)

	queryPVCInfo := fmt.Sprintf(`
		avg(
			avg_over_time(kube_persistentvolumeclaim_info{volumename != ""}[%s]%s)
		) by (persistentvolumeclaim, storageclass, volumename, namespace, cluster_id)
	`, durStr, offStr)
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
	// queryJobLabels := fmt.Sprintf()
	// resChJobLabels := ctx.Query(queryJobLabels)

	// TODO niko/cdmr
	// queryDaemonSetLabels := fmt.Sprintf()
	// resChDaemonSetLabels := ctx.Query(queryDaemonSetLabels)

	resMinutes, _ := resChMinutes.Await()
	resCPUCoresAllocated, _ := resChCPUCoresAllocated.Await()
	resRAMBytesAllocated, _ := resChRAMBytesAllocated.Await()
	resGPUsRequested, _ := resChGPUsRequested.Await()
	resPVCAllocation, _ := resChPVCAllocation.Await()
	resPVBytesRequested, _ := resChPVBytesRequested.Await()
	resPVCostPerGiBHour, _ := resChPVCostPerGiBHour.Await()
	resPVCInfo, _ := resChPVCInfo.Await()

	// Build out a map of allocations, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	// TODO niko/cdmr can we start with a reasonable guess at map size?
	allocMap := map[containerKey]*kubecost.Allocation{}

	// TODO niko/cdmr comment
	podCount := map[podKey]int{}

	// clusterStarts and clusterEnds record the earliest start and latest end
	// times, respectively, on a cluster-basis. These are used for unmounted
	// PVs and other "virtual" Allocations so that minutes are maximally
	// accurate during start-up or spin-down of a cluster
	clusterStart := map[string]time.Time{}
	clusterEnd := map[string]time.Time{}

	for _, res := range resMinutes {
		if len(res.Values) == 0 {
			log.Warningf("CostModel.ComputeAllocation: empty minutes result")
			continue
		}

		values, err := res.GetStrings("cluster_id", "kubernetes_node", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: minutes query result missing field: %s", err)
			continue
		}

		cluster := values["cluster_id"]
		node := values["kubernetes_node"]
		namespace := values["namespace"]
		pod := values["pod"]
		container := values["container"]

		containerKey := newContainerKey(cluster, namespace, pod, container)
		podKey := newPodKey(cluster, namespace, pod)

		// allocStart is the timestamp of the first minute. We subtract 1m because
		// this point will actually represent the end of the first minute. We
		// don't subtract from end (timestamp of the last minute) because it's
		// already the end of the last minute, which is what we want.
		allocStart := time.Unix(int64(res.Values[0].Timestamp), 0).Add(-1 * time.Minute)
		if allocStart.Before(start) {
			log.Warningf("CostModel.ComputeAllocation: allocation %s measured start before window start: %s < %s", containerKey, allocStart, start)
			allocStart = start
		}

		allocEnd := time.Unix(int64(res.Values[len(res.Values)-1].Timestamp), 0)
		if allocEnd.After(end) {
			log.Warningf("CostModel.ComputeAllocation: allocation %s measured end before window end: %s < %s", containerKey, allocEnd, end)
			allocEnd = end
		}

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
			Name:   name,
			Start:  allocStart,
			End:    allocEnd,
			Window: window.Clone(),
		}

		props := kubecost.Properties{}
		props.SetContainer(container)
		props.SetPod(pod)
		props.SetNamespace(namespace)
		props.SetNode(node)
		props.SetCluster(cluster)

		allocMap[containerKey] = alloc

		podCount[podKey]++
	}

	for _, res := range resCPUCoresAllocated {
		// TODO niko/cdmr do we need node here?
		key, err := resultContainerKey(res, "cluster", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified CPU allocation query result: %s", key)
			continue
		}

		cpuCores := res.Values[0].Value
		hours := allocMap[key].Minutes() / 60.0
		allocMap[key].CPUCoreHours = cpuCores * hours
	}

	for _, res := range resRAMBytesAllocated {
		// TODO niko/cdmr do we need node here?
		key, err := resultContainerKey(res, "cluster", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified RAM allocation query result: %s", key)
			continue
		}

		ramBytes := res.Values[0].Value
		hours := allocMap[key].Minutes() / 60.0
		allocMap[key].RAMByteHours = ramBytes * hours
	}

	for _, res := range resGPUsRequested {
		// TODO niko/cdmr do we need node here?
		key, err := resultContainerKey(res, "cluster", "namespace", "pod", "container")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: CPU allocation query result missing field: %s", err)
			continue
		}

		_, ok := allocMap[key]
		if !ok {
			log.Warningf("CostModel.ComputeAllocation: unidentified RAM allocation query result: %s", key)
			continue
		}

		// TODO niko/cdmr complete
		log.Infof("CostModel.ComputeAllocation: GPU results: %s=%f", key, res.Values[0].Value)
	}

	// TODO niko/cdmr comment
	pvMap := map[pvKey]*PV{}

	for _, res := range resPVBytesRequested {
		// TODO niko/cdmr double-check "persistentvolume" vs "volumename"
		key, err := resultPVKey(res, "cluster_id", "persistentvolume")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV bytes requested query result missing field: %s", err)
			continue
		}

		// TODO niko/cdmr double-check "persistentvolume" vs "volumename"
		name, err := res.GetString("persistentvolume")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV bytes requested query result missing field: %s", err)
			continue
		}

		pvMap[key] = &PV{
			Bytes: res.Values[0].Value,
			Name:  name,
		}
	}

	for _, res := range resPVCostPerGiBHour {
		// TODO niko/cdmr double-check "persistentvolume" vs "volumename"
		key, err := resultPVKey(res, "cluster_id", "persistentvolume")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PV cost per byte*hr query result missing field: %s", err)
			continue
		}

		if _, ok := pvMap[key]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV cost per byte*hr for unidentified PV: %s", key)
			continue
		}

		pvMap[key].CostPerGiBHour = res.Values[0].Value
	}

	// TODO niko/cdmr comment
	pvcMap := map[podKey][]*PVC{}

	for _, res := range resPVCAllocation {
		values, err := res.GetStrings("persistentvolume", "persistentvolumeclaim", "pod", "namespace", "cluster_id")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PVC allocation query result missing field: %s", err)
			continue
		}

		cluster := values["cluster_id"]
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

		if _, ok := pvcMap[podKey]; !ok {
			pvcMap[podKey] = []*PVC{}
		}

		pvcMap[podKey] = append(pvcMap[podKey], &PVC{
			Bytes:  res.Values[0].Value,
			Count:  podCount[podKey],
			Name:   name,
			Volume: pvMap[pvKey],
		})
	}

	for _, res := range resPVCInfo {
		values, err := res.GetStrings("persistentvolumeclaim", "storageclass", "volumename", "namespace", "cluster_id")
		if err != nil {
			log.Warningf("CostModel.ComputeAllocation: PVC allocation query result missing field: %s", err)
			continue
		}

		cluster := values["cluster_id"]
		// TODO niko/cdmr ?
		// namespace := values["namespace"]
		// name := values["persistentvolumeclaim"]
		volume := values["volumename"]
		storageClass := values["storageclass"]

		pvKey := newPVKey(cluster, volume)

		if _, ok := pvMap[pvKey]; !ok {
			log.Warningf("CostModel.ComputeAllocation: PV missing for PVC allocation query result: %s", pvKey)
			continue
		}

		pvMap[pvKey].StorageClass = storageClass
	}

	for _, pv := range pvMap {
		log.Infof("CostModel.ComputeAllocation: PV: %v", pv)
	}

	for _, pvc := range pvcMap {
		log.Infof("CostModel.ComputeAllocation: PVC: %v", pvc)
	}

	for _, alloc := range allocMap {
		// TODO niko/cdmr compute costs from resources and prices?

		cluster, _ := alloc.Properties.GetCluster()
		namespace, _ := alloc.Properties.GetNamespace()
		pod, _ := alloc.Properties.GetPod()
		podKey := newPodKey(cluster, namespace, pod)

		if pvcs, ok := pvcMap[podKey]; ok {
			for _, pvc := range pvcs {
				// TODO niko/cdmr this isn't quite right... use PVC info query?
				hrs := alloc.Minutes() / 60.0
				gib := pvc.Bytes / 1024 / 1024 / 1024

				alloc.PVByteHours += pvc.Bytes * hrs
				alloc.PVCost += pvc.Volume.CostPerGiBHour * gib * hrs
			}
		}

		log.Infof("CostModel.ComputeAllocation: %s: %v", alloc.Name, alloc)

		allocSet.Set(alloc)
	}

	return allocSet, nil
}

// TODO niko/cdmr move to pkg/kubecost
// TODO niko/cdmr add PersistenVolumeClaims to type Allocation?
type PVC struct {
	Bytes  float64 `json:"bytes"`
	Count  int     `json:"count"`
	Name   string  `json:"name"`
	Volume *PV     `json:"persistentVolume"`
}

// TODO niko/cdmr move to pkg/kubecost
type PV struct {
	Bytes          float64 `json:"bytes"`
	CostPerGiBHour float64 `json:"costPerGiBHour"` // TODO niko/cdmr GiB or GB?
	Name           string  `json:"name"`
	StorageClass   string  `json:"storageClass"`
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

type pvKey struct {
	Cluster string
	Name    string
}

func (k pvKey) String() string {
	return fmt.Sprintf("%s/%s", k.Cluster, k.Name)
}

func newPVKey(cluster, name string) pvKey {
	return pvKey{
		Cluster: cluster,
		Name:    name,
	}
}

func resultPVKey(res *prom.QueryResult, clusterLabel, nameLabel string) (pvKey, error) {
	key := pvKey{}

	cluster, err := res.GetString(clusterLabel)
	if err != nil {
		cluster = env.GetClusterID()
	}
	key.Cluster = cluster

	name, err := res.GetString(nameLabel)
	if err != nil {
		return key, err
	}
	key.Name = name

	return key, nil
}
