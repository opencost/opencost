package costmodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"
)

const (
	// https://kubecost.atlassian.net/browse/BURNDOWN-234
	// upstream KSM has implementation change vs OC internal KSM - it sets metric to 0 when pod goes down
	// VS OC implementation which stops emitting it
	// by adding != 0 filter, we keep just the active times in the prom result
	queryFmtPods    = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, %s)[%s:%s]`
	queryFmtPodsUID = `avg(kube_pod_container_status_running{%s} != 0) by (pod, namespace, uid, %s)[%s:%s]`

	queryFmtRAMBytesAllocated           = `avg(avg_over_time(container_memory_allocation_bytes{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s, provider_id)`
	queryFmtRAMRequests                 = `avg(avg_over_time(kube_pod_container_resource_requests{resource="memory", unit="byte", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtRAMUsageAvg                 = `avg(avg_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	queryFmtRAMUsageMax                 = `max(max_over_time(container_memory_working_set_bytes{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	queryFmtCPUCoresAllocated           = `avg(avg_over_time(container_cpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtCPURequests                 = `avg(avg_over_time(kube_pod_container_resource_requests{resource="cpu", unit="core", container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtCPUUsageAvg                 = `avg(rate(container_cpu_usage_seconds_total{container!="", container_name!="POD", container!="POD", %s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	queryFmtGPUsRequested               = `avg(avg_over_time(kube_pod_container_resource_requests{resource="nvidia_com_gpu", container!="",container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtGPUsAllocated               = `avg(avg_over_time(container_gpu_allocation{container!="", container!="POD", node!="", %s}[%s])) by (container, pod, namespace, node, %s)`
	queryFmtNodeCostPerCPUHr            = `avg(avg_over_time(node_cpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	queryFmtNodeCostPerRAMGiBHr         = `avg(avg_over_time(node_ram_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	queryFmtNodeCostPerGPUHr            = `avg(avg_over_time(node_gpu_hourly_cost{%s}[%s])) by (node, %s, instance_type, provider_id)`
	queryFmtNodeIsSpot                  = `avg_over_time(kubecost_node_is_spot{%s}[%s])`
	queryFmtPVCInfo                     = `avg(kube_persistentvolumeclaim_info{volumename != "", %s}) by (persistentvolumeclaim, storageclass, volumename, namespace, %s)[%s:%s]`
	queryFmtPodPVCAllocation            = `avg(avg_over_time(pod_pvc_allocation{%s}[%s])) by (persistentvolume, persistentvolumeclaim, pod, namespace, %s)`
	queryFmtPVCBytesRequested           = `avg(avg_over_time(kube_persistentvolumeclaim_resource_requests_storage_bytes{%s}[%s])) by (persistentvolumeclaim, namespace, %s)`
	queryFmtPVActiveMins                = `count(kube_persistentvolume_capacity_bytes{%s}) by (persistentvolume, %s)[%s:%s]`
	queryFmtPVBytes                     = `avg(avg_over_time(kube_persistentvolume_capacity_bytes{%s}[%s])) by (persistentvolume, %s)`
	queryFmtPVCostPerGiBHour            = `avg(avg_over_time(pv_hourly_cost{%s}[%s])) by (volumename, %s)`
	queryFmtPVMeta                      = `avg(avg_over_time(kubecost_pv_info{%s}[%s])) by (%s, persistentvolume, provider_id)`
	queryFmtNetZoneGiB                  = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="true", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	queryFmtNetZoneCostPerGiB           = `avg(avg_over_time(kubecost_network_zone_egress_cost{%s}[%s])) by (%s)`
	queryFmtNetRegionGiB                = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="false", same_zone="false", same_region="false", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	queryFmtNetRegionCostPerGiB         = `avg(avg_over_time(kubecost_network_region_egress_cost{%s}[%s])) by (%s)`
	queryFmtNetInternetGiB              = `sum(increase(kubecost_pod_network_egress_bytes_total{internet="true", %s}[%s])) by (pod_name, namespace, %s) / 1024 / 1024 / 1024`
	queryFmtNetInternetCostPerGiB       = `avg(avg_over_time(kubecost_network_internet_egress_cost{%s}[%s])) by (%s)`
	queryFmtNetReceiveBytes             = `sum(increase(container_network_receive_bytes_total{pod!="", %s}[%s])) by (pod_name, pod, namespace, %s)`
	queryFmtNetTransferBytes            = `sum(increase(container_network_transmit_bytes_total{pod!="", %s}[%s])) by (pod_name, pod, namespace, %s)`
	queryFmtNodeLabels                  = `avg_over_time(kube_node_labels{%s}[%s])`
	queryFmtNamespaceLabels             = `avg_over_time(kube_namespace_labels{%s}[%s])`
	queryFmtNamespaceAnnotations        = `avg_over_time(kube_namespace_annotations{%s}[%s])`
	queryFmtPodLabels                   = `avg_over_time(kube_pod_labels{%s}[%s])`
	queryFmtPodAnnotations              = `avg_over_time(kube_pod_annotations{%s}[%s])`
	queryFmtServiceLabels               = `avg_over_time(service_selector_labels{%s}[%s])`
	queryFmtDeploymentLabels            = `avg_over_time(deployment_match_labels{%s}[%s])`
	queryFmtStatefulSetLabels           = `avg_over_time(statefulSet_match_labels{%s}[%s])`
	queryFmtDaemonSetLabels             = `sum(avg_over_time(kube_pod_owner{owner_kind="DaemonSet", %s}[%s])) by (pod, owner_name, namespace, %s)`
	queryFmtJobLabels                   = `sum(avg_over_time(kube_pod_owner{owner_kind="Job", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	queryFmtPodsWithReplicaSetOwner     = `sum(avg_over_time(kube_pod_owner{owner_kind="ReplicaSet", %s}[%s])) by (pod, owner_name, namespace ,%s)`
	queryFmtReplicaSetsWithoutOwners    = `avg(avg_over_time(kube_replicaset_owner{owner_kind="<none>", owner_name="<none>", %s}[%s])) by (replicaset, namespace, %s)`
	queryFmtReplicaSetsWithRolloutOwner = `avg(avg_over_time(kube_replicaset_owner{owner_kind="Rollout", %s}[%s])) by (replicaset, namespace, owner_kind, owner_name, %s)`
	queryFmtLBCostPerHr                 = `avg(avg_over_time(kubecost_load_balancer_cost{%s}[%s])) by (namespace, service_name, ingress_ip, %s)`
	queryFmtLBActiveMins                = `count(kubecost_load_balancer_cost{%s}) by (namespace, service_name, %s)[%s:%s]`
	queryFmtOldestSample                = `min_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`
	queryFmtNewestSample                = `max_over_time(timestamp(group(node_cpu_hourly_cost{%s}))[%s:%s])`

	// Because we use container_cpu_usage_seconds_total to calculate CPU usage
	// at any given "instant" of time, we need to use an irate or rate. To then
	// calculate a max (or any aggregation) we have to perform an aggregation
	// query on top of an instant-by-instant maximum. Prometheus supports this
	// type of query with a "subquery" [1], however it is reportedly expensive
	// to make such a query. By default, Kubecost's Prometheus config includes
	// a recording rule that keeps track of the instant-by-instant irate for CPU
	// usage. The metric in this query is created by that recording rule.
	//
	// [1] https://prometheus.io/blog/2019/01/28/subquery-support/
	//
	// If changing the name of the recording rule, make sure to update the
	// corresponding diagnostic query to avoid confusion.
	queryFmtCPUUsageMaxRecordingRule = `max(max_over_time(kubecost_container_cpu_usage_irate{%s}[%s])) by (container_name, container, pod_name, pod, namespace, instance, %s)`
	// This is the subquery equivalent of the above recording rule query. It is
	// more expensive, but does not require the recording rule. It should be
	// used as a fallback query if the recording rule data does not exist.
	//
	// The parameter after the colon [:<thisone>] in the subquery affects the
	// resolution of the subquery.
	// The parameter after the metric ...{}[<thisone>] should be set to 2x
	// the resolution, to make sure the irate always has two points to query
	// in case the Prom scrape duration has been reduced to be equal to the
	// ETL resolution.
	queryFmtCPUUsageMaxSubquery = `max(max_over_time(irate(container_cpu_usage_seconds_total{container!="POD", container!="", %s}[%s])[%s:%s])) by (container, pod_name, pod, namespace, instance, %s)`
)

// Constants for Network Cost Subtype
const (
	networkCrossZoneCost   = "NetworkCrossZoneCost"
	networkCrossRegionCost = "NetworkCrossRegionCost"
	networkInternetCost    = "NetworkInternetCost"
)

// CanCompute should return true if CostModel can act as a valid source for the
// given time range. In the case of CostModel we want to attempt to compute as
// long as the range starts in the past. If the CostModel ends up not having
// data to match, that's okay, and should be communicated with an error
// response from ComputeAllocation.
func (cm *CostModel) CanCompute(start, end time.Time) bool {
	return start.Before(time.Now())
}

// Name returns the name of the Source
func (cm *CostModel) Name() string {
	return "CostModel"
}

// ComputeAllocation uses the CostModel instance to compute an AllocationSet
// for the window defined by the given start and end times. The Allocations
// returned are unaggregated (i.e. down to the container level).
func (cm *CostModel) ComputeAllocation(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, error) {

	// If the duration is short enough, compute the AllocationSet directly
	if end.Sub(start) <= cm.MaxPrometheusQueryDuration {
		as, _, err := cm.computeAllocation(start, end, resolution)
		return as, err
	}

	// If the duration exceeds the configured MaxPrometheusQueryDuration, then
	// query for maximum-sized AllocationSets, collect them, and accumulate.

	// s and e track the coverage of the entire given window over multiple
	// internal queries.
	s, e := start, start

	// Collect AllocationSets in a range, then accumulate
	// TODO optimize by collecting consecutive AllocationSets, accumulating as we go
	asr := opencost.NewAllocationSetRange()

	for e.Before(end) {
		// By default, query for the full remaining duration. But do not let
		// any individual query duration exceed the configured max Prometheus
		// query duration.
		duration := end.Sub(e)
		if duration > cm.MaxPrometheusQueryDuration {
			duration = cm.MaxPrometheusQueryDuration
		}

		// Set start and end parameters (s, e) for next individual computation.
		e = s.Add(duration)

		// Compute the individual AllocationSet for just (s, e)
		as, _, err := cm.computeAllocation(s, e, resolution)
		if err != nil {
			return opencost.NewAllocationSet(start, end), fmt.Errorf("error computing allocation for %s: %s", opencost.NewClosedWindow(s, e), err)
		}

		// Append to the range
		asr.Append(as)

		// Set s equal to e to set up the next query, if one exists.
		s = e
	}

	// Populate annotations, labels, and services on each Allocation. This is
	// necessary because Properties.Intersection does not propagate any values
	// stored in maps or slices for performance reasons. In this case, however,
	// it is both acceptable and necessary to do so.
	allocationAnnotations := map[string]map[string]string{}
	allocationLabels := map[string]map[string]string{}
	allocationServices := map[string]map[string]bool{}

	// Also record errors and warnings, then append them to the results later.
	errors := []string{}
	warnings := []string{}

	for _, as := range asr.Allocations {
		for k, a := range as.Allocations {
			if len(a.Properties.Annotations) > 0 {
				if _, ok := allocationAnnotations[k]; !ok {
					allocationAnnotations[k] = map[string]string{}
				}
				for name, val := range a.Properties.Annotations {
					allocationAnnotations[k][name] = val
				}
			}

			if len(a.Properties.Labels) > 0 {
				if _, ok := allocationLabels[k]; !ok {
					allocationLabels[k] = map[string]string{}
				}
				for name, val := range a.Properties.Labels {
					allocationLabels[k][name] = val
				}
			}

			if len(a.Properties.Services) > 0 {
				if _, ok := allocationServices[k]; !ok {
					allocationServices[k] = map[string]bool{}
				}
				for _, val := range a.Properties.Services {
					allocationServices[k][val] = true
				}
			}
		}

		errors = append(errors, as.Errors...)
		warnings = append(warnings, as.Warnings...)
	}

	// Accumulate to yield the result AllocationSet. After this step, we will
	// be nearly complete, but without the raw allocation data, which must be
	// recomputed.
	resultASR, err := asr.Accumulate(opencost.AccumulateOptionAll)
	if err != nil {
		return opencost.NewAllocationSet(start, end), fmt.Errorf("error accumulating data for %s: %s", opencost.NewClosedWindow(s, e), err)
	}
	if resultASR != nil && len(resultASR.Allocations) == 0 {
		return opencost.NewAllocationSet(start, end), nil
	}
	if length := len(resultASR.Allocations); length != 1 {
		return opencost.NewAllocationSet(start, end), fmt.Errorf("expected 1 accumulated allocation set, found %d sets", length)
	}
	result := resultASR.Allocations[0]

	// Apply the annotations, labels, and services to the post-accumulation
	// results. (See above for why this is necessary.)
	for k, a := range result.Allocations {
		if annotations, ok := allocationAnnotations[k]; ok {
			a.Properties.Annotations = annotations
		}

		if labels, ok := allocationLabels[k]; ok {
			a.Properties.Labels = labels
		}

		if services, ok := allocationServices[k]; ok {
			a.Properties.Services = []string{}
			for s := range services {
				a.Properties.Services = append(a.Properties.Services, s)
			}
		}

		// Expand the Window of all Allocations within the AllocationSet
		// to match the Window of the AllocationSet, which gets expanded
		// at the end of this function.
		a.Window = a.Window.ExpandStart(start).ExpandEnd(end)
	}

	// Maintain RAM and CPU max usage values by iterating over the range,
	// computing maximums on a rolling basis, and setting on the result set.
	for _, as := range asr.Allocations {
		for key, alloc := range as.Allocations {
			resultAlloc := result.Get(key)
			if resultAlloc == nil {
				continue
			}

			if resultAlloc.RawAllocationOnly == nil {
				resultAlloc.RawAllocationOnly = &opencost.RawAllocationOnlyData{}
			}

			if alloc.RawAllocationOnly == nil {
				// This will happen inevitably for unmounted disks, but should
				// ideally not happen for any allocation with CPU and RAM data.
				if !alloc.IsUnmounted() {
					log.DedupedWarningf(10, "ComputeAllocation: raw allocation data missing for %s", key)
				}
				continue
			}

			if alloc.RawAllocationOnly.CPUCoreUsageMax > resultAlloc.RawAllocationOnly.CPUCoreUsageMax {
				resultAlloc.RawAllocationOnly.CPUCoreUsageMax = alloc.RawAllocationOnly.CPUCoreUsageMax
			}

			if alloc.RawAllocationOnly.RAMBytesUsageMax > resultAlloc.RawAllocationOnly.RAMBytesUsageMax {
				resultAlloc.RawAllocationOnly.RAMBytesUsageMax = alloc.RawAllocationOnly.RAMBytesUsageMax
			}
		}
	}

	// Expand the window to match the queried time range.
	result.Window = result.Window.ExpandStart(start).ExpandEnd(end)

	// Append errors and warnings
	result.Errors = errors
	result.Warnings = warnings

	// Convert any NaNs to 0 to avoid JSON marshaling issues and avoid cascading NaN appearances elsewhere
	result.SanitizeNaN()

	return result, nil
}

// DateRange checks the data (up to 90 days in the past), and returns the oldest and newest sample timestamp from opencost scraping metric
// it supposed to be a good indicator of available allocation data
func (cm *CostModel) DateRange() (time.Time, time.Time, error) {
	ctx := prom.NewNamedContext(cm.PrometheusClient, prom.AllocationContextName)
	exportCsvDaysFmt := fmt.Sprintf("%dd", env.GetExportCSVMaxDays())

	resOldest, _, err := ctx.QuerySync(fmt.Sprintf(queryFmtOldestSample, env.GetPromClusterFilter(), exportCsvDaysFmt, "1h"), time.Now())
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: %w", err)
	}
	if len(resOldest) == 0 || len(resOldest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying oldest sample: no results")
	}
	oldest := time.Unix(int64(resOldest[0].Values[0].Value), 0)

	resNewest, _, err := ctx.QuerySync(fmt.Sprintf(queryFmtNewestSample, env.GetPromClusterFilter(), exportCsvDaysFmt, "1h"), time.Now())
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: %w", err)
	}
	if len(resNewest) == 0 || len(resNewest[0].Values) == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("querying newest sample: no results")
	}
	newest := time.Unix(int64(resNewest[0].Values[0].Value), 0)

	return oldest, newest, nil
}

type allocationPromData struct {
	RAMBytesAllocated           []*prom.QueryResult
	RAMRequests                 []*prom.QueryResult
	RAMUsageAvg                 []*prom.QueryResult
	RAMUsageMax                 []*prom.QueryResult
	CPUCoresAllocated           []*prom.QueryResult
	CPUCoresRequest             []*prom.QueryResult
	CPUUsageAvg                 []*prom.QueryResult
	GPURequested                []*prom.QueryResult
	GPUAllocated                []*prom.QueryResult
	NodeCostPerCPUHr            []*prom.QueryResult
	NodeCostPerRAMGiBHr         []*prom.QueryResult
	NodeCostPerGPUHr            []*prom.QueryResult
	NodeIsSpot                  []*prom.QueryResult
	PVCInfo                     []*prom.QueryResult
	PodPVCAllocation            []*prom.QueryResult
	PVCBytesRequested           []*prom.QueryResult
	PVActiveMins                []*prom.QueryResult
	PVBytes                     []*prom.QueryResult
	PVCostPerGiBHour            []*prom.QueryResult
	PVMeta                      []*prom.QueryResult
	NetTransferBytes            []*prom.QueryResult
	NetReceiveBytes             []*prom.QueryResult
	NetZoneGiB                  []*prom.QueryResult
	NetZoneCostPerGiB           []*prom.QueryResult
	NetRegionGiB                []*prom.QueryResult
	NetRegionCostPerGiB         []*prom.QueryResult
	NetInternetGiB              []*prom.QueryResult
	NetInternetCostPerGiB       []*prom.QueryResult
	NodeLabels                  []*prom.QueryResult
	NamespaceLabels             []*prom.QueryResult
	NamespaceAnnotations        []*prom.QueryResult
	PodLabels                   []*prom.QueryResult
	PodAnnotations              []*prom.QueryResult
	ServiceLabels               []*prom.QueryResult
	DeploymentLabels            []*prom.QueryResult
	StatefulSetLabels           []*prom.QueryResult
	DaemonSetLabels             []*prom.QueryResult
	PodsWithReplicaSetOwner     []*prom.QueryResult
	ReplicaSetsWithoutOwners    []*prom.QueryResult
	ReplicaSetsWithRolloutOwner []*prom.QueryResult
	JobLabels                   []*prom.QueryResult
	LBCostPerHr                 []*prom.QueryResult
	LBActiveMins                []*prom.QueryResult
	CPUUsageMax                 []*prom.QueryResult

	NodeExtendedData *extendedNodeQueryResults
	PodMap           map[podKey]*pod
	PodUIDKeyMap     map[podKey][]podKey
}

type promQuery struct {
	out   *[]*prom.QueryResult
	query string
}

func (cm *CostModel) execAllPromQueries(queries []promQuery, time time.Time) error {
	ctx := prom.NewNamedContext(cm.PrometheusClient, prom.AllocationContextName)
	// Run all queries concurrently
	resultChs := make([]prom.QueryResultsChan, len(queries))
	for i := range queries {
		resultChs[i] = ctx.QueryAtTime(queries[i].query, time)
	}

	// match result to query by index
	for i := range queries {
		resp, _ := resultChs[i].Await()
		*queries[i].out = resp
	}

	if ctx.HasErrors() {
		for _, err := range ctx.Errors() {
			log.Errorf("CostModel.ComputeAllocation: query context error %s", err)
		}
		return ctx.ErrorCollection()
	}

	return nil
}

func (cm *CostModel) fetchAllocationPromData(start, end time.Time, resolution time.Duration, ingestPodUID bool) (*allocationPromData, error) {
	data := &allocationPromData{}
	// Query for the duration between start and end
	durStr := timeutil.DurationString(end.Sub(start))
	if durStr == "" {
		return nil, fmt.Errorf("illegal duration value for %s", opencost.NewClosedWindow(start, end))
	}

	// Convert resolution duration to a query-ready string
	resStr := timeutil.DurationString(resolution)

	queries := []promQuery{
		{&data.CPUCoresAllocated, fmt.Sprintf(queryFmtCPUCoresAllocated, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.CPUCoresRequest, fmt.Sprintf(queryFmtCPURequests, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.CPUUsageAvg, fmt.Sprintf(queryFmtCPUUsageAvg, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.CPUUsageMax, fmt.Sprintf(queryFmtCPUUsageMaxRecordingRule, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.DaemonSetLabels, fmt.Sprintf(queryFmtDaemonSetLabels, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.DeploymentLabels, fmt.Sprintf(queryFmtDeploymentLabels, env.GetPromClusterFilter(), durStr)},
		{&data.GPUAllocated, fmt.Sprintf(queryFmtGPUsAllocated, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.GPURequested, fmt.Sprintf(queryFmtGPUsRequested, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.JobLabels, fmt.Sprintf(queryFmtJobLabels, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.LBActiveMins, fmt.Sprintf(queryFmtLBActiveMins, env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)},
		{&data.LBCostPerHr, fmt.Sprintf(queryFmtLBCostPerHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NamespaceAnnotations, fmt.Sprintf(queryFmtNamespaceAnnotations, env.GetPromClusterFilter(), durStr)},
		{&data.NamespaceLabels, fmt.Sprintf(queryFmtNamespaceLabels, env.GetPromClusterFilter(), durStr)},
		{&data.NetInternetCostPerGiB, fmt.Sprintf(queryFmtNetInternetCostPerGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetInternetGiB, fmt.Sprintf(queryFmtNetInternetGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetReceiveBytes, fmt.Sprintf(queryFmtNetReceiveBytes, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetRegionCostPerGiB, fmt.Sprintf(queryFmtNetRegionCostPerGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetRegionGiB, fmt.Sprintf(queryFmtNetRegionGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetTransferBytes, fmt.Sprintf(queryFmtNetTransferBytes, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetZoneCostPerGiB, fmt.Sprintf(queryFmtNetZoneCostPerGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NetZoneGiB, fmt.Sprintf(queryFmtNetZoneGiB, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NodeCostPerCPUHr, fmt.Sprintf(queryFmtNodeCostPerCPUHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NodeCostPerGPUHr, fmt.Sprintf(queryFmtNodeCostPerGPUHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NodeCostPerRAMGiBHr, fmt.Sprintf(queryFmtNodeCostPerRAMGiBHr, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.NodeIsSpot, fmt.Sprintf(queryFmtNodeIsSpot, env.GetPromClusterFilter(), durStr)},
		{&data.PVActiveMins, fmt.Sprintf(queryFmtPVActiveMins, env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)},
		{&data.PVBytes, fmt.Sprintf(queryFmtPVBytes, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.PVCBytesRequested, fmt.Sprintf(queryFmtPVCBytesRequested, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.PVCInfo, fmt.Sprintf(queryFmtPVCInfo, env.GetPromClusterFilter(), env.GetPromClusterLabel(), durStr, resStr)},
		{&data.PVCostPerGiBHour, fmt.Sprintf(queryFmtPVCostPerGiBHour, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.PVMeta, fmt.Sprintf(queryFmtPVMeta, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.PodAnnotations, fmt.Sprintf(queryFmtPodAnnotations, env.GetPromClusterFilter(), durStr)},
		{&data.PodLabels, fmt.Sprintf(queryFmtPodLabels, env.GetPromClusterFilter(), durStr)},
		{&data.PodPVCAllocation, fmt.Sprintf(queryFmtPodPVCAllocation, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.PodsWithReplicaSetOwner, fmt.Sprintf(queryFmtPodsWithReplicaSetOwner, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.RAMBytesAllocated, fmt.Sprintf(queryFmtRAMBytesAllocated, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.RAMRequests, fmt.Sprintf(queryFmtRAMRequests, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.RAMUsageAvg, fmt.Sprintf(queryFmtRAMUsageAvg, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.RAMUsageMax, fmt.Sprintf(queryFmtRAMUsageMax, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.ReplicaSetsWithRolloutOwner, fmt.Sprintf(queryFmtReplicaSetsWithRolloutOwner, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.ReplicaSetsWithoutOwners, fmt.Sprintf(queryFmtReplicaSetsWithoutOwners, env.GetPromClusterFilter(), durStr, env.GetPromClusterLabel())},
		{&data.ServiceLabels, fmt.Sprintf(queryFmtServiceLabels, env.GetPromClusterFilter(), durStr)},
		{&data.StatefulSetLabels, fmt.Sprintf(queryFmtStatefulSetLabels, env.GetPromClusterFilter(), durStr)},
	}
	if env.GetAllocationNodeLabelsEnabled() {
		queries = append(queries, promQuery{&data.NodeLabels, fmt.Sprintf(queryFmtNodeLabels, env.GetPromClusterFilter(), durStr)})
	}

	if err := cm.execAllPromQueries(queries, end); err != nil {
		return nil, err
	}

	if len(data.CPUUsageMax) == 0 {
		// The parameter after the metric ...{}[<thisone>] should be set to 2x
		// the resolution, to make sure the irate always has two points to query
		// in case the Prom scrape duration has been reduced to be equal to the
		// resolution.
		doubleResStr := timeutil.DurationString(2 * resolution)
		ctx := prom.NewNamedContext(cm.PrometheusClient, prom.AllocationContextName)
		var err error
		data.CPUUsageMax, _, err = ctx.QuerySync(fmt.Sprintf(queryFmtCPUUsageMaxSubquery, env.GetPromClusterFilter(), doubleResStr, durStr, resStr, env.GetPromClusterLabel()), end)
		if err != nil {
			return nil, fmt.Errorf("querying CPU usage max subquery: %w", err)
		}

		// This avoids logspam if there is no data for either metric (e.g. if
		// the Prometheus didn't exist in the queried window of time).
		if len(data.CPUUsageMax) > 0 {
			log.Debugf("CPU usage recording rule query returned an empty result when queried at %s over %s. Fell back to subquery. Consider setting up Kubecost CPU usage recording role to reduce query load on Prometheus; subqueries are expensive.", end.String(), durStr)
		}
	}
	ctx := prom.NewNamedContext(cm.PrometheusClient, prom.AllocationContextName)
	data.NodeExtendedData, _ = queryExtendedNodeData(ctx, start, end, durStr, resStr)

	// (1) Build out Pod map

	// Build out a map of Allocations as a mapping from pod-to-container-to-
	// underlying-Allocation instance, starting with (start, end) so that we
	// begin with minutes, from which we compute resource allocation and cost
	// totals from measured rate data.
	data.PodMap = make(map[podKey]*pod)

	// If ingesting pod UID, we query kube_pod_container_status_running avg
	// by uid as well as the default values, and all podKeys/pods have their
	// names changed to "<pod_name> <pod_uid>". Because other metrics need
	// to generate keys to match pods but don't have UIDs, podUIDKeyMap
	// stores values of format:

	// default podKey : []{edited podkey 1, edited podkey 2}

	// This is because ingesting UID allows us to catch uncontrolled pods
	// with the same names. However, this will lead to a many-to-one metric
	// to podKey relation, so this map allows us to map the metric's
	// "<pod_name>" key to the edited "<pod_name> <pod_uid>" keys in podMap.
	data.PodUIDKeyMap = make(map[podKey][]podKey)

	if ingestPodUID {
		log.Debugf("CostModel.ComputeAllocation: ingesting UID data from KSM metrics...")
	}
	window := opencost.NewWindow(&start, &end)
	// TODO:CLEANUP remove "max batch" idea and clusterStart/End
	err := cm.buildPodMap(window, resolution, env.GetETLMaxPrometheusQueryDuration(), data.PodMap, ingestPodUID, data.PodUIDKeyMap)
	if err != nil {
		log.Errorf("CostModel.ComputeAllocation: failed to build pod map: %s", err.Error())
	}

	return data, nil
}

func (cm *CostModel) computeAllocation(start, end time.Time, resolution time.Duration) (*opencost.AllocationSet, map[nodeKey]*nodePricing, error) {
	ingestPodUID := env.IsIngestingPodUID()
	data, err := cm.fetchAllocationPromData(start, end, resolution, ingestPodUID)
	if err != nil {
		return nil, nil, err
	}
	return cm.calcAllocation(start, end, resolution, data, ingestPodUID)
}

func (cm *CostModel) calcAllocation(start, end time.Time, resolution time.Duration, promData *allocationPromData, ingestPodUID bool) (*opencost.AllocationSet, map[nodeKey]*nodePricing, error) {
	// 1. Build out Pod map from resolution-tuned, batched Pod start/end query
	// 2. Run and apply the results of the remaining queries to
	// 3. Build out AllocationSet from completed Pod map

	// Create a window spanning the requested query
	window := opencost.NewWindow(&start, &end)

	// Create an empty AllocationSet. For safety, in the case of an error, we
	// should prefer to return this empty set with the error. (In the case of
	// no error, of course we populate the set and return it.)
	allocSet := opencost.NewAllocationSet(start, end)

	// (2) Run and apply remaining queries

	// We choose to apply allocation before requests in the cases of RAM and
	// CPU so that we can assert that allocation should always be greater than
	// or equal to request.
	applyCPUCoresAllocated(promData.PodMap, promData.CPUCoresAllocated, promData.PodUIDKeyMap)
	applyCPUCoresRequested(promData.PodMap, promData.CPUCoresRequest, promData.PodUIDKeyMap)
	applyCPUCoresUsedAvg(promData.PodMap, promData.CPUUsageAvg, promData.PodUIDKeyMap)
	applyCPUCoresUsedMax(promData.PodMap, promData.CPUUsageMax, promData.PodUIDKeyMap)
	applyRAMBytesAllocated(promData.PodMap, promData.RAMBytesAllocated, promData.PodUIDKeyMap)
	applyRAMBytesRequested(promData.PodMap, promData.RAMRequests, promData.PodUIDKeyMap)
	applyRAMBytesUsedAvg(promData.PodMap, promData.RAMUsageAvg, promData.PodUIDKeyMap)
	applyRAMBytesUsedMax(promData.PodMap, promData.RAMUsageMax, promData.PodUIDKeyMap)
	applyGPUsAllocated(promData.PodMap, promData.GPURequested, promData.GPUAllocated, promData.PodUIDKeyMap)
	applyNetworkTotals(promData.PodMap, promData.NetTransferBytes, promData.NetReceiveBytes, promData.PodUIDKeyMap)
	applyNetworkAllocation(promData.PodMap, promData.NetZoneGiB, promData.NetZoneCostPerGiB, promData.PodUIDKeyMap, networkCrossZoneCost)
	applyNetworkAllocation(promData.PodMap, promData.NetRegionGiB, promData.NetRegionCostPerGiB, promData.PodUIDKeyMap, networkCrossRegionCost)
	applyNetworkAllocation(promData.PodMap, promData.NetInternetGiB, promData.NetInternetCostPerGiB, promData.PodUIDKeyMap, networkInternetCost)

	// In the case that a two pods with the same name had different containers,
	// we will double-count the containers. There is no way to associate each
	// container with the proper pod from the usage metrics above. This will
	// show up as a pod having two Allocations running for the whole pod runtime.

	// Other than that case, Allocations should be associated with pods by the
	// above functions.

	// At this point, we expect "Node" to be set by one of the above functions
	// (e.g. applyCPUCoresAllocated, etc.) -- otherwise, node labels will fail
	// to correctly apply to the pods.
	var nodeLabels map[nodeKey]map[string]string
	if env.GetAllocationNodeLabelsEnabled() {
		nodeLabels = resToNodeLabels(promData.NodeLabels)
	}

	namespaceLabels := resToNamespaceLabels(promData.NamespaceLabels)
	podLabels := resToPodLabels(promData.PodLabels, promData.PodUIDKeyMap, ingestPodUID)
	namespaceAnnotations := resToNamespaceAnnotations(promData.NamespaceAnnotations)
	podAnnotations := resToPodAnnotations(promData.PodAnnotations, promData.PodUIDKeyMap, ingestPodUID)
	applyLabels(promData.PodMap, nodeLabels, namespaceLabels, podLabels)
	applyAnnotations(promData.PodMap, namespaceAnnotations, podAnnotations)

	podDeploymentMap := labelsToPodControllerMap(podLabels, resToDeploymentLabels(promData.DeploymentLabels))
	podStatefulSetMap := labelsToPodControllerMap(podLabels, resToStatefulSetLabels(promData.StatefulSetLabels))
	podDaemonSetMap := resToPodDaemonSetMap(promData.DaemonSetLabels, promData.PodUIDKeyMap, ingestPodUID)
	podJobMap := resToPodJobMap(promData.JobLabels, promData.PodUIDKeyMap, ingestPodUID)
	podReplicaSetMap := resToPodReplicaSetMap(promData.PodsWithReplicaSetOwner, promData.ReplicaSetsWithoutOwners, promData.ReplicaSetsWithRolloutOwner, promData.PodUIDKeyMap, ingestPodUID)
	applyControllersToPods(promData.PodMap, podDeploymentMap)
	applyControllersToPods(promData.PodMap, podStatefulSetMap)
	applyControllersToPods(promData.PodMap, podDaemonSetMap)
	applyControllersToPods(promData.PodMap, podJobMap)
	applyControllersToPods(promData.PodMap, podReplicaSetMap)

	serviceLabels := getServiceLabels(promData.ServiceLabels)
	allocsByService := map[serviceKey][]*opencost.Allocation{}
	applyServicesToPods(promData.PodMap, podLabels, allocsByService, serviceLabels)

	// TODO breakdown network costs?

	// Build out the map of all PVs with class, size and cost-per-hour.
	// Note: this does not record time running, which we may want to
	// include later for increased PV precision. (As long as the PV has
	// a PVC, we get time running there, so this is only inaccurate
	// for short-lived, unmounted PVs.)
	pvMap := map[pvKey]*pv{}
	buildPVMap(resolution, pvMap, promData.PVCostPerGiBHour, promData.PVActiveMins, promData.PVMeta, window)
	applyPVBytes(pvMap, promData.PVBytes)

	// Build out the map of all PVCs with time running, bytes requested,
	// and connect to the correct PV from pvMap. (If no PV exists, that
	// is noted, but does not result in any allocation/cost.)
	pvcMap := map[pvcKey]*pvc{}
	buildPVCMap(resolution, pvcMap, pvMap, promData.PVCInfo, window)
	applyPVCBytesRequested(pvcMap, promData.PVCBytesRequested)

	// Build out the relationships of pods to their PVCs. This step
	// populates the pvc.Count field so that pvc allocation can be
	// split appropriately among each pod's container allocation.
	podPVCMap := map[podKey][]*pvc{}
	buildPodPVCMap(podPVCMap, pvMap, pvcMap, promData.PodMap, promData.PodPVCAllocation, promData.PodUIDKeyMap, ingestPodUID)
	applyPVCsToPods(window, promData.PodMap, podPVCMap, pvcMap)

	// Identify PVCs without pods and add pv costs to the unmounted Allocation for the pvc's cluster
	applyUnmountedPVCs(window, promData.PodMap, pvcMap)

	// Identify PVs without PVCs and add PV costs to the unmounted Allocation for the PV's cluster
	applyUnmountedPVs(window, promData.PodMap, pvMap, pvcMap)

	lbMap := make(map[serviceKey]*lbCost)
	getLoadBalancerCosts(lbMap, promData.LBCostPerHr, promData.LBActiveMins, resolution, window)
	applyLoadBalancersToPods(window, promData.PodMap, lbMap, allocsByService)

	// Build out a map of Nodes with resource costs, discounts, and node types
	// for converting resource allocation data to cumulative costs.
	nodeMap := map[nodeKey]*nodePricing{}

	applyNodeCostPerCPUHr(nodeMap, promData.NodeCostPerCPUHr)
	applyNodeCostPerRAMGiBHr(nodeMap, promData.NodeCostPerRAMGiBHr)
	applyNodeCostPerGPUHr(nodeMap, promData.NodeCostPerGPUHr)
	applyNodeSpot(nodeMap, promData.NodeIsSpot)
	applyNodeDiscount(nodeMap, cm)
	applyExtendedNodeData(nodeMap, promData.NodeExtendedData)
	cm.applyNodesToPod(promData.PodMap, nodeMap)

	// (3) Build out AllocationSet from Pod map
	for _, pod := range promData.PodMap {
		for _, alloc := range pod.Allocations {
			cluster := alloc.Properties.Cluster
			nodeName := alloc.Properties.Node
			namespace := alloc.Properties.Namespace
			podName := alloc.Properties.Pod
			container := alloc.Properties.Container

			// Make sure that the name is correct (node may not be present at this
			// point due to it missing from queryMinutes) then insert.
			alloc.Name = fmt.Sprintf("%s/%s/%s/%s/%s", cluster, nodeName, namespace, podName, container)
			allocSet.Set(alloc)
		}
	}

	return allocSet, nodeMap, nil
}
