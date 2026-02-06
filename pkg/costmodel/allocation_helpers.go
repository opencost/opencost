package costmodel

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/provider"
	"k8s.io/apimachinery/pkg/labels"
)

// This is a bit of a hack to work around garbage data from cadvisor
// Ideally you cap each pod to the max CPU on its node, but that involves a bit more complexity, as it it would need to be done when allocations joins with asset data.
const CPU_SANITY_LIMIT = 512

// Sanity Limit for PV usage, set to 10 PB, in bytes for now
const KiB = 1024.0
const MiB = 1024.0 * KiB
const GiB = 1024.0 * MiB
const TiB = 1024.0 * GiB
const PiB = 1024.0 * TiB
const PV_USAGE_SANITY_LIMIT_BYTES = 10.0 * PiB

const (
	GpuUsageAverageMode = "AVERAGE"
	GpuUsageMaxMode     = "MAX"
	GpuIsSharedMode     = "SHARED"
	GpuInfoMode         = "GPU_INFO"
)

/* Pod Helpers */

func (cm *CostModel) buildPodMap(window opencost.Window, podMap map[podKey]*pod, ingestPodUID bool, podUIDKeyMap map[podKey][]podKey) error {
	// Assumes that window is positive and closed
	start, end := *window.Start(), *window.End()

	grp := source.NewQueryGroup()
	ds := cm.DataSource.Metrics()
	resolution := cm.DataSource.Resolution()

	var resPods []*source.PodsResult
	var err error
	maxTries := 3
	numTries := 0
	for resPods == nil && numTries < maxTries {
		numTries++

		// Submit and profile query

		var queryPodsResult *source.QueryGroupFuture[source.PodsResult]
		if ingestPodUID {
			queryPodsResult = source.WithGroup(grp, ds.QueryPodsUID(start, end))
		} else {
			queryPodsResult = source.WithGroup(grp, ds.QueryPods(start, end))
		}

		queryProfile := time.Now()
		resPods, err = queryPodsResult.Await()
		if err != nil {
			log.Profile(queryProfile, fmt.Sprintf("CostModel.ComputeAllocation: pod query try %d failed: %s", numTries, err))
			resPods = nil
		}
	}

	if err != nil {
		return err
	}

	// queryFmtPodsUID will return both UID-containing results, and non-UID-containing results,
	// so filter out the non-containing results so we don't duplicate pods. This is due to the
	// default setup of Kubecost having replicated kube_pod_container_status_running and
	// included KSM kube_pod_container_status_running. Querying w/ UID will return both.
	if ingestPodUID {
		var resPodsUID []*source.PodsResult

		for _, res := range resPods {
			uid := res.UID
			if uid != "" {
				resPodsUID = append(resPodsUID, res)
			}
		}

		if len(resPodsUID) > 0 {
			resPods = resPodsUID
		} else {
			log.DedupedWarningf(5, "CostModel.ComputeAllocation: UID ingestion enabled, but query did not return any results with UID")
		}
	}

	applyPodResults(window, resolution, podMap, resPods, ingestPodUID, podUIDKeyMap)

	return nil
}

func applyPodResults(window opencost.Window, resolution time.Duration, podMap map[podKey]*pod, resPods []*source.PodsResult, ingestPodUID bool, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resPods {
		if len(res.Data) == 0 {
			log.Warnf("CostModel.ComputeAllocation: empty minutes result")
			continue
		}

		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		namespace := res.Namespace
		if namespace == "" {
			log.Warnf("CostModel.ComputeAllocation: minutes query result missing field: namespace")
			continue
		}

		podName := res.Pod
		if podName == "" {
			log.Warnf("CostModel.ComputeAllocation: minutes query result missing field: pod")
			continue
		}

		key := newPodKey(cluster, namespace, podName)

		// If thisPod UIDs are being used to ID pods, append them to the thisPod name in
		// the podKey.
		if ingestPodUID {

			uid := res.UID
			if uid == "" {
				log.Warnf("CostModel.ComputeAllocation: UID ingestion enabled, but query result missing field: uid")
			} else {
				newKey := newPodKey(cluster, namespace, podName+" "+uid)
				podUIDKeyMap[key] = append(podUIDKeyMap[key], newKey)

				key = newKey
			}

		}

		allocStart, allocEnd := calculateStartAndEnd(res.Data, resolution, window)
		if allocStart.IsZero() || allocEnd.IsZero() {
			continue
		}

		if thisPod, ok := podMap[key]; ok {
			// Pod has already been recorded, so update it accordingly
			if allocStart.Before(thisPod.Start) {
				thisPod.Start = allocStart
			}
			if allocEnd.After(thisPod.End) {
				thisPod.End = allocEnd
			}
		} else {
			// pod has not been recorded yet, so insert it
			podMap[key] = &pod{
				Window:      window.Clone(),
				Start:       allocStart,
				End:         allocEnd,
				Key:         key,
				Allocations: map[string]*opencost.Allocation{},
			}
		}
	}
}

func applyCPUCoresAllocated(podMap map[podKey]*pod, resCPUCoresAllocated []*source.CPUCoresAllocatedResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resCPUCoresAllocated {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU allocation result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU allocation query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			cpuCores := res.Data[0].Value
			if cpuCores > CPU_SANITY_LIMIT {
				log.Infof("[WARNING] Very large cpu allocation, clamping to %f", res.Data[0].Value*(thisPod.Allocations[container].Minutes()/60.0))
				cpuCores = 0.0
			}
			hours := thisPod.Allocations[container].Minutes() / 60.0
			thisPod.Allocations[container].CPUCoreHours = cpuCores * hours

			node := res.Node
			if node == "" {
				log.Warnf("CostModel.ComputeAllocation: CPU allocation query result missing 'node': %s", key)
				continue
			}
			thisPod.Allocations[container].Properties.Node = node
			thisPod.Node = node
		}
	}
}

func applyCPUCoresRequested(podMap map[podKey]*pod, resCPUCoresRequested []*source.CPURequestsResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resCPUCoresRequested {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU request result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU request query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			thisPod.Allocations[container].CPUCoreRequestAverage = res.Data[0].Value

			// If CPU allocation is less than requests, set CPUCoreHours to
			// request level.
			if thisPod.Allocations[container].CPUCores() < res.Data[0].Value {
				thisPod.Allocations[container].CPUCoreHours = res.Data[0].Value * (thisPod.Allocations[container].Minutes() / 60.0)
			}
			if thisPod.Allocations[container].CPUCores() > CPU_SANITY_LIMIT {
				log.Infof("[WARNING] Very large cpu allocation, clamping! to %f", res.Data[0].Value*(thisPod.Allocations[container].Minutes()/60.0))
				thisPod.Allocations[container].CPUCoreHours = res.Data[0].Value * (thisPod.Allocations[container].Minutes() / 60.0)
			}
			node := res.Node
			if node == "" {
				log.Warnf("CostModel.ComputeAllocation: CPU request query result missing 'node': %s", key)
				continue
			}
			thisPod.Allocations[container].Properties.Node = node
			thisPod.Node = node
		}
	}
}

func applyCPUCoresLimits(podMap map[podKey]*pod, resCPUCoresLimits []*source.CPULimitsResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resCPUCoresLimits {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU limit result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU limit query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			thisPod.Allocations[container].CPUCoreLimitAverage = res.Data[0].Value
		}
	}
}

func applyCPUCoresUsedAvg(podMap map[podKey]*pod, resCPUCoresUsedAvg []*source.CPUUsageAvgResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resCPUCoresUsedAvg {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage avg result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage avg query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			thisPod.Allocations[container].CPUCoreUsageAverage = res.Data[0].Value
			if res.Data[0].Value > CPU_SANITY_LIMIT {
				log.Infof("[WARNING] Very large cpu USAGE, dropping outlier")
				thisPod.Allocations[container].CPUCoreUsageAverage = 0.0
			}
		}
	}
}

func applyCPUCoresUsedMax(podMap map[podKey]*pod, resCPUCoresUsedMax []*source.CPUUsageMaxResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resCPUCoresUsedMax {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage max result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: CPU usage max query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			if thisPod.Allocations[container].RawAllocationOnly == nil {
				thisPod.Allocations[container].RawAllocationOnly = &opencost.RawAllocationOnlyData{
					CPUCoreUsageMax: res.Data[0].Value,
				}
			} else {
				thisPod.Allocations[container].RawAllocationOnly.CPUCoreUsageMax = res.Data[0].Value
			}
		}
	}
}

func applyRAMBytesAllocated(podMap map[podKey]*pod, resRAMBytesAllocated []*source.RAMBytesAllocatedResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resRAMBytesAllocated {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM allocation result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM allocation query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			ramBytes := res.Data[0].Value
			hours := thisPod.Allocations[container].Minutes() / 60.0
			thisPod.Allocations[container].RAMByteHours = ramBytes * hours

			node := res.Node
			if node == "" {
				log.Warnf("CostModel.ComputeAllocation: RAM allocation query result missing 'node': %s", key)
				continue
			}
			thisPod.Allocations[container].Properties.Node = node
			thisPod.Node = node
		}
	}
}

func applyRAMBytesRequested(podMap map[podKey]*pod, resRAMBytesRequested []*source.RAMRequestsResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resRAMBytesRequested {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM request result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM request query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, pod := range pods {

			if _, ok := pod.Allocations[container]; !ok {
				pod.appendContainer(container)
			}

			pod.Allocations[container].RAMBytesRequestAverage = res.Data[0].Value

			// If RAM allocation is less than requests, set RAMByteHours to
			// request level.
			if pod.Allocations[container].RAMBytes() < res.Data[0].Value {
				pod.Allocations[container].RAMByteHours = res.Data[0].Value * (pod.Allocations[container].Minutes() / 60.0)
			}

			node := res.Node
			if node == "" {
				log.Warnf("CostModel.ComputeAllocation: RAM request query result missing 'node': %s", key)
				continue
			}
			pod.Allocations[container].Properties.Node = node
			pod.Node = node
		}
	}
}

func applyRAMBytesLimits(podMap map[podKey]*pod, resRAMBytesLimits []*source.RAMLimitsResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resRAMBytesLimits {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM limit result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM limit query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, pod := range pods {

			if _, ok := pod.Allocations[container]; !ok {
				pod.appendContainer(container)
			}

			pod.Allocations[container].RAMBytesLimitAverage = res.Data[0].Value
		}
	}
}

func applyRAMBytesUsedAvg(podMap map[podKey]*pod, resRAMBytesUsedAvg []*source.RAMUsageAvgResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resRAMBytesUsedAvg {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM avg usage result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM usage avg query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			thisPod.Allocations[container].RAMBytesUsageAverage = res.Data[0].Value
		}
	}
}

func applyRAMBytesUsedMax(podMap map[podKey]*pod, resRAMBytesUsedMax []*source.RAMUsageMaxResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resRAMBytesUsedMax {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM usage max result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: RAM usage max query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			if thisPod.Allocations[container].RawAllocationOnly == nil {
				thisPod.Allocations[container].RawAllocationOnly = &opencost.RawAllocationOnlyData{
					RAMBytesUsageMax: res.Data[0].Value,
				}
			} else {
				thisPod.Allocations[container].RawAllocationOnly.RAMBytesUsageMax = res.Data[0].Value
			}
		}
	}
}

// apply gpu usage average to allocations
func applyGPUUsageAvg(podMap map[podKey]*pod, resGPUUsageAvg []*source.GPUsUsageAvgResult, podUIDKeyMap map[podKey][]podKey) {
	// Example PromQueryResult: {container="dcgmproftester12", namespace="gpu", pod="dcgmproftester3-deployment-fc89c8dd6-ph7z5"} 0.997307
	for _, res := range resGPUUsageAvg {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU usage avg result missing field: %s", err)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			container := res.Container
			if container == "" {
				log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU usage avg query result missing 'container': %s", key)
				continue
			}
			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			if thisPod.Allocations[container].GPUAllocation == nil {
				thisPod.Allocations[container].GPUAllocation = &opencost.GPUAllocation{GPUUsageAverage: &res.Data[0].Value}
			} else {
				thisPod.Allocations[container].GPUAllocation.GPUUsageAverage = &res.Data[0].Value
			}
		}
	}
}

// apply gpu usage max to allocations
func applyGPUUsageMax(podMap map[podKey]*pod, resGPUUsageMax []*source.GPUsUsageMaxResult, podUIDKeyMap map[podKey][]podKey) {
	// Example PromQueryResult: {container="dcgmproftester12", namespace="gpu", pod="dcgmproftester3-deployment-fc89c8dd6-ph7z5"} 0.997307
	for _, res := range resGPUUsageMax {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU usage max result missing field: %s", err)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			container := res.Container
			if container == "" {
				log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU usage max query result missing 'container': %s", key)
				continue
			}
			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			if thisPod.Allocations[container].RawAllocationOnly == nil {
				thisPod.Allocations[container].RawAllocationOnly = &opencost.RawAllocationOnlyData{
					GPUUsageMax: &res.Data[0].Value,
				}
			} else {
				thisPod.Allocations[container].RawAllocationOnly.GPUUsageMax = &res.Data[0].Value
			}
		}
	}
}

// apply gpu shared data to allocations
func applyGPUUsageShared(podMap map[podKey]*pod, resIsGPUShared []*source.IsGPUSharedResult, podUIDKeyMap map[podKey][]podKey) {
	// Example PromQueryResult: {container="dcgmproftester12", namespace="gpu", pod="dcgmproftester3-deployment-fc89c8dd6-ph7z5"} 0.997307
	for _, res := range resIsGPUShared {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU usage avg/max result missing field: %s", err)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			container := res.Container
			if container == "" {
				log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU is shared query result missing 'container': %s", key)
				continue
			}
			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			// if a container is using a GPU and it is shared, isGPUShared will be true
			// if a container is using GPU and it is NOT shared, isGPUShared will be false
			// if a container is NOT using a GPU, isGPUShared will be null
			if res.Resource == "nvidia_com_gpu_shared" {
				trueVal := true
				if res.Data[0].Value == 1 {
					if thisPod.Allocations[container].GPUAllocation == nil {
						thisPod.Allocations[container].GPUAllocation = &opencost.GPUAllocation{IsGPUShared: &trueVal}
					} else {
						thisPod.Allocations[container].GPUAllocation.IsGPUShared = &trueVal
					}
				}
			} else if res.Resource == "nvidia_com_gpu" {
				falseVal := false
				if res.Data[0].Value == 1 {
					if thisPod.Allocations[container].GPUAllocation == nil {
						thisPod.Allocations[container].GPUAllocation = &opencost.GPUAllocation{IsGPUShared: &falseVal}
					} else {
						thisPod.Allocations[container].GPUAllocation.IsGPUShared = &falseVal
					}
				}
			} else {
				continue
			}
		}
	}
}

// apply gpu info to allocations
func applyGPUInfo(podMap map[podKey]*pod, resGPUInfo []*source.GPUInfoResult, podUIDKeyMap map[podKey][]podKey) {
	// Example PromQueryResult: {container="dcgmproftester12", namespace="gpu", pod="dcgmproftester3-deployment-fc89c8dd6-ph7z5"} 0.997307
	for _, res := range resGPUInfo {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU Info query result missing field: %s", err)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			container := res.Container
			if container == "" {
				log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU Info query result missing 'container': %s", key)
				continue
			}
			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			// DCGM_FI_PROF_GR_ENGINE_ACTIVE metric is a float between 0-1.
			if thisPod.Allocations[container].GPUAllocation == nil {
				thisPod.Allocations[container].GPUAllocation = &opencost.GPUAllocation{
					GPUDevice: getSanitizedDeviceName(res.Device),
					GPUModel:  res.ModelName,
					GPUUUID:   res.UUID,
				}
			} else {
				thisPod.Allocations[container].GPUAllocation.GPUDevice = getSanitizedDeviceName(res.Device)
				thisPod.Allocations[container].GPUAllocation.GPUModel = res.ModelName
				thisPod.Allocations[container].GPUAllocation.GPUUUID = res.UUID
			}
		}
	}
}

func applyGPUsAllocated(podMap map[podKey]*pod, resGPUsRequested []*source.GPUsRequestedResult, resGPUsAllocated []*source.GPUsAllocatedResult, podUIDKeyMap map[podKey][]podKey) {
	if len(resGPUsAllocated) > 0 { // Use the new query, when it's become available in a window
		resGPUsRequested = resGPUsAllocated
	}
	for _, res := range resGPUsRequested {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU request result missing field: %s", err)
			continue
		}

		container := res.Container
		if container == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: GPU request query result missing 'container': %s", key)
			continue
		}

		var pods []*pod
		if thisPod, ok := podMap[key]; !ok {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {

			if _, ok := thisPod.Allocations[container]; !ok {
				thisPod.appendContainer(container)
			}

			hrs := thisPod.Allocations[container].Minutes() / 60.0
			thisPod.Allocations[container].GPUHours = res.Data[0].Value * hrs

			// For now, it will always be the case that Request==Allocation. If
			// you would like to use a GPU you need to request the full GPU.
			// Therefore max(usage,request) will always equal request. In the
			// future this may need to be refactored when building support for
			// GPU Time Slicing.

			if thisPod.Allocations[container].GPUAllocation == nil {
				thisPod.Allocations[container].GPUAllocation = &opencost.GPUAllocation{
					GPURequestAverage: &res.Data[0].Value,
				}
			} else {
				thisPod.Allocations[container].GPUAllocation.GPURequestAverage = &res.Data[0].Value
			}
		}
	}
}

func applyNetworkTotals(podMap map[podKey]*pod, resNetworkTransferBytes []*source.NetTransferBytesResult, resNetworkReceiveBytes []*source.NetReceiveBytesResult, podUIDKeyMap map[podKey][]podKey) {
	for _, res := range resNetworkTransferBytes {
		podKey, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: Network Transfer Bytes query result missing field: %s", err)
			continue
		}

		var pods []*pod

		if thisPod, ok := podMap[podKey]; !ok {
			if uidKeys, ok := podUIDKeyMap[podKey]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			for _, alloc := range thisPod.Allocations {
				alloc.NetworkTransferBytes = res.Data[0].Value / float64(len(thisPod.Allocations)) / float64(len(pods))
			}
		}
	}
	for _, res := range resNetworkReceiveBytes {
		podKey, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: Network Receive Bytes query result missing field: %s", err)
			continue
		}

		var pods []*pod

		if thisPod, ok := podMap[podKey]; !ok {
			if uidKeys, ok := podUIDKeyMap[podKey]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			for _, alloc := range thisPod.Allocations {
				alloc.NetworkReceiveBytes = res.Data[0].Value / float64(len(thisPod.Allocations)) / float64(len(pods))
			}
		}
	}
}

func applyCrossZoneNetworkAllocation(alloc *opencost.Allocation, networkSubCost float64) {
	alloc.NetworkCrossZoneCost = networkSubCost
}

func applyCrossRegionNetworkAllocation(alloc *opencost.Allocation, networkSubCost float64) {
	alloc.NetworkCrossRegionCost = networkSubCost
}

func applyInternetNetworkAllocation(alloc *opencost.Allocation, networkSubCost float64) {
	alloc.NetworkInternetCost = networkSubCost
}

func applyNatGatewayEgressAllocation(alloc *opencost.Allocation, networkSubCost float64) {
	alloc.NetworkNatGatewayEgressCost = networkSubCost
}

func applyNatGatewayIngressAllocation(alloc *opencost.Allocation, networkSubCost float64) {
	alloc.NetworkNatGatewayIngressCost = networkSubCost
}

func applyNetworkAllocation(podMap map[podKey]*pod, resNetworkGiB []*source.NetworkGiBResult, resNetworkCostPerGiB []*source.NetworkPricePerGiBResult, podUIDKeyMap map[podKey][]podKey, applyCostFunc func(*opencost.Allocation, float64)) {
	costPerGiBByCluster := map[string]float64{}

	for _, res := range resNetworkCostPerGiB {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		costPerGiBByCluster[cluster] = res.Data[0].Value
	}

	for _, res := range resNetworkGiB {
		podKey, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: Network allocation query result missing field: %s", err)
			continue
		}

		var pods []*pod

		if thisPod, ok := podMap[podKey]; !ok {
			if uidKeys, ok := podUIDKeyMap[podKey]; ok {
				for _, uidKey := range uidKeys {
					thisPod, ok = podMap[uidKey]
					if ok {
						pods = append(pods, thisPod)
					}
				}
			} else {
				continue
			}
		} else {
			pods = []*pod{thisPod}
		}

		for _, thisPod := range pods {
			for _, alloc := range thisPod.Allocations {
				gib := res.Data[0].Value / float64(len(thisPod.Allocations))
				costPerGiB := costPerGiBByCluster[podKey.Cluster]
				currentNetworkSubCost := gib * costPerGiB / float64(len(pods))
				applyCostFunc(alloc, currentNetworkSubCost)
				alloc.NetworkCost += currentNetworkSubCost
			}
		}
	}
}

func resToNodeLabels(resNodeLabels []*source.NodeLabelsResult) map[nodeKey]map[string]string {
	nodeLabels := map[nodeKey]map[string]string{}

	for _, res := range resNodeLabels {
		nodeKey, err := newResultNodeKey(res.Cluster, res.Node)
		if err != nil {
			continue
		}

		if _, ok := nodeLabels[nodeKey]; !ok {
			nodeLabels[nodeKey] = map[string]string{}
		}

		labels := res.Labels
		// labels are retrieved from prometheus here so it will be in prometheus sanitized state
		// e.g. topology.kubernetes.io/zone => topology_kubernetes_io_zone
		for labelKey, labelValue := range labels {
			nodeLabels[nodeKey][labelKey] = labelValue
		}
	}

	return nodeLabels
}

func resToNamespaceLabels(resNamespaceLabels []*source.NamespaceLabelsResult) map[namespaceKey]map[string]string {
	namespaceLabels := map[namespaceKey]map[string]string{}

	for _, res := range resNamespaceLabels {
		nsKey, err := newResultNamespaceKey(res.Cluster, res.Namespace)
		if err != nil {
			continue
		}

		if _, ok := namespaceLabels[nsKey]; !ok {
			namespaceLabels[nsKey] = map[string]string{}
		}

		for k, l := range res.Labels {
			namespaceLabels[nsKey][k] = l
		}
	}

	return namespaceLabels
}

func resToPodLabels(resPodLabels []*source.PodLabelsResult, podUIDKeyMap map[podKey][]podKey, ingestPodUID bool) map[podKey]map[string]string {
	podLabels := map[podKey]map[string]string{}

	for _, res := range resPodLabels {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			continue
		}

		var keys []podKey

		if ingestPodUID {
			if uidKeys, ok := podUIDKeyMap[key]; ok {

				keys = append(keys, uidKeys...)

			}
		} else {
			keys = []podKey{key}
		}

		for _, key := range keys {
			if _, ok := podLabels[key]; !ok {
				podLabels[key] = map[string]string{}
			}

			for k, l := range res.Labels {
				podLabels[key][k] = l
			}
		}
	}

	return podLabels
}

func resToNamespaceAnnotations(resNamespaceAnnotations []*source.NamespaceAnnotationsResult) map[string]map[string]string {
	namespaceAnnotations := map[string]map[string]string{}

	for _, res := range resNamespaceAnnotations {
		namespace := res.Namespace
		if namespace == "" {
			continue
		}

		if _, ok := namespaceAnnotations[namespace]; !ok {
			namespaceAnnotations[namespace] = map[string]string{}
		}

		for k, l := range res.Annotations {
			namespaceAnnotations[namespace][k] = l
		}
	}

	return namespaceAnnotations
}

func resToPodAnnotations(resPodAnnotations []*source.PodAnnotationsResult, podUIDKeyMap map[podKey][]podKey, ingestPodUID bool) map[podKey]map[string]string {
	podAnnotations := map[podKey]map[string]string{}

	for _, res := range resPodAnnotations {
		key, err := newResultPodKey(res.Cluster, res.Namespace, res.Pod)
		if err != nil {
			continue
		}

		var keys []podKey

		if ingestPodUID {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				keys = append(keys, uidKeys...)
			}
		} else {
			keys = []podKey{key}
		}

		for _, key := range keys {
			if _, ok := podAnnotations[key]; !ok {
				podAnnotations[key] = map[string]string{}
			}

			for k, l := range res.Annotations {
				podAnnotations[key][k] = l
			}
		}
	}

	return podAnnotations
}

func applyLabels(podMap map[podKey]*pod, nodeLabels map[nodeKey]map[string]string, namespaceLabels map[namespaceKey]map[string]string, podLabels map[podKey]map[string]string) {
	for podKey, pod := range podMap {
		for _, alloc := range pod.Allocations {
			allocLabels := alloc.Properties.Labels
			if allocLabels == nil {
				allocLabels = make(map[string]string)
			}

			nsLabels := alloc.Properties.NamespaceLabels
			if nsLabels == nil {
				nsLabels = make(map[string]string)
			}

			// Apply node labels first, then namespace labels, then pod labels
			// so that pod labels overwrite namespace labels, which overwrite
			// node labels.

			if nodeLabels != nil {
				nodeKey := newNodeKey(pod.Key.Cluster, pod.Node)
				if labels, ok := nodeLabels[nodeKey]; ok {
					for k, v := range labels {
						allocLabels[k] = v
					}
				}
			}

			nsKey := podKey.namespaceKey
			if labels, ok := namespaceLabels[nsKey]; ok {
				for k, v := range labels {
					allocLabels[k] = v
					nsLabels[k] = v
				}
			}

			if labels, ok := podLabels[podKey]; ok {
				for k, v := range labels {
					allocLabels[k] = v
				}
			}

			alloc.Properties.Labels = allocLabels
			alloc.Properties.NamespaceLabels = nsLabels

		}
	}
}

func applyAnnotations(podMap map[podKey]*pod, namespaceAnnotations map[string]map[string]string, podAnnotations map[podKey]map[string]string) {
	for key, pod := range podMap {
		for _, alloc := range pod.Allocations {
			allocAnnotations := alloc.Properties.Annotations
			if allocAnnotations == nil {
				allocAnnotations = make(map[string]string)
			}

			nsAnnotations := alloc.Properties.NamespaceAnnotations
			if nsAnnotations == nil {
				nsAnnotations = make(map[string]string)
			}

			// Apply namespace annotations first, then pod annotations so that
			// pod labels overwrite namespace labels.
			if labels, ok := namespaceAnnotations[key.Namespace]; ok {
				for k, v := range labels {
					allocAnnotations[k] = v
					nsAnnotations[k] = v
				}
			}
			if labels, ok := podAnnotations[key]; ok {
				for k, v := range labels {
					allocAnnotations[k] = v
				}
			}

			alloc.Properties.Annotations = allocAnnotations
			alloc.Properties.NamespaceAnnotations = nsAnnotations
		}
	}
}

func resToDeploymentLabels(resDeploymentLabels []*source.DeploymentLabelsResult) map[controllerKey]map[string]string {
	deploymentLabels := map[controllerKey]map[string]string{}

	for _, res := range resDeploymentLabels {
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.Deployment, "deployment")
		if err != nil {
			continue
		}

		if _, ok := deploymentLabels[controllerKey]; !ok {
			deploymentLabels[controllerKey] = map[string]string{}
		}

		for k, l := range res.Labels {
			deploymentLabels[controllerKey][k] = l
		}
	}

	// Prune duplicate deployments. That is, if the same deployment exists with
	// hyphens instead of underscores, keep the one that uses hyphens.
	for key := range deploymentLabels {
		if strings.Contains(key.Controller, "_") {
			duplicateController := strings.Replace(key.Controller, "_", "-", -1)
			duplicateKey := newControllerKey(key.Cluster, key.Namespace, key.ControllerKind, duplicateController)
			if _, ok := deploymentLabels[duplicateKey]; ok {
				delete(deploymentLabels, key)
			}
		}
	}

	return deploymentLabels
}

func resToStatefulSetLabels(resStatefulSetLabels []*source.StatefulSetLabelsResult) map[controllerKey]map[string]string {
	statefulSetLabels := map[controllerKey]map[string]string{}

	for _, res := range resStatefulSetLabels {
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.StatefulSet, "statefulset")
		if err != nil {
			continue
		}

		if _, ok := statefulSetLabels[controllerKey]; !ok {
			statefulSetLabels[controllerKey] = map[string]string{}
		}

		for k, l := range res.Labels {
			statefulSetLabels[controllerKey][k] = l
		}
	}

	// Prune duplicate stateful sets. That is, if the same stateful set exists
	// with hyphens instead of underscores, keep the one that uses hyphens.
	for key := range statefulSetLabels {
		if strings.Contains(key.Controller, "_") {
			duplicateController := strings.Replace(key.Controller, "_", "-", -1)
			duplicateKey := newControllerKey(key.Cluster, key.Namespace, key.ControllerKind, duplicateController)
			if _, ok := statefulSetLabels[duplicateKey]; ok {
				delete(statefulSetLabels, key)
			}
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
					log.DedupedWarningf(5, "CostModel.ComputeAllocation: PodControllerMap match already exists: %s matches %s and %s", pKey, podControllerMap[pKey], cKey)
				}
				podControllerMap[pKey] = cKey
			}
		}
	}

	return podControllerMap
}

func resToPodDaemonSetMap(resDaemonSetLabels []*source.DaemonSetLabelsResult, podUIDKeyMap map[podKey][]podKey, ingestPodUID bool) map[podKey]controllerKey {
	daemonSetLabels := map[podKey]controllerKey{}

	for _, res := range resDaemonSetLabels {
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.DaemonSet, "daemonset")
		if err != nil {
			continue
		}

		pod := res.Pod
		if pod == "" {
			log.Warnf("CostModel.ComputeAllocation: DaemonSetLabel result without pod: %s", controllerKey)
		}

		key := newPodKey(controllerKey.Cluster, controllerKey.Namespace, pod)

		var keys []podKey

		if ingestPodUID {
			if uidKeys, ok := podUIDKeyMap[key]; ok {

				keys = append(keys, uidKeys...)

			}
		} else {
			keys = []podKey{key}
		}

		for _, key := range keys {
			daemonSetLabels[key] = controllerKey
		}
	}

	return daemonSetLabels
}

func resToPodJobMap(resJobLabels []*source.JobLabelsResult, podUIDKeyMap map[podKey][]podKey, ingestPodUID bool) map[podKey]controllerKey {
	jobLabels := map[podKey]controllerKey{}

	for _, res := range resJobLabels {
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.Job, "job")
		if err != nil {
			continue
		}

		// Convert the name of Jobs generated by CronJobs to the name of the
		// CronJob by stripping the timestamp off the end.
		match := isCron.FindStringSubmatch(controllerKey.Controller)
		if match != nil {
			controllerKey.Controller = match[1]
		}

		pod := res.Pod
		if pod == "" {
			log.Warnf("CostModel.ComputeAllocation: JobLabel result without pod: %s", controllerKey)
		}

		key := newPodKey(controllerKey.Cluster, controllerKey.Namespace, pod)

		var keys []podKey

		if ingestPodUID {
			if uidKeys, ok := podUIDKeyMap[key]; ok {

				keys = append(keys, uidKeys...)

			}
		} else {
			keys = []podKey{key}
		}

		for _, key := range keys {
			jobLabels[key] = controllerKey
		}
	}

	return jobLabels
}

func resToPodReplicaSetMap(resPodsWithReplicaSetOwner []*source.PodsWithReplicaSetOwnerResult, resReplicaSetsWithoutOwners []*source.ReplicaSetsWithoutOwnersResult, resReplicaSetsWithRolloutOwner []*source.ReplicaSetsWithRolloutResult, podUIDKeyMap map[podKey][]podKey, ingestPodUID bool) map[podKey]controllerKey {
	// Build out set of ReplicaSets that have no owners, themselves, such that
	// the ReplicaSet should be used as the owner of the Pods it controls.
	// (This should exclude, for example, ReplicaSets that are controlled by
	// Deployments, in which case the Deployment should be the pod's owner.)
	// Additionally, add to this set of ReplicaSets those ReplicaSets that
	// are owned by a Rollout
	replicaSets := map[controllerKey]struct{}{}

	// Create unowned ReplicaSet controller keys
	for _, res := range resReplicaSetsWithoutOwners {
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.ReplicaSet, "replicaset")
		if err != nil {
			continue
		}

		replicaSets[controllerKey] = struct{}{}
	}

	// Create Rollout-owned ReplicaSet controller keys
	for _, res := range resReplicaSetsWithRolloutOwner {
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.ReplicaSet, "rollout")
		if err != nil {
			continue
		}

		replicaSets[controllerKey] = struct{}{}
	}

	// Create the mapping of Pods to ReplicaSets, ignoring any ReplicaSets that
	// do not appear in the set of unowned/Rollout-owned ReplicaSets above.
	podToReplicaSet := map[podKey]controllerKey{}

	for _, res := range resPodsWithReplicaSetOwner {
		// First, check if this pod is owned by an unowned ReplicaSet
		controllerKey, err := newResultControllerKey(res.Cluster, res.Namespace, res.ReplicaSet, "replicaset")
		if err != nil {
			continue
		} else if _, ok := replicaSets[controllerKey]; !ok {
			// If the pod is not owned by an unowned ReplicaSet, check if
			// it's owned by a Rollout-owned ReplicaSet
			controllerKey, err = newResultControllerKey(res.Cluster, res.Namespace, res.ReplicaSet, "rollout")
			if err != nil {
				continue
			} else if _, ok := replicaSets[controllerKey]; !ok {
				continue
			}
		}

		pod := res.Pod
		if pod == "" {
			log.Warnf("CostModel.ComputeAllocation: ReplicaSet result without pod: %s", controllerKey)
		}

		key := newPodKey(controllerKey.Cluster, controllerKey.Namespace, pod)

		var keys []podKey

		if ingestPodUID {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				keys = append(keys, uidKeys...)
			}
		} else {
			keys = []podKey{key}
		}

		for _, key := range keys {
			podToReplicaSet[key] = controllerKey
		}
	}

	return podToReplicaSet
}

func applyControllersToPods(podMap map[podKey]*pod, podControllerMap map[podKey]controllerKey) {
	for key, pod := range podMap {
		for _, alloc := range pod.Allocations {
			if controllerKey, ok := podControllerMap[key]; ok {
				alloc.Properties.ControllerKind = controllerKey.ControllerKind
				alloc.Properties.Controller = controllerKey.Controller
			}
		}
	}
}

/* Service Helpers */

func getServiceLabels(resServiceLabels []*source.ServiceLabelsResult) map[serviceKey]map[string]string {
	serviceLabels := map[serviceKey]map[string]string{}

	for _, res := range resServiceLabels {
		serviceKey, err := newResultServiceKey(res.Cluster, res.Namespace, res.Service)
		if err != nil {
			continue
		}

		if _, ok := serviceLabels[serviceKey]; !ok {
			serviceLabels[serviceKey] = map[string]string{}
		}

		for k, l := range res.Labels {
			serviceLabels[serviceKey][k] = l
		}
	}

	// Prune duplicate services. That is, if the same service exists with
	// hyphens instead of underscores, keep the one that uses hyphens.
	for key := range serviceLabels {
		if strings.Contains(key.Service, "_") {
			duplicateService := strings.Replace(key.Service, "_", "-", -1)
			duplicateKey := newServiceKey(key.Cluster, key.Namespace, duplicateService)
			if _, ok := serviceLabels[duplicateKey]; ok {
				delete(serviceLabels, key)
			}
		}
	}

	return serviceLabels
}

func applyServicesToPods(podMap map[podKey]*pod, podLabels map[podKey]map[string]string, allocsByService map[serviceKey][]*opencost.Allocation, serviceLabels map[serviceKey]map[string]string) {
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
					allocsByService[sKey] = append(allocsByService[sKey], alloc)
				}
				alloc.Properties.Services = services

			}
		}
	}
}

func getLoadBalancerCosts(lbMap map[serviceKey]*lbCost, resLBCost []*source.LBPricePerHrResult, resLBActiveMins []*source.LBActiveMinutesResult, resolution time.Duration, window opencost.Window) {
	for _, res := range resLBActiveMins {
		serviceKey, err := newResultServiceKey(res.Cluster, res.Namespace, res.Service)
		if err != nil || len(res.Data) == 0 {
			continue
		}

		// load balancers have interpolation for costs, we don't need to offset the resolution
		lbStart, lbEnd := calculateStartAndEnd(res.Data, resolution, window)
		if lbStart.IsZero() || lbEnd.IsZero() {
			log.Warnf("CostModel.ComputeAllocation: pvc %s has no running time", serviceKey)
		}

		lbMap[serviceKey] = &lbCost{
			Start: lbStart,
			End:   lbEnd,
		}
	}

	for _, res := range resLBCost {
		serviceKey, err := newResultServiceKey(res.Cluster, res.Namespace, res.Service)
		if err != nil {
			continue
		}

		// get the ingress IP to determine if this is a private LB
		ip := res.IngressIP
		if ip == "" {
			log.Warnf("error getting ingress ip for key %s: %v, skipping", serviceKey, err)
			// do not count the time that the service was being created or deleted
			// ingress IP will be empty string
			// only add cost to allocation when external IP is provisioned
			continue
		}

		// Apply cost as price-per-hour * hours
		if lb, ok := lbMap[serviceKey]; ok {
			lbPricePerHr := res.Data[0].Value
			// interpolate any missing data
			resolutionHours := resolution.Hours()
			resultHours := lb.End.Sub(lb.Start).Hours()
			scaleFactor := (resolutionHours + resultHours) / resultHours

			// after scaling, we can adjust the timings to reflect the interpolated data
			lb.End = lb.End.Add(resolution)

			lb.TotalCost += lbPricePerHr * resultHours * scaleFactor
			lb.Ip = ip
			lb.Private = privateIPCheck(ip)
		} else {
			log.DedupedWarningf(20, "CostModel: found minutes for key that does not exist: %s", serviceKey)
		}
	}
}

func applyLoadBalancersToPods(window opencost.Window, podMap map[podKey]*pod, lbMap map[serviceKey]*lbCost, allocsByService map[serviceKey][]*opencost.Allocation) {
	for sKey, lb := range lbMap {
		totalHours := 0.0
		allocHours := make(map[*opencost.Allocation]float64)

		allocs, ok := allocsByService[sKey]
		// if there are no allocations using the service, add its cost to the Unmounted pod for its cluster
		if !ok {
			pod := getUnmountedPodForCluster(window, podMap, sKey.Cluster)
			pod.Allocations[opencost.UnmountedSuffix].LoadBalancerCost += lb.TotalCost
			pod.Allocations[opencost.UnmountedSuffix].Properties.Services = append(pod.Allocations[opencost.UnmountedSuffix].Properties.Services, sKey.Service)
		}
		// Add portion of load balancing cost to each allocation
		// proportional to the total number of hours allocations used the load balancer
		for _, alloc := range allocs {
			// Determine the (start, end) of the relationship between the
			// given lbCost and the associated Allocation so that a precise
			// number of hours can be used to compute cumulative cost.
			s, e := alloc.Start, alloc.End
			if lb.Start.After(alloc.Start) {
				s = lb.Start
			}
			if lb.End.Before(alloc.End) {
				e = lb.End
			}
			hours := e.Sub(s).Hours()
			// A negative number of hours signifies no overlap between the windows
			if hours > 0 {
				totalHours += hours
				allocHours[alloc] = hours
			}
		}

		// Distribute cost of service once total hours is calculated
		for alloc, hours := range allocHours {
			alloc.LoadBalancerCost += lb.TotalCost * hours / totalHours
		}

		for _, alloc := range allocs {
			// reocord the hours overlapped with the allocation for the load balancer
			// if there was overlap. Otherwise, record a 0.0.
			// TODO: Do we really want to include load balancers that have 0 overlap
			// TODO: hours with the allocation?
			var hours float64 = 0.0
			if _, ok := allocHours[alloc]; ok {
				hours = allocHours[alloc]
			}

			if alloc.LoadBalancers == nil {
				alloc.LoadBalancers = opencost.LbAllocations{}
			}

			if _, found := alloc.LoadBalancers[sKey.String()]; found {
				alloc.LoadBalancers[sKey.String()].Cost += alloc.LoadBalancerCost
				alloc.LoadBalancers[sKey.String()].Hours += hours
			} else {
				alloc.LoadBalancers[sKey.String()] = &opencost.LbAllocation{
					Service: sKey.Namespace + "/" + sKey.Service,
					Cost:    alloc.LoadBalancerCost,
					Private: lb.Private,
					Ip:      lb.Ip,
					Hours:   hours,
				}
			}
		}

		// If there was no overlap apply to Unmounted pod
		if len(allocHours) == 0 {
			pod := getUnmountedPodForCluster(window, podMap, sKey.Cluster)
			pod.Allocations[opencost.UnmountedSuffix].LoadBalancerCost += lb.TotalCost
			pod.Allocations[opencost.UnmountedSuffix].Properties.Services = append(pod.Allocations[opencost.UnmountedSuffix].Properties.Services, sKey.Service)
		}
	}
}

/* Node Helpers */

func applyNodeCostPerCPUHr(nodeMap map[nodeKey]*nodePricing, resNodeCostPerCPUHr []*source.NodeCPUPricePerHrResult) {
	for _, res := range resNodeCostPerCPUHr {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		node := res.Node
		if node == "" {
			log.Warnf("CostModel.ComputeAllocation: Node CPU cost query result missing field: node for node \"%s\"", node)
			continue
		}

		instanceType := res.InstanceType
		if instanceType == "" {
			log.Warnf("CostModel.ComputeAllocation: Node CPU cost query result missing field: instance_type for node \"%s\"", node)
		}

		providerID := res.ProviderID
		if providerID == "" {
			log.Warnf("CostModel.ComputeAllocation: Node CPU cost query result missing field: provider_id for node \"%s\"", node)
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &nodePricing{
				Name:       node,
				NodeType:   instanceType,
				ProviderID: provider.ParseID(providerID),
			}
		}

		nodeMap[key].CostPerCPUHr = res.Data[0].Value
	}
}

func applyNodeCostPerRAMGiBHr(nodeMap map[nodeKey]*nodePricing, resNodeCostPerRAMGiBHr []*source.NodeRAMPricePerGiBHrResult) {
	for _, res := range resNodeCostPerRAMGiBHr {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		node := res.Node
		if node == "" {
			log.Warnf("CostModel.ComputeAllocation: Node RAM cost query result missing field: node for node \"%s\"", node)
			continue
		}

		instanceType := res.InstanceType
		if instanceType == "" {
			log.Warnf("CostModel.ComputeAllocation: Node RAM cost query result missing field: instance_type for node \"%s\"", node)
		}

		providerID := res.ProviderID
		if providerID == "" {
			log.Warnf("CostModel.ComputeAllocation: Node RAM cost query result missing field: provider_id for node \"%s\"", node)
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &nodePricing{
				Name:       node,
				NodeType:   instanceType,
				ProviderID: provider.ParseID(providerID),
			}
		}

		nodeMap[key].CostPerRAMGiBHr = res.Data[0].Value
	}
}

func applyNodeCostPerGPUHr(nodeMap map[nodeKey]*nodePricing, resNodeCostPerGPUHr []*source.NodeGPUPricePerHrResult) {
	for _, res := range resNodeCostPerGPUHr {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		node := res.Node
		if node == "" {
			log.Warnf("CostModel.ComputeAllocation: Node GPU cost query result missing field: node for node \"%s\"", node)
			continue
		}

		instanceType := res.InstanceType
		if instanceType == "" {
			log.Warnf("CostModel.ComputeAllocation: Node GPU cost query result missing field: instance_type for node \"%s\"", node)
		}

		providerID := res.ProviderID
		if providerID == "" {
			log.Warnf("CostModel.ComputeAllocation: Node GPU cost query result missing field: provider_id for node \"%s\"", node)
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			nodeMap[key] = &nodePricing{
				Name:       node,
				NodeType:   instanceType,
				ProviderID: provider.ParseID(providerID),
			}
		}

		nodeMap[key].CostPerGPUHr = res.Data[0].Value
	}
}

func applyNodeSpot(nodeMap map[nodeKey]*nodePricing, resNodeIsSpot []*source.NodeIsSpotResult) {
	for _, res := range resNodeIsSpot {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		node := res.Node
		if node == "" {
			log.Warnf("CostModel.ComputeAllocation: Node spot query result missing field: 'node'")
			continue
		}

		key := newNodeKey(cluster, node)
		if _, ok := nodeMap[key]; !ok {
			log.Warnf("CostModel.ComputeAllocation: Node spot query result for missing node: %s", key)
			continue
		}

		nodeMap[key].Preemptible = res.Data[0].Value > 0
	}
}

func applyNodeDiscount(nodeMap map[nodeKey]*nodePricing, cm *CostModel) {
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
		// TODO GKE Reserved Instances into account
		node.Discount = cm.Provider.CombinedDiscountForNode(node.NodeType, node.Preemptible, discount, negotiatedDiscount)
		node.CostPerCPUHr *= (1.0 - node.Discount)
		node.CostPerRAMGiBHr *= (1.0 - node.Discount)
	}
}

func (cm *CostModel) applyNodesToPod(podMap map[podKey]*pod, nodeMap map[nodeKey]*nodePricing) {
	for _, pod := range podMap {
		for _, alloc := range pod.Allocations {
			cluster := alloc.Properties.Cluster
			nodeName := alloc.Properties.Node
			thisNodeKey := newNodeKey(cluster, nodeName)

			node := cm.getNodePricing(nodeMap, thisNodeKey)
			alloc.Properties.ProviderID = node.ProviderID
			alloc.CPUCost = alloc.CPUCoreHours * node.CostPerCPUHr
			alloc.RAMCost = (alloc.RAMByteHours / 1024 / 1024 / 1024) * node.CostPerRAMGiBHr
			alloc.GPUCost = alloc.GPUHours * node.CostPerGPUHr
		}
	}
}

// getCustomNodePricing converts the CostModel's configured custom pricing
// values into a nodePricing instance.
func (cm *CostModel) getCustomNodePricing(spot bool, providerID string) *nodePricing {
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

	node := &nodePricing{
		Source:     "custom",
		ProviderID: providerID,
	}

	costPerCPUHr, err := strconv.ParseFloat(cpuCostStr, 64)
	if err != nil {
		log.Warnf("CostModel: custom pricing has illegal CPU cost: %s", cpuCostStr)
	}
	node.CostPerCPUHr = costPerCPUHr

	costPerGPUHr, err := strconv.ParseFloat(gpuCostStr, 64)
	if err != nil {
		log.Warnf("CostModel: custom pricing has illegal GPU cost: %s", gpuCostStr)
	}
	node.CostPerGPUHr = costPerGPUHr

	costPerRAMHr, err := strconv.ParseFloat(ramCostStr, 64)
	if err != nil {
		log.Warnf("CostModel: custom pricing has illegal RAM cost: %s", ramCostStr)
	}
	node.CostPerRAMGiBHr = costPerRAMHr

	return node
}

// getNodePricing determines node pricing, given a key and a mapping from keys
// to their nodePricing instances, as well as the custom pricing configuration
// inherent to the CostModel instance. If custom pricing is set, use that. If
// not, use the pricing defined by the given key. If that doesn't exist, fall
// back on custom pricing as a default.
func (cm *CostModel) getNodePricing(nodeMap map[nodeKey]*nodePricing, nodeKey nodeKey) *nodePricing {
	// Find the relevant nodePricing, if it exists. If not, substitute the
	// custom nodePricing as a default.
	node, ok := nodeMap[nodeKey]
	if !ok || node == nil {
		if nodeKey.Node != "" {
			log.DedupedWarningf(5, "CostModel: failed to find node for %s", nodeKey)
		}
		// since the node pricing data is not found, and this won't change for the duration of the allocation
		// build process, we can update the node map with the defaults to prevent future failed lookups
		nodeMap[nodeKey] = cm.getCustomNodePricing(false, "")
		return nodeMap[nodeKey]
	}

	// If custom pricing is enabled and can be retrieved, override detected
	// node pricing with the custom values.
	customPricingConfig, err := cm.Provider.GetConfig()
	if err != nil {
		log.Warnf("CostModel: failed to load custom pricing: %s", err)
	}
	if provider.CustomPricesEnabled(cm.Provider) && customPricingConfig != nil {
		return cm.getCustomNodePricing(node.Preemptible, node.ProviderID)
	}

	node.Source = "prometheus"

	// If any of the values are NaN or zero, replace them with the custom
	// values as default.
	// TODO:CLEANUP can't we parse these custom prices once? why do we store
	// them as strings like this?

	if node.CostPerCPUHr == 0 || math.IsNaN(node.CostPerCPUHr) {
		cpuCostStr := customPricingConfig.CPU
		if node.Preemptible {
			cpuCostStr = customPricingConfig.SpotCPU
		}
		log.Warnf("CostModel: node pricing has illegal CostPerCPUHr; replacing with custom pricing: %s %s", nodeKey, cpuCostStr)
		costPerCPUHr, err := strconv.ParseFloat(cpuCostStr, 64)
		if err != nil {
			log.Warnf("CostModel: custom pricing has illegal CPU cost: %s", cpuCostStr)
		}
		node.CostPerCPUHr = costPerCPUHr
		node.Source += "/customCPU"
	}

	if math.IsNaN(node.CostPerGPUHr) {
		gpuCostStr := customPricingConfig.GPU
		if node.Preemptible {
			gpuCostStr = customPricingConfig.SpotGPU
		}
		log.Warnf("CostModel: node pricing has illegal CostPerGPUHr; replacing with custom pricing: %s %s", nodeKey, gpuCostStr)
		costPerGPUHr, err := strconv.ParseFloat(gpuCostStr, 64)
		if err != nil {
			log.Warnf("CostModel: custom pricing has illegal GPU cost: %s", gpuCostStr)
		}
		node.CostPerGPUHr = costPerGPUHr
		node.Source += "/customGPU"
	}

	if node.CostPerRAMGiBHr == 0 || math.IsNaN(node.CostPerRAMGiBHr) {
		ramCostStr := customPricingConfig.RAM
		if node.Preemptible {
			ramCostStr = customPricingConfig.SpotRAM
		}
		log.Warnf("CostModel: node pricing has illegal CostPerRAMHr; replacing with custom pricing: %s %s", nodeKey, ramCostStr)
		costPerRAMHr, err := strconv.ParseFloat(ramCostStr, 64)
		if err != nil {
			log.Warnf("CostModel: custom pricing has illegal RAM cost: %s", ramCostStr)
		}
		node.CostPerRAMGiBHr = costPerRAMHr
		node.Source += "/customRAM"
	}

	// Double check each for NaNs, as there is a chance that our custom pricing
	// config could, itself, contain NaNs...
	if math.IsNaN(node.CostPerCPUHr) || math.IsInf(node.CostPerCPUHr, 0) {
		log.Warnf("CostModel: %s: node pricing has illegal CPU value: %v (setting to 0.0)", nodeKey, node.CostPerCPUHr)
		node.CostPerCPUHr = 0.0
	}
	if math.IsNaN(node.CostPerGPUHr) || math.IsInf(node.CostPerGPUHr, 0) {
		log.Warnf("CostModel: %s: node pricing has illegal RAM value: %v (setting to 0.0)", nodeKey, node.CostPerGPUHr)
		node.CostPerGPUHr = 0.0
	}
	if math.IsNaN(node.CostPerRAMGiBHr) || math.IsInf(node.CostPerRAMGiBHr, 0) {
		log.Warnf("CostModel: %s: node pricing has illegal RAM value: %v (setting to 0.0)", nodeKey, node.CostPerRAMGiBHr)
		node.CostPerRAMGiBHr = 0.0
	}

	return node
}

/* PV/PVC Helpers */

func buildPVMap(
	resolution time.Duration,
	pvMap map[pvKey]*pv,
	resPVCostPerGiBHour []*source.PVPricePerGiBHourResult,
	resPVActiveMins []*source.PVActiveMinutesResult,
	resPVMeta []*source.PVInfoResult,
	window opencost.Window,
) {
	for _, result := range resPVActiveMins {
		key, err := newResultPVKey(result.Cluster, result.PersistentVolume)
		if err != nil {
			log.Warnf("CostModel.ComputeAllocation: pv bytes query result missing field: %s", err)
			continue
		}

		pvStart, pvEnd := calculateStartAndEnd(result.Data, resolution, window)
		if pvStart.IsZero() || pvEnd.IsZero() {
			log.Warnf("CostModel.ComputeAllocation: pv %s has no running time", key)
		}

		pvMap[key] = &pv{
			Cluster: key.Cluster,
			Name:    key.PersistentVolume,
			Start:   pvStart,
			End:     pvEnd,
		}
	}

	for _, result := range resPVCostPerGiBHour {
		key, err := newResultPVKey(result.Cluster, result.VolumeName)
		if err != nil {
			log.Warnf("CostModel.ComputeAllocation: thisPV bytes query result missing field: %s", err)
			continue
		}

		if _, ok := pvMap[key]; !ok {
			pvMap[key] = &pv{
				Cluster: key.Cluster,
				Name:    key.PersistentVolume,
			}
		}
		pvMap[key].CostPerGiBHour = result.Data[0].Value

	}

	for _, result := range resPVMeta {
		key, err := newResultPVKey(result.Cluster, result.PersistentVolume)
		if err != nil {
			log.Warnf("error getting key for PV: %v", err)
			continue
		}

		// only add metadata for disks that exist in the other metrics
		if _, ok := pvMap[key]; ok {
			provId := result.ProviderID
			if provId == "" {
				log.Warnf("error getting provider id for PV %v: %v", key, err)
				continue
			}
			pvMap[key].ProviderID = provId
		}

	}
}

func applyPVBytes(pvMap map[pvKey]*pv, resPVBytes []*source.PVBytesResult) {
	for _, res := range resPVBytes {
		key, err := newResultPVKey(res.Cluster, res.PersistentVolume)
		if err != nil {
			log.Warnf("CostModel.ComputeAllocation: pv bytes query result missing field: %s", err)
			continue
		}

		if _, ok := pvMap[key]; !ok {
			log.Warnf("CostModel.ComputeAllocation: pv bytes result for missing pv: %s", key)
			continue
		}

		pvBytesUsed := res.Data[0].Value
		if pvBytesUsed < PV_USAGE_SANITY_LIMIT_BYTES {
			pvMap[key].Bytes = pvBytesUsed
		} else {
			pvMap[key].Bytes = 0
			log.Warnf("PV usage exceeds sanity limit, clamping to zero")
		}
	}
}

func buildPVCMap(resolution time.Duration, pvcMap map[pvcKey]*pvc, pvMap map[pvKey]*pv, resPVCInfo []*source.PVCInfoResult, window opencost.Window) {
	for _, res := range resPVCInfo {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		namespace := res.Namespace
		name := res.PersistentVolumeClaim
		volume := res.VolumeName
		storageClass := res.StorageClass

		if namespace == "" || name == "" || volume == "" || storageClass == "" {
			log.DedupedWarningf(10, "CostModel.ComputeAllocation: pvc info query result missing field")
			continue

		}

		pvKey := newPVKey(cluster, volume)
		pvcKey := newPVCKey(cluster, namespace, name)

		pvcStart, pvcEnd := calculateStartAndEnd(res.Data, resolution, window)
		if pvcStart.IsZero() || pvcEnd.IsZero() {
			log.Warnf("CostModel.ComputeAllocation: pvc %s has no running time", pvcKey)
		}

		if _, ok := pvMap[pvKey]; !ok {
			continue
		}

		pvMap[pvKey].StorageClass = storageClass

		if _, ok := pvcMap[pvcKey]; !ok {
			pvcMap[pvcKey] = &pvc{}
		}

		pvcMap[pvcKey].Name = name
		pvcMap[pvcKey].Namespace = namespace
		pvcMap[pvcKey].Cluster = cluster
		pvcMap[pvcKey].Volume = pvMap[pvKey]
		pvcMap[pvcKey].Start = pvcStart
		pvcMap[pvcKey].End = pvcEnd
	}
}

func applyPVCBytesRequested(pvcMap map[pvcKey]*pvc, resPVCBytesRequested []*source.PVCBytesRequestedResult) {
	for _, res := range resPVCBytesRequested {
		key, err := newResultPVCKey(res.Cluster, res.Namespace, res.PersistentVolumeClaim)
		if err != nil {
			continue
		}

		if _, ok := pvcMap[key]; !ok {
			continue
		}

		pvcMap[key].Bytes = res.Data[0].Value
	}
}

func buildPodPVCMap(podPVCMap map[podKey][]*pvc, pvMap map[pvKey]*pv, pvcMap map[pvcKey]*pvc, podMap map[podKey]*pod, resPodPVCAllocation []*source.PodPVCAllocationResult, podUIDKeyMap map[podKey][]podKey, ingestPodUID bool) {
	for _, res := range resPodPVCAllocation {
		cluster := res.Cluster
		if cluster == "" {
			cluster = coreenv.GetClusterID()
		}

		namespace := res.Namespace
		pod := res.Pod
		name := res.PersistentVolumeClaim
		volume := res.PersistentVolume

		if namespace == "" || pod == "" || name == "" || volume == "" {
			log.DedupedWarningf(5, "CostModel.ComputeAllocation: pvc allocation query result missing field")
			continue
		}

		key := newPodKey(cluster, namespace, pod)
		pvKey := newPVKey(cluster, volume)
		pvcKey := newPVCKey(cluster, namespace, name)

		var keys []podKey

		if ingestPodUID {
			if uidKeys, ok := podUIDKeyMap[key]; ok {
				keys = append(keys, uidKeys...)
			}
		} else {
			keys = []podKey{key}
		}

		for _, key := range keys {
			if _, ok := pvMap[pvKey]; !ok {
				log.DedupedWarningf(5, "CostModel.ComputeAllocation: pv missing for pvc allocation query result: %s", pvKey)
				continue
			}

			if _, ok := podPVCMap[key]; !ok {
				podPVCMap[key] = []*pvc{}
			}

			pvc, ok := pvcMap[pvcKey]
			if !ok {
				log.DedupedWarningf(5, "CostModel.ComputeAllocation: pvc missing for pvc allocation query: %s", pvcKey)
				continue
			}

			if pod, ok := podMap[key]; !ok || len(pod.Allocations) <= 0 {
				log.DedupedWarningf(10, "CostModel.ComputeAllocation: pvc %s for missing pod %s", pvcKey, key)
				continue
			}

			pvc.Mounted = true

			podPVCMap[key] = append(podPVCMap[key], pvc)
		}
	}
}

func applyPVCsToPods(window opencost.Window, podMap map[podKey]*pod, podPVCMap map[podKey][]*pvc, pvcMap map[pvcKey]*pvc) {
	// Because PVCs can be shared among pods, the respective pv cost
	// needs to be evenly distributed to those pods based on time
	// running, as well as the amount of time the pvc was shared.

	// Build a relation between every pvc to the pods that mount it
	// and a window representing the interval during which they
	// were associated.
	pvcPodWindowMap := make(map[pvcKey]map[podKey]opencost.Window)

	for thisPodKey, thisPod := range podMap {
		if pvcs, ok := podPVCMap[thisPodKey]; ok {
			for _, thisPVC := range pvcs {

				// Determine the (start, end) of the relationship between the
				// given pvc and the associated Allocation so that a precise
				// number of hours can be used to compute cumulative cost.
				s, e := thisPod.Start, thisPod.End
				if thisPVC.Start.After(thisPod.Start) {
					s = thisPVC.Start
				}
				if thisPVC.End.Before(thisPod.End) {
					e = thisPVC.End
				}

				thisPVCKey := thisPVC.key()
				if pvcPodWindowMap[thisPVCKey] == nil {
					pvcPodWindowMap[thisPVCKey] = make(map[podKey]opencost.Window)
				}

				pvcPodWindowMap[thisPVCKey][thisPodKey] = opencost.NewWindow(&s, &e)
			}
		}
	}

	for thisPVCKey, podWindowMap := range pvcPodWindowMap {
		// Build out a pv price coefficient for each pod with a pvc. Each
		// pvc-pod relation needs a coefficient which modifies the pv cost
		// such that pv costs can be shared between all pods using that pvc.

		// Get single-point intervals from alloc-pvc relation windows.
		intervals := getIntervalPointsFromWindows(podWindowMap)

		pvc, ok := pvcMap[thisPVCKey]
		if !ok {
			log.Warnf("Allocation: Compute: applyPVCsToPods: missing pvc with key %s", thisPVCKey)
			continue
		}
		if pvc == nil {
			log.Warnf("Allocation: Compute: applyPVCsToPods: nil pvc with key %s", thisPVCKey)
			continue
		}

		// Determine coefficients for each pvc-pod relation.
		sharedPVCCostCoefficients, err := getPVCCostCoefficients(intervals, pvc)
		if err != nil {
			log.Warnf("Allocation: Compute: applyPVCsToPods: getPVCCostCoefficients: %s", err)
			continue
		}

		// Distribute pvc costs to Allocations
		for thisPodKey, coeffComponents := range sharedPVCCostCoefficients {
			pod, ok2 := podMap[thisPodKey]
			// If pod does not exist or the pod does not have any allocations
			// get unmounted pod for cluster
			if !ok2 || len(pod.Allocations) == 0 {
				// Get namespace unmounted pod, as pvc will have a namespace
				pod = getUnmountedPodForNamespace(window, podMap, pvc.Cluster, pvc.Namespace)
			}
			for _, alloc := range pod.Allocations {
				s, e := pod.Start, pod.End

				minutes := e.Sub(s).Minutes()
				hrs := minutes / 60.0

				gib := pvc.Bytes / 1024 / 1024 / 1024
				cost := pvc.Volume.CostPerGiBHour * gib * hrs
				byteHours := pvc.Bytes * hrs
				coef := getCoefficientFromComponents(coeffComponents)

				// Apply the size and cost of the pv to the allocation, each
				// weighted by count (i.e. the number of containers in the pod)
				// record the amount of total PVBytes Hours attributable to a given pv
				if alloc.PVs == nil {
					alloc.PVs = opencost.PVAllocations{}
				}
				pvKey := opencost.PVKey{
					Cluster: pvc.Volume.Cluster,
					Name:    pvc.Volume.Name,
				}

				// Both Cost and byteHours should be multiplied by the coef and divided by count
				// so that if all allocations with a given pv key are summed the result of those
				// would be equal to the values of the original pv
				count := float64(len(pod.Allocations))
				alloc.PVs[pvKey] = &opencost.PVAllocation{
					ByteHours:  byteHours * coef / count,
					Cost:       cost * coef / count,
					ProviderID: pvc.Volume.ProviderID,
				}
			}
		}
	}
}

func applyUnmountedPVs(window opencost.Window, podMap map[podKey]*pod, pvMap map[pvKey]*pv, pvcMap map[pvcKey]*pvc) {
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

			// a pv without a pvc will not have a namespace, so get the cluster unmounted pod
			pod := getUnmountedPodForCluster(window, podMap, pv.Cluster)

			// Calculate pv Cost

			// Unmounted pv should have correct keyso it can still reconcile
			thisPVKey := opencost.PVKey{
				Cluster: pv.Cluster,
				Name:    pv.Name,
			}
			gib := pv.Bytes / 1024 / 1024 / 1024
			hrs := pv.minutes() / 60.0
			cost := pv.CostPerGiBHour * gib * hrs
			unmountedPVs := opencost.PVAllocations{
				thisPVKey: {
					ByteHours: pv.Bytes * hrs,
					Cost:      cost,
				},
			}
			pod.Allocations[opencost.UnmountedSuffix].PVs = pod.Allocations[opencost.UnmountedSuffix].PVs.Add(unmountedPVs)
		}
	}
}

func applyUnmountedPVCs(window opencost.Window, podMap map[podKey]*pod, pvcMap map[pvcKey]*pvc) {
	for _, pvc := range pvcMap {
		if !pvc.Mounted && pvc.Volume != nil {

			// Get namespace unmounted pod, as pvc will have a namespace
			pod := getUnmountedPodForNamespace(window, podMap, pvc.Cluster, pvc.Namespace)

			// Calculate pv Cost

			// Unmounted pv should have correct key so it can still reconcile
			thisPVKey := opencost.PVKey{
				Cluster: pvc.Volume.Cluster,
				Name:    pvc.Volume.Name,
			}

			// Use the Volume Bytes here because pvc bytes could be different,
			// however the pv bytes are what are going to determine cost
			gib := pvc.Volume.Bytes / 1024 / 1024 / 1024
			hrs := pvc.Volume.minutes() / 60.0
			cost := pvc.Volume.CostPerGiBHour * gib * hrs
			unmountedPVs := opencost.PVAllocations{
				thisPVKey: {
					ByteHours: pvc.Volume.Bytes * hrs,
					Cost:      cost,
				},
			}
			pod.Allocations[opencost.UnmountedSuffix].PVs = pod.Allocations[opencost.UnmountedSuffix].PVs.Add(unmountedPVs)
		}
	}
}

/* Helper Helpers */

// getUnmountedPodForCluster retrieve the unmounted pod for a cluster and create it if it does not exist
func getUnmountedPodForCluster(window opencost.Window, podMap map[podKey]*pod, cluster string) *pod {
	container := opencost.UnmountedSuffix
	podName := opencost.UnmountedSuffix
	namespace := opencost.UnmountedSuffix
	node := ""

	thisPodKey := getUnmountedPodKey(cluster)
	// Initialize pod and container if they do not already exist
	thisPod, ok := podMap[thisPodKey]
	if !ok {
		thisPod = &pod{
			Window:      window.Clone(),
			Start:       *window.Start(),
			End:         *window.End(),
			Key:         thisPodKey,
			Allocations: map[string]*opencost.Allocation{},
		}

		thisPod.appendContainer(container)
		thisPod.Allocations[container].Properties.Cluster = cluster
		thisPod.Allocations[container].Properties.Node = node
		thisPod.Allocations[container].Properties.Namespace = namespace
		thisPod.Allocations[container].Properties.Pod = podName
		thisPod.Allocations[container].Properties.Container = container

		thisPod.Node = node

		podMap[thisPodKey] = thisPod
	}
	return thisPod
}

// getUnmountedPodForNamespace is as getUnmountedPodForCluster, but keys allocation property pod/namespace field off namespace
// This creates or adds allocations to an unmounted pod in the specified namespace, rather than in __unmounted__
func getUnmountedPodForNamespace(window opencost.Window, podMap map[podKey]*pod, cluster string, namespace string) *pod {
	container := opencost.UnmountedSuffix
	podName := fmt.Sprintf("%s-unmounted-pvcs", namespace)
	node := ""

	thisPodKey := newPodKey(cluster, namespace, podName)
	// Initialize pod and container if they do not already exist
	thisPod, ok := podMap[thisPodKey]
	if !ok {
		thisPod = &pod{
			Window:      window.Clone(),
			Start:       *window.Start(),
			End:         *window.End(),
			Key:         thisPodKey,
			Allocations: map[string]*opencost.Allocation{},
		}

		thisPod.appendContainer(container)
		thisPod.Allocations[container].Properties.Cluster = cluster
		thisPod.Allocations[container].Properties.Node = node
		thisPod.Allocations[container].Properties.Namespace = namespace
		thisPod.Allocations[container].Properties.Pod = podName
		thisPod.Allocations[container].Properties.Container = container

		thisPod.Node = node

		podMap[thisPodKey] = thisPod
	}
	return thisPod
}

func calculateStartAndEnd(result []*util.Vector, resolution time.Duration, window opencost.Window) (time.Time, time.Time) {
	// Start and end for a range vector are pulled from the timestamps of the
	// first and final values in the range. There is no "offsetting" required
	// of the start or the end, as we used to do. If you query for a duration
	// of time that is divisible by the given resolution, and set the end time
	// to be precisely the end of the window, Prometheus should give all the
	// relevant timestamps.
	//
	// E.g. avg(kube_pod_container_status_running{}) by (pod, namespace)[1h:1m]
	// with time=01:00:00 will return, for a pod running the entire time,
	// 61 timestamps where the first is 00:00:00 and the last is 01:00:00.
	s := time.Unix(int64(result[0].Timestamp), 0).UTC()
	e := time.Unix(int64(result[len(result)-1].Timestamp), 0).UTC()

	// The only corner-case here is what to do if you only get one timestamp.
	// This dilemma still requires the use of the resolution, and can be
	// clamped using the window. In this case, we want to honor the existence
	// of the pod by giving "one resolution" worth of duration, half on each
	// side of the given timestamp.
	if s.Equal(e) {
		s = s.Add(-1 * resolution / time.Duration(2))
		e = e.Add(resolution / time.Duration(2))
	}
	if s.Before(*window.Start()) {
		s = *window.Start()
	}
	if e.After(*window.End()) {
		e = *window.End()
	}
	// prevent end times in the future
	now := time.Now().UTC()
	if e.After(now) {
		e = now
	}

	return s, e
}

func getSanitizedDeviceName(deviceName string) string {
	if strings.Contains(deviceName, "nvidia") {
		return "nvidia"
	}

	return deviceName
}
