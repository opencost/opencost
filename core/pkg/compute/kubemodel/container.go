package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

// resourceUnitValue converts prometheus resource/unit strings from ResourceResult
// into kubemodel types, applying any necessary unit conversions.
func resourceUnitValue(resource, unit string, value float64) (kubemodel.Resource, kubemodel.Unit, float64) {
	switch resource {
	case "cpu":
		return kubemodel.ResourceCPU, kubemodel.UnitCore, value
	case "memory":
		return kubemodel.ResourceMemory, kubemodel.UnitByte, value
	default:
		return kubemodel.Resource(resource), kubemodel.Unit(unit), value
	}
}

func (km *KubeModel) computeContainers(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	containerUptimeFuture := source.WithGroup(grp, metrics.QueryContainerUptime(start, end))

	containerResourceRequestsFuture := source.WithGroup(grp, metrics.QueryContainerResourceRequests(start, end))
	containerResourceLimitsFuture := source.WithGroup(grp, metrics.QueryContainerResourceLimits(start, end))

	cpuCoresAllocatedFuture := source.WithGroup(grp, metrics.QueryCPUCoresAllocated(start, end))
	cpuUsageAvgFuture := source.WithGroup(grp, metrics.QueryCPUUsageAvg(start, end))
	cpuUsageMaxFuture := source.WithGroup(grp, metrics.QueryCPUUsageMax(start, end))

	ramBytesAllocatedFuture := source.WithGroup(grp, metrics.QueryRAMBytesAllocated(start, end))
	ramUsageAvgFuture := source.WithGroup(grp, metrics.QueryRAMUsageAvg(start, end))
	ramUsageMaxFuture := source.WithGroup(grp, metrics.QueryRAMUsageMax(start, end))

	type containerKey struct {
		podUID string
		name   string
	}

	containerMap := make(map[containerKey]*kubemodel.Container)

	containerUptimeResult, _ := containerUptimeFuture.Await()
	for _, res := range containerUptimeResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		containerMap[key] = &kubemodel.Container{
			PodUID:           res.UID,
			Name:             res.Container,
			ResourceRequests: make(kubemodel.ResourceQuantities),
			ResourceLimits:   make(kubemodel.ResourceQuantities),
			Start:            s,
			End:              e,
		}
	}

	containerResourceRequestsResult, _ := containerResourceRequestsFuture.Await()
	for _, res := range containerResourceRequestsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add resource requests", res.UID, res.Container)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		container.ResourceRequests.Set(resource, unit, kubemodel.StatAvg, value)
	}

	containerResourceLimitsResult, _ := containerResourceLimitsFuture.Await()
	for _, res := range containerResourceLimitsResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add resource limits", res.UID, res.Container)
			continue
		}
		resource, unit, value := resourceUnitValue(res.Resource, res.Unit, res.Value)
		container.ResourceLimits.Set(resource, unit, kubemodel.StatAvg, value)
	}

	cpuCoresAllocatedResult, _ := cpuCoresAllocatedFuture.Await()
	for _, res := range cpuCoresAllocatedResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU cores allocated", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPUCoresAllocated = res.Data[0].Value
		}
	}

	ramBytesAllocatedResult, _ := ramBytesAllocatedFuture.Await()
	for _, res := range ramBytesAllocatedResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM bytes allocated", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMBytesAllocated = res.Data[0].Value
		}
	}

	cpuUsageAvgResult, _ := cpuUsageAvgFuture.Await()
	for _, res := range cpuUsageAvgResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU usage avg", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPUCoreUsageAvg = res.Data[0].Value
		}
	}

	cpuUsageMaxResult, _ := cpuUsageMaxFuture.Await()
	for _, res := range cpuUsageMaxResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add CPU usage max", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.CPUCoreUsageMax = res.Data[0].Value
		}
	}

	ramUsageAvgResult, _ := ramUsageAvgFuture.Await()
	for _, res := range ramUsageAvgResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM usage avg", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMBytesUsageAvg = res.Data[0].Value
		}
	}

	ramUsageMaxResult, _ := ramUsageMaxFuture.Await()
	for _, res := range ramUsageMaxResult {
		key := containerKey{podUID: res.UID, name: res.Container}
		container, ok := containerMap[key]
		if !ok {
			log.Warnf("container %s/%s has not been initialized to add RAM usage max", res.UID, res.Container)
			continue
		}
		if len(res.Data) > 0 {
			container.RAMBytesUsageMax = res.Data[0].Value
		}
	}

	for _, container := range containerMap {
		err := kms.RegisterContainer(container)
		if err != nil {
			log.Warnf("Failed to register container: %s", err.Error())
		}
	}

	return nil
}