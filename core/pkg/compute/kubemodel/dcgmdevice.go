package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeDCGMDevices(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	dcgmInfoFuture := source.WithGroup(grp, metrics.QueryDCGMDeviceInfo(start, end))
	dcgmUptimeFuture := source.WithGroup(grp, metrics.QueryDCGMDeviceUptime(start, end))
	dcgmUsageAvgFuture := source.WithGroup(grp, metrics.QueryDCGMContainerUsageAvg(start, end))
	dcgmUsageMaxFuture := source.WithGroup(grp, metrics.QueryDCGMContainerUsageMax(start, end))

	deviceMap := make(map[string]*kubemodel.DCGMDevice)

	dcgmInfoResult, _ := dcgmInfoFuture.Await()
	for _, res := range dcgmInfoResult {
		if res.UUID == "" {
			continue
		}
		if _, ok := deviceMap[res.UUID]; ok {
			continue
		}
		deviceMap[res.UUID] = &kubemodel.DCGMDevice{
			UUID:      res.UUID,
			Device:    res.Device,
			ModelName: res.ModelName,
			PodUsages: make(map[string]kubemodel.DCGMPod),
		}
	}

	dcgmUptimeResult, _ := dcgmUptimeFuture.Await()
	for _, res := range dcgmUptimeResult {
		d, ok := deviceMap[res.UUID]
		if !ok {
			log.Warnf("DCGM uptime result for unknown device UUID '%s'", res.UUID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		d.Start = s
		d.End = e
	}

	dcgmUsageAvgResult, _ := dcgmUsageAvgFuture.Await()
	for _, res := range dcgmUsageAvgResult {
		device, ok := deviceMap[res.UUID]
		if !ok || res.PodUID == "" || res.Container == "" {
			continue
		}
		pod, ok := device.PodUsages[res.PodUID]
		if !ok {
			pod = kubemodel.DCGMPod{ContainerUsages: make(map[string]kubemodel.DCGMContainer)}
		}
		c := pod.ContainerUsages[res.Container]
		c.UsageAvg = res.Value
		pod.ContainerUsages[res.Container] = c
		device.PodUsages[res.PodUID] = pod
	}

	dcgmUsageMaxResult, _ := dcgmUsageMaxFuture.Await()
	for _, res := range dcgmUsageMaxResult {
		device, ok := deviceMap[res.UUID]
		if !ok || res.PodUID == "" || res.Container == "" {
			continue
		}
		pod, ok := device.PodUsages[res.PodUID]
		if !ok {
			pod = kubemodel.DCGMPod{ContainerUsages: make(map[string]kubemodel.DCGMContainer)}
		}
		c := pod.ContainerUsages[res.Container]
		c.UsageMax = res.Value
		pod.ContainerUsages[res.Container] = c
		device.PodUsages[res.PodUID] = pod
	}

	for _, device := range deviceMap {
		if err := kms.RegisterDCGMDevice(device); err != nil {
			log.Warnf("Failed to register DCGM device: %s", err.Error())
		}
	}

	return nil
}
