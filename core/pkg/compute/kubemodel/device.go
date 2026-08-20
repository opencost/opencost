package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeDevices(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	infoFuture := source.WithGroup(grp, metrics.QueryDCGMDeviceInfo(start, end))
	uptimeFuture := source.WithGroup(grp, metrics.QueryDCGMDeviceUptime(start, end))
	usageAvgFuture := source.WithGroup(grp, metrics.QueryDCGMContainerUsageAvg(start, end))
	usageMaxFuture := source.WithGroup(grp, metrics.QueryDCGMContainerUsageMax(start, end))

	deviceMap := make(map[string]*kubemodel.Device)

	infoResult, _ := infoFuture.Await()
	for _, res := range infoResult {
		if res.UUID == "" {
			continue
		}
		if _, ok := deviceMap[res.UUID]; ok {
			continue
		}
		deviceMap[res.UUID] = &kubemodel.Device{
			UUID:      res.UUID,
			Device:    res.Device,
			ModelName: res.ModelName,
		}
	}

	uptimeResult, _ := uptimeFuture.Await()
	for _, res := range uptimeResult {
		d, ok := deviceMap[res.UUID]
		if !ok {
			log.Warnf("DCGM uptime result for unknown device UUID '%s'", res.UUID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		d.Start = s
		d.End = e
	}

	for _, device := range deviceMap {
		if err := kms.RegisterDevice(device); err != nil {
			log.Warnf("Failed to register device: %s", err.Error())
		}
	}

	setUsage := func(res *source.DCGMDeviceContainerUsageResult, apply func(*kubemodel.DeviceUsage)) {
		if res.PodUID == "" || res.Container == "" {
			return
		}
		key := (&kubemodel.Container{PodUID: res.PodUID, Name: res.Container}).GetKey()
		container, ok := kms.Containers[key]
		if !ok {
			return
		}
		if container.DeviceUsages == nil {
			container.DeviceUsages = make(map[string]kubemodel.DeviceUsage)
		}
		usage := container.DeviceUsages[res.UUID]
		apply(&usage)
		container.DeviceUsages[res.UUID] = usage
	}

	usageAvgResult, _ := usageAvgFuture.Await()
	for _, res := range usageAvgResult {
		setUsage(res, func(u *kubemodel.DeviceUsage) { u.UsageAvg = res.Value })
	}

	usageMaxResult, _ := usageMaxFuture.Await()
	for _, res := range usageMaxResult {
		setUsage(res, func(u *kubemodel.DeviceUsage) { u.UsageMax = res.Value })
	}

	return nil
}
