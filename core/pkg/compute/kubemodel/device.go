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

	return nil
}
