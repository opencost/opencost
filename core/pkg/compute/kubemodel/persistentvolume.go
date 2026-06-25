package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computePersistentVolumes(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	pvInfoResultFuture := source.WithGroup(grp, metrics.QueryKMPVInfo(start, end))
	pvUptimeResultFuture := source.WithGroup(grp, metrics.QueryPVUptime(start, end))
	pvBytesResultFuture := source.WithGroup(grp, metrics.QueryPVBytes(start, end))

	pvMap := make(map[string]*kubemodel.PersistentVolume)

	pvInfoResult, _ := pvInfoResultFuture.Await()
	for _, res := range pvInfoResult {
		pvMap[res.UID] = &kubemodel.PersistentVolume{
			UID:             res.UID,
			Name:            res.PersistentVolume,
			StorageClass:    res.StorageClass,
			CSIVolumeHandle: res.CSIVolumeHandle,
		}
	}

	pvUptimeResult, _ := pvUptimeResultFuture.Await()
	for _, res := range pvUptimeResult {
		pv, ok := pvMap[res.UID]
		if !ok {
			log.Warnf("persistent volume with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		pv.Start = s
		pv.End = e
	}

	pvBytesResult, _ := pvBytesResultFuture.Await()
	for _, res := range pvBytesResult {
		pv, ok := pvMap[res.UID]
		if !ok {
			log.Warnf("persistent volume with UID '%s' has not been initialized to add bytes", res.UID)
			continue
		}

		pv.SizeBytes = res.Value

	}

	for _, pv := range pvMap {
		err := kms.RegisterPersistentVolume(pv)
		if err != nil {
			log.Warnf("Failed to register persistent volume: %s", err.Error())
		}
	}

	return nil
}
