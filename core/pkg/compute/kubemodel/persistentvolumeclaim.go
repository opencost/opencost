package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computePersistentVolumeClaims(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	pvcInfoResultFuture := source.WithGroup(grp, metrics.QueryKMPVCInfo(start, end))
	pvcUptimeResultFuture := source.WithGroup(grp, metrics.QueryPVCUptime(start, end))
	pvcBytesRequestedResultFuture := source.WithGroup(grp, metrics.QueryPVCBytesRequested(start, end))
	pvcBytesUsedAvgResultFuture := source.WithGroup(grp, metrics.QueryPVCBytesUsedAverage(start, end))
	pvcBytesUsedMaxResultFuture := source.WithGroup(grp, metrics.QueryPVCBytesUsedMax(start, end))

	pvcMap := make(map[string]*kubemodel.PersistentVolumeClaim)

	pvcInfoResult, _ := pvcInfoResultFuture.Await()
	for _, res := range pvcInfoResult {
		pvcMap[res.UID] = &kubemodel.PersistentVolumeClaim{
			UID:                 res.UID,
			Name:                res.PersistentVolumeClaim,
			NamespaceUID:        res.NamespaceUID,
			PersistentVolumeUID: res.PVUID,
			StorageClass:        res.StorageClass,
		}
	}

	pvcUptimeResult, _ := pvcUptimeResultFuture.Await()
	for _, res := range pvcUptimeResult {
		pvc, ok := pvcMap[res.UID]
		if !ok {
			log.Warnf("persistent volume claim with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		pvc.Start = s
		pvc.End = e
	}

	pvcBytesRequestedResult, _ := pvcBytesRequestedResultFuture.Await()
	for _, res := range pvcBytesRequestedResult {
		pvc, ok := pvcMap[res.UID]
		if !ok {
			log.Warnf("persistent volume claim with UID '%s' has not been initialized to add requested bytes", res.UID)
			continue
		}
		if len(res.Data) > 0 {
			pvc.RequestedBytes = res.Data[0].Value
		}
	}

	pvcBytesUsedAvgResult, _ := pvcBytesUsedAvgResultFuture.Await()
	for _, res := range pvcBytesUsedAvgResult {
		pvc, ok := pvcMap[res.UID]
		if !ok {
			log.Warnf("persistent volume claim with UID '%s' has not been initialized to add bytes used average", res.UID)
			continue
		}
		pvc.UsageBytesAvg = res.Value
	}

	pvcBytesUsedMaxResult, _ := pvcBytesUsedMaxResultFuture.Await()
	for _, res := range pvcBytesUsedMaxResult {
		pvc, ok := pvcMap[res.UID]
		if !ok {
			log.Warnf("persistent volume claim with UID '%s' has not been initialized to add bytes used max", res.UID)
			continue
		}
		pvc.UsageBytesMax = res.Value
	}

	for _, pvc := range pvcMap {
		err := kms.RegisterPVC(pvc)
		if err != nil {
			log.Warnf("Failed to register persistent volume claim: %s", err.Error())
		}
	}

	return nil
}
