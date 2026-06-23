package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeResourceQuotas(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	rqInfoResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaInfo(start, end))
	rqUptimeResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaUptime(start, end))

	// spec.hard.requests
	rqSpecCPURequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPURequestAverage(start, end))
	rqSpecCPURequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPURequestMax(start, end))
	rqSpecRAMRequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMRequestAverage(start, end))
	rqSpecRAMRequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMRequestMax(start, end))

	// spec.hard.limits
	rqSpecCPULimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPULimitAverage(start, end))
	rqSpecCPULimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecCPULimitMax(start, end))
	rqSpecRAMLimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMLimitAverage(start, end))
	rqSpecRAMLimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaSpecRAMLimitMax(start, end))

	// status.used.requests
	rqStatusUsedCPURequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPURequestAverage(start, end))
	rqStatusUsedCPURequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPURequestMax(start, end))
	rqStatusUsedRAMRequestAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMRequestAverage(start, end))
	rqStatusUsedRAMRequestMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMRequestMax(start, end))

	// status.used.limits
	rqStatusUsedCPULimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPULimitAverage(start, end))
	rqStatusUsedCPULimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedCPULimitMax(start, end))
	rqStatusUsedRAMLimitAverageResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMLimitAverage(start, end))
	rqStatusUsedRAMLimitMaxResultFuture := source.WithGroup(grp, metrics.QueryResourceQuotaStatusUsedRAMLimitMax(start, end))

	rqMap := make(map[string]*kubemodel.ResourceQuota)

	// Initialize resource quotas from info
	rqInfoResult, _ := rqInfoResultFuture.Await()
	for _, res := range rqInfoResult {
		rqMap[res.UID] = &kubemodel.ResourceQuota{
			UID:          res.UID,
			Name:         res.ResourceQuota,
			NamespaceUID: res.NamespaceUID,
			Spec:         &kubemodel.ResourceQuotaSpec{Hard: &kubemodel.ResourceQuotaSpecHard{}},
			Status:       &kubemodel.ResourceQuotaStatus{Used: &kubemodel.ResourceQuotaStatusUsed{}},
		}
	}

	rqUptimeResult, _ := rqUptimeResultFuture.Await()
	for _, res := range rqUptimeResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		rq.Start = s
		rq.End = e
	}

	rqSpecCPURequestAverageResult, _ := rqSpecCPURequestAverageResultFuture.Await()
	for _, res := range rqSpecCPURequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU request average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)

	}

	rqSpecCPURequestMaxResult, _ := rqSpecCPURequestMaxResultFuture.Await()
	for _, res := range rqSpecCPURequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU request max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMRequestAverageResult, _ := rqSpecRAMRequestAverageResultFuture.Await()
	for _, res := range rqSpecRAMRequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM request average", res.UID)
			continue
		}

		rq.Spec.Hard.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqSpecRAMRequestMaxResult, _ := rqSpecRAMRequestMaxResultFuture.Await()
	for _, res := range rqSpecRAMRequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM request max", res.UID)
			continue
		}

		rq.Spec.Hard.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	rqSpecCPULimitAverageResult, _ := rqSpecCPULimitAverageResultFuture.Await()
	for _, res := range rqSpecCPULimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU limit average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)

	}

	rqSpecCPULimitMaxResult, _ := rqSpecCPULimitMaxResultFuture.Await()
	for _, res := range rqSpecCPULimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec CPU limit max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Spec.Hard.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqSpecRAMLimitAverageResult, _ := rqSpecRAMLimitAverageResultFuture.Await()
	for _, res := range rqSpecRAMLimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM limit average", res.UID)
			continue
		}

		rq.Spec.Hard.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqSpecRAMLimitMaxResult, _ := rqSpecRAMLimitMaxResultFuture.Await()
	for _, res := range rqSpecRAMLimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add spec RAM limit max", res.UID)
			continue
		}

		rq.Spec.Hard.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	rqStatusUsedCPURequestAverageResult, _ := rqStatusUsedCPURequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU request average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPURequestMaxResult, _ := rqStatusUsedCPURequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPURequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU request max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetRequest(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMRequestAverageResult, _ := rqStatusUsedRAMRequestAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM request average", res.UID)
			continue
		}

		rq.Status.Used.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqStatusUsedRAMRequestMaxResult, _ := rqStatusUsedRAMRequestMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMRequestMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM request max", res.UID)
			continue
		}

		rq.Status.Used.SetRequest(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	rqStatusUsedCPULimitAverageResult, _ := rqStatusUsedCPULimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU limit average", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatAvg, mcpu)
	}

	rqStatusUsedCPULimitMaxResult, _ := rqStatusUsedCPULimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedCPULimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status CPU limit max", res.UID)
			continue
		}

		mcpu := res.Value * 1000
		rq.Status.Used.SetLimit(kubemodel.ResourceCPU, kubemodel.UnitMillicore, kubemodel.StatMax, mcpu)
	}

	rqStatusUsedRAMLimitAverageResult, _ := rqStatusUsedRAMLimitAverageResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitAverageResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM limit average", res.UID)
			continue
		}

		rq.Status.Used.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatAvg, res.Value)
	}

	rqStatusUsedRAMLimitMaxResult, _ := rqStatusUsedRAMLimitMaxResultFuture.Await()
	for _, res := range rqStatusUsedRAMLimitMaxResult {
		rq, ok := rqMap[res.UID]
		if !ok {
			log.Warnf("resource quota with UID '%s' has not been initialized to add status RAM limit max", res.UID)
			continue
		}

		rq.Status.Used.SetLimit(kubemodel.ResourceMemory, kubemodel.UnitByte, kubemodel.StatMax, res.Value)
	}

	for _, resourceQuota := range rqMap {
		err := kms.RegisterResourceQuota(resourceQuota)
		if err != nil {
			log.Warnf("Failed to register resource quota: %s", err.Error())
		}
	}

	return nil
}
