package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeCronJobs(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	cronJobInfoResultFuture := source.WithGroup(grp, metrics.QueryCronJobInfo(start, end))
	cronJobUptimeResultFuture := source.WithGroup(grp, metrics.QueryCronJobUptime(start, end))
	cronJobLabelsResultFuture := source.WithGroup(grp, metrics.QueryCronJobLabels(start, end))
	cronJobAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryCronJobAnnotations(start, end))

	cronJobMap := make(map[string]*kubemodel.CronJob)

	cronJobInfoResult, _ := cronJobInfoResultFuture.Await()
	for _, res := range cronJobInfoResult {
		cronJobMap[res.UID] = &kubemodel.CronJob{
			UID:          res.UID,
			Name:         res.CronJob,
			NamespaceUID: res.NamespaceUID,
		}
	}

	cronJobUptimeResult, _ := cronJobUptimeResultFuture.Await()
	for _, res := range cronJobUptimeResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		cronJob.Start = s
		cronJob.End = e
	}

	cronJobLabelsResult, _ := cronJobLabelsResultFuture.Await()
	for _, res := range cronJobLabelsResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		cronJob.Labels = res.Labels
	}

	cronJobAnnotationsResult, _ := cronJobAnnotationsResultFuture.Await()
	for _, res := range cronJobAnnotationsResult {
		cronJob, ok := cronJobMap[res.UID]
		if !ok {
			log.Warnf("cronjob with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		cronJob.Annotations = res.Annotations
	}

	for _, cronJob := range cronJobMap {
		err := kms.RegisterCronJob(cronJob)
		if err != nil {
			log.Warnf("Failed to register cronjob: %s", err.Error())
		}
	}

	return nil
}
