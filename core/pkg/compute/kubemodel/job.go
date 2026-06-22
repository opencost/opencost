package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeJobs(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	jobInfoResultFuture := source.WithGroup(grp, metrics.QueryJobInfo(start, end))
	jobUptimeResultFuture := source.WithGroup(grp, metrics.QueryJobUptime(start, end))
	jobLabelsResultFuture := source.WithGroup(grp, metrics.QueryJobLabels(start, end))
	jobAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryJobAnnotations(start, end))

	jobMap := make(map[string]*kubemodel.Job)

	jobInfoResult, _ := jobInfoResultFuture.Await()
	for _, res := range jobInfoResult {
		jobMap[res.UID] = &kubemodel.Job{
			UID:          res.UID,
			Name:         res.Job,
			NamespaceUID: res.NamespaceUID,
		}
	}

	jobUptimeResult, _ := jobUptimeResultFuture.Await()
	for _, res := range jobUptimeResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		job.Start = s
		job.End = e
	}

	jobLabelsResult, _ := jobLabelsResultFuture.Await()
	for _, res := range jobLabelsResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		job.Labels = res.Labels
	}

	jobAnnotationsResult, _ := jobAnnotationsResultFuture.Await()
	for _, res := range jobAnnotationsResult {
		job, ok := jobMap[res.UID]
		if !ok {
			log.Warnf("job with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		job.Annotations = res.Annotations
	}

	for _, job := range jobMap {
		err := kms.RegisterJob(job)
		if err != nil {
			log.Warnf("Failed to register job: %s", err.Error())
		}
	}

	return nil
}