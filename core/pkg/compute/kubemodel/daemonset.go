package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeDaemonSets(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	daemonSetInfoResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetInfo(start, end))
	daemonSetUptimeResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetUptime(start, end))
	daemonSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetLabels(start, end))
	daemonSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryDaemonSetAnnotations(start, end))

	daemonSetMap := make(map[string]*kubemodel.DaemonSet)

	daemonSetInfoResult, _ := daemonSetInfoResultFuture.Await()
	for _, res := range daemonSetInfoResult {
		daemonSetMap[res.UID] = &kubemodel.DaemonSet{
			UID:          res.UID,
			Name:         res.DaemonSet,
			NamespaceUID: res.NamespaceUID,
		}
	}

	daemonSetUptimeResult, _ := daemonSetUptimeResultFuture.Await()
	for _, res := range daemonSetUptimeResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		daemonSet.Start = s
		daemonSet.End = e
	}

	daemonSetLabelsResult, _ := daemonSetLabelsResultFuture.Await()
	for _, res := range daemonSetLabelsResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		daemonSet.Labels = res.Labels
	}

	daemonSetAnnotationsResult, _ := daemonSetAnnotationsResultFuture.Await()
	for _, res := range daemonSetAnnotationsResult {
		daemonSet, ok := daemonSetMap[res.UID]
		if !ok {
			log.Warnf("daemonset with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		daemonSet.Annotations = res.Annotations
	}

	for _, daemonSet := range daemonSetMap {
		err := kms.RegisterDaemonSet(daemonSet)
		if err != nil {
			log.Warnf("Failed to register daemonset: %s", err.Error())
		}
	}

	return nil
}
