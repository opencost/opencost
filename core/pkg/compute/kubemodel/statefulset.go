package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeStatefulSets(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	statefulSetInfoResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetInfo(start, end))
	statefulSetUptimeResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetUptime(start, end))
	statefulSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetLabels(start, end))
	statefulSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetAnnotations(start, end))
	statefulSetMatchLabelsResultFuture := source.WithGroup(grp, metrics.QueryStatefulSetMatchLabels(start, end))

	statefulSetMap := make(map[string]*kubemodel.StatefulSet)

	statefulSetInfoResult, _ := statefulSetInfoResultFuture.Await()
	for _, res := range statefulSetInfoResult {
		statefulSetMap[res.UID] = &kubemodel.StatefulSet{
			UID:          res.UID,
			Name:         res.StatefulSet,
			NamespaceUID: res.NamespaceUID,
		}
	}

	statefulSetUptimeResult, _ := statefulSetUptimeResultFuture.Await()
	for _, res := range statefulSetUptimeResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		statefulSet.Start = s
		statefulSet.End = e
	}

	statefulSetLabelsResult, _ := statefulSetLabelsResultFuture.Await()
	for _, res := range statefulSetLabelsResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		statefulSet.Labels = res.Labels
	}

	statefulSetAnnotationsResult, _ := statefulSetAnnotationsResultFuture.Await()
	for _, res := range statefulSetAnnotationsResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		statefulSet.Annotations = res.Annotations
	}

	statefulSetMatchLabelsResult, _ := statefulSetMatchLabelsResultFuture.Await()
	for _, res := range statefulSetMatchLabelsResult {
		statefulSet, ok := statefulSetMap[res.UID]
		if !ok {
			log.Warnf("statefulset with UID '%s' has not been initialized to add match labels", res.UID)
			continue
		}
		statefulSet.MatchLabels = res.Labels
	}

	for _, statefulSet := range statefulSetMap {
		err := kms.RegisterStatefulSet(statefulSet)
		if err != nil {
			log.Warnf("Failed to register statefulset: %s", err.Error())
		}
	}

	return nil
}
