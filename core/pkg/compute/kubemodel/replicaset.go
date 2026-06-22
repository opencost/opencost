package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeReplicaSets(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	replicaSetInfoResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetInfo(start, end))
	replicaSetUptimeResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetUptime(start, end))
	replicaSetOwnerResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetOwners(start, end))
	replicaSetLabelsResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetLabels(start, end))
	replicaSetAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryReplicaSetAnnotations(start, end))

	replicaSetMap := make(map[string]*kubemodel.ReplicaSet)

	replicaSetInfoResult, _ := replicaSetInfoResultFuture.Await()
	for _, res := range replicaSetInfoResult {
		replicaSetMap[res.UID] = &kubemodel.ReplicaSet{
			UID:          res.UID,
			Name:         res.ReplicaSet,
			NamespaceUID: res.NamespaceUID,
		}
	}

	replicaSetUptimeResult, _ := replicaSetUptimeResultFuture.Await()
	for _, res := range replicaSetUptimeResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		replicaSet.Start = s
		replicaSet.End = e
	}

	replicaSetOwnersResult, _ := replicaSetOwnerResultFuture.Await()
	for _, res := range replicaSetOwnersResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add owner", res.UID)
			continue
		}
		replicaSet.Owners = append(replicaSet.Owners, kubemodel.Owner{
			UID:        res.OwnerUID,
			Kind:       kubemodel.ParseOwnerKind(res.OwnerKind),
			Controller: res.Controller,
		})
	}

	replicaSetLabelsResult, _ := replicaSetLabelsResultFuture.Await()
	for _, res := range replicaSetLabelsResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		replicaSet.Labels = res.Labels
	}

	replicaSetAnnotationsResult, _ := replicaSetAnnotationsResultFuture.Await()
	for _, res := range replicaSetAnnotationsResult {
		replicaSet, ok := replicaSetMap[res.UID]
		if !ok {
			log.Warnf("replicaset with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		replicaSet.Annotations = res.Annotations
	}

	for _, replicaSet := range replicaSetMap {
		err := kms.RegisterReplicaSet(replicaSet)
		if err != nil {
			log.Warnf("Failed to register replicaset: %s", err.Error())
		}
	}

	return nil
}