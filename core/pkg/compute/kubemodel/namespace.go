package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeNamespaces(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	nsInfoResultFuture := source.WithGroup(grp, metrics.QueryNamespaceInfo(start, end))
	nsUptimeResultFuture := source.WithGroup(grp, metrics.QueryNamespaceUptime(start, end))
	nsLabelsResultFuture := source.WithGroup(grp, metrics.QueryNamespaceLabels(start, end))
	nsAnnosResultFuture := source.WithGroup(grp, metrics.QueryNamespaceAnnotations(start, end))

	nsMap := make(map[string]*kubemodel.Namespace)

	// Initialize namespaces from info
	nsInfoResult, _ := nsInfoResultFuture.Await()
	for _, res := range nsInfoResult {
		nsMap[res.UID] = &kubemodel.Namespace{
			UID:  res.UID,
			Name: res.Namespace,
		}
	}

	nsUptimeResult, _ := nsUptimeResultFuture.Await()
	for _, res := range nsUptimeResult {
		ns, ok := nsMap[res.UID]
		if !ok {
			log.Warnf("namespace with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		ns.Start = s
		ns.End = e
	}

	nsLabelsResult, _ := nsLabelsResultFuture.Await()
	for _, res := range nsLabelsResult {
		ns, ok := nsMap[res.UID]
		if !ok {
			log.Warnf("namespace with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		ns.Labels = res.Labels
	}

	nsAnnosResult, _ := nsAnnosResultFuture.Await()
	for _, res := range nsAnnosResult {
		ns, ok := nsMap[res.UID]
		if !ok {
			log.Warnf("namespace with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		ns.Annotations = res.Annotations
	}

	for _, namespace := range nsMap {
		err := kms.RegisterNamespace(namespace)
		if err != nil {
			log.Warnf("Failed to register namespace: %s", err.Error())
		}
	}

	return nil
}
