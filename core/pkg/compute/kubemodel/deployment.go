package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeDeployments(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	deploymentInfoResultFuture := source.WithGroup(grp, metrics.QueryDeploymentInfo(start, end))
	deploymentUptimeResultFuture := source.WithGroup(grp, metrics.QueryDeploymentUptime(start, end))
	deploymentLabelsResultFuture := source.WithGroup(grp, metrics.QueryDeploymentLabels(start, end))
	deploymentAnnotationsResultFuture := source.WithGroup(grp, metrics.QueryDeploymentAnnotations(start, end))
	deploymentMatchLabelsResultFuture := source.WithGroup(grp, metrics.QueryDeploymentMatchLabels(start, end))

	deploymentMap := make(map[string]*kubemodel.Deployment)

	deploymentInfoResult, _ := deploymentInfoResultFuture.Await()
	for _, res := range deploymentInfoResult {
		deploymentMap[res.UID] = &kubemodel.Deployment{
			UID:          res.UID,
			Name:         res.Deployment,
			NamespaceUID: res.NamespaceUID,
		}
	}

	deploymentUptimeResult, _ := deploymentUptimeResultFuture.Await()
	for _, res := range deploymentUptimeResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		deployment.Start = s
		deployment.End = e
	}

	deploymentLabelsResult, _ := deploymentLabelsResultFuture.Await()
	for _, res := range deploymentLabelsResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		deployment.Labels = res.Labels
	}

	deploymentAnnotationsResult, _ := deploymentAnnotationsResultFuture.Await()
	for _, res := range deploymentAnnotationsResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		deployment.Annotations = res.Annotations
	}

	deploymentMatchLabelsResult, _ := deploymentMatchLabelsResultFuture.Await()
	for _, res := range deploymentMatchLabelsResult {
		deployment, ok := deploymentMap[res.UID]
		if !ok {
			log.Warnf("deployment with UID '%s' has not been initialized to add match labels", res.UID)
			continue
		}
		deployment.MatchLabels = res.Labels
	}

	for _, deployment := range deploymentMap {
		err := kms.RegisterDeployment(deployment)
		if err != nil {
			log.Warnf("Failed to register deployment: %s", err.Error())
		}
	}

	return nil
}
