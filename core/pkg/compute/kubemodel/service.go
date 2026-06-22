package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computeServices(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	serviceInfoResultFuture := source.WithGroup(grp, metrics.QueryServiceInfo(start, end))
	serviceUptimeResultFuture := source.WithGroup(grp, metrics.QueryServiceUptime(start, end))
	serviceSelectorLabelsResultFuture := source.WithGroup(grp, metrics.QueryServiceSelectorLabels(start, end))

	serviceMap := make(map[string]*kubemodel.Service)

	// Initialize services from info
	serviceInfoResult, _ := serviceInfoResultFuture.Await()
	for _, res := range serviceInfoResult {
		serviceMap[res.UID] = &kubemodel.Service{
			UID:              res.UID,
			NamespaceUID:     res.NamespaceUID,
			Name:             res.Service,
			Type:             kubemodel.ParseServiceType(res.ServiceType),
			LBIngressAddress: res.LBIngressAddress,
		}
	}

	serviceUptimeResult, _ := serviceUptimeResultFuture.Await()
	for _, res := range serviceUptimeResult {
		service, ok := serviceMap[res.UID]
		if !ok {
			log.Warnf("service with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		service.Start = s
		service.End = e
	}

	serviceSelectorLabelsResult, _ := serviceSelectorLabelsResultFuture.Await()
	for _, res := range serviceSelectorLabelsResult {
		service, ok := serviceMap[res.UID]
		if !ok {
			log.Warnf("service with UID '%s' has not been initialized to add selector labels", res.UID)
			continue
		}
		service.Selector = res.Labels
	}

	for _, service := range serviceMap {
		err := kms.RegisterService(service)
		if err != nil {
			log.Warnf("Failed to register service: %s", err.Error())
		}
	}

	return nil
}