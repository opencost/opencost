package scrape

import (
	"context"
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// DCGM metrics
const (
	DCGMFIPROFGRENGINEACTIVE = "DCGM_FI_PROF_GR_ENGINE_ACTIVE"
	DCGMFIDEVDECUTIL         = "DCGM_FI_DEV_DEC_UTIL"
)

func newDCGMScrapper(k8s kubernetes.Interface, updater metric.MetricUpdater) Scraper {
	tp := newDCGMTargetProvider(k8s)
	return newDCGMTargetScraper(tp, updater)
}

func newDCGMTargetScraper(provider target.TargetProvider, updater metric.MetricUpdater) *TargetScraper {
	return newTargetScrapper(
		provider,
		updater,
		[]string{
			DCGMFIPROFGRENGINEACTIVE,
			DCGMFIDEVDECUTIL,
		},
		true)
}

type DCGMTargetProvider struct {
	clusterCache clustercache.ClusterCache
}

func newDCGMTargetProvider(clusterCache clustercache.ClusterCache) *DCGMTargetProvider {
	return &DCGMTargetProvider{
		clusterCache: clusterCache,
	}
}

func (p *DCGMTargetProvider) GetTargets() []target.ScrapeTarget {
	svcs := p.clusterCache.GetAllServices()

	var targets []target.ScrapeTarget
	for _, svc := range svcs {
		if svc.ClusterIP == "" || svc.SpecSelector == nil {
			continue
		}
		if name := svc.SpecSelector["app.kubernetes.io/name"]; name != "dcm-collector" {
			continue
		}
		port := 9400

		t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", svc.ClusterIP, port))
		targets = append(targets, t)
	}

	return targets
}
