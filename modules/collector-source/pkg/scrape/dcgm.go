package scrape

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

// DCGM metrics
const (
	DCGMFIPROFGRENGINEACTIVE = "DCGM_FI_PROF_GR_ENGINE_ACTIVE"
	DCGMFIDEVDECUTIL         = "DCGM_FI_DEV_DEC_UTIL"
)

func newDCGMScrapper(clusterCache clustercache.ClusterCache, updater metric.MetricUpdater) Scraper {
	tp := newDCGMTargetProvider(clusterCache)
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
		// TODO do something in relation to Thomas' comment https://github.com/opencost/opencost/pull/3110
		if name := svc.SpecSelector["app.kubernetes.io/name"]; name != "dcgm-exporter" {
			continue
		}
		port := 9400
		log.Debugf("DCGM: found target: http://%s:%d/metrics", svc.ClusterIP, port)
		t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", svc.ClusterIP, port))
		targets = append(targets, t)
	}

	return targets
}
