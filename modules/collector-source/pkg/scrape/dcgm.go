package scrape

import (
	"fmt"
	"regexp"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

var dcgmRegex = regexp.MustCompile("(?i)(.*dcgm-exporter.*)")

func newDCGMScrapper(clusterCache clustercache.ClusterCache) Scraper {
	tp := newDCGMTargetProvider(clusterCache)
	return newDCGMTargetScraper(tp)
}

func newDCGMTargetScraper(provider target.TargetProvider) *TargetScraper {
	return newTargetScrapper(
		event.DCGMScraperName,
		provider,
		[]string{
			metric.DCGMFIPROFGRENGINEACTIVE,
			metric.DCGMFIDEVDECUTIL,
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
		if svc.ClusterIP == "" || !isDCGM(svc.SpecSelector) {
			continue
		}
		port := 9400
		log.Debugf("DCGM: found target: http://%s:%d/metrics", svc.ClusterIP, port)
		t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", svc.ClusterIP, port))
		targets = append(targets, t)
	}

	return targets
}

func isDCGM(labels map[string]string) bool {
	keys := []string{
		"app",
		"app.kubernetes.io/name",
		"app.kubernetes.io/component",
	}

	for _, key := range keys {
		if value, ok := labels[key]; ok {
			if dcgmRegex.MatchString(value) {
				return true
			}
		}
	}

	return false
}
