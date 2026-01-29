package scrape

import (
	"fmt"
	"regexp"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	v1 "k8s.io/api/core/v1"
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
	port         int
}

func newDCGMTargetProvider(clusterCache clustercache.ClusterCache) *DCGMTargetProvider {
	return &DCGMTargetProvider{
		clusterCache: clusterCache,
		port:         9400,
	}
}

func (p *DCGMTargetProvider) GetTargets() []target.ScrapeTarget {
	// NOTE: The proper way to discover these targets is to first identify a Service that
	// NOTE: matches a specific selector. Then, locate the Endpoints kubernetes resource associated
	// NOTE: with that Service. This Endpoints resource has a list of all the targetted pods and their
	// NOTE: addresses. We do _not_ have the Endpoints resource on our cluster cache at the moment,
	// NOTE: so we'll perform this lookup ourselves.
	pods := p.clusterCache.GetAllPods()

	var targets []target.ScrapeTarget
	for _, pod := range pods {
		if pod.Status.Phase == v1.PodRunning && isDCGM(pod.Labels) {
			log.Debugf("DCGM: found target: http://%s:%d/metrics", pod.Status.PodIP, p.port)

			t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, p.port))
			targets = append(targets, t)
		}
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
