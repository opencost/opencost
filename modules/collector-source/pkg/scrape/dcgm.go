package scrape

import (
	"fmt"
	"regexp"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	v1 "k8s.io/api/core/v1"
)

var dcgmRegex = regexp.MustCompile("(?i)(.*dcgm-exporter.*)")

func newDCGMScrapper(clusterCache clustercache.ClusterCache) Scraper {
	tp := newDCGMTargetProvider(clusterCache)
	return newDCGMTargetScraper(tp, podUIDEnricher(clusterCache))
}

func newDCGMTargetScraper(provider target.TargetProvider, enrich UpdateEnricher) *TargetScraper {
	return newTargetScrapper(
		event.DCGMScraperName,
		provider,
		[]string{
			metric.DCGMFIPROFGRENGINEACTIVE,
			metric.DCGMFIDEVDECUTIL,
		},
		true,
		enrich)
}

// podUIDEnricher backfills pod_uid on a DCGM update using its own namespace/pod name labels,
// resolved against a freshly built index of the cluster's current pods. Left unset if pod_uid
// is already present, or if namespace/pod can't be resolved to a known pod.
func podUIDEnricher(clusterCache clustercache.ClusterCache) UpdateEnricher {
	index := buildPodIndex(clusterCache.GetAllPods())
	return func(update metric.Update) metric.Update {
		if update.Labels[source.PodUIDLabel] != "" {
			return update
		}
		namespace, pod := update.Labels[source.NamespaceLabel], update.Labels[source.PodLabel]
		if namespace == "" || pod == "" {
			return update
		}

		if uid, ok := index[podKey{namespace: namespace, name: pod}]; ok {
			update.Labels[source.PodUIDLabel] = string(uid)
		}
		return update
	}
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
