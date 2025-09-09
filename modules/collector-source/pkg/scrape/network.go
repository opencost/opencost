package scrape

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	v1 "k8s.io/api/core/v1"
)

const (
	NetworkCostsNameLabel     = "network-costs"
	NetworkCostsInstanceLabel = "kubecost"
)

func newNetworkScraper(
	port int,
	clusterCache clustercache.ClusterCache,
) Scraper {
	tp := NewNetworkTargetProvider(port, clusterCache)
	return newNetworkTargetScraper(tp)
}

func newNetworkTargetScraper(provider target.TargetProvider) *TargetScraper {
	return newTargetScrapper(
		event.NetworkCostsScraperName,
		provider,
		[]string{
			metric.KubecostPodNetworkEgressBytesTotal,
			metric.KubecostPodNetworkIngressBytesTotal,
		},
		true)
}

type NetworkTargetProvider struct {
	port         int
	clusterCache clustercache.ClusterCache
}

func NewNetworkTargetProvider(port int, clusterCache clustercache.ClusterCache) *NetworkTargetProvider {
	return &NetworkTargetProvider{
		port:         port,
		clusterCache: clusterCache,
	}
}

func (n *NetworkTargetProvider) GetTargets() []target.ScrapeTarget {
	// NOTE: The proper way to discover these targets is to first identify a Service that
	// NOTE: matches a specific selector. Then, locate the Endpoints kubernetes resource associated
	// NOTE: with that Service. This Endpoints resource has a list of all the targetted pods and their
	// NOTE: addresses. We do _not_ have the Endpoints resource on our cluster cache at the moment,
	// NOTE: so we'll perform this lookup ourselves.
	pods := n.clusterCache.GetAllPods()

	var targets []target.ScrapeTarget
	for _, pod := range pods {
		if pod.Status.Phase == v1.PodRunning && isNetworkCosts(pod.Labels) {
			log.Debugf("Network: found target for http://%s:%d/metrics", pod.Status.PodIP, n.port)

			t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, n.port))
			targets = append(targets, t)
		}
	}

	return targets
}

func isNetworkCosts(labels map[string]string) bool {
	return labels["app.kubernetes.io/name"] == NetworkCostsNameLabel &&
		labels["app.kubernetes.io/instance"] == NetworkCostsInstanceLabel
}
