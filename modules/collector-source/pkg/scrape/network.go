package scrape

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
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
	pods := n.clusterCache.GetAllPods()

	var targets []target.ScrapeTarget
	for _, pod := range pods {
		instance := pod.Labels["app.kubernetes.io/instance"]
		name := pod.Labels["app.kubernetes.io/name"]
		if name == "network-costs" && instance == "kubecost" && pod.Status.Phase == "Running" {
			log.Debugf("Network: found target for http://%s:%d/metrics", pod.Status.PodIP, n.port)
			t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, n.port))
			targets = append(targets, t)
		}
	}

	return targets
}
