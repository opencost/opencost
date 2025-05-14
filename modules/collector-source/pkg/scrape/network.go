package scrape

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
)

// Network Metrics
const (
	KubecostPodNetworkEgressBytesTotal  = "kubecost_pod_network_egress_bytes_total"
	KubecostPodNetworkIngressBytesTotal = "kubecost_pod_network_ingress_bytes_total"
)

func newNetworkScraper(
	releaseName string,
	port int,
	clusterCache clustercache.ClusterCache,
	updater metric.MetricUpdater,
) Scraper {
	tp := NewNetworkTargetProvider(releaseName, port, clusterCache)
	return newNetworkTargetScraper(tp, updater)
}

func newNetworkTargetScraper(provider target.TargetProvider, updater metric.MetricUpdater) *TargetScraper {
	return newTargetScrapper(
		provider,
		updater,
		[]string{
			KubecostPodNetworkEgressBytesTotal,
			KubecostPodNetworkIngressBytesTotal,
		},
		true)
}

type NetworkTargetProvider struct {
	releaseName  string
	port         int
	clusterCache clustercache.ClusterCache
}

func NewNetworkTargetProvider(releaseName string, port int, clusterCache clustercache.ClusterCache) *NetworkTargetProvider {
	return &NetworkTargetProvider{
		releaseName:  releaseName,
		port:         port,
		clusterCache: clusterCache,
	}
}

func (n *NetworkTargetProvider) GetTargets() []target.ScrapeTarget {
	pods := n.clusterCache.GetAllPods()
	//pods, err := k8s.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
	//	LabelSelector: fmt.Sprintf("app=%s-network-costs", n.releaseName),
	//})
	//if err != nil {
	//	log.Errorf("NetworkTargetProvider: failed to retieve pods from kubernetes client: %s", err.Error())
	//	return nil
	//}

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
