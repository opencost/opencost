package scrape

import (
	"context"
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Network Metrics
const (
	KubecostPodNetworkEgressBytesTotal = "kubecost_pod_network_egress_bytes_total"
)

func newNetworkScraper(
	releaseName string,
	port int,
	k8s kubernetes.Interface,
	updater metric.MetricUpdater,
) Scraper {
	tp := NewNetworkTargetProvider(releaseName, port, k8s)
	return newNetworkTargetScraper(tp, updater)
}

func newNetworkTargetScraper(provider target.TargetProvider, updater metric.MetricUpdater) *TargetScraper {
	return newTargetScrapper(
		provider,
		updater,
		[]string{
			KubecostPodNetworkEgressBytesTotal,
		},
		true)
}

type NetworkTargetProvider struct {
	releaseName   string
	port          int
	kubeClientSet kubernetes.Interface
}

func NewNetworkTargetProvider(releaseName string, port int, k8s kubernetes.Interface) *NetworkTargetProvider {
	return &NetworkTargetProvider{
		releaseName:   releaseName,
		port:          port,
		kubeClientSet: k8s,
	}
}

func (n *NetworkTargetProvider) GetTargets() []target.ScrapeTarget {
	k8s := n.kubeClientSet

	pods, err := k8s.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s-network-costs", n.releaseName),
	})
	if err != nil {
		log.Errorf("NetworkTargetProvider: failed to retieve pods from kubernetes client: %s", err.Error())
		return nil
	}

	var targets []target.ScrapeTarget
	for _, pod := range pods.Items {
		t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, n.port))
		targets = append(targets, t)
	}

	return targets
}
