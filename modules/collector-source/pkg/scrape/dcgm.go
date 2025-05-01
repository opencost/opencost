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
	k8s kubernetes.Interface
}

func newDCGMTargetProvider(k8s kubernetes.Interface) *DCGMTargetProvider {
	return &DCGMTargetProvider{
		k8s: k8s,
	}
}

func (p *DCGMTargetProvider) GetTargets() []target.ScrapeTarget {
	k8s := p.k8s

	// Find service
	svcs, err := k8s.CoreV1().Services("").List(context.Background(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=dcgm-exporter",
	})
	if err != nil {
		log.Errorf("DCGMTargetProvider: failed to retieve Services from kubernetes client: %s", err.Error())
		return nil
	}

	var targets []target.ScrapeTarget
	for _, svc := range svcs.Items {
		port := 9400
		for _, prt := range svc.Spec.Ports {
			if prt.Name == "metrics" {
				port = int(prt.Port)
			}
		}
		t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", svc.Spec.ClusterIP, port))
		targets = append(targets, t)
	}

	return targets
}
