package collector

import (
	"context"
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type DCGMTargetProvider struct {
	kubeClientSet kubernetes.Interface
}

func NewDCGMTargetProvider(kubeClientSet kubernetes.Interface) *DCGMTargetProvider {
	return &DCGMTargetProvider{
		kubeClientSet: kubeClientSet,
	}
}

func (p *DCGMTargetProvider) GetTargets() []target.ScrapeTarget {
	k8s := p.kubeClientSet

	// Find service
	svcs, err := k8s.CoreV1().Services("").List(context.Background(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=dcgm-exporter",
	})
	if err != nil {
		log.Errorf("DCGMTargetProvider: failed to retieve services from kubernetes client: %s", err.Error())
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
