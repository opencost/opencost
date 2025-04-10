package collector

import (
	"context"
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/modules/collector-source/pkg/metrics/target"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type NetworkTargetProvider struct {
	releaseName   string
	port          int
	kubeClientSet kubernetes.Interface
}

func (n NetworkTargetProvider) GetTargets() []target.ScrapeTarget {
	k8s := n.kubeClientSet

	pods, err := k8s.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s-network-costs", n.releaseName),
	})
	if err != nil {
		log.Errorf("NetworkTargetProvider: failed to retieve nodes from kubernetes client: %s", err.Error())
		return nil
	}

	var targets []target.ScrapeTarget
	for _, pod := range pods.Items {
		t := target.NewUrlTarget(fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, n.port))
		targets = append(targets, t)
	}

	return targets
}
