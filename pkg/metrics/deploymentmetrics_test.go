package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"k8s.io/apimachinery/pkg/types"
)

func TestDeploymentMatchLabelsMetric(t *testing.T) {
	deployment := &clustercache.Deployment{
		UID:       types.UID("deployment-test-uid-123"),
		Name:      "test-deployment",
		Namespace: "test-namespace",
		MatchLabels: map[string]string{
			"app":     "web-server",
			"version": "v1.0",
		},
	}

	sampleDeployments := []*clustercache.Deployment{deployment}
	fc := NewFakeDeploymentCache(sampleDeployments)
	mc := MetricsConfig{
		DisabledMetrics: []string{},
	}

	_ = KubecostDeploymentCollector{
		KubeClusterCache: fc,
		metricsConfig:    mc,
	}

	metric := newDeploymentMatchLabelsMetric(
		deployment.Name,
		deployment.Namespace,
		string(deployment.UID),
		"deployment_match_labels",
		[]string{"label_app", "label_version"},
		[]string{"web-server", "v1.0"},
	)

	if metric.deploymentName != "test-deployment" {
		t.Errorf("Expected deployment name 'test-deployment', got %s", metric.deploymentName)
	}
	if metric.namespace != "test-namespace" {
		t.Errorf("Expected namespace 'test-namespace', got %s", metric.namespace)
	}
	if metric.uid != "deployment-test-uid-123" {
		t.Errorf("Expected UID 'deployment-test-uid-123', got %s", metric.uid)
	}
}

func TestKubeDeploymentReplicasMetric(t *testing.T) {
	replicas := int32(3)
	metric := newKubeDeploymentReplicasMetric(
		"kube_deployment_spec_replicas",
		"test-deployment",
		"test-namespace",
		"deployment-replicas-uid-456",
		replicas,
	)

	if metric.deployment != "test-deployment" {
		t.Errorf("Expected deployment 'test-deployment', got %s", metric.deployment)
	}
	if metric.namespace != "test-namespace" {
		t.Errorf("Expected namespace 'test-namespace', got %s", metric.namespace)
	}
	if metric.uid != "deployment-replicas-uid-456" {
		t.Errorf("Expected UID 'deployment-replicas-uid-456', got %s", metric.uid)
	}
	if metric.replicas != 3.0 {
		t.Errorf("Expected replicas 3.0, got %f", metric.replicas)
	}
}

func TestKubeDeploymentStatusAvailableReplicasMetric(t *testing.T) {
	availableReplicas := int32(2)
	metric := newKubeDeploymentStatusAvailableReplicasMetric(
		"kube_deployment_status_replicas_available",
		"test-deployment",
		"test-namespace",
		"deployment-available-uid-789",
		availableReplicas,
	)

	if metric.deployment != "test-deployment" {
		t.Errorf("Expected deployment 'test-deployment', got %s", metric.deployment)
	}
	if metric.namespace != "test-namespace" {
		t.Errorf("Expected namespace 'test-namespace', got %s", metric.namespace)
	}
	if metric.uid != "deployment-available-uid-789" {
		t.Errorf("Expected UID 'deployment-available-uid-789', got %s", metric.uid)
	}
	if metric.replicasAvailable != 2.0 {
		t.Errorf("Expected available replicas 2.0, got %f", metric.replicasAvailable)
	}
}

type FakeDeploymentCache struct {
	clustercache.ClusterCache
	deployments []*clustercache.Deployment
}

func (f FakeDeploymentCache) GetAllDeployments() []*clustercache.Deployment {
	return f.deployments
}

func NewFakeDeploymentCache(deployments []*clustercache.Deployment) FakeDeploymentCache {
	return FakeDeploymentCache{
		deployments: deployments,
	}
}
