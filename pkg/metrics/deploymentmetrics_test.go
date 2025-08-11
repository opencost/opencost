package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestDeploymentUIDExtraction(t *testing.T) {
	testUID := types.UID("test-deployment-uid-xyz")
	var replicas int32 = 3 // Helper variable for the pointer below

	// The Fix: A fully initialized Deployment object
	testDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			UID:       testUID,
			Name:      "test-deployment",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: appsv1.DeploymentSpec{
			// NEW: Initialize the nested Replicas pointer
			Replicas: &replicas,
			// NEW: Initialize the nested Selector object
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
		},
		// NEW: Initialize the Status field
		Status: appsv1.DeploymentStatus{
			Replicas:          3,
			AvailableReplicas: 3,
		},
	}

	// This is line 27 where the panic was happening
	transformed := clustercache.TransformDeployment(testDeployment)

	assert.NotNil(t, transformed)
	assert.Equal(t, testUID, transformed.UID)
	assert.Equal(t, "test-deployment", transformed.Name)
	assert.Equal(t, "default", transformed.Namespace)
}

func TestDeploymentMatchLabelsMetricWithUID(t *testing.T) {
	labelNames := []string{"app", "tier"}
	labelValues := []string{"backend", "api"}

	metric := newDeploymentMatchLabelsMetric(
		"test-deployment",
		"production",
		"deployment-uid-123",
		"deployment_match_labels",
		labelNames,
		labelValues,
	)

	assert.Equal(t, "test-deployment", metric.deploymentName)
	assert.Equal(t, "production", metric.namespace)
	assert.Equal(t, "deployment-uid-123", metric.uid)
	assert.Len(t, metric.labelNames, 2)
}

func TestKubeDeploymentReplicasMetricWithUID(t *testing.T) {
	metric := newKubeDeploymentReplicasMetric(
		"kube_deployment_spec_replicas",
		"test-deployment",
		"default",
		"deployment-uid-456",
		3,
	)

	assert.Equal(t, "test-deployment", metric.deployment)
	assert.Equal(t, "default", metric.namespace)
	assert.Equal(t, "deployment-uid-456", metric.uid)
	assert.Equal(t, float64(3), metric.replicas)
}