package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNodeUIDExtraction(t *testing.T) {
	testUID := types.UID("test-node-uid-123")
	
	// The Fix: Initialize the full object with ObjectMeta and an empty Status
	testNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			UID:    testUID,
			Name:   "test-node",
			Labels: map[string]string{"test": "label"},
		},
		Status: v1.NodeStatus{}, // This prevents the nil pointer crash
	}

	transformed := clustercache.TransformNode(testNode)

	assert.NotNil(t, transformed)
	assert.Equal(t, testUID, transformed.UID)
	assert.Equal(t, "test-node", transformed.Name)
	assert.Equal(t, "label", transformed.Labels["test"])
}


func TestKubeNodeStatusCapacityMetricWithUID(t *testing.T) {
	metric := newKubeNodeStatusCapacityMetric(
		"kube_node_status_capacity",
		"test-node",
		"test-uid-123",
		"cpu",
		"cores",
		4.0,
	)

	assert.Equal(t, "test-node", metric.node)
	assert.Equal(t, "test-uid-123", metric.uid)
	assert.Equal(t, "cpu", metric.resource)
	assert.Equal(t, 4.0, metric.value)
}

func TestKubeNodeLabelsMetricWithUID(t *testing.T) {
	labelNames := []string{"label_env", "label_region"}
	labelValues := []string{"production", "us-west"}
	
	metric := newKubeNodeLabelsMetric(
		"test-node",
		"test-uid-456",
		"kube_node_labels",
		labelNames,
		labelValues,
	)

	assert.Equal(t, "test-node", metric.node)
	assert.Equal(t, "test-uid-456", metric.uid)
	assert.Len(t, metric.labelNames, 2)
	assert.Contains(t, metric.labelValues, "production")
}