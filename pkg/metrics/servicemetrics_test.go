package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestServiceUIDExtraction(t *testing.T) {
	testUID := types.UID("test-service-uid-789")

	// The Fix: Initialize the full object with ObjectMeta and an empty Spec
	testService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			UID:       testUID,
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: v1.ServiceSpec{ // This prevents the nil pointer crash
			Selector: map[string]string{"app": "test"},
		},
	}

	transformed := clustercache.TransformService(testService)

	assert.NotNil(t, transformed)
	assert.Equal(t, testUID, transformed.UID)
	assert.Equal(t, "test-service", transformed.Name)
	assert.Equal(t, "default", transformed.Namespace)
	assert.Equal(t, "test", transformed.SpecSelector["app"])
}

// NOTE: No changes are needed for your other test functions in this file.

func TestServiceSelectorLabelsMetricWithUID(t *testing.T) {
	labelNames := []string{"app", "version"}
	labelValues := []string{"frontend", "v1"}
	
	metric := newServiceSelectorLabelsMetric(
		"test-service",
		"default",
		"test-uid-789",
		"service_selector_labels",
		labelNames,
		labelValues,
	)

	assert.Equal(t, "test-service", metric.serviceName)
	assert.Equal(t, "default", metric.namespace)
	assert.Equal(t, "test-uid-789", metric.uid)
	assert.Len(t, metric.labelNames, 2)
}

func TestKubeServiceInfoMetricWithUID(t *testing.T) {
	metric := newKubeServiceInfoMetric(
		"kube_service_info",
		"test-service",
		"production",
		"service-uid-abc",
	)

	assert.Equal(t, "test-service", metric.serviceName)
	assert.Equal(t, "production", metric.namespace)
	assert.Equal(t, "service-uid-abc", metric.uid)
}