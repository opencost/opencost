package metrics

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
)

type mockJobCache struct {
	clustercache.ClusterCache
	jobs []*clustercache.Job
}

func (m mockJobCache) GetAllJobs() []*clustercache.Job {
	return m.jobs
}

func TestKubeJobCollector_Collect(t *testing.T) {
	// Test with job that has no failures
	cache := mockJobCache{
		jobs: []*clustercache.Job{
			{
				Name:      "test-job",
				Namespace: "default",
				UID:       types.UID("test-job-uid"),
				Status:    batchv1.JobStatus{Failed: 0},
			},
		},
	}

	collector := KubeJobCollector{
		KubeClusterCache: cache,
		metricsConfig:    MetricsConfig{},
	}

	ch := make(chan prometheus.Metric, 10)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 metric, got %d", count)
	}
}

func TestKubeJobStatusFailedMetric_Write(t *testing.T) {
	metric := newKubeJobStatusFailedMetric(
		"test-job",
		"default",
		"test-job-uid",
		"kube_job_status_failed",
		"",
		0.0,
	)

	pbMetric := &dto.Metric{}
	err := metric.Write(pbMetric)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if pbMetric.Gauge == nil || *pbMetric.Gauge.Value != 0.0 {
		t.Error("Expected gauge value 0.0")
	}

	if len(pbMetric.Label) != 4 { // job_name + namespace + uid + reason
		t.Errorf("Expected 4 labels, got %d", len(pbMetric.Label))
	}

	// Verify UID label is present
	foundUID := false
	for _, label := range pbMetric.Label {
		if *label.Name == "uid" && *label.Value == "test-job-uid" {
			foundUID = true
			break
		}
	}
	if !foundUID {
		t.Error("Expected uid label not found")
	}
}
