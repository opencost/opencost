package cloudcost

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestRepositoryQuerier_QueryViewTable_InvalidCostMetric(t *testing.T) {
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(timeutil.Day)

	repo := NewMemoryRepository()
	if err := repo.Put(DefaultMockCloudCostSet(start, end, "aws", "integration1")); err != nil {
		t.Fatalf("failed to populate the repository: %v", err)
	}

	_, err := NewRepositoryQuerier(repo).QueryViewTable(context.Background(), ViewQueryRequest{
		QueryRequest:   QueryRequest{Start: start, End: end},
		CostMetricName: "notACostMetric",
	})
	if err == nil {
		t.Fatal("expected an error for an invalid cost metric name")
	}
	if !strings.Contains(err.Error(), "invalid Cost Metric: notACostMetric") {
		t.Errorf("the error does not report its cause: %v", err)
	}
}
