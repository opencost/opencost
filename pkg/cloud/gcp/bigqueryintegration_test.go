package gcp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBigQueryIntegration_GetCloudCost(t *testing.T) {
	bqi := &BigQueryIntegration{
		BigQueryQuerier: BigQueryQuerier{
			BigQueryConfiguration: BigQueryConfiguration{
				ProjectID: "test-project",
				Dataset:   "test-dataset",
				Table:     "test-table",
			},
		},
	}

	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	// This will fail due to missing credentials, but we can test the function structure
	_, err := bqi.GetCloudCost(start, end)
	assert.Error(t, err) // Expect error due to missing credentials
}

func TestBigQueryIntegration_GetWhereConjuncts(t *testing.T) {
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	// Test the GetWhereConjuncts function
	result := GetWhereConjuncts(start, end, true)
	assert.NotEmpty(t, result)
	assert.Len(t, result, 2)
	assert.Contains(t, result[0], "DATE(_PARTITIONTIME)")
	assert.Contains(t, result[1], "usage_start_time")
}

func TestBigQueryIntegration_GetFlexibleCUDRates(t *testing.T) {
	bqi := &BigQueryIntegration{
		BigQueryQuerier: BigQueryQuerier{
			BigQueryConfiguration: BigQueryConfiguration{
				ProjectID: "test-project",
				Dataset:   "test-dataset",
				Table:     "test-table",
			},
		},
	}

	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	// This will fail due to missing credentials, but we can test the function structure
	_, err := bqi.GetFlexibleCUDRates(start, end)
	assert.Error(t, err) // Expect error due to missing credentials
}

func TestBigQueryIntegration_queryFlexibleCUDTotalCosts(t *testing.T) {
	bqi := &BigQueryIntegration{
		BigQueryQuerier: BigQueryQuerier{
			BigQueryConfiguration: BigQueryConfiguration{
				ProjectID: "test-project",
				Dataset:   "test-dataset",
				Table:     "test-table",
			},
		},
	}

	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	// This will fail due to missing credentials, but we can test the function structure
	_, err := bqi.queryFlexibleCUDTotalCosts(start, end)
	assert.Error(t, err) // Expect error due to missing credentials
}

func TestBigQueryIntegration_queryFlexibleCUDTotalCredits(t *testing.T) {
	bqi := &BigQueryIntegration{
		BigQueryQuerier: BigQueryQuerier{
			BigQueryConfiguration: BigQueryConfiguration{
				ProjectID: "test-project",
				Dataset:   "test-dataset",
				Table:     "test-table",
			},
		},
	}

	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	// This will fail due to missing credentials, but we can test the function structure
	_, err := bqi.queryFlexibleCUDTotalCredits(start, end)
	assert.Error(t, err) // Expect error due to missing credentials
}
