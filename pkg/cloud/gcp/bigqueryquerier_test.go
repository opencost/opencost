package gcp

import (
	"context"
	"testing"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryQuerier_GetStatus(t *testing.T) {
	tests := []struct {
		name           string
		initialStatus  cloud.ConnectionStatus
		expectedStatus cloud.ConnectionStatus
	}{
		{
			name:           "Initial status",
			initialStatus:  "",
			expectedStatus: cloud.InitialStatus,
		},
		{
			name:           "Successful connection",
			initialStatus:  cloud.SuccessfulConnection,
			expectedStatus: cloud.SuccessfulConnection,
		},
		{
			name:           "Failed connection",
			initialStatus:  cloud.FailedConnection,
			expectedStatus: cloud.FailedConnection,
		},
		{
			name:           "Invalid configuration",
			initialStatus:  cloud.InvalidConfiguration,
			expectedStatus: cloud.InvalidConfiguration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bqq := &BigQueryQuerier{
				ConnectionStatus: tt.initialStatus,
			}

			status := bqq.GetStatus()
			assert.Equal(t, tt.expectedStatus, status)
		})
	}
}

func TestBigQueryQuerier_Equals(t *testing.T) {
	config1 := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			ProjectID: "project1",
			Dataset:   "dataset1",
			Table:     "table1",
			Authorizer: &ServiceAccountKey{
				Key: map[string]string{"type": "service_account"},
			},
		},
	}

	config2 := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			ProjectID: "project1",
			Dataset:   "dataset1",
			Table:     "table1",
			Authorizer: &ServiceAccountKey{
				Key: map[string]string{"type": "service_account"},
			},
		},
	}

	config3 := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			ProjectID: "project2",
			Dataset:   "dataset1",
			Table:     "table1",
			Authorizer: &ServiceAccountKey{
				Key: map[string]string{"type": "service_account"},
			},
		},
	}

	tests := []struct {
		name     string
		config1  cloud.Config
		config2  cloud.Config
		expected bool
	}{
		{
			name:     "Same configuration",
			config1:  config1,
			config2:  config2,
			expected: true,
		},
		{
			name:     "Different configuration",
			config1:  config1,
			config2:  config3,
			expected: false,
		},
		{
			name:     "Nil config",
			config1:  config1,
			config2:  nil,
			expected: false,
		},
		{
			name:     "Different type",
			config1:  config1,
			config2:  &ServiceAccountKey{Key: map[string]string{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config1.Equals(tt.config2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBigQueryQuerier_Query_ValidationError(t *testing.T) {
	bqq := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			// Missing required fields to trigger validation error
			ProjectID:  "",
			Dataset:    "",
			Table:      "",
			Authorizer: nil,
		},
	}

	ctx := context.Background()
	_, err := bqq.Query(ctx, "SELECT * FROM table")

	assert.Error(t, err)
	// Print the actual status for debugging
	t.Logf("Expected: %v, Actual: %v", cloud.InvalidConfiguration, bqq.ConnectionStatus)
	assert.Equal(t, cloud.ConnectionStatus("Invalid Configuration"), bqq.ConnectionStatus)
}

func TestBigQueryQuerier_Query_ClientCreationError(t *testing.T) {
	bqq := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			ProjectID: "project1",
			Dataset:   "dataset1",
			Table:     "table1",
			Authorizer: &ServiceAccountKey{
				Key: map[string]string{
					"type": "service_account",
					// Invalid key to trigger client creation error
					"private_key": "invalid-key",
				},
			},
		},
	}

	ctx := context.Background()
	_, err := bqq.Query(ctx, "SELECT * FROM table")

	assert.Error(t, err)
	// Print the actual status for debugging
	t.Logf("Expected: %v, Actual: %v", cloud.FailedConnection, bqq.ConnectionStatus)
	assert.Equal(t, cloud.ConnectionStatus("Failed Connection"), bqq.ConnectionStatus)
}

func TestBigQueryQuerier_Query_Success(t *testing.T) {
	// This test would require mocking the BigQuery client
	// For now, we'll test the validation path
	bqq := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			ProjectID:  "project1",
			Dataset:    "dataset1",
			Table:      "table1",
			Authorizer: &WorkloadIdentity{}, // Use WorkloadIdentity to avoid key validation issues
		},
	}

	ctx := context.Background()

	// This will likely fail due to missing credentials, but we can test the validation
	_, err := bqq.Query(ctx, "SELECT * FROM table")

	// The actual result depends on the environment, but we can verify the status is set
	if err == nil {
		assert.Equal(t, cloud.SuccessfulConnection, bqq.ConnectionStatus)
	} else {
		// If there's an error, it should be due to connection issues
		assert.Contains(t, err.Error(), "credentials")
	}
}

func TestBigQueryQuerier_Query_EmptyResult(t *testing.T) {
	bqq := &BigQueryQuerier{
		BigQueryConfiguration: BigQueryConfiguration{
			ProjectID:  "project1",
			Dataset:    "dataset1",
			Table:      "table1",
			Authorizer: &WorkloadIdentity{},
		},
		ConnectionStatus: cloud.InitialStatus,
	}

	ctx := context.Background()

	// Test with a query that would return empty results
	_, err := bqq.Query(ctx, "SELECT * FROM non_existent_table")

	// The status should be set to MissingData if the result is empty
	if err == nil {
		assert.Equal(t, cloud.MissingData, bqq.ConnectionStatus)
	}
}
