package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestAthenaQuerier_GetColumns(t *testing.T) {
	// Create mock client
	mockClient := &MockAthenaClient{}

	// Create mock querier with valid configuration
	querier := &MockAthenaQuerier{
		AthenaQuerier: AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
			},
		},
		mockClient: mockClient,
	}

	// Test successful column retrieval
	t.Run("successful_column_retrieval", func(t *testing.T) {
		// Mock successful query results with column names
		// First row is header, subsequent rows are data
		mockClient.GetQueryResultsFunc = func(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
			return &athena.GetQueryResultsOutput{
				ResultSet: &types.ResultSet{
					Rows: []types.Row{
						{Data: []types.Datum{{VarCharValue: aws.String("column_name")}}}, // Header row
						{Data: []types.Datum{{VarCharValue: aws.String("column1")}}},
						{Data: []types.Datum{{VarCharValue: aws.String("column2")}}},
						{Data: []types.Datum{{VarCharValue: aws.String("column3")}}},
					},
				},
			}, nil
		}

		columns, err := querier.GetColumns()
		if err != nil {
			t.Errorf("GetColumns() returned error: %v", err)
		}

		expectedColumns := map[string]bool{
			"column1": true,
			"column2": true,
			"column3": true,
		}

		if len(columns) != len(expectedColumns) {
			t.Errorf("GetColumns() returned %d columns, want %d", len(columns), len(expectedColumns))
		}

		for col := range expectedColumns {
			if !columns[col] {
				t.Errorf("GetColumns() missing expected column: %s", col)
			}
		}
	})

	// Test empty results
	t.Run("empty_results", func(t *testing.T) {
		mockClient.GetQueryResultsFunc = func(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
			return &athena.GetQueryResultsOutput{
				ResultSet: &types.ResultSet{
					Rows: []types.Row{
						{Data: []types.Datum{{VarCharValue: aws.String("column_name")}}}, // Header row only
					},
				},
			}, nil
		}

		columns, err := querier.GetColumns()
		if err != nil {
			t.Errorf("GetColumns() returned error: %v", err)
		}

		if len(columns) != 0 {
			t.Errorf("GetColumns() returned %d columns, want 0", len(columns))
		}
	})

	// Test query error
	t.Run("query_error", func(t *testing.T) {
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			return nil, errors.New("query execution failed")
		}

		columns, err := querier.GetColumns()
		if err == nil {
			t.Error("GetColumns() should return error when query fails")
		}

		if len(columns) != 0 {
			t.Errorf("GetColumns() should return empty map on error, got %d columns", len(columns))
		}
	})
}

func TestAthenaQuerier_Query(t *testing.T) {
	// Create mock client
	mockClient := &MockAthenaClient{}

	// Create mock querier with valid configuration
	querier := &MockAthenaQuerier{
		AthenaQuerier: AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
			},
		},
		mockClient: mockClient,
	}

	// Test successful query
	t.Run("successful_query", func(t *testing.T) {
		queryExecuted := false
		queryString := "SELECT * FROM test_table"

		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			if *params.QueryString != queryString {
				t.Errorf("Expected query string %s, got %s", queryString, *params.QueryString)
			}
			queryExecuted = true
			return &athena.StartQueryExecutionOutput{
				QueryExecutionId: aws.String("test-query-id"),
			}, nil
		}

		mockClient.GetQueryResultsFunc = func(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
			return &athena.GetQueryResultsOutput{
				ResultSet: &types.ResultSet{
					Rows: []types.Row{
						{Data: []types.Datum{{VarCharValue: aws.String("header")}}}, // Header row
						{Data: []types.Datum{{VarCharValue: aws.String("test-data")}}},
					},
				},
			}, nil
		}

		rowsProcessed := 0
		queryFunc := GetAthenaQueryFunc(func(row types.Row) {
			rowsProcessed++
		})

		err := querier.Query(context.Background(), queryString, queryFunc)
		if err != nil {
			t.Errorf("Query() returned error: %v", err)
		}

		if !queryExecuted {
			t.Error("Query execution was not called")
		}

		if rowsProcessed != 1 {
			t.Errorf("Expected 1 row to be processed, got %d", rowsProcessed)
		}

		// Check connection status is successful
		if querier.ConnectionStatus != cloud.SuccessfulConnection {
			t.Errorf("Expected connection status to be SuccessfulConnection, got %s", querier.ConnectionStatus)
		}
	})

	// Test invalid configuration
	t.Run("invalid_configuration", func(t *testing.T) {
		invalidQuerier := &MockAthenaQuerier{
			AthenaQuerier: AthenaQuerier{
				AthenaConfiguration: AthenaConfiguration{
					// Missing required fields
					Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
				},
			},
			mockClient: mockClient,
		}

		err := invalidQuerier.Query(context.Background(), "SELECT * FROM test", GetAthenaQueryFunc(func(row types.Row) {}))
		if err == nil {
			t.Error("Query() should return error for invalid configuration")
		}

		if invalidQuerier.ConnectionStatus != cloud.InvalidConfiguration {
			t.Errorf("Expected connection status to be InvalidConfiguration, got %s", invalidQuerier.ConnectionStatus)
		}
	})

	// Test query execution failure
	t.Run("query_execution_failure", func(t *testing.T) {
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			return nil, errors.New("query execution failed")
		}

		err := querier.Query(context.Background(), "SELECT * FROM test", GetAthenaQueryFunc(func(row types.Row) {}))
		if err == nil {
			t.Error("Query() should return error when query execution fails")
		}

		if querier.ConnectionStatus != cloud.FailedConnection {
			t.Errorf("Expected connection status to be FailedConnection, got %s", querier.ConnectionStatus)
		}
	})

	// Test query waiting failure
	t.Run("query_waiting_failure", func(t *testing.T) {
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			return &athena.StartQueryExecutionOutput{
				QueryExecutionId: aws.String("test-query-id"),
			}, nil
		}

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			return &athena.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: types.QueryExecutionStateFailed,
					},
				},
			}, nil
		}

		err := querier.Query(context.Background(), "SELECT * FROM test", GetAthenaQueryFunc(func(row types.Row) {}))
		if err == nil {
			t.Error("Query() should return error when query waiting fails")
		}

		if querier.ConnectionStatus != cloud.FailedConnection {
			t.Errorf("Expected connection status to be FailedConnection, got %s", querier.ConnectionStatus)
		}
	})
}

func TestAthenaQuerier_GetAthenaClient(t *testing.T) {
	// Test successful client creation
	t.Run("successful_client_creation", func(t *testing.T) {
		querier := &AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
			},
		}

		client, err := querier.GetAthenaClient()
		if err != nil {
			t.Errorf("GetAthenaClient() returned error: %v", err)
		}

		if client == nil {
			t.Error("GetAthenaClient() returned nil client")
		}
	})

	// Test client creation with service account authorizer
	t.Run("service_account_authorizer", func(t *testing.T) {
		querier := &AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &ServiceAccount{},
			},
		}

		client, err := querier.GetAthenaClient()
		if err != nil {
			t.Errorf("GetAthenaClient() with ServiceAccount returned error: %v", err)
		}

		if client == nil {
			t.Error("GetAthenaClient() returned nil client")
		}
	})

	// Test client creation with assume role authorizer
	t.Run("assume_role_authorizer", func(t *testing.T) {
		querier := &AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:   "test-bucket",
				Region:   "us-east-1",
				Database: "test-db",
				Table:    "test-table",
				Account:  "123456789012",
				Authorizer: &AssumeRole{
					Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
					RoleARN:    "arn:aws:iam::123456789012:role/test-role",
				},
			},
		}

		client, err := querier.GetAthenaClient()
		if err != nil {
			t.Errorf("GetAthenaClient() with AssumeRole returned error: %v", err)
		}

		if client == nil {
			t.Error("GetAthenaClient() returned nil client")
		}
	})

	// Test client creation failure with invalid authorizer
	t.Run("invalid_authorizer", func(t *testing.T) {
		querier := &AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &AccessKey{ID: "", Secret: ""}, // Invalid credentials
			},
		}

		client, err := querier.GetAthenaClient()
		if err == nil {
			t.Error("GetAthenaClient() should return error for invalid authorizer")
		}

		if client != nil {
			t.Error("GetAthenaClient() should return nil client on error")
		}
	})

	// Test client creation with different regions
	t.Run("different_regions", func(t *testing.T) {
		regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}

		for _, region := range regions {
			querier := &AthenaQuerier{
				AthenaConfiguration: AthenaConfiguration{
					Bucket:     "test-bucket",
					Region:     region,
					Database:   "test-db",
					Table:      "test-table",
					Account:    "123456789012",
					Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
				},
			}

			client, err := querier.GetAthenaClient()
			if err != nil {
				t.Errorf("GetAthenaClient() for region %s returned error: %v", region, err)
			}

			if client == nil {
				t.Errorf("GetAthenaClient() for region %s returned nil client", region)
			}
		}
	})
}

func TestAthenaQuerier_queryAthenaPaginated(t *testing.T) {
	// Create mock client
	mockClient := &MockAthenaClient{}

	// Create mock querier with valid configuration
	querier := &MockAthenaQuerier{
		AthenaQuerier: AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
			},
		},
		mockClient: mockClient,
	}

	// Test successful paginated query
	t.Run("successful_paginated_query", func(t *testing.T) {
		queryString := "SELECT * FROM test_table"
		queryExecuted := false

		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			if *params.QueryString != queryString {
				t.Errorf("Expected query string %s, got %s", queryString, *params.QueryString)
			}
			if *params.QueryExecutionContext.Database != "test-db" {
				t.Errorf("Expected database test-db, got %s", *params.QueryExecutionContext.Database)
			}
			if *params.ResultConfiguration.OutputLocation != "test-bucket" {
				t.Errorf("Expected bucket test-bucket, got %s", *params.ResultConfiguration.OutputLocation)
			}
			queryExecuted = true
			return &athena.StartQueryExecutionOutput{
				QueryExecutionId: aws.String("test-query-id"),
			}, nil
		}

		mockClient.GetQueryResultsFunc = func(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
			return &athena.GetQueryResultsOutput{
				ResultSet: &types.ResultSet{
					Rows: []types.Row{
						{Data: []types.Datum{{VarCharValue: aws.String("row1")}}},
						{Data: []types.Datum{{VarCharValue: aws.String("row2")}}},
					},
				},
			}, nil
		}

		rowsProcessed := 0
		queryFunc := func(page *athena.GetQueryResultsOutput) bool {
			for range page.ResultSet.Rows {
				rowsProcessed++
			}
			return true
		}

		err := querier.queryAthenaPaginated(context.Background(), queryString, queryFunc)
		if err != nil {
			t.Errorf("queryAthenaPaginated() returned error: %v", err)
		}

		if !queryExecuted {
			t.Error("Query execution was not called")
		}

		if rowsProcessed != 2 {
			t.Errorf("Expected 2 rows to be processed, got %d", rowsProcessed)
		}
	})

	// Test query with catalog
	t.Run("query_with_catalog", func(t *testing.T) {
		querierWithCatalog := &MockAthenaQuerier{
			AthenaQuerier: AthenaQuerier{
				AthenaConfiguration: AthenaConfiguration{
					Bucket:     "test-bucket",
					Region:     "us-east-1",
					Database:   "test-db",
					Catalog:    "test-catalog",
					Table:      "test-table",
					Account:    "123456789012",
					Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
				},
			},
			mockClient: mockClient,
		}

		catalogSet := false
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			if params.QueryExecutionContext.Catalog != nil && *params.QueryExecutionContext.Catalog == "test-catalog" {
				catalogSet = true
			}
			return &athena.StartQueryExecutionOutput{
				QueryExecutionId: aws.String("test-query-id"),
			}, nil
		}

		err := querierWithCatalog.queryAthenaPaginated(context.Background(), "SELECT * FROM test", func(page *athena.GetQueryResultsOutput) bool { return true })
		if err != nil {
			t.Errorf("queryAthenaPaginated() with catalog returned error: %v", err)
		}

		if !catalogSet {
			t.Error("Catalog was not set in query execution context")
		}
	})

	// Test query with workgroup
	t.Run("query_with_workgroup", func(t *testing.T) {
		querierWithWorkgroup := &MockAthenaQuerier{
			AthenaQuerier: AthenaQuerier{
				AthenaConfiguration: AthenaConfiguration{
					Bucket:     "test-bucket",
					Region:     "us-east-1",
					Database:   "test-db",
					Table:      "test-table",
					Workgroup:  "test-workgroup",
					Account:    "123456789012",
					Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
				},
			},
			mockClient: mockClient,
		}

		workgroupSet := false
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			if params.WorkGroup != nil && *params.WorkGroup == "test-workgroup" {
				workgroupSet = true
			}
			return &athena.StartQueryExecutionOutput{
				QueryExecutionId: aws.String("test-query-id"),
			}, nil
		}

		err := querierWithWorkgroup.queryAthenaPaginated(context.Background(), "SELECT * FROM test", func(page *athena.GetQueryResultsOutput) bool { return true })
		if err != nil {
			t.Errorf("queryAthenaPaginated() with workgroup returned error: %v", err)
		}

		if !workgroupSet {
			t.Error("Workgroup was not set in query execution input")
		}
	})

	// Test query execution failure
	t.Run("query_execution_failure", func(t *testing.T) {
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			return nil, errors.New("query execution failed")
		}

		err := querier.queryAthenaPaginated(context.Background(), "SELECT * FROM test", func(page *athena.GetQueryResultsOutput) bool { return true })
		if err == nil {
			t.Error("queryAthenaPaginated() should return error when query execution fails")
		}

		expectedError := "QueryAthenaPaginated: start query error: query execution failed"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	// Test query waiting failure
	t.Run("query_waiting_failure", func(t *testing.T) {
		mockClient.StartQueryExecutionFunc = func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			return &athena.StartQueryExecutionOutput{
				QueryExecutionId: aws.String("test-query-id"),
			}, nil
		}

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			return &athena.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: types.QueryExecutionStateFailed,
					},
				},
			}, nil
		}

		err := querier.queryAthenaPaginated(context.Background(), "SELECT * FROM test", func(page *athena.GetQueryResultsOutput) bool { return true })
		if err == nil {
			t.Error("queryAthenaPaginated() should return error when query waiting fails")
		}

		expectedError := "QueryAthenaPaginated: query execution error: no query results available for query test-query-id"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	// Test get client failure
	t.Run("get_client_failure", func(t *testing.T) {
		invalidQuerier := &FailingQueryAthenaQuerier{
			MockAthenaQuerier: MockAthenaQuerier{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "test-bucket",
						Region:     "us-east-1",
						Database:   "test-db",
						Table:      "test-table",
						Account:    "123456789012",
						Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
					},
				},
				mockClient: mockClient,
			},
		}

		err := invalidQuerier.queryAthenaPaginated(context.Background(), "SELECT * FROM test", func(page *athena.GetQueryResultsOutput) bool { return true })
		if err == nil {
			t.Error("queryAthenaPaginated() should return error when client creation fails")
		}

		expectedError := "QueryAthenaPaginated: GetAthenaClient error: failed to create client"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})
}

func TestAthenaQuerier_waitForQueryToComplete(t *testing.T) {
	// Create mock client
	mockClient := &MockAthenaClient{}

	// Create mock querier
	querier := &MockAthenaQuerier{
		AthenaQuerier: AthenaQuerier{
			AthenaConfiguration: AthenaConfiguration{
				Bucket:     "test-bucket",
				Region:     "us-east-1",
				Database:   "test-db",
				Table:      "test-table",
				Account:    "123456789012",
				Authorizer: &AccessKey{ID: "test-key", Secret: "test-secret"},
			},
		},
		mockClient: mockClient,
	}

	// Test successful query completion
	t.Run("successful_query_completion", func(t *testing.T) {
		queryID := "test-query-id"
		callCount := 0

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			callCount++
			if *params.QueryExecutionId != queryID {
				t.Errorf("Expected query ID %s, got %s", queryID, *params.QueryExecutionId)
			}

			// Return SUCCEEDED on first call
			return &athena.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: types.QueryExecutionStateSucceeded,
					},
				},
			}, nil
		}

		err := querier.waitForQueryToComplete(context.Background(), mockClient, &queryID)
		if err != nil {
			t.Errorf("waitForQueryToComplete() returned error: %v", err)
		}

		if callCount != 1 {
			t.Errorf("Expected 1 call to GetQueryExecution, got %d", callCount)
		}
	})

	// Test query failure
	t.Run("query_failure", func(t *testing.T) {
		queryID := "test-query-id"

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			return &athena.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: types.QueryExecutionStateFailed,
					},
				},
			}, nil
		}

		err := querier.waitForQueryToComplete(context.Background(), mockClient, &queryID)
		if err == nil {
			t.Error("waitForQueryToComplete() should return error when query fails")
		}

		expectedError := "no query results available for query test-query-id"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	// Test query cancellation
	t.Run("query_cancellation", func(t *testing.T) {
		queryID := "test-query-id"

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			return &athena.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: types.QueryExecutionStateCancelled,
					},
				},
			}, nil
		}

		err := querier.waitForQueryToComplete(context.Background(), mockClient, &queryID)
		if err == nil {
			t.Error("waitForQueryToComplete() should return error when query is cancelled")
		}

		expectedError := "no query results available for query test-query-id"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	// Test query timeout
	t.Run("query_timeout", func(t *testing.T) {
		queryID := "test-query-id"

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			return &athena.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: "TIMED_OUT", // Use string literal since QueryExecutionStateTimedOut doesn't exist
					},
				},
			}, nil
		}

		err := querier.waitForQueryToComplete(context.Background(), mockClient, &queryID)
		if err == nil {
			t.Error("waitForQueryToComplete() should return error when query times out")
		}

		expectedError := "no query results available for query test-query-id"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	// Test GetQueryExecution error
	t.Run("get_query_execution_error", func(t *testing.T) {
		queryID := "test-query-id"

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			return nil, errors.New("failed to get query execution")
		}

		err := querier.waitForQueryToComplete(context.Background(), mockClient, &queryID)
		if err == nil {
			t.Error("waitForQueryToComplete() should return error when GetQueryExecution fails")
		}

		expectedError := "failed to get query execution"
		if err.Error() != expectedError {
			t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})

	// Test context cancellation
	t.Run("context_cancellation", func(t *testing.T) {
		queryID := "test-query-id"
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		mockClient.GetQueryExecutionFunc = func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
			// Check if context is cancelled
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &athena.GetQueryExecutionOutput{
					QueryExecution: &types.QueryExecution{
						Status: &types.QueryExecutionStatus{
							State: types.QueryExecutionStateSucceeded,
						},
					},
				}, nil
			}
		}

		err := querier.waitForQueryToComplete(ctx, mockClient, &queryID)
		if err == nil {
			t.Error("waitForQueryToComplete() should return error when context is cancelled")
		}

		if err != context.Canceled {
			t.Errorf("Expected context.Canceled error, got %v", err)
		}
	})

	// Test with nil query execution ID
	t.Run("nil_query_execution_id", func(t *testing.T) {
		err := querier.waitForQueryToComplete(context.Background(), mockClient, nil)
		if err == nil {
			t.Error("waitForQueryToComplete() should return error when query execution ID is nil")
		}
	})
}

func TestAthenaQuerier_GetAthenaQueryFunc(t *testing.T) {
	// Test that GetAthenaQueryFunc returns a function
	queryFunc := GetAthenaQueryFunc(func(row types.Row) {
		// Do nothing
	})

	if queryFunc == nil {
		t.Error("GetAthenaQueryFunc should return a non-nil function")
	}

	// Test that the returned function can be called
	result := &athena.GetQueryResultsOutput{
		ResultSet: &types.ResultSet{
			Rows: []types.Row{
				{Data: []types.Datum{}},
			},
		},
	}

	// Should not panic
	queryFunc(result)
}

func TestGetAthenaRowValue(t *testing.T) {
	// Test with valid data
	row := types.Row{
		Data: []types.Datum{
			{VarCharValue: stringPtr("test-value")},
		},
	}

	queryColumnIndexes := map[string]int{
		"test-column": 0,
	}

	result := GetAthenaRowValue(row, queryColumnIndexes, "test-column")
	if result != "test-value" {
		t.Errorf("GetAthenaRowValue() = %v, want %v", result, "test-value")
	}

	// Test with missing column
	result = GetAthenaRowValue(row, queryColumnIndexes, "missing-column")
	if result != "" {
		t.Errorf("GetAthenaRowValue() with missing column = %v, want %v", result, "")
	}

	// Test with nil value
	rowWithNil := types.Row{
		Data: []types.Datum{
			{VarCharValue: nil},
		},
	}

	result = GetAthenaRowValue(rowWithNil, queryColumnIndexes, "test-column")
	if result != "" {
		t.Errorf("GetAthenaRowValue() with nil value = %v, want %v", result, "")
	}
}

func TestGetAthenaRowValueFloat(t *testing.T) {
	// Test with valid data
	row := types.Row{
		Data: []types.Datum{
			{VarCharValue: stringPtr("3.14159")},
		},
	}

	queryColumnIndexes := map[string]int{
		"test-column": 0,
	}

	result, err := GetAthenaRowValueFloat(row, queryColumnIndexes, "test-column")
	if err != nil {
		t.Errorf("GetAthenaRowValueFloat() returned error: %v", err)
	}

	if result != 3.14159 {
		t.Errorf("GetAthenaRowValueFloat() = %v, want %v", result, 3.14159)
	}

	// Test with missing column
	_, err = GetAthenaRowValueFloat(row, queryColumnIndexes, "missing-column")
	if err == nil {
		t.Error("GetAthenaRowValueFloat() should return error for missing column")
	}

	// Test with nil value
	rowWithNil := types.Row{
		Data: []types.Datum{
			{VarCharValue: nil},
		},
	}

	_, err = GetAthenaRowValueFloat(rowWithNil, queryColumnIndexes, "test-column")
	if err == nil {
		t.Error("GetAthenaRowValueFloat() should return error for nil value")
	}

	// Test with invalid float
	rowWithInvalid := types.Row{
		Data: []types.Datum{
			{VarCharValue: stringPtr("not-a-number")},
		},
	}

	_, err = GetAthenaRowValueFloat(rowWithInvalid, queryColumnIndexes, "test-column")
	if err == nil {
		t.Error("GetAthenaRowValueFloat() should return error for invalid float")
	}
}

func TestSelectAWSCategory(t *testing.T) {
	// Test network category (usage type ending in "Bytes")
	category := SelectAWSCategory("", "DataTransfer-Bytes", "")
	if category != "Network" {
		t.Errorf("SelectAWSCategory() for network = %v, want %v", category, "Network")
	}

	// Test compute category (provider ID with "i-" prefix)
	category = SelectAWSCategory("i-123456789", "", "")
	if category != "Compute" {
		t.Errorf("SelectAWSCategory() for compute = %v, want %v", category, "Compute")
	}

	// Test GuardDuty special case
	category = SelectAWSCategory("i-123456789", "", "AmazonGuardDuty")
	if category != "Other" {
		t.Errorf("SelectAWSCategory() for GuardDuty = %v, want %v", category, "Other")
	}

	// Test storage category (provider ID with "vol-" prefix)
	category = SelectAWSCategory("vol-123456789", "", "")
	if category != "Storage" {
		t.Errorf("SelectAWSCategory() for storage = %v, want %v", category, "Storage")
	}

	// Test service-based categories
	category = SelectAWSCategory("", "", "AmazonEKS")
	if category != "Management" {
		t.Errorf("SelectAWSCategory() for EKS = %v, want %v", category, "Management")
	}

	// Test fargate pod in EKS
	category = SelectAWSCategory("arn:aws:eks:us-west-2:123456789012:pod/cluster-name/pod-name", "", "AmazonEKS")
	if category != "Compute" {
		t.Errorf("SelectAWSCategory() for EKS fargate pod = %v, want %v", category, "Compute")
	}

	// Test other category as default
	category = SelectAWSCategory("", "", "SomeUnknownService")
	if category != "Other" {
		t.Errorf("SelectAWSCategory() for unknown service = %v, want %v", category, "Other")
	}
}

func TestParseARN(t *testing.T) {
	// Test valid ARN
	id := "arn:aws:elasticloadbalancing:us-east-1:297945954695:loadbalancer/a406f7761142e4ef58a8f2ba478d2db2"
	expected := "a406f7761142e4ef58a8f2ba478d2db2"
	result := ParseARN(id)
	if result != expected {
		t.Errorf("ParseARN() = %v, want %v", result, expected)
	}

	// Test invalid ARN (no match)
	id = "not-an-arn"
	result = ParseARN(id)
	if result != id {
		t.Errorf("ParseARN() for invalid ARN = %v, want %v", result, id)
	}

	// Test empty string
	id = ""
	result = ParseARN(id)
	if result != id {
		t.Errorf("ParseARN() for empty string = %v, want %v", result, id)
	}
}
