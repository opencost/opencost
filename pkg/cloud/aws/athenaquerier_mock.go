package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// MockAthenaClient is a mock implementation of the Athena client for testing
type MockAthenaClient struct {
	StartQueryExecutionFunc func(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecutionFunc   func(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResultsFunc     func(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
}

func (m *MockAthenaClient) StartQueryExecution(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	if m.StartQueryExecutionFunc != nil {
		return m.StartQueryExecutionFunc(ctx, params, optFns...)
	}
	return &athena.StartQueryExecutionOutput{
		QueryExecutionId: aws.String("mock-query-id-123"),
	}, nil
}

func (m *MockAthenaClient) GetQueryExecution(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
	if m.GetQueryExecutionFunc != nil {
		return m.GetQueryExecutionFunc(ctx, params, optFns...)
	}
	return &athena.GetQueryExecutionOutput{
		QueryExecution: &types.QueryExecution{
			Status: &types.QueryExecutionStatus{
				State: types.QueryExecutionStateSucceeded,
			},
		},
	}, nil
}

func (m *MockAthenaClient) GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	if m.GetQueryResultsFunc != nil {
		return m.GetQueryResultsFunc(ctx, params, optFns...)
	}
	return &athena.GetQueryResultsOutput{
		ResultSet: &types.ResultSet{
			Rows: []types.Row{
				{Data: []types.Datum{}},
			},
		},
	}, nil
}

func stringPtr(s string) *string {
	return &s
}
