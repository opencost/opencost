package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/opencost/opencost/pkg/cloud"
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

// MockAthenaQuerier wraps AthenaQuerier with a mock client for testing
type MockAthenaQuerier struct {
	AthenaQuerier
	mockClient *MockAthenaClient
}

// FailingMockAthenaQuerier is a mock querier that fails on GetAthenaClient
type FailingMockAthenaQuerier struct {
	MockAthenaQuerier
}

func (fmaq *FailingMockAthenaQuerier) GetAthenaClient() (*athena.Client, error) {
	return nil, errors.New("failed to create client")
}

// FailingQueryAthenaQuerier is a mock querier that fails in queryAthenaPaginated
type FailingQueryAthenaQuerier struct {
	MockAthenaQuerier
}

func (fqaq *FailingQueryAthenaQuerier) GetAthenaClient() (*athena.Client, error) {
	return nil, errors.New("failed to create client")
}

func (fqaq *FailingQueryAthenaQuerier) queryAthenaPaginated(ctx context.Context, query string, fn func(*athena.GetQueryResultsOutput) bool) error {
	// Simulate GetAthenaClient failure
	_, err := fqaq.GetAthenaClient()
	if err != nil {
		return fmt.Errorf("QueryAthenaPaginated: GetAthenaClient error: %s", err.Error())
	}

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Acknowledge the query parameter to avoid unused parameter warning
	_ = query

	// Call the function with empty result to simulate no data
	fn(&athena.GetQueryResultsOutput{})
	return nil
}

func (maq *MockAthenaQuerier) GetAthenaClient() (*athena.Client, error) {
	// Return a real client but we'll override the methods in tests
	cfg, err := maq.Authorizer.CreateAWSConfig(maq.Region)
	if err != nil {
		return nil, err
	}
	cli := athena.NewFromConfig(cfg)
	return cli, nil
}

func (maq *MockAthenaQuerier) GetColumns() (map[string]bool, error) {
	columnSet := map[string]bool{}

	// This Query is supported by Athena tables and views
	q := `SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'`
	query := fmt.Sprintf(q, maq.Database, maq.Table)

	athenaErr := maq.Query(context.TODO(), query, GetAthenaQueryFunc(func(row types.Row) {
		columnSet[*row.Data[0].VarCharValue] = true
	}))

	if athenaErr != nil {
		return columnSet, athenaErr
	}

	if len(columnSet) == 0 {
		// Don't log in tests
	}

	return columnSet, nil
}

func (maq *MockAthenaQuerier) Query(ctx context.Context, query string, fn func(*athena.GetQueryResultsOutput) bool) error {
	err := maq.Validate()
	if err != nil {
		maq.ConnectionStatus = cloud.InvalidConfiguration
		return err
	}

	// Use mock client instead of real one
	queryExecutionCtx := &types.QueryExecutionContext{
		Database: aws.String(maq.Database),
	}

	if maq.Catalog != "" {
		queryExecutionCtx.Catalog = aws.String(maq.Catalog)
	}
	resultConfiguration := &types.ResultConfiguration{
		OutputLocation: aws.String(maq.Bucket),
	}
	startQueryExecutionInput := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		QueryExecutionContext: queryExecutionCtx,
		ResultConfiguration:   resultConfiguration,
	}

	if maq.Workgroup != "" {
		startQueryExecutionInput.WorkGroup = aws.String(maq.Workgroup)
	}

	// Use mock client
	startQueryExecutionOutput, err := maq.mockClient.StartQueryExecution(ctx, startQueryExecutionInput)
	if err != nil {
		maq.ConnectionStatus = cloud.FailedConnection
		return fmt.Errorf("QueryAthenaPaginated: start query error: %s", err.Error())
	}
	err = maq.waitForQueryToComplete(ctx, maq.mockClient, startQueryExecutionOutput.QueryExecutionId)
	if err != nil {
		maq.ConnectionStatus = cloud.FailedConnection
		return fmt.Errorf("QueryAthenaPaginated: query execution error: %s", err.Error())
	}
	queryResultsInput := &athena.GetQueryResultsInput{
		QueryExecutionId: startQueryExecutionOutput.QueryExecutionId,
		MaxResults:       aws.Int32(1000),
	}

	// Simulate pagination
	pg, err := maq.mockClient.GetQueryResults(ctx, queryResultsInput)
	if err != nil {
		maq.ConnectionStatus = cloud.FailedConnection
		return err
	}
	fn(pg)
	maq.ConnectionStatus = cloud.SuccessfulConnection
	return nil
}

func (maq *MockAthenaQuerier) queryAthenaPaginated(ctx context.Context, query string, fn func(*athena.GetQueryResultsOutput) bool) error {
	queryExecutionCtx := &types.QueryExecutionContext{
		Database: aws.String(maq.Database),
	}

	if maq.Catalog != "" {
		queryExecutionCtx.Catalog = aws.String(maq.Catalog)
	}
	resultConfiguration := &types.ResultConfiguration{
		OutputLocation: aws.String(maq.Bucket),
	}
	startQueryExecutionInput := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		QueryExecutionContext: queryExecutionCtx,
		ResultConfiguration:   resultConfiguration,
	}

	if maq.Workgroup != "" {
		startQueryExecutionInput.WorkGroup = aws.String(maq.Workgroup)
	}

	// Use mock client
	startQueryExecutionOutput, err := maq.mockClient.StartQueryExecution(ctx, startQueryExecutionInput)
	if err != nil {
		return fmt.Errorf("QueryAthenaPaginated: start query error: %s", err.Error())
	}
	err = maq.waitForQueryToComplete(ctx, maq.mockClient, startQueryExecutionOutput.QueryExecutionId)
	if err != nil {
		return fmt.Errorf("QueryAthenaPaginated: query execution error: %s", err.Error())
	}
	queryResultsInput := &athena.GetQueryResultsInput{
		QueryExecutionId: startQueryExecutionOutput.QueryExecutionId,
		MaxResults:       aws.Int32(1000),
	}

	// Simulate pagination
	pg, err := maq.mockClient.GetQueryResults(ctx, queryResultsInput)
	if err != nil {
		return err
	}
	fn(pg)
	return nil
}

func (maq *MockAthenaQuerier) waitForQueryToComplete(ctx context.Context, client *MockAthenaClient, queryExecutionID *string) error {
	if queryExecutionID == nil {
		return fmt.Errorf("query execution ID is nil")
	}

	inp := &athena.GetQueryExecutionInput{
		QueryExecutionId: queryExecutionID,
	}

	// Simulate waiting with mock
	qe, err := client.GetQueryExecution(ctx, inp)
	if err != nil {
		return err
	}
	if qe.QueryExecution.Status.State != "SUCCEEDED" {
		return fmt.Errorf("no query results available for query %s", *queryExecutionID)
	}
	return nil
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
