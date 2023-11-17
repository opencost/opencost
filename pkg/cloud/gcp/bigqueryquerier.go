package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/opencost/opencost/pkg/cloud"
)

type BigQueryQuerier struct {
	BigQueryConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (bqq *BigQueryQuerier) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if bqq.ConnectionStatus.String() == "" {
		bqq.ConnectionStatus = cloud.InitialStatus
	}
	return bqq.ConnectionStatus
}

func (bqq *BigQueryQuerier) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*BigQueryQuerier)
	if !ok {
		return false
	}

	return bqq.BigQueryConfiguration.Equals(&thatConfig.BigQueryConfiguration)
}

func (bqq *BigQueryQuerier) Query(ctx context.Context, queryStr string) (*bigquery.RowIterator, error) {
	err := bqq.Validate()

	if err != nil {
		bqq.ConnectionStatus = cloud.InvalidConfiguration
		return nil, err
	}

	client, err := bqq.GetBigQueryClient(ctx)
	if err != nil {
		bqq.ConnectionStatus = cloud.FailedConnection
		return nil, err
	}

	query := client.Query(queryStr)
	iter, err := query.Read(ctx)

	// If result is empty and connection status is not already successful update status to missing data
	if iter == nil && bqq.ConnectionStatus != cloud.SuccessfulConnection {
		bqq.ConnectionStatus = cloud.MissingData
	} else {
		bqq.ConnectionStatus = cloud.SuccessfulConnection
	}

	if err != nil {
		return iter, fmt.Errorf("BigQueryQuerier: Query: error reading query results: %w", err)
	}
	return iter, nil
}
