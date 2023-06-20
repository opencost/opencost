package gcp

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/opencost/opencost/pkg/cloud"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
)

type BigQueryQuerier struct {
	BigQueryConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (bqq *BigQueryQuerier) Equals(config cloudconfig.Config) bool {
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
	return query.Read(ctx)
}
