package gcp

import (
	"context"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	cloudconfig "github.com/opencost/opencost/pkg/cloud/config"
	"github.com/opencost/opencost/pkg/kubecost"
)

type BigQueryQuerier struct {
	BigQueryConfiguration
}

func (bqq *BigQueryQuerier) Equals(config cloudconfig.Config) bool {
	thatConfig, ok := config.(*BigQueryQuerier)
	if !ok {
		return false
	}

	return bqq.BigQueryConfiguration.Equals(&thatConfig.BigQueryConfiguration)
}

func (bqq *BigQueryQuerier) QueryBigQuery(ctx context.Context, queryStr string) (*bigquery.RowIterator, error) {
	client, err := bqq.GetBigQueryClient(ctx)
	if err != nil {
		return nil, err
	}

	query := client.Query(queryStr)
	return query.Read(ctx)
}

func GCPSelectCategory(service, description string) string {
	s := strings.ToLower(service)
	d := strings.ToLower(description)

	// Network descriptions
	if strings.Contains(d, "download") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "network") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "ingress") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "egress") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "static ip") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "external ip") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "load balanced") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "licensing fee") {
		return kubecost.OtherCategory
	}

	// Storage Descriptions
	if strings.Contains(d, "storage") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd capacity") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd iops") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd snapshot") {
		return kubecost.StorageCategory
	}

	// Service Defaults
	if strings.Contains(s, "storage") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "compute") {
		return kubecost.ComputeCategory
	}
	if strings.Contains(s, "sql") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "bigquery") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "kubernetes") {
		return kubecost.ManagementCategory
	} else if strings.Contains(s, "pub/sub") {
		return kubecost.NetworkCategory
	}

	return kubecost.OtherCategory
}

var parseProviderIDRx = regexp.MustCompile("^.+\\/(.+)?") // Capture "gke-cluster-3-default-pool-xxxx-yy" from "projects/###/instances/gke-cluster-3-default-pool-xxxx-yy"

func GCPParseProviderID(id string) string {
	match := parseProviderIDRx.FindStringSubmatch(id)
	if len(match) == 0 {
		return id
	}
	return match[len(match)-1]
}
