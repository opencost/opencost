package aws

import (
	"context"
	"fmt"
	"strconv"
	"time"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/opencost/opencost/core/pkg/log"
)

// SpotPrice holds the most recent spot price for a single instance type in a region.
type SpotPrice struct {
	Region       string
	InstanceType string
	Price        float64
	Timestamp    time.Time
}

const osDesc = "Linux/UNIX (Amazon VPC)"

type spotPriceHistoryClient interface {
	ec2.DescribeSpotPriceHistoryAPIClient
}

// QuerySpotPrices fetches the current spot price for every Linux/UNIX instance
// type available in the given region
func QuerySpotPrices(ctx context.Context, region string) ([]SpotPrice, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config for region %s: %w", region, err)
	}
	return querySpotPrices(ctx, region, ec2.NewFromConfig(cfg))
}

func querySpotPrices(ctx context.Context, region string, client spotPriceHistoryClient) ([]SpotPrice, error) {
	paginator := ec2.NewDescribeSpotPriceHistoryPaginator(client, &ec2.DescribeSpotPriceHistoryInput{
		ProductDescriptions: []string{osDesc},
	})

	seen := make(map[ec2Types.InstanceType]struct{})
	var results []SpotPrice

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("fetching spot price history page for region %s: %w", region, err)
		}

		for _, item := range page.SpotPriceHistory {
			if _, ok := seen[item.InstanceType]; ok {
				continue
			}
			seen[item.InstanceType] = struct{}{}

			if item.SpotPrice == nil || item.Timestamp == nil {
				log.Warnf("SpotAPI: skipping %s/%s — missing price or timestamp", region, item.InstanceType)
				continue
			}

			price, err := strconv.ParseFloat(*item.SpotPrice, 64)
			if err != nil {
				log.Warnf("SpotAPI: skipping %s/%s — could not parse price %q: %v", region, item.InstanceType, *item.SpotPrice, err)
				continue
			}

			results = append(results, SpotPrice{
				Region:       region,
				InstanceType: string(item.InstanceType),
				Price:        price,
				Timestamp:    *item.Timestamp,
			})
		}
	}

	log.Infof("SpotAPI: fetched %d spot prices for region %s", len(results), region)
	return results, nil
}
