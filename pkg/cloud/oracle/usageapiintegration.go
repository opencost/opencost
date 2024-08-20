package oracle

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/example/helpers"
	"github.com/oracle/oci-go-sdk/v65/usageapi"
)

type UsageApiIntegration struct {
	UsageApiConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (uai *UsageApiIntegration) GetCloudCost(start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {
	client, err := uai.GetUsageApiClient()
	if err != nil {
		return nil, fmt.Errorf("getting oracle usage api client: %s", err.Error())
	}

	req := usageapi.RequestSummarizedUsagesRequest{
		RequestSummarizedUsagesDetails: usageapi.RequestSummarizedUsagesDetails{
			CompartmentDepth: common.Float32(5.749769), //todo: what does this mean
			Granularity:      usageapi.RequestSummarizedUsagesDetailsGranularityDaily,
			// GroupBy:          []string{"group-by"},
			// GroupByTag: []usageapi.Tag{
			// 	{
			// 		Key:       common.String("key"),
			// 		Namespace: common.String("namespace"),
			// 		Value:     common.String("value"),
			// 	},
			// },
			IsAggregateByTime: common.Bool(false),
			TimeUsageStarted:  &common.SDKTime{Time: start},
			TimeUsageEnded:    &common.SDKTime{Time: end},
			// Filter: &usageapi.Filter{
			// 	Dimensions: []usageapi.Dimension{
			// 		{
			// 			Key:   common.String("key"),
			// 			Value: common.String("value"),
			// 		},
			// 	},
			// 	Operator: usageapi.FilterOperatorAnd,
			// 	Tags: []usageapi.Tag{
			// 		{
			// 			Key:       common.String("key"),
			// 			Namespace: common.String("namespace"),
			// 			Value:     common.String("value"),
			// 		},
			// 	},
			// },
			QueryType: usageapi.RequestSummarizedUsagesDetailsQueryTypeCost,
			TenantId:  common.String(uai.TenancyID),
		},
		Limit: common.Int(533),
	}

	resp, err := client.RequestSummarizedUsages(context.Background(), req)
	helpers.FatalIfError(err)

	log.Infof("%+v", resp.UsageAggregation)

	ccsr := &opencost.CloudCostSetRange{}

	for _, item := range resp.Items {
		properties := &opencost.CloudCostProperties{
			ProviderID:      opencost.OracleProvider,
			Provider:        opencost.OracleProvider,
			AccountID:       *item.TenantId,
			AccountName:     *item.TenantName,
			InvoiceEntityID: *item.SubscriptionId,
			RegionID:        *item.Region,
			Service:         *item.Service,
			Category:        *item.Platform, //todo: is this accurate to Category?
			//Labels:        item.Tags, //todo: build out tags, oc labels
		}

		//todo: is this an entire window, or just a day?
		winStart := item.TimeUsageStarted.Time
		winEnd := start.AddDate(0, 0, 1)

		attrCost, err := strconv.ParseFloat(*item.AttributedCost, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to parse float '%s': %s", *item.AttributedCost, err.Error())
		}

		cc := &opencost.CloudCost{
			Properties: properties,
			Window:     opencost.NewWindow(&winStart, &winEnd),
			ListCost: opencost.CostMetric{ //todo: which costs go where? should we query against credits to get amortized?
				Cost: float64(*item.ListRate),
			},
			NetCost: opencost.CostMetric{
				Cost: float64(*item.ComputedAmount),
			},
			AmortizedNetCost: opencost.CostMetric{
				Cost: attrCost,
			},
		}

		ccsr.LoadCloudCost(cc)
	}

	return ccsr, nil
}

func (uai *UsageApiIntegration) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if uai.ConnectionStatus.String() == "" {
		uai.ConnectionStatus = cloud.InitialStatus
	}
	return uai.ConnectionStatus
}
