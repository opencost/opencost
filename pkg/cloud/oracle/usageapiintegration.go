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
	"github.com/oracle/oci-go-sdk/v65/usageapi"
)

type UsageApiIntegration struct {
	UsageApiConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

type usageAPIClient interface {
	RequestSummarizedUsages(context.Context, usageapi.RequestSummarizedUsagesRequest) (usageapi.RequestSummarizedUsagesResponse, error)
}

func (uai *UsageApiIntegration) GetCloudCost(start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {
	client, err := uai.GetUsageApiClient()
	if err != nil {
		uai.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("getting oracle usage api client: %s", err.Error())
	}
	return uai.getCloudCost(context.Background(), client, start, end)
}

func (uai *UsageApiIntegration) getCloudCost(ctx context.Context, client usageAPIClient, start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {

	req := usageapi.RequestSummarizedUsagesRequest{
		RequestSummarizedUsagesDetails: usageapi.RequestSummarizedUsagesDetails{
			Granularity:       usageapi.RequestSummarizedUsagesDetailsGranularityDaily,
			GroupBy:           []string{"resourceId", "service", "subscriptionId", "tenantName"},
			IsAggregateByTime: common.Bool(false),
			TimeUsageStarted:  &common.SDKTime{Time: start},
			TimeUsageEnded:    &common.SDKTime{Time: end},
			QueryType:         usageapi.RequestSummarizedUsagesDetailsQueryTypeCost,
			TenantId:          common.String(uai.TenancyID),
		},
		Limit: common.Int(500),
	}

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, uai.Key())
	if err != nil {
		return nil, err
	}

	hasItems := false
	seenPageTokens := map[string]struct{}{}
	for page := 1; ; page++ {
		resp, err := client.RequestSummarizedUsages(ctx, req)
		if err != nil {
			uai.ConnectionStatus = cloud.FailedConnection
			return nil, fmt.Errorf("failed to query usage: %w", err)
		}
		log.Debugf("UsageApiIntegration[%s]: received %d usage items from page %d", uai.Key(), len(resp.Items), page)

		if len(resp.Items) > 0 {
			hasItems = true
		}

		for _, item := range resp.Items {
			if item.TimeUsageStarted == nil || item.TimeUsageEnded == nil {
				log.Warnf("UsageApiIntegration[%s]: skipping usage item without a usage window", uai.Key())
				continue
			}

			cc, err := uai.usageSummaryToCloudCost(item)
			if err != nil {
				return nil, err
			}
			ccsr.LoadCloudCost(cc)
		}

		if resp.OpcNextPage == nil || *resp.OpcNextPage == "" {
			break
		}
		if _, ok := seenPageTokens[*resp.OpcNextPage]; ok {
			uai.ConnectionStatus = cloud.FailedConnection
			return nil, fmt.Errorf("received a repeated OCI usage API page token")
		}
		seenPageTokens[*resp.OpcNextPage] = struct{}{}
		req.Page = resp.OpcNextPage
	}

	// Set status to missing data if every response page was empty and the status isn't already successful.
	if !hasItems && uai.ConnectionStatus != cloud.SuccessfulConnection {
		uai.ConnectionStatus = cloud.MissingData
		return ccsr, nil
	}

	uai.ConnectionStatus = cloud.SuccessfulConnection
	return ccsr, nil
}

func (uai *UsageApiIntegration) usageSummaryToCloudCost(item usageapi.UsageSummary) (*opencost.CloudCost, error) {
	resourceID := ""
	if item.ResourceId != nil {
		resourceID = *item.ResourceId
	}

	tenantName := ""
	if item.TenantName != nil {
		tenantName = *item.TenantName
	}

	subscriptionID := ""
	if item.SubscriptionId != nil {
		subscriptionID = *item.SubscriptionId
	}

	service := ""
	if item.Service != nil {
		service = *item.Service
	}

	labels := opencost.CloudCostLabels{}
	for _, tag := range item.Tags {
		if tag.Key == nil || tag.Value == nil {
			continue
		}
		labels[*tag.Key] = *tag.Value
	}

	listRate := 0.0
	if item.ListRate != nil {
		listRate = float64(*item.ListRate)
	}

	attributedCost, err := parseAttributedCost(item.AttributedCost)
	if err != nil {
		return nil, err
	}

	computedAmount := 0.0
	if item.ComputedAmount != nil {
		computedAmount = float64(*item.ComputedAmount)
	}

	winStart := item.TimeUsageStarted.Time
	winEnd := item.TimeUsageEnded.Time
	return &opencost.CloudCost{
		Properties: &opencost.CloudCostProperties{
			ProviderID:      resourceID,
			Provider:        opencost.OracleProvider,
			AccountID:       uai.TenancyID,
			AccountName:     tenantName,
			InvoiceEntityID: subscriptionID,
			RegionID:        uai.Region,
			Service:         service,
			Category:        SelectOCICategory(service),
			Labels:          labels,
		},
		Window: opencost.NewWindow(&winStart, &winEnd),
		ListCost: opencost.CostMetric{
			Cost: listRate,
		},
		NetCost: opencost.CostMetric{
			Cost: computedAmount,
		},
		AmortizedNetCost: opencost.CostMetric{
			Cost: attributedCost,
		},
		AmortizedCost: opencost.CostMetric{
			Cost: attributedCost,
		},
		InvoicedCost: opencost.CostMetric{
			Cost: computedAmount,
		},
	}, nil
}

func (uai *UsageApiIntegration) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if uai.ConnectionStatus.String() == "" {
		uai.ConnectionStatus = cloud.InitialStatus
	}
	return uai.ConnectionStatus
}

func (uai *UsageApiIntegration) RefreshStatus() cloud.ConnectionStatus {
	log.Warn("status refresh is not supported for the Oracle provider")
	return uai.ConnectionStatus
}

func parseAttributedCost(s *string) (float64, error) {
	if s == nil || *s == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(*s, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse float '%s': %s", *s, err.Error())
	}
	return f, nil
}

func SelectOCICategory(service string) string {
	if service == "Compute" {
		return opencost.ComputeCategory
	} else if service == "Block Storage" || service == "Object Storage" {
		return opencost.StorageCategory
	} else if service == "Load Balancer" || service == "Virtual Cloud Network" {
		return opencost.NetworkCategory
	} else {
		return opencost.OtherCategory
	}
}
