package stackit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/stackitcloud/stackit-sdk-go/core/config"
	cost "github.com/stackitcloud/stackit-sdk-go/services/cost/v3api"
)

type CostIntegration struct {
	CostConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (ci *CostIntegration) GetCloudCost(start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {
	var opts []config.ConfigurationOption
	if ci.ServiceAccountKeyPath != "" {
		opts = append(opts, config.WithServiceAccountKeyPath(ci.ServiceAccountKeyPath))
	}

	client, err := cost.NewAPIClient(opts...)
	if err != nil {
		ci.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("creating STACKIT cost API client: %w", err)
	}

	fromStr := start.Format("2006-01-02")
	toStr := end.Format("2006-01-02")

	resp, err := client.DefaultAPI.
		GetCostsForProject(context.Background(), ci.CustomerAccountID, ci.ProjectID).
		From(fromStr).
		To(toStr).
		Depth("service").
		Granularity("daily").
		Execute()
	if err != nil {
		ci.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("querying STACKIT costs: %w", err)
	}

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, ci.Key())
	if err != nil {
		return nil, err
	}

	if resp == nil || resp.ProjectCostWithDetailedServices == nil {
		if ci.ConnectionStatus != cloud.SuccessfulConnection {
			ci.ConnectionStatus = cloud.MissingData
		}
		return ccsr, nil
	}

	detailed := resp.ProjectCostWithDetailedServices

	for _, svc := range detailed.GetServices() {
		serviceName := svc.GetServiceName()
		category := selectSTACKITCategory(serviceName)
		sku := svc.GetSku()

		reportData := svc.GetReportData()
		if len(reportData) == 0 {
			// No daily granularity data; use total charge
			totalChargeCents := svc.GetTotalCharge()
			totalDiscountCents := svc.GetTotalDiscount()
			totalCharge := totalChargeCents / 100.0
			totalDiscount := totalDiscountCents / 100.0
			netCost := totalCharge

			properties := &opencost.CloudCostProperties{
				Provider:        opencost.STACKITProvider,
				AccountID:       ci.CustomerAccountID,
				InvoiceEntityID: ci.CustomerAccountID,
				RegionID:        "eu01",
				Service:         serviceName,
				Category:        category,
				ProviderID:      sku,
				Labels:          opencost.CloudCostLabels{},
			}

			cc := &opencost.CloudCost{
				Properties: properties,
				Window:     opencost.NewWindow(&start, &end),
				ListCost: opencost.CostMetric{
					Cost: totalCharge + totalDiscount,
				},
				NetCost: opencost.CostMetric{
					Cost: netCost,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost: netCost,
				},
				AmortizedCost: opencost.CostMetric{
					Cost: netCost,
				},
				InvoicedCost: opencost.CostMetric{
					Cost: netCost,
				},
			}

			ccsr.LoadCloudCost(cc)
			continue
		}

		for _, rd := range reportData {
			chargeCents := rd.GetCharge()
			discountCents := rd.GetDiscount()
			charge := chargeCents / 100.0
			discount := discountCents / 100.0

			tp := rd.GetTimePeriod()
			periodStart, periodEnd := parsePeriod(tp.GetStart(), tp.GetEnd(), start, end)

			properties := &opencost.CloudCostProperties{
				Provider:        opencost.STACKITProvider,
				AccountID:       ci.CustomerAccountID,
				InvoiceEntityID: ci.CustomerAccountID,
				RegionID:        "eu01",
				Service:         serviceName,
				Category:        category,
				ProviderID:      sku,
				Labels:          opencost.CloudCostLabels{},
			}

			cc := &opencost.CloudCost{
				Properties: properties,
				Window:     opencost.NewWindow(&periodStart, &periodEnd),
				ListCost: opencost.CostMetric{
					Cost: charge + discount,
				},
				NetCost: opencost.CostMetric{
					Cost: charge,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost: charge,
				},
				AmortizedCost: opencost.CostMetric{
					Cost: charge,
				},
				InvoicedCost: opencost.CostMetric{
					Cost: charge,
				},
			}

			ccsr.LoadCloudCost(cc)
		}
	}

	ci.ConnectionStatus = cloud.SuccessfulConnection
	return ccsr, nil
}

// parsePeriod parses start/end date strings from the STACKIT API, falling back to the given defaults.
func parsePeriod(startStr, endStr string, defaultStart, defaultEnd time.Time) (time.Time, time.Time) {
	periodStart := defaultStart
	periodEnd := defaultEnd

	if startStr != "" {
		if t, err := time.Parse("2006-01-02", startStr); err == nil {
			periodStart = t
		} else if t, err := time.Parse(time.RFC3339, startStr); err == nil {
			periodStart = t
		}
	}

	if endStr != "" {
		if t, err := time.Parse("2006-01-02", endStr); err == nil {
			// End date is inclusive in the API, add one day for the window
			periodEnd = t.AddDate(0, 0, 1)
		} else if t, err := time.Parse(time.RFC3339, endStr); err == nil {
			periodEnd = t
		}
	}

	return periodStart, periodEnd
}

func (ci *CostIntegration) GetStatus() cloud.ConnectionStatus {
	if ci.ConnectionStatus.String() == "" {
		ci.ConnectionStatus = cloud.InitialStatus
	}
	return ci.ConnectionStatus
}

func (ci *CostIntegration) RefreshStatus() cloud.ConnectionStatus {
	log.Warn("status refresh is not supported for the STACKIT provider")
	return ci.ConnectionStatus
}

func selectSTACKITCategory(serviceName string) string {
	lower := strings.ToLower(serviceName)
	switch {
	case strings.Contains(lower, "compute") || strings.Contains(lower, "server") || strings.Contains(lower, "ske"):
		return opencost.ComputeCategory
	case strings.Contains(lower, "storage") || strings.Contains(lower, "object store") || strings.Contains(lower, "backup"):
		return opencost.StorageCategory
	case strings.Contains(lower, "network") || strings.Contains(lower, "load balancer") || strings.Contains(lower, "dns"):
		return opencost.NetworkCategory
	default:
		return opencost.OtherCategory
	}
}
