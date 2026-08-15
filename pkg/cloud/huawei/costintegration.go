package huawei

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

// bssDateLayout is the date format the BSS cost-analysed-bills API expects/returns
// for daily granularity (TimeCondition.TimeMeasureId == 1).
const bssDateLayout = "2006-01-02"

// CostIntegration implements cloud.CloudCostIntegration (see pkg/cloudcost),
// retrieving actual historical billing costs from the Huawei Cloud BSS
// cost-analysed-bills API, as opposed to pkg/cloud/huawei's Provider, which prices
// the cluster's resources going forward via the BSS demandPrice API.
type CostIntegration struct {
	CostConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

// GetCloudCost queries BSS for costs across [start, end) and returns them as a
// CloudCostSetRange, one CloudCost per resource/service/region combination per day.
//
// BSS's cost query separates "cost_type" (ORIGINAL_COST vs AMORTIZED_COST, i.e.
// whether upfront/reserved charges are spread across their billing period) from
// "amount_type" (PAYMENT_AMOUNT vs NET_AMOUNT, i.e. before or after account-level
// discounts). A single query with cost_type=ORIGINAL_COST, amount_type=NET_AMOUNT
// returns, per Cost entry, both the requested net original amount and the
// account-independent list/official price (Cost.OfficialAmount), which is enough to
// populate ListCost and NetCost accurately. AmortizedCost/AmortizedNetCost reuse
// those same two values as an approximation (this does not yet issue the second,
// cost_type=AMORTIZED_COST query that would let those differ from ListCost/NetCost)
// -- the same simplification pkg/cloud/stackit's CostIntegration makes for the same
// reason: the two cost_type variants require separate API calls.
func (ci *CostIntegration) GetCloudCost(start, end time.Time) (*opencost.CloudCostSetRange, error) {
	beginTime := start.Format(bssDateLayout)
	// BSS's end_time is inclusive; OpenCost windows are end-exclusive.
	endTime := end.AddDate(0, 0, -1).Format(bssDateLayout)

	rows, err := fetchCostAnalysedBills(ci.ProjectID, beginTime, endTime, "ORIGINAL_COST", "NET_AMOUNT")
	if err != nil {
		ci.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("getting huawei cloud cost data: %w", err)
	}

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, ci.Key())
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		if ci.ConnectionStatus != cloud.SuccessfulConnection {
			ci.ConnectionStatus = cloud.MissingData
		}
		return ccsr, nil
	}

	for _, row := range rows {
		resourceID := dimensionValue(row.Dimensions, "RESOURCE_ID")
		serviceType := dimensionValue(row.Dimensions, "CLOUD_SERVICE_TYPE")
		region := dimensionValue(row.Dimensions, "REGION_CODE")

		// A billed resource has no CPU/RAM figures to report the way a Node
		// does, so what it is and what it's called is all there is to identify
		// it by. Both come out of RESOURCE_ID itself (see describeResource):
		// BSS caps the group-by at three dimensions, which the identifying ones
		// already use up, so they can't be requested as dimensions of their own.
		labels := opencost.CloudCostLabels{}
		if resourceType, name := describeResource(resourceID); resourceType != "" || name != "" {
			if resourceType != "" {
				labels[opencost.AssetResourceTypeLabel] = resourceType
			}
			if name != "" {
				labels[opencost.AssetResourceNameLabel] = name
			}
		}

		properties := &opencost.CloudCostProperties{
			ProviderID: resourceID,
			Provider:   opencost.HuaweiProvider,
			AccountID:  ci.ProjectID,
			RegionID:   region,
			Service:    serviceType,
			Category:   selectHuaweiCategory(serviceType),
			Labels:     labels,
		}

		if row.Costs == nil {
			continue
		}

		for _, item := range *row.Costs {
			winStart, ok := parseCostDay(item.TimeDimensionValue, start)
			if !ok {
				continue
			}
			winEnd := winStart.AddDate(0, 0, 1)

			netAmount, err := parseCostAmount(item.Amount)
			if err != nil {
				return nil, fmt.Errorf("parsing huawei cloud cost amount: %w", err)
			}
			listAmount, err := parseCostAmount(item.OfficialAmount)
			if err != nil {
				return nil, fmt.Errorf("parsing huawei cloud official cost amount: %w", err)
			}

			cc := &opencost.CloudCost{
				Properties: properties,
				Window:     opencost.NewWindow(&winStart, &winEnd),
				ListCost: opencost.CostMetric{
					Cost: listAmount,
				},
				NetCost: opencost.CostMetric{
					Cost: netAmount,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost: netAmount,
				},
				AmortizedCost: opencost.CostMetric{
					Cost: listAmount,
				},
				InvoicedCost: opencost.CostMetric{
					Cost: netAmount,
				},
			}

			ccsr.LoadCloudCost(cc)
		}
	}

	ci.ConnectionStatus = cloud.SuccessfulConnection
	return ccsr, nil
}

func (ci *CostIntegration) GetStatus() cloud.ConnectionStatus {
	if ci.ConnectionStatus.String() == "" {
		ci.ConnectionStatus = cloud.InitialStatus
	}
	return ci.ConnectionStatus
}

func (ci *CostIntegration) RefreshStatus() cloud.ConnectionStatus {
	log.Warn("status refresh is not supported for the Huawei Cloud provider")
	return ci.ConnectionStatus
}

// huaweiResourceTypeCodePrefix prefixes every Huawei Cloud "Resource Type Code"
// (hws.resource.type.vm, hws.resource.type.rds.instance, ...).
const huaweiResourceTypeCodePrefix = "hws.resource.type."

// bssResourceIDFields is the number of colon-separated fields BSS packs into the
// RESOURCE_ID dimension: service type code, resource type code, resource ID and
// resource name, e.g.
//
//	hws.service.type.rds:hws.resource.type.rds.instance:57907bc4...in01:rds-mlops-mysql
const bssResourceIDFields = 4

// describeResource extracts what a billed resource is and what it is called
// from a BSS RESOURCE_ID. Returns empty strings for a value that isn't in the
// composite form, and for the fields BSS leaves empty (or reports as the string
// "null", as it does for the name of resources that have none). The raw value
// stays the resource's identifier either way.
func describeResource(resourceID string) (resourceType, name string) {
	fields := strings.Split(resourceID, ":")
	if len(fields) != bssResourceIDFields {
		return "", ""
	}

	resourceType = strings.TrimPrefix(fields[1], huaweiResourceTypeCodePrefix)
	name = fields[3]
	if name == "null" {
		name = ""
	}
	return resourceType, name
}

// dimensionValue looks up a dimension's value by key from a BSS DimensionGroup
// list (order is not guaranteed to match the request's groupby order).
func dimensionValue(dimensions *[]bssintlmodel.DimensionGroup, key string) string {
	if dimensions == nil {
		return ""
	}
	for _, d := range *dimensions {
		if d.Key != nil && *d.Key == key && d.Value != nil {
			return *d.Value
		}
	}
	return ""
}

// parseCostDay parses a BSS Cost.TimeDimensionValue ("yyyy-mm-dd" for daily
// granularity) into the start of that day. It falls back to the query's start time
// if the value is missing or unparsable, matching the graceful-degradation pattern
// other integrations (e.g. STACKIT's parsePeriod) use for malformed API dates.
func parseCostDay(timeDimensionValue *string, fallback time.Time) (time.Time, bool) {
	if timeDimensionValue == nil || *timeDimensionValue == "" {
		return fallback, true
	}
	t, err := time.Parse(bssDateLayout, *timeDimensionValue)
	if err != nil {
		log.Warnf("huawei cloud cost: unable to parse cost day %q, using window start: %v", *timeDimensionValue, err)
		return fallback, true
	}
	return t, true
}

func parseCostAmount(s *string) (float64, error) {
	if s == nil || *s == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(*s, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse float %q: %w", *s, err)
	}
	return f, nil
}

// selectHuaweiCategory maps a BSS CLOUD_SERVICE_TYPE dimension value (an
// English-language service name, since GetCloudCost requests X-Language: en_us)
// to an OpenCost asset category. The service catalogue itself lives in
// core/pkg/opencost, next to the AssetType each service is reported as, so that
// a service is described in exactly one place.
func selectHuaweiCategory(serviceType string) string {
	return opencost.HuaweiServiceCategory(serviceType)
}
