package huawei

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
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

	// The ingestor's build and run goroutines call GetCloudCost while the
	// status endpoint calls GetStatus, so the status is only reached through
	// statusLock (see setStatus).
	statusLock sync.Mutex
	status     cloud.ConnectionStatus
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

	// The bill is account-wide and BSS authenticates with the account's
	// credentials, so no project is resolved here: costs outside a known
	// Enterprise Project are attributed to the account itself.
	accountKey := ci.Key()

	rows, err := fetchCostAnalysedBills(beginTime, endTime, "ORIGINAL_COST", "NET_AMOUNT")
	if err != nil {
		ci.setStatus(cloud.FailedConnection)
		return nil, fmt.Errorf("getting huawei cloud cost data: %w", err)
	}

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, ci.Key())
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		ci.updateStatus(func(status cloud.ConnectionStatus) cloud.ConnectionStatus {
			if status != cloud.SuccessfulConnection {
				return cloud.MissingData
			}
			return status
		})
		return ccsr, nil
	}

	// Without Enterprise Projects the bill cannot be split by platform, but it
	// is still correct, so a failure here is not fatal.
	enterpriseProjects, err := fetchResourceEnterpriseProjects(beginTime, endTime)
	if err != nil {
		log.Warnf("huawei cloud cost: reporting costs without Enterprise Projects, the lookup failed: %v", err)
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
		resource := describeResource(resourceID)

		// The service code comes from a composite RESOURCE_ID; a bare one
		// carries none, so the CLOUD_SERVICE_TYPE dimension (a code or an
		// English name) identifies the service instead.
		service := serviceCodeOf(resource.ID)
		if service == "" {
			service = serviceType
		}
		priceFactor := ci.priceFactor(service, resource.Type)

		// BSS reports the service as a code or as an English name; both are
		// reported as the code and its display name, so one service is not
		// split in two by the form a row happens to use. An unrecognized
		// name is kept as is.
		serviceCode, serviceName := serviceTypeCode(serviceType), serviceType
		if serviceCode != "" {
			serviceName = serviceDisplayName(serviceCode)
		} else {
			serviceCode = serviceType
		}

		// The properties depend on the Enterprise Project, which is per day
		// (see resourceEnterpriseProjects): built once per Enterprise Project.
		propsByEP := map[string]*opencost.CloudCostProperties{}
		propertiesFor := func(epID string, known bool) *opencost.CloudCostProperties {
			if p, ok := propsByEP[epID]; ok {
				return p
			}
			labels := opencost.CloudCostLabels{}
			if serviceCode != "" {
				labels[ServiceCodeLabel] = serviceCode
			}
			if resource.Type != "" {
				labels[ResourceTypeLabel] = resource.Type
			}
			if resource.Name != "" {
				labels[ResourceNameLabel] = resource.Name
			}
			// The OpenCost UI breaks cloud costs down by a fixed set of
			// properties, not by label: the Enterprise Project goes in as the
			// account and its platform as the invoice entity, so "Account" and
			// "Invoice Entity" in the UI split the bill by Enterprise Project
			// and by platform.
			accountID, accountName, invoiceEntity := accountKey, "", ""
			if known {
				ep := ci.enterpriseProject(epID)
				labels[EnterpriseProjectIDLabel] = ep.ID
				labels[EnterpriseProjectLabel] = ep.Name
				accountID, accountName = ep.Name, ep.Name
				if ep.Platform != "" {
					labels[PlatformLabel] = ep.Platform
					invoiceEntity = ep.Platform
				}
			}
			p := &opencost.CloudCostProperties{
				ProviderID:        resource.ID,
				Provider:          opencost.HuaweiProvider,
				AccountID:         accountID,
				AccountName:       accountName,
				InvoiceEntityID:   invoiceEntity,
				InvoiceEntityName: invoiceEntity,
				RegionID:          region,
				Service:           serviceName,
				Category:          selectHuaweiCategory(serviceType),
				Labels:            labels,
			}
			propsByEP[epID] = p
			return p
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

			day := ""
			if item.TimeDimensionValue != nil {
				day = *item.TimeDimensionValue
			}
			// A day billed to several Enterprise Projects is split across
			// them, one CloudCost each (see resourceEnterpriseProjects.split).
			// The price factor scales each project's net amount.
			costs := enterpriseProjects.split(resource.ID, day, netAmount, listAmount)
			if len(costs) == 0 {
				net := netAmount * priceFactor
				ccsr.LoadCloudCost(opencost.NewCloudCost(winStart, winEnd, propertiesFor("", false), 0,
					listAmount, net, net, net, listAmount))
				continue
			}
			for _, ep := range costs {
				net := ep.net * priceFactor
				ccsr.LoadCloudCost(opencost.NewCloudCost(winStart, winEnd, propertiesFor(ep.id, true), 0,
					ep.list, net, net, net, ep.list))
			}
		}
	}

	ci.setStatus(cloud.SuccessfulConnection)
	return ccsr, nil
}

func (ci *CostIntegration) GetStatus() cloud.ConnectionStatus {
	return ci.updateStatus(func(status cloud.ConnectionStatus) cloud.ConnectionStatus {
		if status.String() == "" {
			return cloud.InitialStatus
		}
		return status
	})
}

func (ci *CostIntegration) RefreshStatus() cloud.ConnectionStatus {
	log.Warn("status refresh is not supported for the Huawei Cloud provider")
	return ci.GetStatus()
}

func (ci *CostIntegration) setStatus(status cloud.ConnectionStatus) {
	ci.updateStatus(func(cloud.ConnectionStatus) cloud.ConnectionStatus { return status })
}

// updateStatus replaces the status with update(current) under statusLock, so
// a read-then-write such as "MissingData unless already successful" is atomic,
// and returns the new status.
func (ci *CostIntegration) updateStatus(update func(cloud.ConnectionStatus) cloud.ConnectionStatus) cloud.ConnectionStatus {
	ci.statusLock.Lock()
	defer ci.statusLock.Unlock()
	ci.status = update(ci.status)
	return ci.status
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

// bssNullField is the placeholder BSS puts in a composite RESOURCE_ID field it
// has no value for. It also, inconsistently and for the same resource, just
// leaves the field empty: an unnamed bucket arrives both as
// "...:obs-mlops-build-29074b:" and "...:obs-mlops-build-29074b:null", which
// unless normalized spells one resource as two assets splitting its cost.
const bssNullField = "null"

// bssResource is what a composite RESOURCE_ID says about a billed resource.
type bssResource struct {
	// ID is the RESOURCE_ID with its absent fields spelled consistently, so
	// that the spellings of one resource agree on one identifier.
	ID string

	// Type is the resource type code without its prefix: "vm", "volume",
	// "rds.instance". Empty when BSS reports none.
	Type string

	// Name is what the resource is called in the console, falling back to its
	// ID within the service -- a bucket name, a log stream ID -- for the
	// resources BSS reports no name for. Empty when it reports neither.
	Name string
}

// describeResource reads a BSS RESOURCE_ID, which is not a bare ID but
// "<service code>:<resource type code>:<resource id>:<resource name>", e.g.
//
//	hws.service.type.rds:hws.resource.type.rds.instance:57907bc4...in01:rds-mlops-mysql
//
// A value not in that form is passed through as the resource's ID, undescribed.
func describeResource(resourceID string) bssResource {
	fields := strings.Split(resourceID, ":")
	if len(fields) != bssResourceIDFields {
		return bssResource{ID: resourceID}
	}

	for i, field := range fields {
		if field == bssNullField {
			fields[i] = ""
		}
	}

	name := fields[3]
	if name == "" {
		name = fields[2]
	}

	return bssResource{
		ID:   strings.Join(fields, ":"),
		Type: strings.TrimPrefix(fields[1], huaweiResourceTypeCodePrefix),
		Name: name,
	}
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
