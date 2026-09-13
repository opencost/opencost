package ibm

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/platform-services-go-sdk/usagereportsv4"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
)

// Field-mapping contract for IBM CloudCost (shared with Cloudability CAC path):
//
//	Provider          = "IBM"
//	ProviderID        = ResourceInstanceID from Usage Reports (typically a full CRN)
//	AccountID         = account_id normalized to bare 32-hex (strip leading "a/" if present)
//	InvoiceEntityID   = AccountID (no payer/enterprise column in row data)
//	Service           = ResourceID (stable service id — not the display name)
//	Category          = selectIBMCategory(ResourceID) only — pure function of service id
//	UsageType         = N/A on CloudCostProperties in this tree (and must remain unset if added later)
//	ListCost / AmortizedCost           = sum(rated_cost) converted to USD, prorated
//	NetCost / AmortizedNetCost / InvoicedCost = sum(cost) converted to USD, prorated
//
// ResourceName (when names=true) is stored as label "ibm_resource_name", not Service.
// Daily values are synthetic: report totals ÷ covered days (full month, or MTD day-of-month),
// emitted for days overlapping the query window. Non-billable instances are skipped.

// UsageIntegration ingests IBM Cloud Usage Reports into CloudCost.
type UsageIntegration struct {
	UsageConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (ui *UsageIntegration) GetCloudCost(start, end time.Time) (*opencost.CloudCostSetRange, error) {
	return ui.getCloudCost(start, end, time.Now().UTC())
}

func (ui *UsageIntegration) getCloudCost(start, end, asOf time.Time) (*opencost.CloudCostSetRange, error) {
	client, err := ui.GetUsageReportsClient()
	if err != nil {
		ui.ConnectionStatus = cloud.FailedConnection
		return nil, fmt.Errorf("getting IBM usage reports client: %w", err)
	}

	// Cover whole months: cloudCostsFromInstance prorates a month total across every covered day,
	// so all of those days have to be rewritten together for the stored month to equal IBM's total.
	rangeStart, rangeEnd := monthRangeCovering(start, end)
	ccsr, err := opencost.NewCloudCostSetRange(rangeStart, rangeEnd, opencost.AccumulateOptionDay, ui.Key())
	if err != nil {
		return nil, err
	}

	months := monthsOverlapping(start, end)
	itemsSeen := 0
	for _, month := range months {
		options := client.NewGetResourceUsageAccountOptions(normalizeAccountID(ui.AccountID), month)
		options.SetLimit(200)
		// names=true populates ResourceName for the ibm_resource_name label only;
		// Service always keys on ResourceID for CAC / billing-export agreement.
		options.SetNames(true)
		options.SetTags(true)

		pager, err := client.NewGetResourceUsageAccountPager(options)
		if err != nil {
			ui.ConnectionStatus = cloud.FailedConnection
			return nil, fmt.Errorf("creating usage pager for %s: %w", month, err)
		}

		for pager.HasNext() {
			page, err := pager.GetNext()
			if err != nil {
				ui.ConnectionStatus = cloud.FailedConnection
				return nil, fmt.Errorf("querying IBM resource usage for %s: %w", month, err)
			}
			for _, item := range page {
				itemsSeen++
				record, ok := instanceUsageFromSDK(item)
				if !ok {
					continue
				}
				for _, cc := range cloudCostsFromInstance(record, start, end, asOf) {
					ccsr.LoadCloudCost(cc)
				}
			}
		}
	}

	if itemsSeen == 0 && ui.ConnectionStatus != cloud.SuccessfulConnection {
		ui.ConnectionStatus = cloud.MissingData
		return ccsr, nil
	}

	ui.ConnectionStatus = cloud.SuccessfulConnection
	return ccsr, nil
}

func (ui *UsageIntegration) GetStatus() cloud.ConnectionStatus {
	if ui.ConnectionStatus.String() == "" {
		ui.ConnectionStatus = cloud.InitialStatus
	}
	return ui.ConnectionStatus
}

func (ui *UsageIntegration) RefreshStatus() cloud.ConnectionStatus {
	log.Warn("status refresh is not supported for the IBM Cloud provider")
	return ui.ConnectionStatus
}

// instanceUsageRecord is a testable projection of Usage Reports instance usage.
// Costs are stored after conversion to USD.
type instanceUsageRecord struct {
	AccountID          string
	ResourceInstanceID string
	ResourceID         string
	ResourceName       string
	Region             string
	Month              string
	Cost               float64
	RatedCost          float64
	Tags               []any
}

// instanceUsageFromSDK projects an SDK row. ok is false when the row should be skipped
// (explicitly non-billable).
func instanceUsageFromSDK(item usagereportsv4.InstanceUsage) (instanceUsageRecord, bool) {
	if item.Billable != nil && !*item.Billable {
		return instanceUsageRecord{}, false
	}

	record := instanceUsageRecord{
		Tags: mergeTagSlices(item.Tags, item.ServiceTags),
	}
	if item.AccountID != nil {
		record.AccountID = normalizeAccountID(*item.AccountID)
	}
	if item.ResourceInstanceID != nil {
		record.ResourceInstanceID = *item.ResourceInstanceID
	}
	if item.ResourceID != nil {
		record.ResourceID = *item.ResourceID
	}
	if item.ResourceName != nil {
		record.ResourceName = *item.ResourceName
	}
	if item.Region != nil {
		record.Region = *item.Region
	}
	if item.Month != nil {
		record.Month = *item.Month
	}

	rate := 1.0
	if item.CurrencyRate != nil && *item.CurrencyRate > 0 {
		rate = *item.CurrencyRate
	}

	for _, metric := range item.Usage {
		if metric.NonChargeable != nil && *metric.NonChargeable {
			continue
		}
		if metric.Cost != nil {
			record.Cost += *metric.Cost * rate
		}
		if metric.RatedCost != nil {
			record.RatedCost += *metric.RatedCost * rate
		}
	}
	return record, true
}

func mergeTagSlices(parts ...[]any) []any {
	var out []any
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

func cloudCostsFromInstance(item instanceUsageRecord, start, end, asOf time.Time) []*opencost.CloudCost {
	if item.Month == "" || (item.Cost == 0 && item.RatedCost == 0) {
		return nil
	}
	monthStart, err := time.Parse("2006-01", item.Month)
	if err != nil {
		return nil
	}
	monthStart = time.Date(monthStart.Year(), monthStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	days := prorationDays(monthStart, asOf)
	if days <= 0 {
		return nil
	}

	dailyNet := item.Cost / float64(days)
	dailyList := item.RatedCost / float64(days)

	labels := parseTags(item.Tags)
	if item.ResourceName != "" {
		labels["ibm_resource_name"] = item.ResourceName
	}

	properties := &opencost.CloudCostProperties{
		ProviderID: item.ResourceInstanceID,
		Provider:   opencost.IBMProvider,
		AccountID:  item.AccountID,
		// IBM billing data carries no account display name. The billing-export producer collapses
		// both names onto the account id; match it exactly or the two split rows on aggregation.
		AccountName:       item.AccountID,
		InvoiceEntityID:   item.AccountID,
		InvoiceEntityName: item.AccountID,
		RegionID:          item.Region,
		Service:           item.ResourceID,
		Category:          selectIBMCategory(item.ResourceID),
		Labels:            labels,
	}

	k8sPct := 0.0
	if isKubernetesResource(item.ResourceID, item.ResourceInstanceID) {
		k8sPct = 1.0
	}

	var costs []*opencost.CloudCost
	// Every day the total was divided across must be emitted, not just the days inside the caller's
	// window. The repository replaces whole day-sets, so a partial rewrite leaves the rest of the
	// month holding a rate computed at a different asOf and the stored month sums to neither total.
	// getCloudCost widens its range to the months covered here so all of these days are persisted.
	for d := 0; d < days; d++ {
		dayStart := monthStart.AddDate(0, 0, d)
		dayEnd := dayStart.AddDate(0, 0, 1)
		ds := dayStart
		de := dayEnd
		costs = append(costs, &opencost.CloudCost{
			Properties: properties,
			Window:     opencost.NewWindow(&ds, &de),
			ListCost: opencost.CostMetric{
				Cost:              dailyList,
				KubernetesPercent: k8sPct,
			},
			NetCost: opencost.CostMetric{
				Cost:              dailyNet,
				KubernetesPercent: k8sPct,
			},
			AmortizedNetCost: opencost.CostMetric{
				Cost:              dailyNet,
				KubernetesPercent: k8sPct,
			},
			AmortizedCost: opencost.CostMetric{
				Cost:              dailyList,
				KubernetesPercent: k8sPct,
			},
			InvoicedCost: opencost.CostMetric{
				Cost:              dailyNet,
				KubernetesPercent: k8sPct,
			},
		})
	}
	return costs
}

// kubernetesServiceID is IBM's service identifier for both IKS and ROKS clusters.
const kubernetesServiceID = "containers-kubernetes"

// isKubernetesResource reports whether a usage row belongs to an IKS or ROKS cluster, by service
// identifier or by the service segment of the resource CRN. Mirrors the billing-export producer so
// both paths mark the same rows as Kubernetes.
func isKubernetesResource(serviceID, providerID string) bool {
	if strings.EqualFold(strings.TrimSpace(serviceID), kubernetesServiceID) {
		return true
	}
	return strings.Contains(strings.ToLower(providerID), ":"+kubernetesServiceID+":")
}

// prorationDays returns the divisor for spreading a monthly (or MTD) report total.
// Completed months use calendar days; the asOf month uses day-of-month (MTD).
func prorationDays(monthStart, asOf time.Time) int {
	asOf = asOf.UTC()
	monthStart = monthStart.UTC()
	full := daysInMonth(monthStart.Year(), int(monthStart.Month()))
	if asOf.Year() == monthStart.Year() && asOf.Month() == monthStart.Month() {
		if asOf.Day() < 1 {
			return full
		}
		if asOf.Day() < full {
			return asOf.Day()
		}
	}
	return full
}

// monthRangeCovering returns the half-open range spanning every whole month that [start, end)
// touches. It mirrors monthsOverlapping's exclusive-end convention: an end landing exactly on a
// month boundary does not pull in that month.
func monthRangeCovering(start, end time.Time) (time.Time, time.Time) {
	rangeStart := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	lastMonth := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	if end.Equal(lastMonth) {
		lastMonth = lastMonth.AddDate(0, -1, 0)
	}
	rangeEnd := lastMonth.AddDate(0, 1, 0)
	if !rangeStart.Before(rangeEnd) {
		return start, end
	}
	return rangeStart, rangeEnd
}

func monthsOverlapping(start, end time.Time) []string {
	if !start.Before(end) {
		return nil
	}
	cursor := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	// end is exclusive; if end is exactly month start, previous month is last needed
	if end.Equal(last) {
		last = last.AddDate(0, -1, 0)
	}
	var months []string
	for !cursor.After(last) {
		months = append(months, cursor.Format("2006-01"))
		cursor = cursor.AddDate(0, 1, 0)
	}
	return months
}

func daysInMonth(year, month int) int {
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	return start.AddDate(0, 1, -1).Day()
}

func parseTags(raw []any) opencost.CloudCostLabels {
	labels := opencost.CloudCostLabels{}
	for _, tag := range raw {
		switch v := tag.(type) {
		case string:
			key, value, ok := splitTagString(v)
			if !ok {
				continue
			}
			labels[key] = value
		case map[string]any:
			key, _ := v["key"].(string)
			if key == "" {
				key, _ = v["Key"].(string)
			}
			value, _ := v["value"].(string)
			if value == "" {
				value, _ = v["Value"].(string)
			}
			if key == "" || value == "" {
				continue
			}
			labels[key] = value
		}
	}
	return labels
}

func splitTagString(tag string) (string, string, bool) {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return "", "", false
	}
	key, value, found := strings.Cut(tag, ":")
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if !found || key == "" || value == "" {
		return "", "", false
	}
	return key, value, true
}

// normalizeAccountID returns the bare IBM account GUID. Billing exports and CRNs
// may carry "a/<hex>"; CloudCost AccountID must not contain "/" (aggregation key
// and storage path). Published Usage Reports samples are already bare hex.
func normalizeAccountID(accountID string) string {
	accountID = strings.TrimSpace(accountID)
	return strings.TrimPrefix(accountID, "a/")
}

// selectIBMCategory maps IBM Usage Reports resource_id (service id) to an OpenCost category.
// Pure function of resourceID. Exact matches only, except the documented databases-for-* family.
func selectIBMCategory(resourceID string) string {
	id := strings.ToLower(strings.TrimSpace(resourceID))

	switch id {
	case "is.instance",
		"is.bare-metal-server",
		"is.dedicated-host",
		"codeengine",
		"containers-kubernetes":
		return opencost.ComputeCategory
	case "is.volume",
		"is.snapshot",
		"is.share",
		"cloud-object-storage":
		return opencost.StorageCategory
	case "is.load-balancer",
		"is.floating-ip",
		"is.public-gateway",
		"is.vpn",
		"transit",
		"internet-svcs":
		return opencost.NetworkCategory
	}

	// Prefix family (shared with billing-export / CAC): all IBM Databases for X services.
	if strings.HasPrefix(id, "databases-for-") {
		return opencost.StorageCategory
	}
	return opencost.OtherCategory
}
