package huawei

import (
	"sort"
	"strings"
	"sync"
	"time"

	bssintlmodel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/bssintl/v2/model"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
)

// cceCloudServiceType is the Service Type Code CCE clusters are billed under.
const cceCloudServiceType = "hws.service.type.cce"

// cceProvisionerName is what ClusterManagementPricing reports as the
// provisioner of a CCE cluster.
const cceProvisionerName = "CCE"

// billedHourlyServices are the services whose resources this provider prices
// from what BSS actually billed rather than from a list price: the CCE
// cluster itself, which has no on-demand list price to query.
var billedHourlyServices = []string{cceCloudServiceType}

const (
	// billLookbackDays is how many complete days of billing history are
	// averaged into a resource's hourly price. A week smooths the day-to-day
	// swing of usage-billed resources without lagging far behind a change.
	billLookbackDays = 7

	// billRefreshInterval bounds how often the billing history is re-read.
	// BSS publishes costs daily, so re-reading more often than a few times a
	// day only spends API quota.
	billRefreshInterval = 6 * time.Hour

	// billRetryInterval is how soon a failed read is retried.
	billRetryInterval = 30 * time.Minute
)

// billCache holds the average hourly cost BSS billed for each resource of the
// billedHourlyServices, keyed by lower-cased resource ID.
type billCache struct {
	mu        sync.Mutex
	nextFetch time.Time
	hourly    map[string]float64
}

// ClusterManagementPricing prices the CCE cluster itself (its control plane)
// at what BSS billed for it, averaged per hour over the last billLookbackDays
// complete days. The cluster is identified by HUAWEICLOUD_CCE_CLUSTER_ID; without it, or
// until the cluster has a complete day of billing history, the cost is 0, as
// it was before this was implemented.
func (h *Huawei) ClusterManagementPricing() (string, float64, error) {
	clusterID := env.GetHuaweiCCEClusterID()
	if clusterID == "" {
		return "", 0.0, nil
	}
	cost, _ := h.billedHourlyCost(clusterID)
	return cceProvisionerName, cost, nil
}

// billedHourlyCost returns the average hourly cost BSS billed for the
// resource, refreshing the cache when it is due.
func (h *Huawei) billedHourlyCost(resourceID string) (float64, bool) {
	c := &h.bills
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC()
	if !now.Before(c.nextFetch) {
		hourly, err := fetchBilledHourlyCosts(now)
		if err != nil {
			log.Warnf("huawei cloud: reading billing history failed, the CCE cluster is priced at 0: %v", err)
			c.nextFetch = now.Add(billRetryInterval)
		} else {
			c.hourly = hourly
			c.nextFetch = now.Add(billRefreshInterval)
		}
	}

	cost, ok := c.hourly[strings.ToLower(strings.TrimSpace(resourceID))]
	return cost, ok
}

// fetchBilledHourlyCosts reads the last billLookbackDays complete days of
// billing and returns the average hourly cost of every resource of the
// billedHourlyServices in it.
func fetchBilledHourlyCosts(now time.Time) (map[string]float64, error) {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	begin := today.AddDate(0, 0, -billLookbackDays).Format(bssDateLayout)
	// BSS's end_time is inclusive: yesterday is the last complete day.
	end := today.AddDate(0, 0, -1).Format(bssDateLayout)

	// RESOURCE_ID alone is enough: the service code is packed into it.
	rows, err := fetchCostAnalysedBillsBy([]string{"RESOURCE_ID"}, begin, end, "ORIGINAL_COST", "NET_AMOUNT")
	if err != nil {
		return nil, err
	}
	return billedHourlyCosts(rows), nil
}

// billedHourlyCosts averages the daily costs in rows into an hourly cost per
// resource of the billedHourlyServices.
//
// Only the days a resource was billed count towards its average, so one
// created mid-window is not diluted by the days before it existed. Its first
// billed day is dropped when there are others, since that day is most likely
// partial and would drag the average down.
func billedHourlyCosts(rows []bssintlmodel.CostDataByDimension) map[string]float64 {
	daily := make(map[string]map[string]float64) // resource ID -> day -> cost
	for _, row := range rows {
		id, ok := billedResourceID(dimensionValue(row.Dimensions, "RESOURCE_ID"))
		if !ok || row.Costs == nil {
			continue
		}
		for _, item := range *row.Costs {
			if item.TimeDimensionValue == nil || *item.TimeDimensionValue == "" {
				continue
			}
			amount, err := parseCostAmount(item.Amount)
			if err != nil {
				log.Warnf("huawei cloud: skipping unparsable cost for %s: %v", id, err)
				continue
			}
			if daily[id] == nil {
				daily[id] = make(map[string]float64)
			}
			daily[id][*item.TimeDimensionValue] += amount
		}
	}

	hourly := make(map[string]float64, len(daily))
	for id, byDay := range daily {
		days := make([]string, 0, len(byDay))
		for day := range byDay {
			days = append(days, day)
		}
		sort.Strings(days)
		if len(days) > 1 {
			days = days[1:]
		}
		total := 0.0
		for _, day := range days {
			total += byDay[day]
		}
		hourly[id] = total / float64(len(days)*24)
	}
	return hourly
}

// billedResourceID returns the lower-cased resource ID a composite BSS
// RESOURCE_ID refers to, if it is a resource of the billedHourlyServices.
func billedResourceID(resourceID string) (string, bool) {
	fields := strings.Split(resourceID, ":")
	if len(fields) != bssResourceIDFields {
		return "", false
	}
	tracked := false
	for _, svc := range billedHourlyServices {
		if fields[0] == svc {
			tracked = true
			break
		}
	}
	id := strings.ToLower(strings.TrimSpace(fields[2]))
	if !tracked || id == "" || id == bssNullField {
		return "", false
	}
	return id, true
}
