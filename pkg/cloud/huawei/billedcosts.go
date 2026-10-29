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
	mu         sync.Mutex
	nextFetch  time.Time
	hourly     map[string]float64
	refreshing bool

	// refreshes tracks the background refresh, so tests can wait for it.
	refreshes sync.WaitGroup
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
// resource, as cached. When the cache is due, it is refreshed in the
// background: ClusterManagementPricing runs on the metrics update path, which
// must not wait on paginated BSS queries. Until the first refresh lands, the
// resource has no cost, as before this was implemented.
func (h *Huawei) billedHourlyCost(resourceID string) (float64, bool) {
	c := &h.bills
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC()
	if !c.refreshing && !now.Before(c.nextFetch) {
		c.refreshing = true
		c.refreshes.Add(1)
		go c.refresh(now)
	}

	cost, ok := c.hourly[strings.ToLower(strings.TrimSpace(resourceID))]
	return cost, ok
}

// refresh reads the billing history and updates the cache. Only one runs at
// a time (see billedHourlyCost); the read itself is outside the lock.
func (c *billCache) refresh(now time.Time) {
	defer c.refreshes.Done()
	hourly, err := fetchBilledHourlyCosts(now)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshing = false
	switch {
	case err != nil:
		log.Warnf("huawei cloud: reading billing history failed, keeping the last known CCE cost: %v", err)
		c.nextFetch = now.Add(billRetryInterval)
	case len(hourly) == 0 && len(c.hourly) > 0:
		// A successful read with nothing in it is more likely a gap in BSS's
		// data than every tracked resource vanishing: keep the last known
		// costs and read again soon.
		log.Warnf("huawei cloud: billing history came back empty, keeping the last known CCE cost")
		c.nextFetch = now.Add(billRetryInterval)
	default:
		c.hourly = hourly
		c.nextFetch = now.Add(billRefreshInterval)
	}
}

// fetchBilledHourlyCosts reads the last billLookbackDays complete days of
// billing and returns the average hourly cost of every resource of the
// billedHourlyServices in it.
func fetchBilledHourlyCosts(now time.Time) (map[string]float64, error) {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	begin := today.AddDate(0, 0, -billLookbackDays).Format(bssDateLayout)
	// BSS's end_time is inclusive: yesterday is the last complete day.
	end := today.AddDate(0, 0, -1).Format(bssDateLayout)

	// Only the billedHourlyServices are read, filtered on the BSS side: the
	// account's whole bill can run to many pages. A composite RESOURCE_ID
	// carries its service code; a bare one does not, so the service is
	// requested as a dimension too.
	onlyBilledServices := bssintlmodel.FilterV2{
		Operator:     0, // include
		FilterFactor: &bssintlmodel.FilterFactor{Key: "CLOUD_SERVICE_TYPE", Value: billedHourlyServices},
	}
	// AMORTIZED_COST spreads a prepaid (yearly/monthly) cluster's payment
	// over the days it covers; ORIGINAL_COST would put it all on the day it
	// was paid, pricing that week at many times the real hourly rate and the
	// following weeks at nothing. Pay-per-use costs are the same either way.
	rows, err := fetchCostAnalysedBillsBy([]string{"RESOURCE_ID", "CLOUD_SERVICE_TYPE"}, begin, end, "AMORTIZED_COST", "NET_AMOUNT", onlyBilledServices)
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
		id, ok := billedResourceID(dimensionValue(row.Dimensions, "RESOURCE_ID"), dimensionValue(row.Dimensions, "CLOUD_SERVICE_TYPE"))
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

// billedResourceID returns the lower-cased ID of the resource a BSS row
// refers to, if it is a resource of the billedHourlyServices. A composite
// RESOURCE_ID carries both the service code and the resource ID; a bare one
// is the resource ID itself, and serviceType -- the CLOUD_SERVICE_TYPE
// dimension, as a code or an English name -- names the service.
func billedResourceID(resourceID, serviceType string) (string, bool) {
	id, service := resourceID, serviceType
	if fields := strings.Split(resourceID, ":"); len(fields) == bssResourceIDFields {
		id, service = fields[2], fields[0]
	}
	tracked := false
	for _, svc := range billedHourlyServices {
		if canonicalServiceCode(service) == canonicalServiceCode(svc) {
			tracked = true
			break
		}
	}
	id = strings.ToLower(strings.TrimSpace(id))
	if !tracked || id == "" || id == bssNullField {
		return "", false
	}
	return id, true
}
