package aws

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

// curNodePricingCache holds the effective hourly $/hr cost per EC2 instance ID
// as retrieved from the AWS Cost and Usage Report (CUR) via Athena.
type curNodePricingCache struct {
	mu          sync.RWMutex
	rates       map[string]float64 // instanceID -> effective $/hr
	lastRefresh time.Time
	refreshing  bool // guards against concurrent background refreshes
}

// curPricingCache is embedded in the AWS provider struct via the pointer below;
// it is allocated lazily on first use so that nil-initialised AWS structs are safe.
//
// NOTE: The AWS struct is defined in provider.go which we do not modify. Instead,
// we attach the cache to a package-level map keyed on the provider pointer. This
// avoids touching the struct definition while keeping state per provider instance.
var (
	curCacheMu sync.Mutex
	curCaches  = map[*AWS]*curNodePricingCache{}
)

// getCURCache returns (creating if needed) the curNodePricingCache for this provider.
func getCURCache(a *AWS) *curNodePricingCache {
	curCacheMu.Lock()
	defer curCacheMu.Unlock()
	if c, ok := curCaches[a]; ok {
		return c
	}
	c := &curNodePricingCache{rates: make(map[string]float64)}
	curCaches[a] = c
	return c
}

// instanceIDFromProviderID extracts the EC2 instance ID from a Kubernetes
// provider ID of the form aws:///<az>/i-xxxx or bare i-xxxx.
func instanceIDFromProviderID(providerID string) string {
	// Fast path: already bare instance ID.
	if strings.HasPrefix(providerID, "i-") {
		return providerID
	}
	// Typical form: aws:///us-east-2a/i-0fea4fd46592d050b
	matches := provIdRx.FindStringSubmatch(providerID)
	if len(matches) == 3 {
		return matches[2]
	}
	return ""
}

// queryCUREffectiveRates executes the CUR Athena query and populates the provided
// map with instanceID -> effective hourly cost. It is called from a background
// goroutine; errors are logged but do not propagate.
func (a *AWS) queryCUREffectiveRates(rates map[string]float64) error {
	athenaInfo, err := a.GetAWSAthenaInfo()
	if err != nil {
		return fmt.Errorf("queryCUREffectiveRates: GetAWSAthenaInfo: %w", err)
	}
	if athenaInfo.AthenaDatabase == "" || athenaInfo.AthenaTable == "" ||
		athenaInfo.AthenaRegion == "" || athenaInfo.AthenaBucketName == "" {
		return fmt.Errorf("queryCUREffectiveRates: Athena configuration incomplete")
	}

	// This query computes the effective $/hr for each EC2 instance over the last 2 days.
	// For Savings Plan covered hours we use savings_plan_savings_plan_effective_cost,
	// for Reserved Instance hours we use reservation_effective_cost, and for on-demand /
	// spot hours we use line_item_unblended_cost. The denominator (covered hours)
	// depends on the CUR export granularity — see curHoursDenominator.
	q := `SELECT
	line_item_resource_id,
	sum(CASE line_item_line_item_type
		WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost
		WHEN 'DiscountedUsage' THEN reservation_effective_cost
		ELSE line_item_unblended_cost
	END) / nullif(%s, 0) AS hourly
FROM %s
WHERE line_item_product_code = 'AmazonEC2'
  AND (line_item_usage_type LIKE '%%BoxUsage%%' OR line_item_usage_type LIKE '%%SpotUsage%%')
  AND line_item_resource_id LIKE 'i-%%'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage')
  AND line_item_usage_start_date >= now() - interval '2' day
GROUP BY 1`

	granularity := env.GetCURNodePricingGranularity()
	query := fmt.Sprintf(q, curHoursDenominator(granularity), athenaInfo.AthenaTable)
	log.Debugf("CUR node pricing: granularity mode %q", granularity)
	log.Debugf("CUR node pricing: running Athena query against table %s", athenaInfo.AthenaTable)

	pageNum := 0
	err = a.QueryAthenaPaginated(context.Background(), query, func(page *athena.GetQueryResultsOutput) bool {
		if page == nil || page.ResultSet == nil {
			log.Errorf("queryCUREffectiveRates: nil page or ResultSet")
			return false
		}
		rows := page.ResultSet.Rows
		if pageNum == 0 && len(rows) > 0 {
			rows = rows[1:] // skip header
		}
		pageNum++
		for _, row := range rows {
			if len(row.Data) < 2 {
				continue
			}
			idPtr := row.Data[0].VarCharValue
			costPtr := row.Data[1].VarCharValue
			if idPtr == nil || costPtr == nil {
				continue
			}
			instanceID := *idPtr
			cost, err := strconv.ParseFloat(*costPtr, 64)
			if err != nil {
				log.Debugf("queryCUREffectiveRates: could not parse cost for %s: %v", instanceID, err)
				continue
			}
			rates[instanceID] = cost
		}
		return true
	})
	return err
}

// refreshCURCache performs a blocking CUR Athena query and, on success, atomically
// replaces the cache contents and updates lastRefresh.
func (a *AWS) refreshCURCache(cache *curNodePricingCache) {
	newRates := make(map[string]float64)
	if err := a.queryCUREffectiveRates(newRates); err != nil {
		log.Warnf("CUR node pricing: refresh failed: %v", err)
		// Leave existing cache intact.
	} else {
		cache.mu.Lock()
		cache.rates = newRates
		cache.lastRefresh = time.Now()
		cache.mu.Unlock()
		log.Infof("CUR node pricing: refreshed cache with %d instance rates", len(newRates))
	}

	// Clear the refreshing flag regardless of outcome so the next call can retry.
	cache.mu.Lock()
	cache.refreshing = false
	cache.mu.Unlock()
}

// triggerCURRefreshIfStale kicks off a single background goroutine to refresh the
// CUR cache when the cache is empty or older than the configured refresh interval.
// It is non-blocking: callers apply whatever is in the cache at the time of the call.
func (a *AWS) triggerCURRefreshIfStale(cache *curNodePricingCache) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	refreshInterval := env.GetCURNodePricingRefreshHours()
	stale := cache.lastRefresh.IsZero() || time.Since(cache.lastRefresh) >= refreshInterval
	if !stale || cache.refreshing {
		return
	}
	cache.refreshing = true
	go a.refreshCURCache(cache)
}

// ApplyReservedInstancePricing reconciles each node's pricing to the actual
// effective hourly cost (Savings Plans / Reserved Instances / spot) as reported
// by the AWS Cost and Usage Report (CUR) via Athena.
//
// Behaviour:
//   - No-op when CUR_NODE_PRICING_ENABLED is false (default) or when the Athena
//     configuration is not set.
//   - Non-blocking: if the cache is stale a background refresh is started; the
//     current (possibly empty) cache is applied immediately.
//   - CPU/RAM cost ratio is preserved: the effective rate is distributed across
//     VCPUCost and RAMCost proportionally to their current values, and Cost is
//     updated to the new total. If VCPUCost and RAMCost are both zero or
//     unset, the full effective rate is placed on VCPUCost.
func (a *AWS) ApplyReservedInstancePricing(nodes map[string]*models.Node) {
	if !env.IsCURNodePricingEnabled() {
		return
	}

	// Gate on Athena being configured (check cheaply using cached config).
	cfg, err := a.GetConfig()
	if err != nil || cfg.AthenaBucketName == "" || cfg.AthenaTable == "" {
		return
	}

	cache := getCURCache(a)
	a.triggerCURRefreshIfStale(cache)

	cache.mu.RLock()
	rates := cache.rates
	cache.mu.RUnlock()

	if len(rates) == 0 {
		// First call before the initial refresh completes — nothing to apply.
		return
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}
		instanceID := instanceIDFromProviderID(node.ProviderID)
		if instanceID == "" {
			continue
		}
		effectiveRate, ok := rates[instanceID]
		if !ok {
			continue
		}
		if err := applyEffectiveRate(node, effectiveRate); err != nil {
			log.Debugf("CUR node pricing: could not apply rate for %s: %v", instanceID, err)
			continue
		}
		node.PricingType = models.Reserved // covers SP/RI/spot-from-CUR equally well
	}
}

// curHoursDenominator returns the SQL expression for the number of covered hours
// per instance, according to the configured CUR export granularity.
//
//   - "auto" (default): derives hours from usage_start/usage_end per row — exact
//     for hourly, daily and mixed-granularity exports.
//   - "hourly": one row per instance-hour; distinct start timestamps == hours.
//   - "daily": one row per instance-day; distinct start timestamps x 24 == hours.
func curHoursDenominator(granularity string) string {
	switch granularity {
	case "hourly":
		return "count(distinct line_item_usage_start_date)"
	case "daily":
		return "count(distinct line_item_usage_start_date) * 24"
	default: // auto
		return "sum(greatest(1, date_diff('hour', line_item_usage_start_date, line_item_usage_end_date)))"
	}
}

// applyEffectiveRate reconciles the node's PER-UNIT cost fields to the actual
// effective total hourly rate.
//
// IMPORTANT: models.Node.VCPUCost is $/vCPU/hr and models.Node.RAMCost is
// $/GiB/hr (json names CPUHourlyCost / RAMGBHourlyCost). The effective rate is a
// node-TOTAL $/hr, so the per-unit fields must be divided by the node's vCPU
// count and RAM GiB respectively. (Writing totals into these fields inflates
// node cost by roughly vCPUs+GiB x — production incident 2026-06-07.)
//
// The node-total CPU:RAM cost ratio implied by the existing per-unit prices is
// preserved; GPU cost is treated as fixed and subtracted first.
func applyEffectiveRate(node *models.Node, effectiveRate float64) error {
	vcpus, _ := strconv.ParseFloat(node.VCPU, 64)
	ramGiB := 0.0
	if rb, err := strconv.ParseFloat(node.RAMBytes, 64); err == nil && rb > 0 {
		ramGiB = rb / (1024 * 1024 * 1024)
	} else if r, err := strconv.ParseFloat(node.RAM, 64); err == nil && r > 0 {
		ramGiB = r
	}
	if vcpus <= 0 && ramGiB <= 0 {
		return fmt.Errorf("node has no parsable vCPU or RAM capacity")
	}

	// GPU cost is fixed and not redistributed.
	var gpuTotal float64
	if gpuCost, err := strconv.ParseFloat(node.GPUCost, 64); err == nil {
		gpuCount, _ := strconv.ParseFloat(node.GPU, 64)
		gpuTotal = gpuCost * gpuCount
	}

	remainder := effectiveRate - gpuTotal
	if remainder < 0 {
		remainder = 0
	}

	// Node-total CPU and RAM cost implied by current per-unit prices.
	cpuUnit, cpuErr := strconv.ParseFloat(node.VCPUCost, 64)
	ramUnit, ramErr := strconv.ParseFloat(node.RAMCost, 64)
	cpuTotal, ramTotal := 0.0, 0.0
	if cpuErr == nil {
		cpuTotal = cpuUnit * vcpus
	}
	if ramErr == nil {
		ramTotal = ramUnit * ramGiB
	}

	cpuFraction := 0.5
	if total := cpuTotal + ramTotal; total > 0 {
		cpuFraction = cpuTotal / total
	} else if vcpus <= 0 {
		cpuFraction = 0
	} else if ramGiB <= 0 {
		cpuFraction = 1
	}
	ramFraction := 1 - cpuFraction

	if vcpus > 0 {
		node.VCPUCost = strconv.FormatFloat(remainder*cpuFraction/vcpus, 'f', -1, 64)
	} else {
		node.VCPUCost = "0"
	}
	if ramGiB > 0 {
		node.RAMCost = strconv.FormatFloat(remainder*ramFraction/ramGiB, 'f', -1, 64)
	} else {
		node.RAMCost = "0"
	}
	node.Cost = strconv.FormatFloat(effectiveRate, 'f', -1, 64)

	return nil
}
