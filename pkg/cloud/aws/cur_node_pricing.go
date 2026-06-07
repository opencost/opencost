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
	// spot hours we use line_item_unblended_cost. Dividing the sum by the distinct number
	// of hourly periods gives an average effective hourly rate.
	q := `SELECT
	line_item_resource_id,
	sum(CASE line_item_line_item_type
		WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost
		WHEN 'DiscountedUsage' THEN reservation_effective_cost
		ELSE line_item_unblended_cost
	END) / nullif(count(distinct line_item_usage_start_date), 0) AS hourly
FROM %s
WHERE line_item_product_code = 'AmazonEC2'
  AND (line_item_usage_type LIKE '%%BoxUsage%%' OR line_item_usage_type LIKE '%%SpotUsage%%')
  AND line_item_resource_id LIKE 'i-%%'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage')
  AND line_item_usage_start_date >= now() - interval '2' day
GROUP BY 1`

	query := fmt.Sprintf(q, athenaInfo.AthenaTable)
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

// applyEffectiveRate distributes effectiveRate across the node's cost fields while
// preserving the existing VCPUCost:RAMCost ratio. GPUCost is treated as fixed.
func applyEffectiveRate(node *models.Node, effectiveRate float64) error {
	cpuCost, cpuErr := strconv.ParseFloat(node.VCPUCost, 64)
	ramCost, ramErr := strconv.ParseFloat(node.RAMCost, 64)
	gpuCost, gpuErr := strconv.ParseFloat(node.GPUCost, 64)

	// GPU cost is fixed and not redistributed.
	var gpuTotal float64
	if gpuErr == nil {
		gpuCost, _ = strconv.ParseFloat(node.GPUCost, 64)
		gpuCount, _ := strconv.ParseFloat(node.GPU, 64)
		gpuTotal = gpuCost * gpuCount
	}

	remainder := effectiveRate - gpuTotal
	if remainder < 0 {
		remainder = 0
	}

	if cpuErr != nil || ramErr != nil {
		// Cannot parse existing costs — put everything on VCPUCost.
		node.VCPUCost = strconv.FormatFloat(remainder, 'f', -1, 64)
		node.RAMCost = "0"
		node.Cost = strconv.FormatFloat(effectiveRate, 'f', -1, 64)
		return nil
	}

	total := cpuCost + ramCost
	if total <= 0 {
		// No existing ratio — assign all remainder to VCPUCost.
		node.VCPUCost = strconv.FormatFloat(remainder, 'f', -1, 64)
		node.RAMCost = "0"
		node.Cost = strconv.FormatFloat(effectiveRate, 'f', -1, 64)
		return nil
	}

	cpuFraction := cpuCost / total
	ramFraction := ramCost / total

	node.VCPUCost = strconv.FormatFloat(remainder*cpuFraction, 'f', -1, 64)
	node.RAMCost = strconv.FormatFloat(remainder*ramFraction, 'f', -1, 64)
	node.Cost = strconv.FormatFloat(effectiveRate, 'f', -1, 64)

	_ = gpuCost // already accounted for in gpuTotal
	return nil
}
