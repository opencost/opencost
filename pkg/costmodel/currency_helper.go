package costmodel

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/currency"
)

// Currency conversion uses a two-layer approach:
//   - The handler-facing entry (Range / Set / top-level Allocation) calls
//     Converter.GetRate once to validate the target currency. If that
//     fails, the helper returns an error without mutating anything and
//     the caller logs a single per-request warning -- preventing log
//     floods when the API is unreachable or the currency code is bogus.
//   - Per-field conversion is best-effort: if an individual Convert call
//     fails after the rate probe succeeded (exotic case) the original
//     USD value is retained and logged. Responses may therefore contain
//     mixed currencies under partial converter failures.

// tryConvert converts val from USD to target, returning the original
// value (and logging) on converter error. Only fires after the outer
// helper has already validated the target currency, so this path is
// rarely hit in practice. logCtx identifies the field being converted.
func tryConvert(converter currency.Converter, val float64, target, logCtx string) float64 {
	if val == 0 {
		return val
	}
	converted, err := converter.Convert(val, "USD", target)
	if err != nil {
		log.Warnf("currency: leaving %s in USD (convert to %s failed): %v", logCtx, target, err)
		return val
	}
	return converted
}

// normalizeAndProbe normalizes the target currency code and, if it is
// not a no-op (USD or empty), probes the converter for the rate. Returns
// the normalized currency, a no-op flag, and any rate-lookup error.
func normalizeAndProbe(converter currency.Converter, target string) (normalized string, noop bool, err error) {
	normalized = strings.ToUpper(strings.TrimSpace(target))
	if normalized == "" || normalized == "USD" {
		return normalized, true, nil
	}
	if _, rateErr := converter.GetRate("USD", normalized); rateErr != nil {
		return normalized, false, fmt.Errorf("currency rate lookup USD->%s failed: %w", normalized, rateErr)
	}
	return normalized, false, nil
}

// ConvertAllocation converts all cost fields in an Allocation from USD
// to target currency in place. Returns an error if the target rate
// cannot be looked up (no mutation occurs); per-field converter failures
// encountered after the rate probe succeeded are handled best-effort and
// logged in place.
func ConvertAllocation(alloc *opencost.Allocation, converter currency.Converter, targetCurrency string) error {
	if alloc == nil || converter == nil {
		return nil
	}

	targetCurrency, noop, err := normalizeAndProbe(converter, targetCurrency)
	if err != nil {
		return err
	}
	if noop {
		return nil
	}

	return convertAllocationFields(alloc, converter, targetCurrency)
}

// convertAllocationFields performs the per-field best-effort conversion.
// The caller must have already normalised the target currency and
// probed the rate. Returns nil today; retains the error return so the
// signature can carry structured failure info in the future.
func convertAllocationFields(alloc *opencost.Allocation, converter currency.Converter, targetCurrency string) error {
	if alloc == nil {
		return nil
	}

	// Named cost fields. Keep in sync with Allocation cost fields in
	// core/pkg/opencost/allocation.go. Includes both JSON-serialised
	// costs and internal ones like UnmountedPVCost (json:"-") which are
	// used downstream in SummaryAllocation / Totals computations --
	// leaving those in USD would cause unit mismatches with the
	// converted primary costs.
	type namedCost struct {
		name string
		ptr  *float64
	}
	costFields := []namedCost{
		{"CPUCost", &alloc.CPUCost},
		{"CPUCostAdjustment", &alloc.CPUCostAdjustment},
		{"CPUCostIdle", &alloc.CPUCostIdle},
		{"GPUCost", &alloc.GPUCost},
		{"GPUCostAdjustment", &alloc.GPUCostAdjustment},
		{"GPUCostIdle", &alloc.GPUCostIdle},
		{"NetworkCost", &alloc.NetworkCost},
		{"NetworkCrossZoneCost", &alloc.NetworkCrossZoneCost},
		{"NetworkCrossRegionCost", &alloc.NetworkCrossRegionCost},
		{"NetworkInternetCost", &alloc.NetworkInternetCost},
		{"NetworkCostAdjustment", &alloc.NetworkCostAdjustment},
		{"NetworkNatGatewayEgressCost", &alloc.NetworkNatGatewayEgressCost},
		{"NetworkNatGatewayIngressCost", &alloc.NetworkNatGatewayIngressCost},
		{"LoadBalancerCost", &alloc.LoadBalancerCost},
		{"LoadBalancerCostAdjustment", &alloc.LoadBalancerCostAdjustment},
		{"PVCostAdjustment", &alloc.PVCostAdjustment},
		{"RAMCost", &alloc.RAMCost},
		{"RAMCostAdjustment", &alloc.RAMCostAdjustment},
		{"RAMCostIdle", &alloc.RAMCostIdle},
		{"SharedCost", &alloc.SharedCost},
		{"ExternalCost", &alloc.ExternalCost},
		{"UnmountedPVCost", &alloc.UnmountedPVCost},
	}
	for _, f := range costFields {
		*f.ptr = tryConvert(converter, *f.ptr, targetCurrency, "Allocation."+f.name)
	}

	for pvKey, pv := range alloc.PVs {
		if pv == nil {
			continue
		}
		pvCtx := "Allocation.PVs." + pvKey.String()
		pv.Cost = tryConvert(converter, pv.Cost, targetCurrency, pvCtx+".Cost")
		pv.Adjustment = tryConvert(converter, pv.Adjustment, targetCurrency, pvCtx+".Adjustment")
		alloc.PVs[pvKey] = pv
	}

	for lbKey, lb := range alloc.LoadBalancers {
		if lb == nil {
			continue
		}
		lbCtx := "Allocation.LoadBalancers." + lbKey
		lb.Cost = tryConvert(converter, lb.Cost, targetCurrency, lbCtx+".Cost")
		lb.Adjustment = tryConvert(converter, lb.Adjustment, targetCurrency, lbCtx+".Adjustment")
		alloc.LoadBalancers[lbKey] = lb
	}

	// SharedCostBreakdowns is map[string]SharedCostBreakdown (value type);
	// mutate a local copy and re-assign.
	for key, scb := range alloc.SharedCostBreakdown {
		scb.TotalCost = tryConvert(converter, scb.TotalCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".TotalCost")
		scb.CPUCost = tryConvert(converter, scb.CPUCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".CPUCost")
		scb.GPUCost = tryConvert(converter, scb.GPUCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".GPUCost")
		scb.RAMCost = tryConvert(converter, scb.RAMCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".RAMCost")
		scb.PVCost = tryConvert(converter, scb.PVCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".PVCost")
		scb.NetworkCost = tryConvert(converter, scb.NetworkCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".NetworkCost")
		scb.LBCost = tryConvert(converter, scb.LBCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".LBCost")
		scb.ExternalCost = tryConvert(converter, scb.ExternalCost, targetCurrency, "Allocation.SharedCostBreakdown."+key+".ExternalCost")
		alloc.SharedCostBreakdown[key] = scb
	}

	return nil
}

// ConvertAllocationSet converts all allocations in a set. Returns an
// error if the target rate cannot be looked up (no mutation occurs).
// Per-allocation conversion itself is best-effort.
func ConvertAllocationSet(set *opencost.AllocationSet, converter currency.Converter, targetCurrency string) error {
	if set == nil || converter == nil {
		return nil
	}
	targetCurrency, noop, err := normalizeAndProbe(converter, targetCurrency)
	if err != nil {
		return err
	}
	if noop {
		return nil
	}

	for _, alloc := range set.Allocations {
		_ = convertAllocationFields(alloc, converter, targetCurrency)
	}
	return nil
}

// ConvertAllocationSetRange converts all sets in a range. Returns an
// error if the target rate cannot be looked up (no mutation occurs).
// Per-allocation conversion itself is best-effort.
func ConvertAllocationSetRange(asr *opencost.AllocationSetRange, converter currency.Converter, targetCurrency string) error {
	if asr == nil || converter == nil {
		return nil
	}
	targetCurrency, noop, err := normalizeAndProbe(converter, targetCurrency)
	if err != nil {
		return err
	}
	if noop {
		return nil
	}

	for _, set := range asr.Allocations {
		if set == nil {
			continue
		}
		for _, alloc := range set.Allocations {
			_ = convertAllocationFields(alloc, converter, targetCurrency)
		}
	}
	return nil
}
