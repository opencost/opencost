package costmodel

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/currency"
)

// Currency conversion uses best-effort semantics: if a single field fails
// to convert, it is left in USD and a warning is logged. The response may
// therefore contain mixed currencies under partial converter failures.
// Callers treat a non-nil error from these helpers as advisory only --
// the mutation has already been applied where it succeeded.

// tryConvert converts val from USD to target, returning the original
// value (and logging) on converter error. logCtx identifies the field
// being converted so operators can triage which fields are failing.
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

// ConvertAllocation converts all cost fields in an Allocation from USD
// to target currency in place. Best-effort: per-field failures are logged
// and skipped rather than aborting the whole allocation.
func ConvertAllocation(alloc *opencost.Allocation, converter currency.Converter, targetCurrency string) error {
	if alloc == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))

	// Named cost fields. Keep in sync with Allocation cost fields in
	// core/pkg/opencost/allocation.go that appear in JSON API responses.
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

	// Convert PV costs (nested structure). Both Cost and Adjustment appear
	// in JSON responses and must be converted.
	for pvKey, pv := range alloc.PVs {
		if pv == nil {
			continue
		}
		pvCtx := "Allocation.PVs." + pvKey.String()
		pv.Cost = tryConvert(converter, pv.Cost, targetCurrency, pvCtx+".Cost")
		pv.Adjustment = tryConvert(converter, pv.Adjustment, targetCurrency, pvCtx+".Adjustment")
		alloc.PVs[pvKey] = pv
	}

	// Convert LoadBalancer costs (nested structure)
	for lbKey, lb := range alloc.LoadBalancers {
		if lb == nil {
			continue
		}
		lb.Cost = tryConvert(converter, lb.Cost, targetCurrency, "Allocation.LoadBalancers."+lbKey+".Cost")
		alloc.LoadBalancers[lbKey] = lb
	}

	// Convert SharedCostBreakdown entries. SharedCostBreakdowns is a
	// map[string]SharedCostBreakdown (value, not pointer), so mutate a
	// local copy and re-assign.
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

// ConvertAllocationSet converts all allocations in a set (best-effort).
func ConvertAllocationSet(set *opencost.AllocationSet, converter currency.Converter, targetCurrency string) error {
	if set == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	for _, alloc := range set.Allocations {
		// ConvertAllocation is best-effort and never returns a non-nil
		// error today, but retain the error-return contract in case the
		// helper is extended later.
		_ = ConvertAllocation(alloc, converter, targetCurrency)
	}

	return nil
}

// ConvertAllocationSetRange converts all sets in a range (best-effort).
func ConvertAllocationSetRange(asr *opencost.AllocationSetRange, converter currency.Converter, targetCurrency string) error {
	if asr == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	for _, set := range asr.Allocations {
		_ = ConvertAllocationSet(set, converter, targetCurrency)
	}

	return nil
}

