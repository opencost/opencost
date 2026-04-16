package costmodel

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/currency"
)

// ConvertAllocation converts all cost fields in an Allocation from USD to target currency
func ConvertAllocation(alloc *opencost.Allocation, converter currency.Converter, targetCurrency string) error {
	if alloc == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))

	// List of all float64 cost fields to convert. Keep in sync with
	// Allocation cost fields in core/pkg/opencost/allocation.go that appear
	// in JSON API responses.
	costFields := []*float64{
		&alloc.CPUCost,
		&alloc.CPUCostAdjustment,
		&alloc.CPUCostIdle,
		&alloc.GPUCost,
		&alloc.GPUCostAdjustment,
		&alloc.GPUCostIdle,
		&alloc.NetworkCost,
		&alloc.NetworkCrossZoneCost,
		&alloc.NetworkCrossRegionCost,
		&alloc.NetworkInternetCost,
		&alloc.NetworkCostAdjustment,
		&alloc.NetworkNatGatewayEgressCost,
		&alloc.NetworkNatGatewayIngressCost,
		&alloc.LoadBalancerCost,
		&alloc.LoadBalancerCostAdjustment,
		&alloc.PVCostAdjustment,
		&alloc.RAMCost,
		&alloc.RAMCostAdjustment,
		&alloc.RAMCostIdle,
		&alloc.SharedCost,
		&alloc.ExternalCost,
		&alloc.UnmountedPVCost,
	}

	// Convert each cost field
	for _, costPtr := range costFields {
		if *costPtr != 0 {
			converted, err := converter.Convert(*costPtr, "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert cost: %w", err)
			}
			*costPtr = converted
		}
	}

	// Convert PV costs (nested structure). Both Cost and Adjustment appear
	// in JSON responses and must be converted.
	for pvKey, pv := range alloc.PVs {
		if pv == nil {
			continue
		}
		if pv.Cost != 0 {
			converted, err := converter.Convert(pv.Cost, "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert PV cost: %w", err)
			}
			pv.Cost = converted
		}
		if pv.Adjustment != 0 {
			converted, err := converter.Convert(pv.Adjustment, "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert PV adjustment: %w", err)
			}
			pv.Adjustment = converted
		}
		alloc.PVs[pvKey] = pv
	}

	// Convert LoadBalancer costs (nested structure)
	for lbKey, lb := range alloc.LoadBalancers {
		if lb == nil {
			continue
		}
		if lb.Cost != 0 {
			converted, err := converter.Convert(lb.Cost, "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert LB cost: %w", err)
			}
			lb.Cost = converted
			alloc.LoadBalancers[lbKey] = lb
		}
	}

	// Convert SharedCostBreakdown entries. SharedCostBreakdowns is a
	// map[string]SharedCostBreakdown (value, not pointer), so mutate a
	// local copy and re-assign.
	for key, scb := range alloc.SharedCostBreakdown {
		scbFields := []*float64{
			&scb.TotalCost,
			&scb.CPUCost,
			&scb.GPUCost,
			&scb.RAMCost,
			&scb.PVCost,
			&scb.NetworkCost,
			&scb.LBCost,
			&scb.ExternalCost,
		}
		mutated := false
		for _, costPtr := range scbFields {
			if *costPtr == 0 {
				continue
			}
			converted, err := converter.Convert(*costPtr, "USD", targetCurrency)
			if err != nil {
				return fmt.Errorf("failed to convert SharedCostBreakdown %q: %w", key, err)
			}
			*costPtr = converted
			mutated = true
		}
		if mutated {
			alloc.SharedCostBreakdown[key] = scb
		}
	}

	return nil
}

// ConvertAllocationSet converts all allocations in a set
func ConvertAllocationSet(set *opencost.AllocationSet, converter currency.Converter, targetCurrency string) error {
	if set == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	for _, alloc := range set.Allocations {
		if err := ConvertAllocation(alloc, converter, targetCurrency); err != nil {
			return err
		}
	}

	return nil
}

// ConvertAllocationSetRange converts all sets in a range
func ConvertAllocationSetRange(asr *opencost.AllocationSetRange, converter currency.Converter, targetCurrency string) error {
	if asr == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	for _, set := range asr.Allocations {
		if err := ConvertAllocationSet(set, converter, targetCurrency); err != nil {
			return err
		}
	}

	return nil
}

// ConvertCloudCost converts all cost metrics in a CloudCost
func ConvertCloudCost(cc *opencost.CloudCost, converter currency.Converter, targetCurrency string) error {
	if cc == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))

	// Helper to convert CostMetric (only the Cost field, not KubernetesPercent)
	convertCostMetric := func(cm *opencost.CostMetric) error {
		if cm.Cost != 0 {
			converted, err := converter.Convert(cm.Cost, "USD", targetCurrency)
			if err != nil {
				return err
			}
			cm.Cost = converted
		}
		return nil
	}

	// Convert all cost metrics
	if err := convertCostMetric(&cc.ListCost); err != nil {
		return fmt.Errorf("failed to convert ListCost: %w", err)
	}
	if err := convertCostMetric(&cc.NetCost); err != nil {
		return fmt.Errorf("failed to convert NetCost: %w", err)
	}
	if err := convertCostMetric(&cc.AmortizedNetCost); err != nil {
		return fmt.Errorf("failed to convert AmortizedNetCost: %w", err)
	}
	if err := convertCostMetric(&cc.InvoicedCost); err != nil {
		return fmt.Errorf("failed to convert InvoicedCost: %w", err)
	}
	if err := convertCostMetric(&cc.AmortizedCost); err != nil {
		return fmt.Errorf("failed to convert AmortizedCost: %w", err)
	}

	return nil
}

// ConvertCloudCostSet converts all cloud costs in a set
func ConvertCloudCostSet(set *opencost.CloudCostSet, converter currency.Converter, targetCurrency string) error {
	if set == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	for _, cc := range set.CloudCosts {
		if cc == nil {
			continue
		}
		if err := ConvertCloudCost(cc, converter, targetCurrency); err != nil {
			return err
		}
	}

	return nil
}

// ConvertCloudCostSetRange converts all sets in a range
func ConvertCloudCostSetRange(ccsr *opencost.CloudCostSetRange, converter currency.Converter, targetCurrency string) error {
	if ccsr == nil || converter == nil || targetCurrency == "USD" {
		return nil
	}

	targetCurrency = strings.ToUpper(strings.TrimSpace(targetCurrency))

	// Convert all cloud cost sets in the range
	for _, set := range ccsr.CloudCostSets {
		if set == nil {
			continue
		}
		if err := ConvertCloudCostSet(set, converter, targetCurrency); err != nil {
			return err
		}
	}

	return nil
}
