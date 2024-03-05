package model

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/opencost"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ToCustomCostResponse(ccrpb *pb.CustomCostResponse) *CustomCostResponse {
	window := opencost.NewClosedWindow(ccrpb.Start.AsTime().UTC(), ccrpb.End.AsTime().UTC())
	var errs []error
	for _, errStr := range ccrpb.Errors {
		errs = append(errs, fmt.Errorf(errStr))
	}
	var costs []*CustomCost
	for _, cost := range ccrpb.Costs {
		costs = append(costs, ToCustomCost(cost, &window))
	}
	return &CustomCostResponse{
		Metadata:   ccrpb.Metadata,
		Costsource: ccrpb.CostSource,
		Domain:     ccrpb.Domain,
		Version:    ccrpb.Version,
		Currency:   ccrpb.Currency,
		Window:     window,
		Costs:      costs,
		Errors:     errs,
	}
}

func ToCustomCost(ccpb *pb.CustomCost, window *opencost.Window) *CustomCost {
	return &CustomCost{
		Metadata:           ccpb.Metadata,
		Zone:               ccpb.Zone,
		BilledCost:         ccpb.BilledCost,
		AccountName:        ccpb.AccountName,
		ChargeCategory:     ccpb.ChargeCategory,
		Description:        ccpb.Description,
		ListCost:           ccpb.ListCost,
		ListUnitPrice:      ccpb.ListUnitPrice,
		ResourceName:       ccpb.ResourceName,
		ResourceType:       ccpb.ResourceType,
		Id:                 ccpb.Id,
		ProviderId:         ccpb.ProviderId,
		Window:             window,
		Labels:             ccpb.Labels,
		UsageQty:           ccpb.UsageQuantity,
		UsageUnit:          ccpb.UsageUnit,
		ExtendedAttributes: ToCustomCostExtendedAttributes(ccpb.ExtendedAttributes),
	}
}

func ToCustomCostExtendedAttributes(ea *pb.CustomCostExtendedAttributes) *ExtendedCustomCostAttributes {
	if ea == nil {
		return nil
	}
	billingPeriod := opencost.NewClosedWindow(ea.BillingPeriodStart.AsTime().UTC(), ea.BillingPeriodEnd.AsTime().UTC())
	return &ExtendedCustomCostAttributes{
		BillingPeriod:              &billingPeriod,
		AccountID:                  *ea.AccountId,
		ChargeFrequency:            *ea.ChargeFrequency,
		Subcategory:                *ea.Subcategory,
		CommitmentDiscountCategory: *ea.CommitmentDiscountCategory,
		CommitmentDiscountID:       *ea.CommitmentDiscountId,
		CommitmentDiscountName:     *ea.CommitmentDiscountName,
		CommitmentDiscountType:     *ea.CommitmentDiscountType,
		EffectiveCost:              *ea.EffectiveCost,
		InvoiceIssuer:              *ea.InvoiceIssuer,
		Provider:                   *ea.Provider,
		Publisher:                  *ea.Publisher,
		ServiceCategory:            *ea.ServiceCategory,
		ServiceName:                *ea.ServiceName,
		SkuID:                      *ea.SkuId,
		SkuPriceID:                 *ea.SkuPriceId,
		SubAccountID:               *ea.SubAccountId,
		SubAccountName:             *ea.SubAccountName,
		PricingQuantity:            *ea.PricingQuantity,
		PricingUnit:                *ea.PricingUnit,
		PricingCategory:            *ea.PricingCategory,
	}
}

func ToCustomCostResponsePB(ccr *CustomCostResponse) *pb.CustomCostResponse {
	var errs []string
	for _, err := range ccr.Errors {
		errs = append(errs, err.Error())
	}
	var costs []*pb.CustomCost
	for _, cost := range ccr.Costs {
		costs = append(costs, ToCustomCostPB(cost))
	}

	return &pb.CustomCostResponse{
		Metadata:   ccr.Metadata,
		CostSource: ccr.Costsource,
		Domain:     ccr.Domain,
		Version:    ccr.Version,
		Currency:   ccr.Currency,
		Start:      timestamppb.New(ccr.Window.Start().UTC()),
		End:        timestamppb.New(ccr.Window.End().UTC()),
		Costs:      costs,
		Errors:     errs,
	}
}

func ToCustomCostPB(cc *CustomCost) *pb.CustomCost {
	return &pb.CustomCost{
		Metadata:           cc.Metadata,
		Zone:               cc.Zone,
		AccountName:        cc.AccountName,
		ChargeCategory:     cc.ChargeCategory,
		Description:        cc.Description,
		ResourceName:       cc.ResourceName,
		ResourceType:       cc.ResourceType,
		Id:                 cc.Id,
		ProviderId:         cc.ProviderId,
		BilledCost:         cc.BilledCost,
		ListCost:           cc.ListCost,
		ListUnitPrice:      cc.ListUnitPrice,
		UsageQuantity:      cc.UsageQty,
		UsageUnit:          cc.UsageUnit,
		Labels:             cc.Labels,
		ExtendedAttributes: ToCustomCostExtendedAttributesPB(cc.ExtendedAttributes),
	}
}

func ToCustomCostExtendedAttributesPB(ecca *ExtendedCustomCostAttributes) *pb.CustomCostExtendedAttributes {
	if ecca == nil {
		return nil
	}

	var billingPeriodStart *timestamppb.Timestamp
	var billingPeriodEnd *timestamppb.Timestamp
	if ecca != nil && !ecca.BillingPeriod.IsOpen() {
		billingPeriodStart = timestamppb.New(ecca.BillingPeriod.Start().UTC())
		billingPeriodEnd = timestamppb.New(ecca.BillingPeriod.End().UTC())
	}

	return &pb.CustomCostExtendedAttributes{
		BillingPeriodStart:         billingPeriodStart,
		BillingPeriodEnd:           billingPeriodEnd,
		AccountId:                  &ecca.AccountID,
		ChargeFrequency:            &ecca.ChargeFrequency,
		Subcategory:                &ecca.Subcategory,
		CommitmentDiscountCategory: &ecca.CommitmentDiscountCategory,
		CommitmentDiscountId:       &ecca.CommitmentDiscountID,
		CommitmentDiscountName:     &ecca.CommitmentDiscountName,
		CommitmentDiscountType:     &ecca.CommitmentDiscountType,
		EffectiveCost:              &ecca.EffectiveCost,
		InvoiceIssuer:              &ecca.InvoiceIssuer,
		Provider:                   &ecca.Provider,
		Publisher:                  &ecca.Publisher,
		ServiceCategory:            &ecca.ServiceCategory,
		ServiceName:                &ecca.ServiceName,
		SkuId:                      &ecca.SkuID,
		SkuPriceId:                 &ecca.SkuPriceID,
		SubAccountId:               &ecca.SubAccountID,
		SubAccountName:             &ecca.SubAccountName,
		PricingQuantity:            &ecca.PricingQuantity,
		PricingUnit:                &ecca.PricingUnit,
		PricingCategory:            &ecca.PricingCategory,
	}
}
