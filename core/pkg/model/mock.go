package model

import (
	"fmt"
	"math/rand"
	"time"

	commonpb "github.com/opencost/opencost/core/pkg/model/pb/common"
	custompb "github.com/opencost/opencost/core/pkg/model/pb/customcost"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func createCustomCost(postfix string) *custompb.CustomCost {
	n := func(a string) string {
		return fmt.Sprintf("%s_%s", a, postfix)
	}

	cost := rand.Float32() * 250.0

	return &custompb.CustomCost{
		Metadata: map[string]string{
			n("custom_cost"): n("metadata"),
		},
		Zone:           "zone-a",
		AccountName:    n("account"),
		ChargeCategory: n("charge"),
		Description:    "this is a test cost description(" + postfix + ")",
		ResourceName:   "test-custom-cost-" + postfix,
		ResourceType:   "custom",
		Id:             n("id"),
		ProviderId:     "gke",
		BilledCost:     cost,
		ListCost:       cost,
		ListUnitPrice:  cost,
		UsageQuantity:  1.0,
		UsageUnit:      n("unit"),
		Labels: map[string]string{
			n("label"): n("value"),
		},
	}
}

func GenerateMockCustomCostSet(start, end time.Time) *custompb.CustomCostResponse {
	costs := []*custompb.CustomCost{}

	for i := 0; i < 50; i++ {
		costs = append(costs, createCustomCost(fmt.Sprintf("%d", i)))
	}

	return &custompb.CustomCostResponse{
		Metadata: map[string]string{
			"key1": "value1",
			"test": "1, 2, 3",
		},
		CostSource: "none",
		Domain:     "testing",
		Version:    "v1",
		Currency:   "USD",
		Start:      timestamppb.New(start),
		End:        timestamppb.New(end),
		Costs:      costs,
	}
}

func GenerateMockLabelResponse(start time.Time, res commonpb.Resolution) *commonpb.LabelsResponse {
	return &commonpb.LabelsResponse{
		Type:    "account-labels",
		GroupId: "billing_account_xzy",
		Window: &commonpb.Window{
			Resolution: res,
			Start:      timestamppb.New(start),
		},
		LabelSets: map[string]*commonpb.LabelSet{
			"account1": {Labels: map[string]string{
				"account": "account1",
				"test":    "test1",
			}},
			"account2": {Labels: map[string]string{
				"account": "account2",
				"test":    "test2",
			}},
		},
	}
}
