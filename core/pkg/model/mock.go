package model

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func createCustomCost(postfix string) *pb.CustomCost {
	n := func(a string) string {
		return fmt.Sprintf("%s_%s", a, postfix)
	}

	cost := rand.Float32() * 250.0

	return &pb.CustomCost{
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

func GenerateMockCustomCostSet(start, end time.Time) *pb.CustomCostResponse {
	costs := []*pb.CustomCost{}

	for i := 0; i < 50; i++ {
		costs = append(costs, createCustomCost(fmt.Sprintf("%d", i)))
	}

	return &pb.CustomCostResponse{
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
