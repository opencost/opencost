package customcost

import (
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/opencost"
)

type CostTotalRequest struct {
	Start       time.Time
	End         time.Time
	AggregateBy []CustomCostProperty
	Accumulate  opencost.AccumulateOption
	Filter      filter.Filter
}

type CostTimeseriesRequest struct {
	Start       time.Time
	End         time.Time
	AggregateBy []CustomCostProperty
	Accumulate  opencost.AccumulateOption
	Filter      filter.Filter
}

type CostResponse struct {
	Window          opencost.Window `json:"window"`
	TotalBilledCost float32         `json:"totalBilledCost"`
	TotalListCost   float32         `json:"totalListCost"`
	CustomCosts     []*CustomCost   `json:"customCosts"`
}

type CustomCost struct {
	Id             string  `json:"id"`
	Zone           string  `json:"zone"`
	AccountName    string  `json:"account_name"`
	ChargeCategory string  `json:"charge_category"`
	Description    string  `json:"description"`
	ResourceName   string  `json:"resource_name"`
	ResourceType   string  `json:"resource_type"`
	ProviderId     string  `json:"provider_id"`
	BilledCost     float32 `json:"billedCost"`
	ListCost       float32 `json:"listCost"`
	ListUnitPrice  float32 `json:"list_unit_price"`
	UsageQuantity  float32 `json:"usage_quantity"`
	UsageUnit      string  `json:"usage_unit"`
	Domain         string  `json:"domain"`
	CostSource     string  `json:"cost_source"`
	Aggregate      string  `json:"aggregate"`
}

type CostTimeseriesResponse struct {
	Window     opencost.Window `json:"window"`
	Timeseries []*CostResponse `json:"timeseries"`
}

func NewCostResponse(ccs *CustomCostSet) *CostResponse {
	costResponse := &CostResponse{
		Window:      ccs.Window,
		CustomCosts: []*CustomCost{},
	}

	for _, cc := range ccs.CustomCosts {
		costResponse.TotalBilledCost += cc.BilledCost
		costResponse.TotalListCost += cc.ListCost
		costResponse.CustomCosts = append(costResponse.CustomCosts, cc)
	}

	return costResponse
}

func ParseCustomCostResponse(ccResponse *pb.CustomCostResponse) []*CustomCost {
	costs := ccResponse.GetCosts()

	customCosts := make([]*CustomCost, len(costs))
	for i, cost := range costs {
		customCosts[i] = &CustomCost{
			Id:             cost.GetId(),
			Zone:           cost.GetZone(),
			AccountName:    cost.GetAccountName(),
			ChargeCategory: cost.GetChargeCategory(),
			Description:    cost.GetDescription(),
			ResourceName:   cost.GetResourceName(),
			ResourceType:   cost.GetResourceType(),
			ProviderId:     cost.GetProviderId(),
			BilledCost:     cost.GetBilledCost(),
			ListCost:       cost.GetListCost(),
			ListUnitPrice:  cost.GetListUnitPrice(),
			UsageQuantity:  cost.GetUsageQuantity(),
			UsageUnit:      cost.GetUsageUnit(),
			Domain:         ccResponse.GetDomain(),
			CostSource:     ccResponse.GetCostSource(),
		}
	}

	return customCosts
}

func (cc *CustomCost) Add(other *CustomCost) {
	cc.BilledCost += other.BilledCost
	cc.ListCost += other.ListCost
	cc.ListUnitPrice += other.ListUnitPrice

	if cc.Id != other.Id {
		cc.Id = ""
	}

	if cc.Zone != other.Zone {
		cc.Zone = ""
	}

	if cc.AccountName != other.AccountName {
		cc.AccountName = ""
	}

	if cc.ChargeCategory != other.ChargeCategory {
		cc.ChargeCategory = ""
	}

	if cc.Description != other.Description {
		cc.Description = ""
	}

	if cc.ResourceName != other.ResourceName {
		cc.ResourceName = ""
	}

	if cc.ResourceType != other.ResourceType {
		cc.ResourceType = ""
	}

	if cc.ProviderId != other.ProviderId {
		cc.ProviderId = ""
	}

	if cc.UsageUnit != other.UsageUnit {
		cc.UsageUnit = ""
	} else {
		// when usage units are the same, then we can sum the usages
		cc.UsageQuantity += other.UsageQuantity
	}

	if cc.Domain != other.Domain {
		cc.Domain = ""
	}

	if cc.CostSource != other.CostSource {
		cc.CostSource = ""
	}

	if cc.Aggregate != other.Aggregate {
		cc.Aggregate = ""
	}

}

type CustomCostSet struct {
	CustomCosts []*CustomCost
	Window      opencost.Window
}

func NewCustomCostSet(window opencost.Window) *CustomCostSet {
	return &CustomCostSet{
		CustomCosts: []*CustomCost{},
		Window:      window,
	}
}

func (ccs *CustomCostSet) Add(customCost *CustomCost) {
	ccs.CustomCosts = append(ccs.CustomCosts, customCost)
}

func (ccs *CustomCostSet) Aggregate(aggregateBy []CustomCostProperty) error {
	// when no aggregation, return the original CustomCostSet
	if len(aggregateBy) == 0 {
		return nil
	}

	aggMap := make(map[string]*CustomCost)
	for _, cc := range ccs.CustomCosts {
		aggKey, err := generateAggKey(cc, aggregateBy)
		if err != nil {
			return fmt.Errorf("failed to aggregate CustomCostSet: %w", err)
		}
		cc.Aggregate = aggKey

		if existing, ok := aggMap[aggKey]; ok {
			existing.Add(cc)
		} else {
			aggMap[aggKey] = cc
		}
	}

	var newCustomCosts []*CustomCost
	for _, customCost := range aggMap {
		newCustomCosts = append(newCustomCosts, customCost)
	}
	ccs.CustomCosts = newCustomCosts

	return nil
}

func generateAggKey(cc *CustomCost, aggregateBy []CustomCostProperty) (string, error) {
	var aggKeys []string
	for _, agg := range aggregateBy {
		var aggKey string
		if agg == CustomCostZoneProp {
			aggKey = cc.Zone
		} else if agg == CustomCostAccountNameProp {
			aggKey = cc.AccountName
		} else if agg == CustomCostChargeCategoryProp {
			aggKey = cc.ChargeCategory
		} else if agg == CustomCostDescriptionProp {
			aggKey = cc.Description
		} else if agg == CustomCostResourceNameProp {
			aggKey = cc.ResourceName
		} else if agg == CustomCostResourceTypeProp {
			aggKey = cc.ResourceType
		} else if agg == CustomCostProviderIdProp {
			aggKey = cc.ProviderId
		} else if agg == CustomCostUsageUnitProp {
			aggKey = cc.UsageUnit
		} else if agg == CustomCostDomainProp {
			aggKey = cc.Domain
		} else if agg == CustomCostCostSourceProp {
			aggKey = cc.CostSource
		} else {
			return "", fmt.Errorf("unsupported aggregation type: %s", agg)
		}

		if len(aggKey) == 0 {
			aggKey = opencost.UnallocatedSuffix
		}
		aggKeys = append(aggKeys, aggKey)
	}
	aggKey := strings.Join(aggKeys, "/")

	return aggKey, nil
}
