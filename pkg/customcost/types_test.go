package customcost

import (
	"testing"
)

func TestSortByCostAsc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Cost: 300.0},
			{Cost: 100.0},
			{Cost: 200.0},
		},
	}

	ccs.Sort(SortPropertyCost, SortDirectionAsc)

	expected := []float32{100.0, 200.0, 300.0}
	for i, cc := range ccs.CustomCosts {
		if cc.Cost != expected[i] {
			t.Errorf("expected cost %f, got %f", expected[i], cc.Cost)
		}
	}
}

func TestSortByCostDesc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Cost: 300.0},
			{Cost: 100.0},
			{Cost: 200.0},
		},
	}

	ccs.Sort(SortPropertyCost, SortDirectionDesc)

	expected := []float32{300.0, 200.0, 100.0}
	for i, cc := range ccs.CustomCosts {
		if cc.Cost != expected[i] {
			t.Errorf("expected cost %f, got %f", expected[i], cc.Cost)
		}
	}
}

func TestSortByAggregateAsc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Aggregate: "c"},
			{Aggregate: "a"},
			{Aggregate: "b"},
		},
	}

	ccs.Sort(SortPropertyAggregate, SortDirectionAsc)

	expected := []string{"a", "b", "c"}
	for i, cc := range ccs.CustomCosts {
		if cc.Aggregate != expected[i] {
			t.Errorf("expected aggregate %s, got %s", expected[i], cc.Aggregate)
		}
	}
}

func TestSortByAggregateDesc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{Aggregate: "c"},
			{Aggregate: "a"},
			{Aggregate: "b"},
		},
	}

	ccs.Sort(SortPropertyAggregate, SortDirectionDesc)

	expected := []string{"c", "b", "a"}
	for i, cc := range ccs.CustomCosts {
		if cc.Aggregate != expected[i] {
			t.Errorf("expected aggregate %s, got %s", expected[i], cc.Aggregate)
		}
	}
}

func TestSortByCostTypeAsc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{CostType: CostTypeBilled},
			{CostType: CostTypeList},
			{CostType: CostTypeBlended},
		},
	}

	ccs.Sort(SortPropertyCostType, SortDirectionAsc)

	expected := []CostType{CostTypeBilled, CostTypeBlended, CostTypeList}
	for i, cc := range ccs.CustomCosts {
		if cc.CostType != expected[i] {
			t.Errorf("expected cost type %s, got %s", expected[i], cc.CostType)
		}
	}
}

func TestSortByCostTypeDesc(t *testing.T) {
	ccs := &CustomCostSet{
		CustomCosts: []*CustomCost{
			{CostType: CostTypeBilled},
			{CostType: CostTypeList},
			{CostType: CostTypeBlended},
		},
	}

	ccs.Sort(SortPropertyCostType, SortDirectionDesc)

	expected := []CostType{CostTypeList, CostTypeBlended, CostTypeBilled}
	for i, cc := range ccs.CustomCosts {
		if cc.CostType != expected[i] {
			t.Errorf("expected cost type %s, got %s", expected[i], cc.CostType)
		}
	}
}
