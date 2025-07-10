package pipelines

import (
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
)

const (
	AllocationPipelineName     string = "allocations"
	AssetsPipelineName         string = "assets"
	CloudCostsPipelineName     string = "cloudcosts"
	NetworkInsightPipelineName string = "networkinsights"
	CustomCostsPipelineName    string = "customcosts"
)

var nameByType map[string]string

// initializes the package, creates type -> pipeline mapping
func init() {
	allocSetKey := typeutil.TypeOf[opencost.AllocationSet]()
	allocKey := typeutil.TypeOf[opencost.Allocation]()

	assetSetKey := typeutil.TypeOf[opencost.AssetSet]()
	assetKey := typeutil.TypeOf[opencost.Asset]()

	cloudCostsSetKey := typeutil.TypeOf[opencost.CloudCostSet]()
	cloudCostKey := typeutil.TypeOf[opencost.CloudCost]()

	networkInsightSetKey := typeutil.TypeOf[opencost.NetworkInsightSet]()
	networkInsightKey := typeutil.TypeOf[opencost.NetworkInsight]()

	nameByType = map[string]string{
		allocSetKey:          AllocationPipelineName,
		allocKey:             AllocationPipelineName,
		assetSetKey:          AssetsPipelineName,
		assetKey:             AssetsPipelineName,
		cloudCostsSetKey:     CloudCostsPipelineName,
		cloudCostKey:         CloudCostsPipelineName,
		networkInsightSetKey: NetworkInsightPipelineName,
		networkInsightKey:    NetworkInsightPipelineName,
	}
}

func NameFor[T any]() string {
	return nameByType[typeutil.TypeOf[T]()]
}
