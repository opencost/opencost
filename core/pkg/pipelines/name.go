package pipelines

import (
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/typeutil"
)

const (
	AllocationPipelineName        string = "allocations"
	AssetsPipelineName            string = "assets"
	CloudCostsPipelineName        string = "cloudcosts"
	NetworkInsightPipelineName    string = "networkinsights"
	CustomCostsPipelineName       string = "customcosts"
	TurbonomicActionsPipelineName string = "turbonomicactions"
	HeartbeatPipelineName         string = "heartbeat"
	DiagnosticsPipelineName       string = "diagnostics"
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

	heartbeatKey := typeutil.TypeOf[heartbeat.Heartbeat]()
	diagnosticsKey := typeutil.TypeOf[diagnostics.DiagnosticsRunReport]()

	nameByType = map[string]string{
		allocSetKey:          AllocationPipelineName,
		allocKey:             AllocationPipelineName,
		assetSetKey:          AssetsPipelineName,
		assetKey:             AssetsPipelineName,
		cloudCostsSetKey:     CloudCostsPipelineName,
		cloudCostKey:         CloudCostsPipelineName,
		networkInsightSetKey: NetworkInsightPipelineName,
		networkInsightKey:    NetworkInsightPipelineName,
		heartbeatKey:         HeartbeatPipelineName,
		diagnosticsKey:       DiagnosticsPipelineName,
	}
}

func NameFor[T any]() string {
	return nameByType[typeutil.TypeOf[T]()]
}
