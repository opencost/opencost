package exporter

import (
	export "github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/opencost"
)

// NewAllocationEncoder creates an `export.Encoder[opencost.AllocationSet]` implementation for
// encoding AllocationSet data.
func NewAllocationEncoder() export.Encoder[opencost.AllocationSet] {
	return export.NewBingenEncoder[opencost.AllocationSet]()
}

// NewAssetsEncoder creates an `export.Encoder[opencost.AssetSet]` implementation for
// encoding AssetSet data.
func NewAssetsEncoder() export.Encoder[opencost.AssetSet] {
	return export.NewBingenEncoder[opencost.AssetSet]()
}

// NewNetworkInsightEncoder creates an `export.Encoder[opencost.NetworkInsightSet]` implementation for
// encoding NetworkInsightSet data.
func NewNetworkInsightEncoder() export.Encoder[opencost.NetworkInsightSet] {
	return export.NewBingenEncoder[opencost.NetworkInsightSet]()
}
