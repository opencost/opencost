package cloudutil

// PartialCPUMap GKE lies about the number of cores e2 nodes have. This table
// contains a mapping from node type -> actual CPU cores
// for those cases.
var PartialCPUMap = map[string]float64{
	"e2-micro":  0.25,
	"e2-small":  0.5,
	"e2-medium": 1.0,
}
