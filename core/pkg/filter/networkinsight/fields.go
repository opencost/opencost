package networkinsight

import "github.com/opencost/opencost/core/pkg/filter/fieldstrings"

type NetworkInsightField string
type NetworkInsightDetailField string

// Field used for Network Insight filtering
const (
	FieldClusterID NetworkInsightField = NetworkInsightField(fieldstrings.FieldClusterID)
	FieldNamespace NetworkInsightField = NetworkInsightField(fieldstrings.FieldNamespace)
	FieldPod       NetworkInsightField = NetworkInsightField(fieldstrings.FieldPod)
	FieldAccount   NetworkInsightField = NetworkInsightField(fieldstrings.FieldAccount)
)

// Field used for Network Insight Details filtering
const (
	FieldEndPoint NetworkInsightDetailField = "endPoint"
)
