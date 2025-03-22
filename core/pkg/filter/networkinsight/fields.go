package networkinsight

type NetworkInsightField string
type NetworkInsightDetailField string

// Only support namespace, pod and cluster on day 1
// Field used for Network Insight filtering
const (
	FieldClusterID NetworkInsightField = "cluster"
	FieldNamespace NetworkInsightField = "namespace"
	FieldPod       NetworkInsightField = "pod"
)

// Field used for Network Insight Details filtering
const (
	FieldEndPoint NetworkInsightDetailField = "endPoint"
)
