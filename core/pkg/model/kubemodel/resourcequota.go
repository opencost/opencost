package kubemodel

type ResourceQuotaKind string

const (
	ResourceQuotaKindCompute = "compute"
)

type ResourceQuota struct {
	ID          string
	NamespaceID string
	Name        string
	Kind        ResourceQuotaKind
	Spec        *ResourceQuotaSpec
	Status      *ResourceQuotaStatus
}

type ResourceQuotaSpec struct {
	Hard ResourceQuotaSpecHard
}

type ResourceQuotaSpecHard struct {
	Requests []ResourceQuantity
	Limits   []ResourceQuantity
}

type ResourceQuotaStatus struct {
	Used ResourceQuotaStatusUsed
}

type ResourceQuotaStatusUsed struct {
	Requests []ResourceQuantity
	Limits   []ResourceQuantity
}
