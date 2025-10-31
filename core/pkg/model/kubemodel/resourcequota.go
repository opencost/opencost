package kubemodel

type ResourceQuotaKind string

type ResourceQuota struct {
	UID          string
	NamespaceUID string
	Name         string
	Spec         *ResourceQuotaSpec
	Status       *ResourceQuotaStatus
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
