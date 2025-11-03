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
	Hard *ResourceQuotaSpecHard
}

type ResourceQuotaSpecHard struct {
	Requests ResourceQuantities
	Limits   ResourceQuantities
}

type ResourceQuotaStatus struct {
	Used *ResourceQuotaStatusUsed
}

type ResourceQuotaStatusUsed struct {
	Requests ResourceQuantities
	Limits   ResourceQuantities
}
