package kubemodel

// @bingen:generate:ResourceQuota
type ResourceQuota struct {
	UID          string               `json:"uid"`          // @bingen:field[version=1]
	NamespaceUID string               `json:"namespaceUID"` // @bingen:field[version=1]
	Name         string               `json:"name"`         // @bingen:field[version=1]
	Spec         *ResourceQuotaSpec   `json:"spec"`         // @bingen:field[version=1]
	Status       *ResourceQuotaStatus `json:"status"`       // @bingen:field[version=1]
}

// @bingen:generate:ResourceQuotaSpec
type ResourceQuotaSpec struct {
	Hard *ResourceQuotaSpecHard `json:"hard"` // @bingen:field[version=1]
}

// @bingen:generate:ResourceQuotaSpecHard
type ResourceQuotaSpecHard struct {
	Requests ResourceQuantities `json:"requests"` // @bingen:field[version=1]
	Limits   ResourceQuantities `json:"limits"`   // @bingen:field[version=1]
}

// @bingen:generate:ResourceQuotaStatus
type ResourceQuotaStatus struct {
	Used *ResourceQuotaStatusUsed `json:"used"` // @bingen:field[version=1]
}

// @bingen:generate:ResourceQuotaStatusUsed
type ResourceQuotaStatusUsed struct {
	Requests ResourceQuantities `json:"requests"` // @bingen:field[version=1]
	Limits   ResourceQuantities `json:"limits"`   // @bingen:field[version=1]
}
