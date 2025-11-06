package kubemodel

// ResourceQuota represents a Kubernetes resource quota
type ResourceQuota struct {
	UID          string                `json:"uid"`          // @bingen:field[version=1]
	NamespaceUID string                `json:"namespaceUID"` // @bingen:field[version=1]
	Name         string                `json:"name"`         // @bingen:field[version=1]
	Spec         *ResourceQuotaSpec    `json:"spec"`         // @bingen:field[version=1]
	Status       *ResourceQuotaStatus  `json:"status"`       // @bingen:field[version=1]
}

// ResourceQuotaSpec defines the desired hard limits to enforce
type ResourceQuotaSpec struct {
	Hard *ResourceQuotaSpecHard `json:"hard"` // @bingen:field[version=1]
}

// ResourceQuotaSpecHard defines the hard resource limits
type ResourceQuotaSpecHard struct {
	Requests ResourceQuantities `json:"requests"` // @bingen:field[version=1]
	Limits   ResourceQuantities `json:"limits"`   // @bingen:field[version=1]
}

// ResourceQuotaStatus defines the observed usage of resources
type ResourceQuotaStatus struct {
	Used *ResourceQuotaStatusUsed `json:"used"` // @bingen:field[version=1]
}

// ResourceQuotaStatusUsed tracks the currently used resources
type ResourceQuotaStatusUsed struct {
	Requests ResourceQuantities `json:"requests"` // @bingen:field[version=1]
	Limits   ResourceQuantities `json:"limits"`   // @bingen:field[version=1]
}
