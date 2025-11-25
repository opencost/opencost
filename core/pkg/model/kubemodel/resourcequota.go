package kubemodel

import (
	"fmt"
	"time"
)

// TODO: Do we need (Start, End) for these?

// @bingen:generate:ResourceQuota
type ResourceQuota struct {
	UID          string               `json:"uid"`          // @bingen:field[version=1]
	NamespaceUID string               `json:"namespaceUID"` // @bingen:field[version=1]
	Name         string               `json:"name"`         // @bingen:field[version=1]
	Spec         *ResourceQuotaSpec   `json:"spec"`         // @bingen:field[version=1]
	Status       *ResourceQuotaStatus `json:"status"`       // @bingen:field[version=1]
	Start        time.Time            `json:"start"`        // @bingen:field[version=1]
	End          time.Time            `json:"end"`          // @bingen:field[version=1]
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

func (spec *ResourceQuotaSpecHard) SetRequest(resource Resource, unit Unit, statType StatType, value uint64) {
	if spec.Requests == nil {
		spec.Requests = ResourceQuantities{}
	}

	spec.Requests.Set(resource, unit, statType, value)
}

func (spec *ResourceQuotaSpecHard) SetLimit(resource Resource, unit Unit, statType StatType, value uint64) {
	if spec.Limits == nil {
		spec.Limits = ResourceQuantities{}
	}

	spec.Limits.Set(resource, unit, statType, value)
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

func (stat *ResourceQuotaStatusUsed) SetRequest(resource Resource, unit Unit, statType StatType, value uint64) {
	if stat.Requests == nil {
		stat.Requests = ResourceQuantities{}
	}

	stat.Requests.Set(resource, unit, statType, value)
}

func (stat *ResourceQuotaStatusUsed) SetLimit(resource Resource, unit Unit, statType StatType, value uint64) {
	if stat.Limits == nil {
		stat.Limits = ResourceQuantities{}
	}

	stat.Limits.Set(resource, unit, statType, value)
}

func (kms *KubeModelSet) RegisterResourceQuota(uid, name, namespace string) {
	if uid == "" {
		kms.RegisterError(fmt.Sprintf("RegisterResourceQuota: uid is nil for ResourceQuota '%s'", name))
		return
	}

	if _, ok := kms.ResourceQuotas[uid]; !ok {
		namespaceUID := ""

		if _, ok := kms.idx.namespaceByName[namespace]; !ok {
			kms.RegisterError(fmt.Sprintf("RegisterResourceQuota(%s, %s, %s): missing namespace", uid, name, namespace))
		} else {
			namespaceUID = kms.idx.namespaceByName[namespace].UID
		}

		kms.ResourceQuotas[uid] = &ResourceQuota{
			UID:          uid,
			Name:         name,
			NamespaceUID: namespaceUID,
			Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
			Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
		}

		kms.Metadata.ObjectCount++
	}
}
