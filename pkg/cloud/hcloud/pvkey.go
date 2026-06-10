package hcloud

import (
	"fmt"
	"strconv"

	"github.com/opencost/opencost/pkg/cloud/models"
)

// hcloudPVKey implements [models.PVKey] for Hetzner Cloud volumes.
type hcloudPVKey struct {
	providerID   string
	sizeBytes    int64
	storageClass string
	region       string
}

// ID implements [models.PVKey].
func (k *hcloudPVKey) ID() string {
	return k.providerID
}

// Features implements [models.PVKey]. Returns a comma-separated string of persistent volume features for pricing lookup in the format "${REGION},${SIZE_BYTES}", e.g. "fsn1,10485760".
func (k *hcloudPVKey) Features() string {
	return fmt.Sprintf("%s,%s", k.region, strconv.FormatInt(k.sizeBytes, 10))
}

// GetStorageClass implements [models.PVKey].
func (k *hcloudPVKey) GetStorageClass() string {
	return k.storageClass
}

// Interface guard
var _ models.PVKey = (*hcloudPVKey)(nil)
