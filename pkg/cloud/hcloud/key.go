package hcloud

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/pkg/cloud/models"
)

// hcloudKey implements [models.Key] for Hetzner Cloud nodes.
type hcloudKey struct {
	providerID string
	labels     map[string]string
}

// ID implements [models.Key].
func (k *hcloudKey) ID() string {
	return k.providerID
}

// Features implements [models.Key]. Returns a comma-separated string of node features for pricing lookup in the format "${REGION},${INSTANCE_TYPE", e.g. "fsn1.cpx22".
func (k *hcloudKey) Features() string {
	region, _ := util.GetRegion(k.labels)
	instanceType, _ := util.GetInstanceType(k.labels)
	return fmt.Sprintf("%s,%s", region, instanceType)
}

// GPUType implements [models.Key]. Returns an empty string, as Hetzner Cloud does not currently offer GPU instances.
func (k *hcloudKey) GPUType() string {
	return ""
}

// GPUCount implements [models.Key]. Returns 0, as Hetzner Cloud does not currently offer GPU instances.
func (k *hcloudKey) GPUCount() int {
	return 0
}

// Interface guard
var _ models.Key = (*hcloudKey)(nil)
