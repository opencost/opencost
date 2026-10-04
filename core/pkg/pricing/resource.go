package pricing

import (
	"fmt"
	"strings"
)

type Resource string

const (
	ResourceNil               Resource = ""
	ResourceNode              Resource = "node"
	ResourceCPU               Resource = "cpu"
	ResourceRAM               Resource = "ram"
	ResourceGPU               Resource = "gpu"
	ResourceStorage           Resource = "storage"
	ResourceCluster           Resource = "cluster"
	ResourceService           Resource = "service"
	ResourceLocalEgress       Resource = "local-egress"
	ResourceCrossZoneEgress   Resource = "x-zone-egress"
	ResourceCrossRegionEgress Resource = "x-region-egress"
	ResourceInternetEgress    Resource = "internet-egress"
	ResourceNATGatewayEgress  Resource = "nat-egress"
	ResourceNATGatewayIngress Resource = "nat-ingress"
)

func ParseResource(str string) (Resource, error) {
	switch strings.ToLower(str) {
	case string(ResourceNode):
		return ResourceNode, nil
	case string(ResourceCPU):
		return ResourceCPU, nil
	case string(ResourceRAM):
		return ResourceRAM, nil
	case string(ResourceGPU):
		return ResourceGPU, nil
	case string(ResourceStorage):
		return ResourceStorage, nil
	case string(ResourceCluster):
		return ResourceCluster, nil
	case string(ResourceService):
		return ResourceService, nil
	case string(ResourceLocalEgress):
		return ResourceLocalEgress, nil
	case string(ResourceCrossZoneEgress):
		return ResourceCrossZoneEgress, nil
	case string(ResourceCrossRegionEgress):
		return ResourceCrossRegionEgress, nil
	case string(ResourceInternetEgress):
		return ResourceInternetEgress, nil
	case string(ResourceNATGatewayEgress):
		return ResourceNATGatewayEgress, nil
	case string(ResourceNATGatewayIngress):
		return ResourceNATGatewayIngress, nil
	default:
		return ResourceNil, fmt.Errorf("unknown resource %q", str)
	}
}
