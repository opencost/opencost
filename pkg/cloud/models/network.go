package models

import (
	"github.com/opencost/opencost/core/pkg/util"
)

// TODO: used for dynamic cloud provider price fetching.
// determine what identifies a load balancer in the json returned from the cloud provider pricing API call
// type LBKey interface {
// }

// NetworkKey represents metadata identifying a network resource and its topology (e.g. region, zone, cluster)
// for multi-dimensional network pricing resolution.
//
// Semantics of the NetworkKey interface methods:
//   - ID(): Primary identifier for exact matching and caching (zone if present, otherwise region).
//   - Features(): Comma-separated string encoding available topology dimensions
//     (e.g. "us-east-1,us-east-1a", "us-east-1", or "us-east-1a").
//   - GetZone(): Returns the availability zone string (empty string when unspecified).
//   - GetRegion(): Returns the cloud region string (empty string when unspecified).
//   - GetClusterID(): Returns the cluster identifier string (empty string when unspecified).
//   - GetLabels(): Returns a copy of the node labels map.
type NetworkKey interface {
	ID() string
	Features() string
	GetZone() string
	GetRegion() string
	GetClusterID() string
	GetLabels() map[string]string
}

// DefaultNetworkKey is the standard implementation of NetworkKey derived from
// Kubernetes node labels and the cluster identifier.
type DefaultNetworkKey struct {
	Zone      string
	Region    string
	Labels    map[string]string
	ClusterID string
}

// NewNetworkKey constructs a NetworkKey by extracting zone and region from the
// provided node labels map, falling back to empty strings when labels are absent.
// The provided labels map is defensively cloned so future mutations of the input map don't affect the key.
func NewNetworkKey(labels map[string]string, clusterID string) NetworkKey {
	var cloned map[string]string
	if labels != nil {
		cloned = make(map[string]string, len(labels))
		for k, v := range labels {
			cloned[k] = v
		}
	} else {
		cloned = make(map[string]string)
	}

	zone, _ := util.GetZone(cloned)
	region, _ := util.GetRegion(cloned)
	return &DefaultNetworkKey{
		Zone:      zone,
		Region:    region,
		Labels:    cloned,
		ClusterID: clusterID,
	}
}

// ID returns the primary network topology identifier.
// It returns Zone when non-empty, otherwise Region.
func (n *DefaultNetworkKey) ID() string {
	if n == nil {
		return ""
	}
	if n.Zone != "" {
		return n.Zone
	}
	return n.Region
}

// Features returns a comma-separated string encoding available topology dimensions.
//   - Both Region and Zone set → "Region,Zone"
//   - Only Zone set            → "Zone"
//   - Only Region set          → "Region"
//   - Neither set              → ""
func (n *DefaultNetworkKey) Features() string {
	if n == nil {
		return ""
	}
	if n.Region != "" && n.Zone != "" {
		return n.Region + "," + n.Zone
	}
	if n.Zone != "" {
		return n.Zone
	}
	return n.Region
}

// GetZone returns the availability zone, or an empty string when unspecified.
func (n *DefaultNetworkKey) GetZone() string {
	if n == nil {
		return ""
	}
	return n.Zone
}

// GetRegion returns the cloud region, or an empty string when unspecified.
func (n *DefaultNetworkKey) GetRegion() string {
	if n == nil {
		return ""
	}
	return n.Region
}

// GetClusterID returns the cluster identifier, or an empty string when unspecified.
func (n *DefaultNetworkKey) GetClusterID() string {
	if n == nil {
		return ""
	}
	return n.ClusterID
}

// GetLabels returns a defensive copy of the node labels map.
func (n *DefaultNetworkKey) GetLabels() map[string]string {
	if n == nil || n.Labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(n.Labels))
	for k, v := range n.Labels {
		cloned[k] = v
	}
	return cloned
}

// Network is the interface by which the provider and cost model communicate network egress prices.
// The provider will best-effort try to fill out this struct.
type Network struct {
	ZoneNetworkEgressCost     float64
	RegionNetworkEgressCost   float64
	InternetNetworkEgressCost float64
	NatGatewayEgressCost      float64
	NatGatewayIngressCost     float64
}

// LoadBalancer is the interface by which the provider and cost model communicate LoadBalancer prices.
// The provider will best-effort try to fill out this struct.
type LoadBalancer struct {
	IngressIPAddresses []string `json:"IngressIPAddresses"`
	Cost               float64  `json:"hourlyCost"`
}
