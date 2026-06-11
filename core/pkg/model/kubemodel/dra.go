package kubemodel

import (
	"fmt"
	"strings"
)

// Dynamic Resource Allocation (resource.k8s.io/v1) model. Per the
// accelerator device design, device plugins / DRA provide the requests and
// capacity half of device allocation; telemetry (e.g. DCGM) provides the
// observed-utilization half. ResourceSlices record what device capacity
// drivers advertise per node; ResourceClaims record what was requested,
// what the scheduler allocated, and which pods reserved the allocation.
// The two halves join on driver-published device attributes: hydration
// resolves each allocated device's UUID from its slice so consumers can
// match claims to DCGMDevice telemetry directly.
//
// DeviceClass objects are intentionally not modeled: the class name is
// already captured on each request, and the class's selectors/config add
// no allocation or capacity information.
//
// Claims and slices are cluster state, not time series: the model carries
// the state observed at hydration time within the set's window.

// DRADeviceRequest is one device request within a claim spec.
// @bingen:generate:DRADeviceRequest
type DRADeviceRequest struct {
	Name            string `json:"name"`
	DeviceClassName string `json:"deviceClassName,omitempty"`
	Count           int64  `json:"count,omitempty"`
}

// DRAAllocatedDevice is one device the scheduler allocated to a claim.
// @bingen:generate:DRAAllocatedDevice
type DRAAllocatedDevice struct {
	Request string `json:"request"`
	Driver  string `json:"driver"`
	Pool    string `json:"pool"`
	Device  string `json:"device"`
	// DeviceUUID is resolved at hydration from the device's slice
	// attributes (any attribute named "uuid", optionally driver-qualified)
	// and links the allocation to telemetry such as DCGMDevice.UUID. Empty
	// when the driver publishes no UUID attribute.
	DeviceUUID string `json:"deviceUuid,omitempty"`
}

// DRAResourceClaim is the requests half of device allocation for one claim.
// @bingen:generate:DRAResourceClaim
type DRAResourceClaim struct {
	UID              string               `json:"uid"`
	Name             string               `json:"name"`
	Namespace        string               `json:"namespace"`
	DeviceRequests   []DRADeviceRequest   `json:"deviceRequests,omitempty"`
	Allocated        bool                 `json:"allocated"`
	AllocatedDevices []DRAAllocatedDevice `json:"allocatedDevices,omitempty"`
	// ReservedForPodUIDs lists the pods that reserved this claim,
	// associating allocated devices with workloads independently of
	// observed telemetry (a reserved-but-idle device appears here and
	// nowhere in DCGM usage).
	ReservedForPodUIDs []string `json:"reservedForPodUids,omitempty"`
}

// DRASliceDevice is one device advertised by a ResourceSlice, carrying the
// capacity half of allocation.
// @bingen:generate:DRASliceDevice
type DRASliceDevice struct {
	Name string `json:"name"`
	// UUID is the driver-published device identifier extracted from the
	// attributes (see DRAAllocatedDevice.DeviceUUID); empty when absent.
	UUID       string            `json:"uuid,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Capacity   map[string]string `json:"capacity,omitempty"`
}

// DRAResourceSlice is a driver's advertisement of device capacity for one
// pool/node.
// @bingen:generate:DRAResourceSlice
type DRAResourceSlice struct {
	Name     string           `json:"name"`
	Driver   string           `json:"driver"`
	Pool     string           `json:"pool"`
	NodeName string           `json:"nodeName,omitempty"`
	Devices  []DRASliceDevice `json:"devices,omitempty"`
}

// DeviceUUIDFromAttributes extracts a driver-published device UUID from a
// slice device's attributes: an attribute named exactly "uuid" or with a
// "/uuid" qualified-name suffix (e.g. "gpu.nvidia.com/uuid").
func DeviceUUIDFromAttributes(attributes map[string]string) string {
	if uuid, ok := attributes["uuid"]; ok {
		return uuid
	}
	for name, value := range attributes {
		if strings.HasSuffix(name, "/uuid") {
			return value
		}
	}
	return ""
}

// Validate validates the DRAResourceClaim fields.
func (c *DRAResourceClaim) Validate() error {
	if c.UID == "" {
		return fmt.Errorf("UID is missing for DRAResourceClaim '%s/%s'", c.Namespace, c.Name)
	}
	if c.Name == "" {
		return fmt.Errorf("name is missing for DRAResourceClaim '%s'", c.UID)
	}
	if !c.Allocated && len(c.AllocatedDevices) > 0 {
		return fmt.Errorf("DRAResourceClaim '%s/%s' has allocated devices but is not marked allocated", c.Namespace, c.Name)
	}
	for _, request := range c.DeviceRequests {
		if request.Count < 0 {
			return fmt.Errorf("DRAResourceClaim '%s/%s' request '%s' has negative count %d", c.Namespace, c.Name, request.Name, request.Count)
		}
	}
	return nil
}

// Validate validates the DRAResourceSlice fields.
func (s *DRAResourceSlice) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is missing for DRAResourceSlice (driver '%s', pool '%s')", s.Driver, s.Pool)
	}
	if s.Driver == "" {
		return fmt.Errorf("driver is missing for DRAResourceSlice '%s'", s.Name)
	}
	return nil
}

// RegisterDRAResourceClaim validates and adds a claim to the set, keyed by
// UID.
func (kms *KubeModelSet) RegisterDRAResourceClaim(claim *DRAResourceClaim) error {
	if err := claim.Validate(); err != nil {
		err = fmt.Errorf("RegisterDRAResourceClaim: invalid claim: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.ResourceClaims[claim.UID]; !ok {
		kms.ResourceClaims[claim.UID] = claim
		kms.Metadata.ObjectCount++
	}

	return nil
}

// RegisterDRAResourceSlice validates and adds a slice to the set, keyed by
// name.
func (kms *KubeModelSet) RegisterDRAResourceSlice(slice *DRAResourceSlice) error {
	if err := slice.Validate(); err != nil {
		err = fmt.Errorf("RegisterDRAResourceSlice: invalid slice: %w", err)
		kms.Error(err)
		return err
	}

	if _, ok := kms.ResourceSlices[slice.Name]; !ok {
		kms.ResourceSlices[slice.Name] = slice
		kms.Metadata.ObjectCount++
	}

	return nil
}
