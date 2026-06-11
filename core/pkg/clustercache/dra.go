package clustercache

import (
	"strconv"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Dynamic Resource Allocation (resource.k8s.io/v1) cache types. DRA is the
// requests/capacity half of device allocation: ResourceSlices publish what
// device capacity drivers advertise per node, and ResourceClaims record what
// was requested, what the scheduler allocated, and which pods reserved the
// allocation. Telemetry (e.g. DCGM) provides the other half — observed
// utilization and saturation — and the two join on driver-published device
// attributes such as the GPU UUID.

// ResourceSliceDevice is one device advertised by a ResourceSlice. Attribute
// and capacity values are stringified so the cache stays vendor-neutral;
// drivers publish identifiers (e.g. NVIDIA's GPU UUID) as attributes.
type ResourceSliceDevice struct {
	Name       string
	Attributes map[string]string
	Capacity   map[string]string
}

// ResourceSlice is a slim resource.k8s.io/v1 ResourceSlice.
type ResourceSlice struct {
	Name     string
	Driver   string
	Pool     string
	NodeName string
	Devices  []ResourceSliceDevice
}

// ResourceClaimDeviceRequest is one device request within a claim spec: the
// "requests" half of device allocation.
type ResourceClaimDeviceRequest struct {
	Name            string
	DeviceClassName string
	Count           int64
}

// ResourceClaimAllocatedDevice is one device the scheduler allocated to a
// claim, identified by driver/pool/device exactly as ResourceSlices publish
// them.
type ResourceClaimAllocatedDevice struct {
	Request string
	Driver  string
	Pool    string
	Device  string
}

// ResourceClaimConsumer is an object that reserved the claim (typically a
// pod).
type ResourceClaimConsumer struct {
	Resource string
	Name     string
	UID      string
}

// ResourceClaim is a slim resource.k8s.io/v1 ResourceClaim.
type ResourceClaim struct {
	UID              types.UID
	Name             string
	Namespace        string
	DeviceRequests   []ResourceClaimDeviceRequest
	Allocated        bool
	AllocatedDevices []ResourceClaimAllocatedDevice
	ReservedFor      []ResourceClaimConsumer
}

// deviceAttributeString flattens the DeviceAttribute union into a string.
func deviceAttributeString(attr resourcev1.DeviceAttribute) string {
	switch {
	case attr.StringValue != nil:
		return *attr.StringValue
	case attr.IntValue != nil:
		return strconv.FormatInt(*attr.IntValue, 10)
	case attr.BoolValue != nil:
		return strconv.FormatBool(*attr.BoolValue)
	case attr.VersionValue != nil:
		return *attr.VersionValue
	}
	return ""
}

// TransformResourceSlice converts a resource.k8s.io/v1 ResourceSlice into
// its slim cached form.
func TransformResourceSlice(slice *resourcev1.ResourceSlice) *ResourceSlice {
	if slice == nil {
		return nil
	}

	out := &ResourceSlice{
		Name:   slice.Name,
		Driver: slice.Spec.Driver,
		Pool:   slice.Spec.Pool.Name,
	}
	if slice.Spec.NodeName != nil {
		out.NodeName = *slice.Spec.NodeName
	}

	for _, device := range slice.Spec.Devices {
		cached := ResourceSliceDevice{Name: device.Name}
		if len(device.Attributes) > 0 {
			cached.Attributes = make(map[string]string, len(device.Attributes))
			for name, attr := range device.Attributes {
				cached.Attributes[string(name)] = deviceAttributeString(attr)
			}
		}
		if len(device.Capacity) > 0 {
			cached.Capacity = make(map[string]string, len(device.Capacity))
			for name, capacity := range device.Capacity {
				cached.Capacity[string(name)] = capacity.Value.String()
			}
		}
		out.Devices = append(out.Devices, cached)
	}

	return out
}

// TransformResourceClaim converts a resource.k8s.io/v1 ResourceClaim into
// its slim cached form.
func TransformResourceClaim(claim *resourcev1.ResourceClaim) *ResourceClaim {
	if claim == nil {
		return nil
	}

	out := &ResourceClaim{
		UID:       claim.UID,
		Name:      claim.Name,
		Namespace: claim.Namespace,
	}

	for _, request := range claim.Spec.Devices.Requests {
		cached := ResourceClaimDeviceRequest{Name: request.Name}
		// only the "exactly" form carries a class and count; the
		// firstAvailable form resolves to one of several subrequests and
		// its outcome is read from the allocation results instead
		if request.Exactly != nil {
			cached.DeviceClassName = request.Exactly.DeviceClassName
			cached.Count = request.Exactly.Count
		}
		out.DeviceRequests = append(out.DeviceRequests, cached)
	}

	if claim.Status.Allocation != nil {
		out.Allocated = true
		for _, result := range claim.Status.Allocation.Devices.Results {
			out.AllocatedDevices = append(out.AllocatedDevices, ResourceClaimAllocatedDevice{
				Request: result.Request,
				Driver:  result.Driver,
				Pool:    result.Pool,
				Device:  result.Device,
			})
		}
	}

	for _, consumer := range claim.Status.ReservedFor {
		out.ReservedFor = append(out.ReservedFor, ResourceClaimConsumer{
			Resource: consumer.Resource,
			Name:     consumer.Name,
			UID:      string(consumer.UID),
		})
	}

	return out
}
