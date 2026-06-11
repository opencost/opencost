package kubemodel

import (
	cc "github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
)

// DRA hydration. Claims and slices are Kubernetes API state, not time
// series, so they hydrate directly from the cluster cache rather than
// through the metrics queriers — identical behavior under either data
// source, and no Prometheus roundtrip for allocation state. The model
// carries the state observed at hydration time within the set's window.
//
// Hydration also performs the join between the two halves of device
// allocation: each allocated device in a claim is resolved to its
// driver-published UUID via the slices, which is the same identifier DCGM
// telemetry reports (DCGMDevice.UUID). RBAC note: the service account
// needs list/watch on resourceclaims and resourceslices in
// resource.k8s.io; without it (or without the API) the cache returns nil
// and hydration is a no-op.

// computeDRA hydrates DRA resource slices and claims from the cluster
// cache into the KubeModelSet.
func (km *KubeModel) computeDRA(kms *kubemodel.KubeModelSet) error {
	if km.clusterCache == nil {
		return nil
	}

	slices := km.clusterCache.GetAllResourceSlices()
	claims := km.clusterCache.GetAllResourceClaims()
	if len(slices) == 0 && len(claims) == 0 {
		return nil
	}

	modelSlices, deviceUUIDs := transformDRAResourceSlices(slices)
	for _, slice := range modelSlices {
		if err := kms.RegisterDRAResourceSlice(slice); err != nil {
			log.Warnf("Failed to register DRA resource slice: %s", err.Error())
		}
	}

	for _, claim := range transformDRAResourceClaims(claims, deviceUUIDs) {
		if err := kms.RegisterDRAResourceClaim(claim); err != nil {
			log.Warnf("Failed to register DRA resource claim: %s", err.Error())
		}
	}

	return nil
}

// draDeviceKey identifies a device as claims reference it: by driver, pool,
// and device name.
type draDeviceKey struct {
	driver string
	pool   string
	device string
}

// transformDRAResourceSlices converts cached slices to model slices and
// builds the (driver, pool, device) -> UUID index used to resolve claim
// allocations to telemetry identifiers.
func transformDRAResourceSlices(slices []*cc.ResourceSlice) ([]*kubemodel.DRAResourceSlice, map[draDeviceKey]string) {
	modelSlices := make([]*kubemodel.DRAResourceSlice, 0, len(slices))
	deviceUUIDs := make(map[draDeviceKey]string)

	for _, slice := range slices {
		modelSlice := &kubemodel.DRAResourceSlice{
			Name:     slice.Name,
			Driver:   slice.Driver,
			Pool:     slice.Pool,
			NodeName: slice.NodeName,
		}

		for _, device := range slice.Devices {
			uuid := kubemodel.DeviceUUIDFromAttributes(device.Attributes)
			if uuid != "" {
				deviceUUIDs[draDeviceKey{driver: slice.Driver, pool: slice.Pool, device: device.Name}] = uuid
			}
			modelSlice.Devices = append(modelSlice.Devices, kubemodel.DRASliceDevice{
				Name:       device.Name,
				UUID:       uuid,
				Attributes: device.Attributes,
				Capacity:   device.Capacity,
			})
		}

		modelSlices = append(modelSlices, modelSlice)
	}

	return modelSlices, deviceUUIDs
}

// transformDRAResourceClaims converts cached claims to model claims,
// resolving allocated devices to UUIDs and reducing consumers to pod UIDs.
func transformDRAResourceClaims(claims []*cc.ResourceClaim, deviceUUIDs map[draDeviceKey]string) []*kubemodel.DRAResourceClaim {
	modelClaims := make([]*kubemodel.DRAResourceClaim, 0, len(claims))

	for _, claim := range claims {
		modelClaim := &kubemodel.DRAResourceClaim{
			UID:       string(claim.UID),
			Name:      claim.Name,
			Namespace: claim.Namespace,
			Allocated: claim.Allocated,
		}

		for _, request := range claim.DeviceRequests {
			modelClaim.DeviceRequests = append(modelClaim.DeviceRequests, kubemodel.DRADeviceRequest{
				Name:            request.Name,
				DeviceClassName: request.DeviceClassName,
				Count:           request.Count,
			})
		}

		for _, allocated := range claim.AllocatedDevices {
			modelClaim.AllocatedDevices = append(modelClaim.AllocatedDevices, kubemodel.DRAAllocatedDevice{
				Request:    allocated.Request,
				Driver:     allocated.Driver,
				Pool:       allocated.Pool,
				Device:     allocated.Device,
				DeviceUUID: deviceUUIDs[draDeviceKey{driver: allocated.Driver, pool: allocated.Pool, device: allocated.Device}],
			})
		}

		for _, consumer := range claim.ReservedFor {
			// only pod consumers associate the claim with workloads; other
			// consumer kinds (rare) are intentionally dropped from the model
			if consumer.Resource == "pods" {
				modelClaim.ReservedForPodUIDs = append(modelClaim.ReservedForPodUIDs, consumer.UID)
			}
		}

		modelClaims = append(modelClaims, modelClaim)
	}

	return modelClaims
}
