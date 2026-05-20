package kubemodel

import (
	"fmt"
	"maps"
	"math"
	"slices"
)

func Merge(kms1, kms2 *KubeModelSet) (*KubeModelSet, error) {
	if kms1 == nil && kms2 == nil {
		return nil, fmt.Errorf("both KubeModelSets are nil")
	}
	if kms1 == nil {
		return kms2, nil
	}
	if kms2 == nil {
		return kms1, nil
	}

	if kms1.Cluster != nil && kms2.Cluster != nil && kms1.Cluster.UID != kms2.Cluster.UID {
		return nil, fmt.Errorf(
			"cannot merge KubeModelSets from different clusters: %s vs %s",
			kms1.Cluster.UID, kms2.Cluster.UID)
	}

	windowStart := kms1.Window.Start
	if kms2.Window.Start.Before(windowStart) {
		windowStart = kms2.Window.Start
	}
	windowEnd := kms1.Window.End
	if kms2.Window.End.After(windowEnd) {
		windowEnd = kms2.Window.End
	}

	merged := NewKubeModelSet(windowStart, windowEnd)

	if kms1.Metadata != nil && kms2.Metadata != nil {
		if kms2.Metadata.CreatedAt.Before(kms1.Metadata.CreatedAt) {
			merged.Metadata.CreatedAt = kms2.Metadata.CreatedAt
		} else {
			merged.Metadata.CreatedAt = kms1.Metadata.CreatedAt
		}
		if kms2.Metadata.CompletedAt.After(kms1.Metadata.CompletedAt) {
			merged.Metadata.CompletedAt = kms2.Metadata.CompletedAt
		} else {
			merged.Metadata.CompletedAt = kms1.Metadata.CompletedAt
		}
		merged.Metadata.ObjectCount = kms1.Metadata.ObjectCount + kms2.Metadata.ObjectCount
		merged.Metadata.Diagnostics = append(
			append([]Diagnostic{}, kms1.Metadata.Diagnostics...),
			kms2.Metadata.Diagnostics...,
		)
	} else if kms1.Metadata != nil {
		merged.Metadata.CreatedAt = kms1.Metadata.CreatedAt
		merged.Metadata.CompletedAt = kms1.Metadata.CompletedAt
		merged.Metadata.ObjectCount = kms1.Metadata.ObjectCount
		merged.Metadata.Diagnostics = append([]Diagnostic{}, kms1.Metadata.Diagnostics...)
	} else if kms2.Metadata != nil {
		merged.Metadata.CreatedAt = kms2.Metadata.CreatedAt
		merged.Metadata.CompletedAt = kms2.Metadata.CompletedAt
		merged.Metadata.ObjectCount = kms2.Metadata.ObjectCount
		merged.Metadata.Diagnostics = append([]Diagnostic{}, kms2.Metadata.Diagnostics...)
	}

	merged.Cluster = kms1.Cluster
	if merged.Cluster == nil {
		merged.Cluster = kms2.Cluster
	}

	mergeNamespaces(merged, kms1, kms2)
	mergeResourceQuotas(merged, kms1, kms2)
	mergeNodes(merged, kms1, kms2)
	mergePods(merged, kms1, kms2)
	mergeContainers(merged, kms1, kms2)
	mergeOwners(merged, kms1, kms2)
	mergeServices(merged, kms1, kms2)
	mergeVolumes(merged, kms1, kms2)
	mergePVCs(merged, kms1, kms2)
	mergeDevices(merged, kms1, kms2)
	mergeDeviceUsages(merged, kms1, kms2)

	return merged, nil
}

func mergeNamespaces(merged, kms1, kms2 *KubeModelSet) {
	for uid, ns := range kms1.Namespaces {
		merged.Namespaces[uid] = copyNamespace(ns)
		merged.idx.namespaceNameToID[ns.Name] = ns.UID
		merged.Metadata.ObjectCount++
	}
	for uid, ns2 := range kms2.Namespaces {
		if ns1, exists := merged.Namespaces[uid]; exists {
			// Merge Start/End timestamps for existing namespace
			if ns2.Start.Before(ns1.Start) {
				ns1.Start = ns2.Start
			}
			if ns2.End.After(ns1.End) {
				ns1.End = ns2.End
			}
		} else {
			merged.Namespaces[uid] = copyNamespace(ns2)
			merged.idx.namespaceNameToID[ns2.Name] = ns2.UID
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeResourceQuotas(merged, kms1, kms2 *KubeModelSet) {
	for uid, rq := range kms1.ResourceQuotas {
		merged.ResourceQuotas[uid] = copyResourceQuota(rq)
		merged.Metadata.ObjectCount++
	}
	for uid, rq2 := range kms2.ResourceQuotas {
		if rq1, exists := merged.ResourceQuotas[uid]; exists {
			// Merge Start/End timestamps for existing resource quota
			if rq2.Start.Before(rq1.Start) {
				rq1.Start = rq2.Start
			}
			if rq2.End.After(rq1.End) {
				rq1.End = rq2.End
			}
		} else {
			merged.ResourceQuotas[uid] = copyResourceQuota(rq2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeNodes(merged, kms1, kms2 *KubeModelSet) {
	for uid, node := range kms1.Nodes {
		merged.Nodes[uid] = copyNode(node)
		merged.Metadata.ObjectCount++
	}
	for uid, node2 := range kms2.Nodes {
		if node1, exists := merged.Nodes[uid]; exists {
			node1.CpuMillicoreSeconds += node2.CpuMillicoreSeconds
			node1.RAMByteSeconds += node2.RAMByteSeconds
			node1.CpuMillicoreUsageMax = max(node1.CpuMillicoreUsageMax, node2.CpuMillicoreUsageMax)
			node1.RAMByteUsageMax = max(node1.RAMByteUsageMax, node2.RAMByteUsageMax)
			node1.DurationSeconds += node2.DurationSeconds

			if node2.Start.Before(node1.Start) {
				node1.Start = node2.Start
			}
			if node2.End.After(node1.End) {
				node1.End = node2.End
			}

			for volumeUID, volume2 := range node2.AttachedVolumes {
				if volume1, exists := node1.AttachedVolumes[volumeUID]; exists {
					volume1.UsageByteSeconds += volume2.UsageByteSeconds
					volume1.DurationSeconds += volume2.DurationSeconds
					if volume2.CapacityBytes > volume1.CapacityBytes {
						volume1.CapacityBytes = volume2.CapacityBytes
					}
				} else {
					node1.AttachedVolumes[volumeUID] = &NodeVolumeUsage{
						VolumeUID:        volume2.VolumeUID,
						CapacityBytes:    volume2.CapacityBytes,
						UsageByteSeconds: volume2.UsageByteSeconds,
						VolumeType:       volume2.VolumeType,
						ProviderID:       volume2.ProviderID,
						DurationSeconds:  volume2.DurationSeconds,
					}
				}
			}
		} else {
			merged.Nodes[uid] = copyNode(node2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergePods(merged, kms1, kms2 *KubeModelSet) {
	for uid, pod := range kms1.Pods {
		merged.Pods[uid] = copyPod(pod)
		merged.Metadata.ObjectCount++
	}
	for uid, pod2 := range kms2.Pods {
		if pod1, exists := merged.Pods[uid]; exists {
			pod1.NetworkReceiveBytes += pod2.NetworkReceiveBytes
			pod1.NetworkTransferBytes += pod2.NetworkTransferBytes
			pod1.DurationSeconds += pod2.DurationSeconds

			if pod2.Start.Before(pod1.Start) {
				pod1.Start = pod2.Start
			}
			if pod2.End.After(pod1.End) {
				pod1.End = pod2.End
			}
		} else {
			merged.Pods[uid] = copyPod(pod2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeContainers(merged, kms1, kms2 *KubeModelSet) {
	for uid, container := range kms1.Containers {
		merged.Containers[uid] = copyContainer(container)
		merged.Metadata.ObjectCount++
	}
	for uid, container2 := range kms2.Containers {
		if container1, exists := merged.Containers[uid]; exists {
			container1.CpuMillicoreSeconds += container2.CpuMillicoreSeconds
			container1.RAMByteSeconds += container2.RAMByteSeconds
			container1.CpuMillicoreUsageMax = max(container1.CpuMillicoreUsageMax, container2.CpuMillicoreUsageMax)
			container1.RAMByteUsageMax = max(container1.RAMByteUsageMax, container2.RAMByteUsageMax)

			for volumeUID, ByteSeconds := range container2.VolumeStorageByteSeconds {
				container1.VolumeStorageByteSeconds[volumeUID] += ByteSeconds
			}
			for volumeUID, usageMax := range container2.VolumeStorageByteUsageMax {
				if currentMax, exists := container1.VolumeStorageByteUsageMax[volumeUID]; exists {
					container1.VolumeStorageByteUsageMax[volumeUID] = max(currentMax, usageMax)
				} else {
					container1.VolumeStorageByteUsageMax[volumeUID] = usageMax
				}
			}

			container1.CpuMillicoreRequestSeconds += container2.CpuMillicoreRequestSeconds
			container1.RAMByteSecondRequest += container2.RAMByteSecondRequest
			container1.CpuMillicoreLimitSeconds += container2.CpuMillicoreLimitSeconds
			container1.RAMByteSecondsLimit += container2.RAMByteSecondsLimit

			container1.DurationSeconds += container2.DurationSeconds

			// Merge Start/End timestamps
			if container2.Start.Before(container1.Start) {
				container1.Start = container2.Start
			}
			if container2.End.After(container1.End) {
				container1.End = container2.End
			}
		} else {
			merged.Containers[uid] = copyContainer(container2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeOwners(merged, kms1, kms2 *KubeModelSet) {
	for uid, owner := range kms1.Owners {
		merged.Owners[uid] = copyOwner(owner)
		merged.Metadata.ObjectCount++
	}
	for uid, owner2 := range kms2.Owners {
		if owner1, exists := merged.Owners[uid]; exists {
			if owner2.Start.Before(owner1.Start) {
				owner1.Start = owner2.Start
			}
			if owner2.End.After(owner1.End) {
				owner1.End = owner2.End
			}
		} else {
			merged.Owners[uid] = copyOwner(owner2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeServices(merged, kms1, kms2 *KubeModelSet) {
	for uid, svc := range kms1.Services {
		merged.Services[uid] = copyService(svc)
		merged.Metadata.ObjectCount++
	}
	for uid, svc2 := range kms2.Services {
		if svc1, exists := merged.Services[uid]; exists {
			svc1.NetworkTransferBytes += svc2.NetworkTransferBytes
			svc1.NetworkReceiveBytes += svc2.NetworkReceiveBytes
			svc1.DurationSeconds += svc2.DurationSeconds

			if svc2.Start.Before(svc1.Start) {
				svc1.Start = svc2.Start
			}
			if svc2.End.After(svc1.End) {
				svc1.End = svc2.End
			}
		} else {
			merged.Services[uid] = copyService(svc2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeVolumes(merged, kms1, kms2 *KubeModelSet) {
	for uid, vol := range kms1.Volumes {
		merged.Volumes[uid] = copyVolume(vol)
		merged.Metadata.ObjectCount++
	}
	for uid, vol2 := range kms2.Volumes {
		if vol1, exists := merged.Volumes[uid]; exists {
			if vol2.Start.Before(vol1.Start) {
				vol1.Start = vol2.Start
			}
			if vol2.End.After(vol1.End) {
				vol1.End = vol2.End
			}
			vol1.DurationSeconds += vol2.DurationSeconds
		} else {
			merged.Volumes[uid] = copyVolume(vol2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergePVCs(merged, kms1, kms2 *KubeModelSet) {
	for uid, pvc := range kms1.PersistentVolumeClaims {
		merged.PersistentVolumeClaims[uid] = copyPVC(pvc)
		merged.Metadata.ObjectCount++
	}
	for uid, pvc2 := range kms2.PersistentVolumeClaims {
		if pvc1, exists := merged.PersistentVolumeClaims[uid]; exists {
			pvc1.StorageByteSeconds += pvc2.StorageByteSeconds
			pvc1.ActualUsedByteSeconds += pvc2.ActualUsedByteSeconds
			pvc1.DurationSeconds += pvc2.DurationSeconds

			if pvc2.Start.Before(pvc1.Start) {
				pvc1.Start = pvc2.Start
			}
			if pvc2.End.After(pvc1.End) {
				pvc1.End = pvc2.End
			}
			if pvc2.BoundAt.After(pvc1.BoundAt) {
				pvc1.BoundAt = pvc2.BoundAt
			}
		} else {
			merged.PersistentVolumeClaims[uid] = copyPVC(pvc2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeDevices(merged, kms1, kms2 *KubeModelSet) {
	for uid, dev := range kms1.Devices {
		merged.Devices[uid] = copyDevice(dev)
		merged.Metadata.ObjectCount++
	}
	for uid, dev2 := range kms2.Devices {
		if dev1, exists := merged.Devices[uid]; exists {
			dev1.UsageSeconds += dev2.UsageSeconds
			dev1.MemoryByteSeconds += dev2.MemoryByteSeconds
			dev1.PowerWattSeconds += dev2.PowerWattSeconds
			dev1.PowerWattMax = math.Max(dev1.PowerWattMax, dev2.PowerWattMax)
			dev1.DurationSeconds += dev2.DurationSeconds

			if dev2.Start.Before(dev1.Start) {
				dev1.Start = dev2.Start
			}
			if dev2.End.After(dev1.End) {
				dev1.End = dev2.End
			}
		} else {
			merged.Devices[uid] = copyDevice(dev2)
			merged.Metadata.ObjectCount++
		}
	}
}

func mergeDeviceUsages(merged, kms1, kms2 *KubeModelSet) {
	for uid, usage := range kms1.DeviceUsages {
		merged.DeviceUsages[uid] = copyDeviceUsage(usage)
		merged.Metadata.ObjectCount++
	}
	for uid, usage2 := range kms2.DeviceUsages {
		if usage1, exists := merged.DeviceUsages[uid]; exists {
			usage1.UsageSeconds += usage2.UsageSeconds
			usage1.MemoryByteSecondsUsed += usage2.MemoryByteSecondsUsed
			usage1.UsagePercentageMax = math.Max(usage1.UsagePercentageMax, usage2.UsagePercentageMax)
			usage1.DurationSeconds += usage2.DurationSeconds

			// Merge Start/End timestamps
			if usage2.Start.Before(usage1.Start) {
				usage1.Start = usage2.Start
			}
			if usage2.End.After(usage1.End) {
				usage1.End = usage2.End
			}
		} else {
			merged.DeviceUsages[uid] = copyDeviceUsage(usage2)
			merged.Metadata.ObjectCount++
		}
	}
}

func copyNamespace(ns *Namespace) *Namespace {
	return &Namespace{
		ClusterUID:  ns.ClusterUID,
		UID:         ns.UID,
		Name:        ns.Name,
		Labels:      maps.Clone(ns.Labels),
		Annotations: maps.Clone(ns.Annotations),
		Start:       ns.Start,
		End:         ns.End,
	}
}

func copyResourceQuota(rq *ResourceQuota) *ResourceQuota {
	copied := &ResourceQuota{
		UID:          rq.UID,
		Name:         rq.Name,
		NamespaceUID: rq.NamespaceUID,
		Start:        rq.Start,
		End:          rq.End,
	}
	if rq.Spec != nil {
		copied.Spec = &ResourceQuotaSpec{}
		if rq.Spec.Hard != nil {
			copied.Spec.Hard = &ResourceQuotaSpecHard{
				Requests: copyResourceQuantities(rq.Spec.Hard.Requests),
				Limits:   copyResourceQuantities(rq.Spec.Hard.Limits),
			}
		}
	}
	if rq.Status != nil {
		copied.Status = &ResourceQuotaStatus{}
		if rq.Status.Used != nil {
			copied.Status.Used = &ResourceQuotaStatusUsed{
				Requests: copyResourceQuantities(rq.Status.Used.Requests),
				Limits:   copyResourceQuantities(rq.Status.Used.Limits),
			}
		}
	}
	return copied
}

func copyResourceQuantities(rq ResourceQuantities) ResourceQuantities {
	if rq == nil {
		return nil
	}
	copied := make(ResourceQuantities, len(rq))
	for k, v := range rq {
		copied[k] = v
	}
	return copied
}

func copyNode(node *Node) *Node {
	copied := &Node{
		UID:                  node.UID,
		Name:                 node.Name,
		ProviderResourceUID:  node.ProviderResourceUID,
		Labels:               maps.Clone(node.Labels),
		Annotations:          maps.Clone(node.Annotations),
		CpuMillicoreSeconds:  node.CpuMillicoreSeconds,
		RAMByteSeconds:       node.RAMByteSeconds,
		CpuMillicoreUsageMax: node.CpuMillicoreUsageMax,
		RAMByteUsageMax:      node.RAMByteUsageMax,
		DurationSeconds:      node.DurationSeconds,
		AttachedVolumes:      make(map[string]*NodeVolumeUsage),
		Start:                node.Start,
		End:                  node.End,
	}

	for volumeUID, volume := range node.AttachedVolumes {
		copied.AttachedVolumes[volumeUID] = &NodeVolumeUsage{
			VolumeUID:        volume.VolumeUID,
			CapacityBytes:    volume.CapacityBytes,
			UsageByteSeconds: volume.UsageByteSeconds,
			VolumeType:       volume.VolumeType,
			ProviderID:       volume.ProviderID,
			DurationSeconds:  volume.DurationSeconds,
		}
	}

	return copied
}

func copyPod(pod *Pod) *Pod {
	return &Pod{
		UID:                  pod.UID,
		Name:                 pod.Name,
		NamespaceUID:         pod.NamespaceUID,
		OwnerUID:             pod.OwnerUID,
		NodeUID:              pod.NodeUID,
		Labels:               maps.Clone(pod.Labels),
		Annotations:          maps.Clone(pod.Annotations),
		NetworkReceiveBytes:  pod.NetworkReceiveBytes,
		NetworkTransferBytes: pod.NetworkTransferBytes,
		DurationSeconds:      pod.DurationSeconds,
		Start:                pod.Start,
		End:                  pod.End,
	}
}

func copyContainer(container *Container) *Container {
	return &Container{
		PodUID:                     container.PodUID,
		Name:                       container.Name,
		CpuMillicoreSeconds:        container.CpuMillicoreSeconds,
		RAMByteSeconds:             container.RAMByteSeconds,
		CpuMillicoreUsageMax:       container.CpuMillicoreUsageMax,
		RAMByteUsageMax:            container.RAMByteUsageMax,
		VolumeStorageByteSeconds:   maps.Clone(container.VolumeStorageByteSeconds),
		VolumeStorageByteUsageMax:  maps.Clone(container.VolumeStorageByteUsageMax),
		DurationSeconds:            container.DurationSeconds,
		CpuMillicoreRequestSeconds: container.CpuMillicoreRequestSeconds,
		RAMByteSecondRequest:       container.RAMByteSecondRequest,
		CpuMillicoreLimitSeconds:   container.CpuMillicoreLimitSeconds,
		RAMByteSecondsLimit:        container.RAMByteSecondsLimit,
		Start:                      container.Start,
		End:                        container.End,
	}
}

func copyOwner(owner *Owner) *Owner {
	return &Owner{
		UID:          owner.UID,
		Name:         owner.Name,
		NamespaceUID: owner.NamespaceUID,
		Kind:         owner.Kind,
		Labels:       maps.Clone(owner.Labels),
		Annotations:  maps.Clone(owner.Annotations),
		Start:        owner.Start,
		End:          owner.End,
	}
}

func copyService(svc *Service) *Service {
	return &Service{
		UID:                  svc.UID,
		NamespaceUID:         svc.NamespaceUID,
		Name:                 svc.Name,
		Type:                 svc.Type,
		Hostname:             svc.Hostname,
		Labels:               maps.Clone(svc.Labels),
		Annotations:          maps.Clone(svc.Annotations),
		NetworkTransferBytes: svc.NetworkTransferBytes,
		NetworkReceiveBytes:  svc.NetworkReceiveBytes,
		DurationSeconds:      svc.DurationSeconds,
		Selector:             maps.Clone(svc.Selector),
		Ports:                slices.Clone(svc.Ports),
		Start:                svc.Start,
		End:                  svc.End,
	}
}

func copyVolume(vol *PersistentVolume) *PersistentVolume {
	return &PersistentVolume{
		UID:                   vol.UID,
		ClusterUID:            vol.ClusterUID,
		Name:                  vol.Name,
		Namespace:             vol.Namespace,
		Labels:                maps.Clone(vol.Labels),
		Annotations:           maps.Clone(vol.Annotations),
		StorageClass:          vol.StorageClass,
		SizeBytes:             vol.SizeBytes,
		Type:                  vol.Type,
		CSIDriver:             vol.CSIDriver,
		ProviderVolumeID:      vol.ProviderVolumeID,
		AccessModes:           slices.Clone(vol.AccessModes),
		ReclaimPolicy:         vol.ReclaimPolicy,
		Region:                vol.Region,
		Zone:                  vol.Zone,
		Start:                 vol.Start,
		End:                   vol.End,
		DurationSeconds:       vol.DurationSeconds,
		NodeAffinity:          vol.NodeAffinity,
		ProvisionedIOPS:       vol.ProvisionedIOPS,
		ProvisionedThroughput: vol.ProvisionedThroughput,
		PerformanceMode:       vol.PerformanceMode,
	}
}

func copyPVC(pvc *PersistentVolumeClaim) *PersistentVolumeClaim {
	copied := &PersistentVolumeClaim{
		UID:                   pvc.UID,
		NamespaceUID:          pvc.NamespaceUID,
		Name:                  pvc.Name,
		Labels:                maps.Clone(pvc.Labels),
		Annotations:           maps.Clone(pvc.Annotations),
		StorageClass:          pvc.StorageClass,
		StorageByteSeconds:    pvc.StorageByteSeconds,
		RequestedBytes:        pvc.RequestedBytes,
		Size:                  pvc.Size,
		VolumeName:            pvc.VolumeName,
		AccessModes:           slices.Clone(pvc.AccessModes),
		Start:                 pvc.Start,
		End:                   pvc.End,
		BoundAt:               pvc.BoundAt,
		DurationSeconds:       pvc.DurationSeconds,
		ActualUsedByteSeconds: pvc.ActualUsedByteSeconds,
	}
	if pvc.VolumeUID != nil {
		volumeUID := *pvc.VolumeUID
		copied.VolumeUID = &volumeUID
	}
	if pvc.PodUID != nil {
		podUID := *pvc.PodUID
		copied.PodUID = &podUID
	}
	return copied
}

func copyDevice(dev *Device) *Device {
	return &Device{
		UID:               dev.UID,
		Type:              dev.Type,
		NodeUID:           dev.NodeUID,
		DeviceNumber:      dev.DeviceNumber,
		ModelName:         dev.ModelName,
		IsShared:          dev.IsShared,
		SharePercentage:   dev.SharePercentage,
		UsageSeconds:      dev.UsageSeconds,
		MemoryByteSeconds: dev.MemoryByteSeconds,
		PowerWattSeconds:  dev.PowerWattSeconds,
		PowerWattMax:      dev.PowerWattMax,
		DurationSeconds:   dev.DurationSeconds,
		Start:             dev.Start,
		End:               dev.End,
	}
}

func copyDeviceUsage(usage *DeviceUsage) *DeviceUsage {
	return &DeviceUsage{
		ContainerUID:          usage.ContainerUID,
		DeviceUID:             usage.DeviceUID,
		UsageSeconds:          usage.UsageSeconds,
		UsagePercentageMax:    usage.UsagePercentageMax,
		MemoryByteSecondsUsed: usage.MemoryByteSecondsUsed,
		DeviceType:            usage.DeviceType,
		DurationSeconds:       usage.DurationSeconds,
		Start:                 usage.Start,
		End:                   usage.End,
	}
}
