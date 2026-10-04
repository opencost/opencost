package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

func (km *KubeModel) computePods(kms *kubemodel.KubeModelSet, start, end time.Time) error {
	grp := source.NewQueryGroup()
	metrics := km.ds.Metrics()

	podInfoResultFuture := source.WithGroup(grp, metrics.QueryPodInfo(start, end))
	podUptimeResultFuture := source.WithGroup(grp, metrics.QueryPodUptime(start, end))
	podOwnerResultFuture := source.WithGroup(grp, metrics.QueryPodOwners(start, end))
	podPVCVolumesResultFuture := source.WithGroup(grp, metrics.QueryPodPVCVolumes(start, end))
	podLabelsResultFuture := source.WithGroup(grp, metrics.QueryPodLabels(start, end))
	podAnnosResultFuture := source.WithGroup(grp, metrics.QueryPodAnnotations(start, end))

	podNetworkEgressBytesResultFuture := source.WithGroup(grp, metrics.QueryPodNetworkEgressBytes(start, end))
	podNetworkIngressBytesResultFuture := source.WithGroup(grp, metrics.QueryPodNetworkIngressBytes(start, end))

	podMap := make(map[string]*kubemodel.Pod)

	podInfoResult, _ := podInfoResultFuture.Await()
	for _, res := range podInfoResult {
		podMap[res.UID] = &kubemodel.Pod{
			UID:          res.UID,
			Name:         res.Pod,
			NamespaceUID: res.NamespaceUID,
			NodeUID:      res.NodeUID,
		}
	}

	podUptimeResult, _ := podUptimeResultFuture.Await()
	for _, res := range podUptimeResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add uptime", res.UID)
			continue
		}
		s, e := res.GetStartEnd(start, end, km.ds.Resolution())
		pod.Start = s
		pod.End = e
	}

	podOwnersResult, _ := podOwnerResultFuture.Await()
	for _, res := range podOwnersResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		pod.Owners = append(pod.Owners, kubemodel.Owner{
			UID:        res.OwnerUID,
			Kind:       kubemodel.ParseOwnerKind(res.OwnerKind),
			Controller: res.Controller,
		})
	}

	podPVCVolumesResult, _ := podPVCVolumesResultFuture.Await()
	for _, res := range podPVCVolumesResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add PVC volumes", res.UID)
			continue
		}
		pod.PVCVolumes = append(pod.PVCVolumes, kubemodel.PodPVCVolume{
			Name:                     res.PodVolumeName,
			PersistentVolumeClaimUID: res.PVCUID,
		})
	}

	podLabelsResult, _ := podLabelsResultFuture.Await()
	for _, res := range podLabelsResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add labels", res.UID)
			continue
		}
		pod.Labels = res.Labels
	}

	podAnnosResult, _ := podAnnosResultFuture.Await()
	for _, res := range podAnnosResult {
		pod, ok := podMap[res.UID]
		if !ok {
			log.Warnf("pod with UID '%s' has not been initialized to add annotations", res.UID)
			continue
		}
		pod.Annotations = res.Annotations
	}

	appendDetail := func(uid string, dir kubemodel.TrafficDirection, tt kubemodel.TrafficType, isNatGateway bool, endpoint string, bytes float64) {
		pod, ok := podMap[uid]
		if !ok || bytes <= 0 {
			return
		}
		pod.NetworkTrafficDetails = append(pod.NetworkTrafficDetails, kubemodel.NetworkTrafficDetail{
			PodUID:           uid,
			TrafficDirection: dir,
			TrafficType:      tt,
			IsNatGateway:     isNatGateway,
			Endpoint:         endpoint,
			Bytes:            bytes,
		})
	}

	networkTrafficType := func(res *source.PodNetworkBytesResult) (kubemodel.TrafficType, bool) {
		if res.Internet {
			return kubemodel.TrafficTypeInternet, true
		}
		if !res.SameRegion {
			return kubemodel.TrafficTypeCrossRegion, true
		}
		if !res.SameZone {
			return kubemodel.TrafficTypeCrossZone, true
		}
		return "", false
	}

	podNetworkEgressResult, _ := podNetworkEgressBytesResultFuture.Await()
	for _, res := range podNetworkEgressResult {
		tt, ok := networkTrafficType(res)
		if !ok {
			continue
		}
		appendDetail(res.UID, kubemodel.TrafficDirectionEgress, tt, res.NatGateway, res.Service, res.Value)
	}

	podNetworkIngressResult, _ := podNetworkIngressBytesResultFuture.Await()
	for _, res := range podNetworkIngressResult {
		tt, ok := networkTrafficType(res)
		if !ok {
			continue
		}
		appendDetail(res.UID, kubemodel.TrafficDirectionIngress, tt, res.NatGateway, res.Service, res.Value)
	}

	for _, pod := range podMap {
		err := kms.RegisterPod(pod)
		if err != nil {
			log.Warnf("Failed to register pod: %s", err.Error())
		}
	}

	return nil
}
