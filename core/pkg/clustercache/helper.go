package clustercache

import (
	"regexp"
)

func GetLoadBalancerIngressAddress(service *Service) []string {
	var addresses []string
	for _, loadBalancerIngress := range service.Status.LoadBalancer.Ingress {
		address := loadBalancerIngress.IP
		// Some cloud providers use hostname rather than IP
		if address == "" {
			address = loadBalancerIngress.Hostname
		}
		addresses = append(addresses, address)

	}
	return addresses
}

// Capture "vol-0fc54c5e83b8d2b76" from "aws://us-east-2a/vol-0fc54c5e83b8d2b76"
var persistentVolumeAWSRegex = regexp.MustCompile("aws:/[^/]*/[^/]*/([^/]+)")

func GetPVProviderID(pv *PersistentVolume) string {
	providerID := pv.Name
	if pv.Spec.GCEPersistentDisk != nil {
		providerID = pv.Spec.GCEPersistentDisk.PDName
	} else if pv.Spec.AzureDisk != nil {
		providerID = pv.Spec.AzureDisk.DiskName
	} else if pv.Spec.AWSElasticBlockStore != nil {
		providerID = pv.Spec.AWSElasticBlockStore.VolumeID
		match := persistentVolumeAWSRegex.FindStringSubmatch(providerID)
		if len(match) >= 2 {
			providerID = match[1]
		}
	} else if pv.Spec.CSI != nil {
		providerID = pv.Spec.CSI.VolumeHandle
	}
	return providerID
}
