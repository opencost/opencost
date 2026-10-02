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

var (
	// Capture "vol-0fc54c5e83b8d2b76" from "aws://us-east-2a/vol-0fc54c5e83b8d2b76"
	persistentVolumeAWSRegex = regexp.MustCompile("aws:/[^/]*/[^/]*/([^/]+)")
	// Capture "pvc-8f38beb3-47ee-4fe0-978f-0ada2bb87eb1" from the GCE PD CSI volume handle
	// "projects/guestbook-227502/zones/us-central1-a/disks/pvc-8f38beb3-47ee-4fe0-978f-0ada2bb87eb1".
	// Regional disks use "regions/<region>" in place of "zones/<zone>".
	persistentVolumeGCPRegex = regexp.MustCompile("projects/[^/]+/(?:zones|regions)/[^/]+/disks/([^/]+)$")
	// Capture "pvc-8f38beb3-47ee-4fe0-978f-0ada2bb87eb1" from the Azure Disk CSI volume handle
	// "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Compute/disks/pvc-8f38beb3-47ee-4fe0-978f-0ada2bb87eb1".
	// Azure resource IDs are case-insensitive, so the match is too.
	persistentVolumeAzureRegex = regexp.MustCompile("(?i)/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\\.Compute/disks/([^/]+)$")

	persistentVolumeProviderIDRegexes = []*regexp.Regexp{
		persistentVolumeAWSRegex,
		persistentVolumeGCPRegex,
		persistentVolumeAzureRegex,
	}
)

func GetPVProviderID(pv *PersistentVolume) string {
	providerID := pv.Name
	if pv.Spec.GCEPersistentDisk != nil {
		providerID = pv.Spec.GCEPersistentDisk.PDName
	} else if pv.Spec.AzureDisk != nil {
		providerID = pv.Spec.AzureDisk.DiskName
	} else if pv.Spec.AWSElasticBlockStore != nil {
		providerID = pv.Spec.AWSElasticBlockStore.VolumeID
	} else if pv.Spec.CSI != nil {
		providerID = pv.Spec.CSI.VolumeHandle
	}

	// Reduce cloud resource paths to the bare volume name so that in-tree and CSI
	// volumes produce the same provider ID.
	for _, re := range persistentVolumeProviderIDRegexes {
		match := re.FindStringSubmatch(providerID)
		if len(match) >= 2 {
			return match[1]
		}
	}
	return providerID
}
