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
	// Capture "pvc-00000000-0000-0000-0000-000000000001" from the GCE PD CSI volume handle
	// "projects/my-project/zones/us-central1-a/disks/pvc-00000000-0000-0000-0000-000000000001".
	// Regional disks use "regions/<region>" in place of "zones/<zone>".
	persistentVolumeGCPRegex = regexp.MustCompile("projects/[^/]+/(?:zones|regions)/[^/]+/disks/([^/]+)$")
	// Capture "pvc-00000000-0000-0000-0000-000000000001" from the Azure Disk CSI volume handle
	// "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Compute/disks/pvc-00000000-0000-0000-0000-000000000001".
	// Azure resource IDs are case-insensitive, so the match is too.
	persistentVolumeAzureRegex = regexp.MustCompile("(?i)/subscriptions/[^/]+/resourceGroups/[^/]+/providers/Microsoft\\.Compute/disks/([^/]+)$")

	persistentVolumeProviderIDRegexes = []*regexp.Regexp{
		persistentVolumeAWSRegex,
		persistentVolumeGCPRegex,
		persistentVolumeAzureRegex,
	}
)

// GetPVProviderID returns the provider ID of the given PV's underlying volume,
// normalized by ParsePVProviderID. If the PV has no recognized volume source, the PV
// name is used.
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
	return ParsePVProviderID(providerID)
}

// ParsePVProviderID reduces an AWS, GCP or Azure volume path to the bare volume name
// so that in-tree and CSI volumes produce the same provider ID. IDs that do not match
// a known format are returned unchanged.
func ParsePVProviderID(providerID string) string {
	for _, re := range persistentVolumeProviderIDRegexes {
		match := re.FindStringSubmatch(providerID)
		if len(match) >= 2 {
			return match[1]
		}
	}
	return providerID
}
