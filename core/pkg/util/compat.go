package util

import (
	"net"

	v1 "k8s.io/api/core/v1"
)

// See https://kubernetes.io/docs/reference/labels-annotations-taints/

// Spot/preemptible node label constants across cloud providers.
const (
	// KarpenterCapacityTypeLabel is set by Karpenter on AWS and GCP.
	KarpenterCapacityTypeLabel = "karpenter.sh/capacity-type"

	// GKE native labels
	GKEPreemptibleLabel = "cloud.google.com/gke-preemptible"
	GKESpotLabel        = "cloud.google.com/gke-spot"

	// Azure VMSS priority label
	AzureSpotLabel = "kubernetes.azure.com/scalesetpriority"

	// Oracle OKE preemptible label
	OCIPreemptibleLabel = "oci.oraclecloud.com/oke-is-preemptible"
)

func GetZone(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelTopologyZone]; ok { // Label as of 1.17
		return labels[v1.LabelTopologyZone], true
	} else if _, ok := labels[v1.LabelZoneFailureDomain]; ok { // deprecated label
		return labels[v1.LabelZoneFailureDomain], true
	} else {
		return "", false
	}
}

func GetRegion(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelTopologyRegion]; ok { // Label as of 1.17
		return labels[v1.LabelTopologyRegion], true
	} else if _, ok := labels[v1.LabelZoneRegion]; ok { // deprecated label
		return labels[v1.LabelZoneRegion], true
	} else {
		return "", false
	}
}

func GetInstanceType(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelInstanceTypeStable]; ok {
		return labels[v1.LabelInstanceTypeStable], true
	} else if _, ok := labels[v1.LabelInstanceType]; ok {
		return labels[v1.LabelInstanceType], true
	} else {
		return "", false
	}
}

func GetOperatingSystem(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelOSStable]; ok {
		return labels[v1.LabelOSStable], true
	} else if _, ok := labels["beta.kubernetes.io/os"]; ok {
		return labels["beta.kubernetes.io/os"], true
	} else {
		return "", false
	}
}

func GetArchType(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelArchStable]; ok {
		return labels[v1.LabelArchStable], true
	} else if _, ok := labels["beta.kubernetes.io/arch"]; ok {
		return labels["beta.kubernetes.io/arch"], true
	} else {
		return "", false
	}
}

// IsPreemptible returns true if the node labels indicate a spot or preemptible
// instance. It covers GKE (preemptible + Spot VMs), AWS/GCP via Karpenter,
// Azure VMSS spot, and Oracle OKE preemptible nodes.
// this function does not currently support user set `SpotLabel` and SpotLabelValue`
// we could add this here via environment variables
func IsPreemptible(labels map[string]string) bool {
	if labels[GKEPreemptibleLabel] == "true" {
		return true
	}
	if labels[GKESpotLabel] == "true" {
		return true
	}
	if labels[KarpenterCapacityTypeLabel] == "spot" {
		return true
	}
	if labels[AzureSpotLabel] == "spot" {
		return true
	}
	if labels[OCIPreemptibleLabel] == "true" {
		return true
	}
	return false
}

func PrivateIPCheck(ip string) bool {
	ipAddress := net.ParseIP(ip)
	return ipAddress.IsPrivate()
}
