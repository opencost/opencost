package gcp

import (
	"regexp"
	"strings"

	"github.com/opencost/opencost/pkg/kubecost"
)

func IsK8s(labels map[string]string) bool {
	if _, ok := labels["goog-gke-volume"]; ok {
		return true
	}

	if _, ok := labels["goog-gke-node"]; ok {
		return true
	}

	if _, ok := labels["goog-k8s-cluster-name"]; ok {
		return true
	}

	return false
}

var parseProviderIDRx = regexp.MustCompile("^.+\\/(.+)?") // Capture "gke-cluster-3-default-pool-xxxx-yy" from "projects/###/instances/gke-cluster-3-default-pool-xxxx-yy"

func ParseProviderID(id string) string {
	match := parseProviderIDRx.FindStringSubmatch(id)
	if len(match) == 0 {
		return id
	}
	return match[len(match)-1]
}

func SelectCategory(service, description string) string {
	s := strings.ToLower(service)
	d := strings.ToLower(description)

	// Network descriptions
	if strings.Contains(d, "download") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "network") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "ingress") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "egress") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "static ip") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "external ip") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "load balanced") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "licensing fee") {
		return kubecost.OtherCategory
	}

	// Storage Descriptions
	if strings.Contains(d, "storage") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd capacity") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd iops") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd snapshot") {
		return kubecost.StorageCategory
	}

	// Service Defaults
	if strings.Contains(s, "storage") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "compute") {
		return kubecost.ComputeCategory
	}
	if strings.Contains(s, "sql") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "bigquery") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "kubernetes") {
		return kubecost.ManagementCategory
	} else if strings.Contains(s, "pub/sub") {
		return kubecost.NetworkCategory
	}

	return kubecost.OtherCategory
}
