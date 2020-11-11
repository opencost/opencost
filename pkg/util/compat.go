package util

import (
	v1 "k8s.io/api/core/v1"
)

func GetRegion(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelZoneRegion]; ok {
		return labels[v1.LabelZoneRegion], true
	} else if _, ok := labels["topology.kubernetes.io/region"]; ok { // Label as of 1.17
		return labels["topology.kubernetes.io/region"], true
	} else {
		return "", false
	}
}

func GetInstanceType(labels map[string]string) (string, bool) {
	if _, ok := labels[v1.LabelInstanceType]; ok {
		return labels[v1.LabelInstanceType], true
	} else if _, ok := labels["node.kubernetes.io/instance-type"]; ok {
		return labels["node.kubernetes.io/instance-type"], true
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
