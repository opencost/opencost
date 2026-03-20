package kubemodel

// @bingen:generate:TrafficDirection
type TrafficDirection string

const (
	TrafficDirectionEgress  TrafficDirection = "Egress"
	TrafficDirectionIngress TrafficDirection = "Ingress"
)

// @bingen:generate:TrafficType
type TrafficType string

const (
	TrafficTypeCrossZone   TrafficType = "CrossZone"
	TrafficTypeCrossRegion TrafficType = "CrossRegion"
	TrafficTypeInternet    TrafficType = "Internet"
	TrafficTypeNatGateway  TrafficType = "NatGateway"
)

// @bingen:generate:NetworkTrafficDetail
type NetworkTrafficDetail struct {
	PodUID           string           `json:"podUid"`
	Endpoint         string           `json:"endpoint,omitempty"` // destination service/IP, e.g. "aws-s3", "10.0.1.5" (AKA Service Name)
	TrafficDirection TrafficDirection `json:"trafficDirection"`   // "Egress" or "Ingress"
	TrafficType      TrafficType      `json:"trafficType"`        // "CrossZone", "CrossRegion", "Internet", "NatGateway"
	Bytes            float64          `json:"bytes"`
}
