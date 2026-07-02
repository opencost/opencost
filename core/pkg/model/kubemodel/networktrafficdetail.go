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
)

// @bingen:generate:NetworkTrafficDetail
type NetworkTrafficDetail struct {
	PodUID           string           `json:"podUid"`
	Endpoint         string           `json:"endpoint,omitempty"`
	TrafficDirection TrafficDirection `json:"trafficDirection"`
	TrafficType      TrafficType      `json:"trafficType"`
	IsNatGateway     bool             `json:"isNatGateway"`
	Bytes            float64          `json:"bytes"`
}
