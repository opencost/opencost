package opencost

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/log"
)

const (
	NetworkInsightsNodeUnknown  = "unknownNode"
	NetworkInsightsCloudService = "cloudService"
	NetworkInsightsOther        = "other"
	NetworkInsightsInternet     = "internet"
	NetworkInsightsSameZone     = "sameZone"
	NetworkInsightsSameRegion   = "sameRegion"
)

type NetworkInsightsEdge struct {
	Source string  `json:"source"`
	Target string  `json:"target"`
	Cost   float64 `json:"cost"`
	Bytes  float64 `json:"BytesInGiB"`
}

func NewNetworkInsightsEdge(source string,
	target string, cost float64, bytes float64) *NetworkInsightsEdge {
	return &NetworkInsightsEdge{
		Source: source,
		Target: target,
		Cost:   cost,
		Bytes:  bytes,
	}
}

func (nie *NetworkInsightsEdge) Key() string {
	return fmt.Sprintf("%s/%s", nie.Source, nie.Target)
}

func (nie *NetworkInsightsEdge) add(that *NetworkInsightsEdge) {
	nie.Cost += that.Cost
	nie.Bytes += that.Bytes
}

type NetworkInsightsNode struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	TotalEgress  float64 `json:"totalEgressInGiB"`
	TotalIngress float64 `json:"totalIngressInGiB"`
	Cost         float64 `json:"cost"`
	Internet     bool    `json:"internet"`
	SameZone     bool    `json:"sameZone"`
	SameRegion   bool    `json:"sameRegion"`
}

func NewNetworkInsightsNode(name string,
	networkType string, totalEgress float64, totalIngress float64, cost float64,
	internet, sameZone, sameRegion bool) *NetworkInsightsNode {
	return &NetworkInsightsNode{
		Name:         name,
		Type:         networkType,
		TotalEgress:  totalEgress,
		TotalIngress: totalIngress,
		Cost:         cost,
		Internet:     internet,
		SameZone:     sameZone,
		SameRegion:   sameRegion,
	}
}

func (nin *NetworkInsightsNode) Key() string {
	if nin.Name != NetworkInsightsServiceUnknown {
		return nin.Name
	}
	if nin.Name == NetworkInsightsServiceUnknown && nin.Internet {
		return NetworkInsightsInternet
	}
	if nin.Name == NetworkInsightsServiceUnknown && nin.SameZone {
		return NetworkInsightsSameZone
	}
	if nin.Name == NetworkInsightsServiceUnknown && nin.SameRegion {
		return NetworkInsightsSameRegion
	}
	return NetworkInsightsNodeUnknown
}

func (nin *NetworkInsightsNode) add(that *NetworkInsightsNode) {
	nin.TotalEgress += that.TotalEgress
	nin.TotalIngress += that.TotalIngress
	nin.Cost += that.Cost
}

type NetworkInsightsGraphData struct {
	Nodes map[string]*NetworkInsightsNode `json:"nodes"`
	Edges map[string]*NetworkInsightsEdge `json:"edges"`
}

type NetworkInsightsGraphDataResponse struct {
	Nodes map[string]*NetworkInsightsNode `json:"nodes"`
	Edges []*NetworkInsightsEdge          `json:"edges"`
}

func NewNetworkInsightsGraphData() *NetworkInsightsGraphData {
	return &NetworkInsightsGraphData{
		Nodes: make(map[string]*NetworkInsightsNode, 0),
		Edges: make(map[string]*NetworkInsightsEdge, 0),
	}
}

func (nigd *NetworkInsightsGraphData) AddNode(node *NetworkInsightsNode) {
	key := node.Key()
	if _, ok := nigd.Nodes[key]; ok {
		nigd.Nodes[key].add(node)
	} else {
		nigd.Nodes[key] = node
	}
}

func (nigd *NetworkInsightsGraphData) AddEdge(edge *NetworkInsightsEdge) {
	key := edge.Key()
	if _, ok := nigd.Edges[key]; ok {
		nigd.Edges[key].add(edge)
	} else {
		nigd.Edges[key] = edge
	}
}

func (nigd *NetworkInsightsGraphData) ToGraphDataResponse() *NetworkInsightsGraphDataResponse {
	if nigd == nil {
		return nil
	}
	nigdr := &NetworkInsightsGraphDataResponse{
		Nodes: nigd.Nodes,
		Edges: make([]*NetworkInsightsEdge, 0),
	}
	for _, edge := range nigd.Edges {
		nigdr.Edges = append(nigdr.Edges, edge)
	}
	return nigdr
}

// Converts the given NetworkInsightSetRange into the graph data to be presented.
func (nisr *NetworkInsightSetRange) ToNetworkInsightsGraphData(aggregateBy []NetworkInsightProperty) (*NetworkInsightsGraphData, error) {
	nis, err := nisr.newAccumulation(aggregateBy)
	if err != nil {
		return nil, err
	}

	if nis == nil {
		return nil, err
	}
	// No graph data for empty network insight set
	if len(nis.NetworkInsights) == 0 {
		return &NetworkInsightsGraphData{}, err
	}
	data := NewNetworkInsightsGraphData()

	graphNodeType := ConvertNetworkInsightPropertiesToString(aggregateBy)
	for _, ni := range nis.NetworkInsights {
		// When using filtering with network insight details
		// if network detail set is empty dont create node and edges
		if len(ni.NetworkDetails) == 0 {
			continue
		}
		key, err := ni.Key(aggregateBy)
		if err != nil {
			return &NetworkInsightsGraphData{}, err
		}
		baseNodeCost := 0.0
		for _, nd := range ni.NetworkDetails {
			destination := nd.EndPoint
			// TO-DO: At this time everything will be cloud service, can be a simply if to make it like IP etc here!
			networkType := NetworkInsightsCloudService
			sameZone, sameRegion, internet := getNetworkBools(nd.TrafficType)
			if destination == NetworkInsightsServiceUnknown {
				if internet {
					destination = NetworkInsightsInternet
				}
				if sameZone {
					destination = NetworkInsightsSameZone
				}
				if sameRegion {
					destination = NetworkInsightsSameRegion
				}
				networkType = NetworkInsightsOther
			}

			var communicatingNode *NetworkInsightsNode
			var edge *NetworkInsightsEdge
			if nd.TrafficDirection == NetworkTrafficDirectionNone {
				log.Warnf("ToNetworkInsightsGraphData: unknown traffic type: %s", nd.TrafficDirection)
				continue
			}
			if nd.TrafficDirection == NetworkTrafficDirectionEgress {
				communicatingNode = NewNetworkInsightsNode(destination, networkType, 0, nd.Bytes, 0, internet, sameZone, sameRegion)
				edge = NewNetworkInsightsEdge(key, destination, nd.Cost, nd.Bytes)
			}
			if nd.TrafficDirection == NetworkTrafficDirectionIngress {
				communicatingNode = NewNetworkInsightsNode(destination, networkType, nd.Bytes, 0, 0, internet, sameZone, sameRegion)
				edge = NewNetworkInsightsEdge(destination, key, nd.Cost, nd.Bytes)
			}
			baseNodeCost += nd.Cost
			data.AddNode(communicatingNode)
			data.AddEdge(edge)
		}
		baseNode := NewNetworkInsightsNode(key, graphNodeType, ni.GetTotalEgressByte(), ni.GetTotalIngressByte(), baseNodeCost, false, false, false)
		data.AddNode(baseNode)
	}

	return data, nil
}

func getNetworkBools(networkType NetworkTrafficType) (sameZone bool, sameRegion bool, internet bool) {
	switch networkType {
	case NetworkTrafficTypeCrossZone:
		return false, true, false
	case NetworkTrafficTypeCrossRegion:
		return false, false, false
	case NetworkTrafficTypeInternet:
		return false, false, true
	default:
		log.Warnf("unknown string passed: %s defaulting to internet", networkType)
		return false, false, true
	}
}
