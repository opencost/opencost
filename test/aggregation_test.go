package costmodel_test

import (
	"log"
	"testing"

	"gotest.tools/assert"

	"github.com/kubecost/cost-model/cloud"
	costModel "github.com/kubecost/cost-model/costmodel"
)

func TestAggregation(t *testing.T) {
	cd1 := &costModel.CostData{
		Namespace: "test1",
		NodeName:  "testnode",
		NodeData: &cloud.Node{
			VCPUCost: "1.0",
			RAMCost:  "1.0",
		},
		RAMAllocation: []*costModel.Vector{&costModel.Vector{
			Timestamp: 10,
			Value:     1073741824,
		}},
		CPUAllocation: []*costModel.Vector{&costModel.Vector{
			Timestamp: 10,
			Value:     1.0,
		}},
		GPUReq: []*costModel.Vector{&costModel.Vector{}},
		PVCData: []*costModel.PersistentVolumeClaimData{
			&costModel.PersistentVolumeClaimData{
				Namespace:  "test1",
				VolumeName: "foo",
				Volume: &cloud.PV{
					Cost: "1.0",
					Size: "1073741824",
				},
				Values: []*costModel.Vector{&costModel.Vector{
					Timestamp: 10,
					Value:     1073741824,
				}},
			},
			&costModel.PersistentVolumeClaimData{
				Namespace:  "test1",
				VolumeName: "bar",
				Volume: &cloud.PV{
					Cost: "1.0",
					Size: "1073741824",
				},
				Values: []*costModel.Vector{&costModel.Vector{
					Timestamp: 10,
					Value:     1073741824,
				}},
			},
		},
	}
	cd2 := &costModel.CostData{
		Namespace: "test1",
		NodeName:  "testnode",
		NodeData: &cloud.Node{
			VCPUCost: "1.0",
			RAMCost:  "1.0",
		},
		RAMAllocation: []*costModel.Vector{&costModel.Vector{
			Timestamp: 10,
			Value:     1073741824,
		}},
		CPUAllocation: []*costModel.Vector{&costModel.Vector{
			Timestamp: 10,
			Value:     1.0,
		}},
		GPUReq: []*costModel.Vector{&costModel.Vector{}},
		PVCData: []*costModel.PersistentVolumeClaimData{
			&costModel.PersistentVolumeClaimData{
				Namespace:  "test1",
				VolumeName: "foo",
				Volume: &cloud.PV{
					Cost: "1.0",
					Size: "1073741824",
				},
				Values: []*costModel.Vector{&costModel.Vector{
					Timestamp: 10,
					Value:     1073741824,
				}},
			},
			&costModel.PersistentVolumeClaimData{
				Namespace:  "test1",
				VolumeName: "bar",
				Volume: &cloud.PV{
					Cost: "1.0",
					Size: "1073741824",
				},
				Values: []*costModel.Vector{&costModel.Vector{
					Timestamp: 10,
					Value:     1073741824,
				}},
			},
		},
	}

	costData := make(map[string]*costModel.CostData)
	costData["test1,foo,nginx,testnode"] = cd1
	costData["test1,bar,nginx,testnode"] = cd2
	agg := costModel.AggregateCostModel(costData, "namespace", "", false, 0.0, 1.0, nil)
	log.Printf("agg: %+v", agg["test1"])
	assert.Equal(t, agg["test1"].TotalCost, 8.0)
}
