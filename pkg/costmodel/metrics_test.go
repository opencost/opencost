package costmodel

import (
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
)

func TestGPUAllocationForRequest(t *testing.T) {
	tests := []struct {
		name    string
		request float64
		node    *models.Node
		want    float64
	}{
		{
			name:    "node not found keeps the request",
			request: 1,
			node:    nil,
			want:    1,
		},
		{
			name:    "dedicated GPUs keep the request",
			request: 2,
			node:    &models.Node{GPU: "4.000000", VGPU: "4.000000"},
			want:    2,
		},
		{
			name:    "time-sliced GPU with 4 replicas",
			request: 1,
			node:    &models.Node{GPU: "1.000000", VGPU: "4.000000"},
			want:    0.25,
		},
		{
			name:    "two replicas on a node with 2 GPUs shared 8 ways",
			request: 2,
			node:    &models.Node{GPU: "2.000000", VGPU: "16.000000"},
			want:    0.25,
		},
		{
			name:    "AWS vGPU",
			request: 5,
			node:    &models.Node{GPU: "1.000000", VGPU: "10.000000"},
			want:    0.5,
		},
		{
			name:    "missing VGPU keeps the request",
			request: 1,
			node:    &models.Node{GPU: "1.000000"},
			want:    1,
		},
		{
			name:    "zero VGPU keeps the request",
			request: 1,
			node:    &models.Node{GPU: "1.000000", VGPU: "0"},
			want:    1,
		},
		{
			name:    "unparsable GPU keeps the request",
			request: 1,
			node:    &models.Node{GPU: "", VGPU: "4.000000"},
			want:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := gpuAllocationForRequest(tt.request, tt.node); got != tt.want {
				t.Errorf("gpuAllocationForRequest(%v) = %v, want %v", tt.request, got, tt.want)
			}
		})
	}
}
