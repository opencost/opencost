package costmodel

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestIsValidNodeName(t *testing.T) {
	tests := []string{
		"ip-10-1-2-3.ec2.internal",
		"node-1",
		"another.test.node",
		"10-55.23-10",
	}

	for _, test := range tests {
		if !isValidNodeName(test) {
			t.Errorf("Expected %s to be a valid node name", test)
		}
	}

	chars := "abcdefghijklmnopqrstuvwxyz"
	longName := ""
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 255; i++ {
		longName += string(chars[r.Intn(len(chars))])
	}

	fails := []string{
		longName,
		"192.168.1.1:80",
		"10.0.0.1:443",
		"127.0.0.1:8080",
		"172.16.254.1:22",
		"0.0.0.0:5000",
		"::1:80",
		"2001:db8::1:443",
		"2001:0db8:85a3:0000:0000:8a2e:0370:7334:8080",
		"fe80::1:22",
		"10.1.2.3:10240",
		":::80",
		"node$-15",
		"not:valid",
		".hello-world",
		"hello-world.",
		"i--",
	}

	for _, fail := range fails {
		if isValidNodeName(fail) {
			t.Errorf("Expected %s to be an invalid node name", fail)
		}
	}
}

func TestGetGPUCount(t *testing.T) {
	tests := []struct {
		name          string
		node          *clustercache.Node
		expectedGPU   float64
		expectedVGPU  float64
		expectedError bool
	}{
		{
			name: "Standard NVIDIA GPU",
			node: &clustercache.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						"nvidia.com/gpu": resource.MustParse("2"),
					},
				},
			},
			expectedGPU:  2.0,
			expectedVGPU: 2.0,
		},
		{
			name: "NVIDIA GPU with GFD - renameByDefault=true",
			node: &clustercache.Node{
				Labels: map[string]string{
					"nvidia.com/gpu.replicas": "4",
					"nvidia.com/gpu.count":    "1",
				},
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						"nvidia.com/gpu.shared": resource.MustParse("4"),
					},
				},
			},
			expectedGPU:  1.0,
			expectedVGPU: 4.0,
		},
		{
			name: "NVIDIA GPU with GFD - renameByDefault=false",
			node: &clustercache.Node{
				Labels: map[string]string{
					"nvidia.com/gpu.replicas": "4",
					"nvidia.com/gpu.count":    "1",
				},
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						"nvidia.com/gpu": resource.MustParse("4"),
					},
				},
			},
			expectedGPU:  1.0,
			expectedVGPU: 4.0,
		},
		{
			name: "No GPU",
			node: &clustercache.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{},
				},
			},
			expectedGPU:  -1.0,
			expectedVGPU: -1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gpu, vgpu, err := getGPUCount(nil, tt.node)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedGPU, gpu)
				assert.Equal(t, tt.expectedVGPU, vgpu)
			}
		})
	}
}

func Test_CostData_GetController_CronJob(t *testing.T) {
	cases := []struct {
		name string
		cd   CostData

		expectedName          string
		expectedKind          string
		expectedHasController bool
	}{
		{
			name: "batch/v1beta1 CronJob Job name",
			cd: CostData{
				// batch/v1beta1 CronJobs create Jobs with a 10 character
				// timestamp appended to the end of the name.
				//
				// It looks like this:
				// CronJob: cronjob-1
				// Job: cronjob-1-1651057200
				// Pod: cronjob-1-1651057200-mf5c9
				Jobs: []string{"cronjob-1-1651057200"},
			},

			expectedName:          "cronjob-1",
			expectedKind:          "job",
			expectedHasController: true,
		},
		{
			name: "batch/v1 CronJob Job name",
			cd: CostData{
				// batch/v1CronJobs create Jobs with an 8 character timestamp
				// appended to the end of the name.
				//
				// It looks like this:
				// CronJob: cj-v1
				// Job: cj-v1-27517770
				// Pod: cj-v1-27517770-xkrgn
				Jobs: []string{"cj-v1-27517770"},
			},

			expectedName:          "cj-v1",
			expectedKind:          "job",
			expectedHasController: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			name, kind, hasController := c.cd.GetController()

			if name != c.expectedName {
				t.Errorf("Name mismatch. Expected: %s. Got: %s", c.expectedName, name)
			}
			if kind != c.expectedKind {
				t.Errorf("Kind mismatch. Expected: %s. Got: %s", c.expectedKind, kind)
			}
			if hasController != c.expectedHasController {
				t.Errorf("HasController mismatch. Expected: %t. Got: %t", c.expectedHasController, hasController)
			}
		})
	}
}

func TestGetContainerAllocation(t *testing.T) {
	cases := []struct {
		name           string
		req            *util.Vector
		used           *util.Vector
		allocationType string
		expected       []*util.Vector
	}{
		{
			name: "request > usage",
			req: &util.Vector{
				Value:     100,
				Timestamp: 1672531200,
			},
			used: &util.Vector{
				Value:     50,
				Timestamp: 1672531200,
			},
			allocationType: "RAM",
			expected: []*util.Vector{
				{
					Value:     100,
					Timestamp: 1672531200,
				},
			},
		},
		{
			name: "usage > request",
			req: &util.Vector{
				Value:     50,
				Timestamp: 1672531200,
			},
			used: &util.Vector{
				Value:     100,
				Timestamp: 1672531200,
			},
			allocationType: "RAM",
			expected: []*util.Vector{
				{
					Value:     100,
					Timestamp: 1672531200,
				},
			},
		},
		{
			name: "only request is non-nil",
			req: &util.Vector{
				Value:     100,
				Timestamp: 1672531200,
			},
			used:           nil,
			allocationType: "CPU",
			expected: []*util.Vector{
				{
					Value:     100,
					Timestamp: 1672531200,
				},
			},
		},
		{
			name: "only used is non-nil",
			req:  nil,
			used: &util.Vector{
				Value:     100,
				Timestamp: 1672531200,
			},
			allocationType: "CPU",
			expected: []*util.Vector{
				{
					Value:     100,
					Timestamp: 1672531200,
				},
			},
		},
		{
			name:           "both req and used are nil",
			req:            nil,
			used:           nil,
			allocationType: "GPU",
			expected: []*util.Vector{
				{
					Value:     0,
					Timestamp: float64(time.Now().UTC().Unix()),
				},
			},
		},
		{
			name: "NaN in request value",
			req: &util.Vector{
				Value:     math.NaN(),
				Timestamp: 1672531200,
			},
			used: &util.Vector{
				Value:     50,
				Timestamp: 1672531200,
			},
			allocationType: "RAM",
			expected: []*util.Vector{
				{
					Value:     50,
					Timestamp: 1672531200,
				},
			},
		},
		{
			name: "NaN in used value",
			req: &util.Vector{
				Value:     100,
				Timestamp: 1672531200,
			},
			used: &util.Vector{
				Value:     math.NaN(),
				Timestamp: 1672531200,
			},
			allocationType: "CPU",
			expected: []*util.Vector{
				{
					Value:     100,
					Timestamp: 1672531200,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// For the nil case, the timestamp is dynamic, so we need to handle it separately
			if tc.name == "both req and used are nil" {
				result := getContainerAllocation(tc.req, tc.used, tc.allocationType)
				if result[0].Value != 0 {
					t.Errorf("Expected value to be 0, but got %f", result[0].Value)
				}
				if time.Now().UTC().Unix()-int64(result[0].Timestamp) > 5 {
					t.Errorf("Expected timestamp to be recent, but it was not")
				}
				return
			}
			result := getContainerAllocation(tc.req, tc.used, tc.allocationType)
			if diff := cmp.Diff(tc.expected, result); diff != "" {
				t.Errorf("getContainerAllocation() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
