package filterutil

import (
	"testing"

	"github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/util/mapper"
)

var assetCompiler = kubecost.NewAssetMatchCompiler()

func TestAssetFiltersFromParamsV1(t *testing.T) {
	cases := []struct {
		name           string
		qp             map[string]string
		shouldMatch    []kubecost.Asset
		shouldNotMatch []kubecost.Asset
	}{
		{
			name: "empty",
			qp:   map[string]string{},
			shouldMatch: []kubecost.Asset{
				&kubecost.Node{},
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.LoadBalancer{},
				&kubecost.ClusterManagement{},
				&kubecost.Disk{},
				&kubecost.Network{},
				&kubecost.SharedAsset{},
			},
			shouldNotMatch: []kubecost.Asset{},
		},
		{
			name: "type: node",
			qp: map[string]string{
				ParamFilterTypes: "node",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Node{},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.LoadBalancer{},
				&kubecost.ClusterManagement{},
				&kubecost.Disk{},
				&kubecost.Network{},
				&kubecost.SharedAsset{},
			},
		},
		{
			name: "type: node capitalized",
			qp: map[string]string{
				ParamFilterTypes: "Node",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Node{},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.LoadBalancer{},
				&kubecost.ClusterManagement{},
				&kubecost.Disk{},
				&kubecost.Network{},
				&kubecost.SharedAsset{},
			},
		},
		{
			name: "type: disk",
			qp: map[string]string{
				ParamFilterTypes: "disk",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.Network{},
				&kubecost.Node{},
				&kubecost.LoadBalancer{},
				&kubecost.ClusterManagement{},
				&kubecost.SharedAsset{},
			},
		},
		{
			name: "type: loadbalancer",
			qp: map[string]string{
				ParamFilterTypes: "loadbalancer",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.LoadBalancer{},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.Node{},
				&kubecost.ClusterManagement{},
				&kubecost.Disk{},
				&kubecost.Network{},
				&kubecost.SharedAsset{},
			},
		},
		{
			name: "type: clustermanagement",
			qp: map[string]string{
				ParamFilterTypes: "clustermanagement",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.ClusterManagement{},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.LoadBalancer{},
				&kubecost.Node{},
				&kubecost.Disk{},
				&kubecost.Network{},
				&kubecost.SharedAsset{},
			},
		},
		{
			name: "type: network",
			qp: map[string]string{
				ParamFilterTypes: "network",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Network{},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Any{},
				&kubecost.Cloud{},
				&kubecost.LoadBalancer{},
				&kubecost.ClusterManagement{},
				&kubecost.Node{},
				&kubecost.Disk{},
				&kubecost.SharedAsset{},
			},
		},
		{
			name: "account",
			qp: map[string]string{
				ParamFilterAccounts: "foo,bar",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Account: "foo",
					},
				},
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						Account: "bar",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						Account: "baz",
					},
				},
			},
		},
		{
			name: "category",
			qp: map[string]string{
				ParamFilterCategories: "Network,Compute",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						Category: kubecost.NetworkCategory,
					},
				},
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Category: kubecost.ComputeCategory,
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.ClusterManagement{
					Properties: &kubecost.AssetProperties{
						Category: kubecost.ManagementCategory,
					},
				},
			},
		},
		{
			name: "cluster",
			qp: map[string]string{
				ParamFilterClusters: "cluster-one",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.LoadBalancer{
					Properties: &kubecost.AssetProperties{
						Cluster: "cluster-one",
					},
				},
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Cluster: "cluster-one",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.ClusterManagement{
					Properties: &kubecost.AssetProperties{
						Cluster: "cluster-two",
					},
				},
			},
		},
		{
			name: "project",
			qp: map[string]string{
				ParamFilterProjects: "proj1,proj2",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{
					Properties: &kubecost.AssetProperties{
						Project: "proj1",
					},
				},
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Project: "proj2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.ClusterManagement{
					Properties: &kubecost.AssetProperties{
						Project: "proj3",
					},
				},
			},
		},
		{
			name: "provider",
			qp: map[string]string{
				ParamFilterProviders: "p1,p2",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{
					Properties: &kubecost.AssetProperties{
						Provider: "p1",
					},
				},
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						Provider: "p2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Provider: "p3",
					},
				},
			},
		},
		{
			name: "providerID v1",
			qp: map[string]string{
				ParamFilterProviderIDs: "p1,p2",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{
					Properties: &kubecost.AssetProperties{
						ProviderID: "p1",
					},
				},
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						ProviderID: "p2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						ProviderID: "p3",
					},
				},
			},
		},
		{
			name: "providerID v2",
			qp: map[string]string{
				ParamFilterProviderIDsV2: "p1,p2",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{
					Properties: &kubecost.AssetProperties{
						ProviderID: "p1",
					},
				},
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						ProviderID: "p2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						ProviderID: "p3",
					},
				},
			},
		},
		{
			name: "service",
			qp: map[string]string{
				ParamFilterServices: "p1,p2",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{
					Properties: &kubecost.AssetProperties{
						Service: "p1",
					},
				},
				&kubecost.Network{
					Properties: &kubecost.AssetProperties{
						Service: "p2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Service: "p3",
					},
				},
			},
		},
		{
			name: "label",
			qp: map[string]string{
				ParamFilterLabels: "foo:bar,baz:qux",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Disk{
					Labels: kubecost.AssetLabels{
						"foo": "bar",
						"baz": "other",
					},
				},
				&kubecost.Node{
					Labels: kubecost.AssetLabels{
						"baz": "qux",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.ClusterManagement{
					Labels: kubecost.AssetLabels{
						"baz": "other",
					},
				},
			},
		},
		{
			name: "region",
			qp: map[string]string{
				ParamFilterRegions: "r1,r2",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Node{
					Labels: kubecost.AssetLabels{
						"label_topology_kubernetes_io_region": "r1",
					},
				},
				&kubecost.Node{
					Labels: kubecost.AssetLabels{
						"label_topology_kubernetes_io_region": "r2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Node{
					Labels: kubecost.AssetLabels{
						"label_topology_kubernetes_io_region": "r3",
					},
				},
			},
		},
		{
			name: "complex",
			qp: map[string]string{
				ParamFilterRegions:  "r1,r2",
				ParamFilterTypes:    "node",
				ParamFilterAccounts: "a*",
			},
			shouldMatch: []kubecost.Asset{
				&kubecost.Node{
					Labels: kubecost.AssetLabels{
						"label_topology_kubernetes_io_region": "r1",
					},
					Properties: &kubecost.AssetProperties{
						Account: "a1",
					},
				},
				&kubecost.Node{
					Labels: kubecost.AssetLabels{
						"label_topology_kubernetes_io_region": "r2",
					},
					Properties: &kubecost.AssetProperties{
						Account: "a2",
					},
				},
			},
			shouldNotMatch: []kubecost.Asset{
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Account: "b1",
					},
				},
				&kubecost.Node{
					Properties: &kubecost.AssetProperties{
						Account: "3a",
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Convert map[string]string representation to the mapper
			// library type
			qpMap := mapper.NewMap()
			for k, v := range c.qp {
				qpMap.Set(k, v)
			}
			qpMapper := mapper.NewMapper(qpMap)

			clustersMap := mockClusterMap{
				m: map[string]*clusters.ClusterInfo{
					"mapped-cluster-ID-1": {
						ID:   "mapped-cluster-ID-ABC",
						Name: "cluster ABC",
					},
				},
			}

			filterTree := AssetFilterFromParamsV1(qpMapper, clustersMap)
			filter, err := assetCompiler.Compile(filterTree)
			if err != nil {
				t.Fatalf("compiling filter: %s", err)
			}
			for _, asset := range c.shouldMatch {
				if !filter.Matches(asset) {
					t.Errorf("should have matched: %s", asset.String())
				}
			}
			for _, asset := range c.shouldNotMatch {
				if filter.Matches(asset) {
					t.Errorf("incorrectly matched: %s", asset.String())
				}
			}
		})
	}
}
