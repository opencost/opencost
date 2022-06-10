package kubecost

import (
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestDiff(t *testing.T) {

	start := time.Now().AddDate(0, 0, -1)
	end :=  time.Now()
	window1 := NewWindow(&start, &end)

	node1 := NewNode("node1", "cluster1", "123abc", start, end, window1)
	node2 := NewNode("node2", "cluster1", "123abc", start, end, window1)
	node3 := NewNode("node3", "cluster1", "123abc", start, end, window1)
	node4 := NewNode("node4", "cluster1", "123abc", start, end, window1)
	disk1 := NewDisk("disk1", "cluster1", "123abc", start, end, window1)
	disk2 := NewDisk("disk2", "cluster1", "123abc", start, end, window1)

	cases := map[string]struct {
		inputAssetsBefore []Asset
		inputAssetsAfter  []Asset
		expected          []Diff[Asset]
	}{
		"added node":            {
			inputAssetsBefore: []Asset{node1, node2}, 
			inputAssetsAfter:  []Asset{node1, node2, node3}, 
			expected:          []Diff[Asset]{{node3, DiffAdded}},
		},
		"multiple adds":         {
			inputAssetsBefore: []Asset{node1, node2}, 
			inputAssetsAfter:  []Asset{node1, node2, node3, node4}, 
			expected:          []Diff[Asset]{{node3, DiffAdded}, {node4, DiffAdded}},
		},
		"removed node":          {
			inputAssetsBefore: []Asset{node1, node2}, 
			inputAssetsAfter:  []Asset{node2}, 
			expected:          []Diff[Asset]{{node1, DiffRemoved}},
		},
		"multiple removes":      {
			inputAssetsBefore: []Asset{node1, node2, node3}, 
			inputAssetsAfter:  []Asset{node2}, 
			expected:          []Diff[Asset]{{node1, DiffRemoved}, {node3, DiffRemoved}},
		},
		"remove all":            {
			inputAssetsBefore: []Asset{node1, node2}, 
			inputAssetsAfter:  []Asset{}, 
			expected:          []Diff[Asset]{{node1, DiffRemoved}, {node2, DiffRemoved}},
		},
		"add and remove":        {
			inputAssetsBefore: []Asset{node1, node2}, 
			inputAssetsAfter:  []Asset{node2, node3}, 
			expected:          []Diff[Asset]{{node1, DiffRemoved}, {node3, DiffAdded}},
		},
		"no change":             {
			inputAssetsBefore: []Asset{node1, node2}, 
			inputAssetsAfter:  []Asset{node1, node2}, 
			expected:          []Diff[Asset]{},
		},
		"order switch":          {
			inputAssetsBefore: []Asset{node2, node1}, 
			inputAssetsAfter:  []Asset{node1, node2}, 
			expected:          []Diff[Asset]{},
		},
		"disk add":              {
			inputAssetsBefore: []Asset{disk1, node1}, 
			inputAssetsAfter:  []Asset{disk1, node1, disk2}, 
			expected:          []Diff[Asset]{{disk2, DiffAdded}},
		},
		"disk and node add":     {
			inputAssetsBefore: []Asset{disk1, node1}, 
			inputAssetsAfter:  []Asset{disk1, node1, disk2, node2}, 
			expected:          []Diff[Asset]{{disk2, DiffAdded}, {node2, DiffAdded}},
		},
		"disk and node removed": {
			inputAssetsBefore: []Asset{disk1, node1, disk2, node2}, 
			inputAssetsAfter:  []Asset{disk2, node2}, 
			expected:          []Diff[Asset]{{disk1, DiffRemoved}, {node1, DiffRemoved}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			as1 := NewAssetSet(start, end, tc.inputAssetsBefore...)
			as2 := NewAssetSet(start, end, tc.inputAssetsAfter...)

			result := DiffAsset(as1.Clone(), as2.Clone())

			trans := cmp.Transformer("Sort", func(in []Diff[Asset]) []Diff[Asset] {
				out := append([]Diff[Asset](nil), in...) // Copy input to avoid mutating it
				sort.Slice(out, func(i, j int) bool {
					return out[i].Entity.Properties().Name < out[i].Entity.Properties().Name
				})
				return out
			})
			diff := cmp.Diff(tc.expected, result, trans) 

			if diff != "" {
				t.Fatalf(diff)
			}
			
		})
	}

}