package kubecost

import (
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
		expected          []Diff
	}{
		"added node":       {inputAssetsBefore: []Asset{node1, node2}, inputAssetsAfter: []Asset{node1, node2, node3}, expected: []Diff{{node3, "added"}}},
		"multiple adds":    {inputAssetsBefore: []Asset{node1, node2}, inputAssetsAfter: []Asset{node1, node2, node3, node4}, expected: []Diff{{node3, "added"}, {node4, "added"}}},
		"removed node":     {inputAssetsBefore: []Asset{node1, node2}, inputAssetsAfter: []Asset{node2}, expected: []Diff{{node1, "removed"}}},
		"multiple removes": {inputAssetsBefore: []Asset{node1, node2, node3}, inputAssetsAfter: []Asset{node2}, expected: []Diff{{node1, "removed"}, {node3, "removed"}}},
		"remove all":       {inputAssetsBefore: []Asset{node1, node2}, inputAssetsAfter: []Asset{}, expected: []Diff{{node1, "removed"}, {node2, "removed"}}},
		"add and remove":   {inputAssetsBefore: []Asset{node1, node2}, inputAssetsAfter: []Asset{node2, node3}, expected: []Diff{{node1, "removed"}, {node3, "added"}}},
		"no change":        {inputAssetsBefore: []Asset{node1, node2}, inputAssetsAfter: []Asset{node1, node2}, expected: []Diff{}},
		"order switch":     {inputAssetsBefore: []Asset{node2, node1}, inputAssetsAfter: []Asset{node1, node2}, expected: []Diff{}},
		"disk add":         {inputAssetsBefore: []Asset{disk1, node1}, inputAssetsAfter: []Asset{disk1, node1, disk2}, expected: []Diff{{disk2, "added"}}},
		"disk and node add": {inputAssetsBefore: []Asset{disk1, node1}, inputAssetsAfter: []Asset{disk1, node1, disk2, node2}, expected: []Diff{{disk2, "added"}, {node2, "added"}}},
		"disk and node removed": {inputAssetsBefore: []Asset{disk1, node1, disk2, node2}, inputAssetsAfter: []Asset{disk2, node2}, expected: []Diff{{disk1, "removed"}, {node1, "removed"}}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			as1 := NewAssetSet(start, end, tc.inputAssetsBefore...)
			as2 := NewAssetSet(start, end, tc.inputAssetsAfter...)

			result := DiffAsset(as1.Clone(), as2.Clone())

			diff := cmp.Diff(tc.expected, result) 

			if diff != "" {
				t.Fatalf(diff)
			}
			
		})
	}
	// as1 := NewAssetSet(start, end, 
	// 	NewNode("node1", "cluster1", "123abc", start, end, window1), 
	// 	NewNode("node2", "cluster1", "123abc", start, end, window1)
	// 	NewNode("node3", "cluster1", "123abc", start, end, window1))
	// as2 := NewAssetSet(start, end, 
	// 	NewNode("node2", "cluster1", "123abc", start, end, window1),
	// 	NewNode("node4", "cluster1", "123abc", start, end, window1))

	// t.Logf("testing")

	// result := DiffAsset(as1, as2)

	// for i := range result {
	// 	t.Logf("%+v", result[i])
	// }

}