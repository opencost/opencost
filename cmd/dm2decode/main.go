//go:build dm2emitter

package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	dm2pb "github.com/opencost/opencost/protos/dm2"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file.pb.gz>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Decodes and displays DM2 protobuf files\n")
		os.Exit(1)
	}

	filename := os.Args[1]
	
	// Read the file
	bz, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file %s: %v\n", filename, err)
		os.Exit(1)
	}
	
	// Decompress
	gr, err := gzip.NewReader(bytes.NewReader(bz))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v\n", err)
		os.Exit(1)
	}
	defer gr.Close()
	
	raw, err := io.ReadAll(gr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading gzip data: %v\n", err)
		os.Exit(1)
	}
	
	// Unmarshal protobuf
	var cluster dm2pb.Cluster
	if err := proto.Unmarshal(raw, &cluster); err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshaling protobuf: %v\n", err)
		os.Exit(1)
	}
	
	// Display summary
	fmt.Printf("=== DM2 Cluster Snapshot ===\n")
	fmt.Printf("Cluster Name: %s\n", cluster.GetClusterName())
	fmt.Printf("Cluster UID:  %s\n", cluster.GetClusterUid())
	fmt.Printf("\n")
	
	// Count entities
	totalNamespaces := len(cluster.GetNamespaces())
	totalWorkloads := 0
	totalPods := 0
	totalContainers := 0
	
	for _, ns := range cluster.GetNamespaces() {
		totalWorkloads += len(ns.GetWorkloads())
		for _, wl := range ns.GetWorkloads() {
			totalPods += len(wl.GetPods())
			for _, pod := range wl.GetPods() {
				totalContainers += len(pod.GetContainers())
			}
		}
	}
	
	fmt.Printf("=== Summary ===\n")
	fmt.Printf("Namespaces:  %d\n", totalNamespaces)
	fmt.Printf("Workloads:   %d\n", totalWorkloads)
	fmt.Printf("Pods:        %d\n", totalPods)
	fmt.Printf("Containers:  %d\n", totalContainers)
	fmt.Printf("\n")
	
	// Display detailed hierarchy if requested
	if len(os.Args) > 2 && os.Args[2] == "-v" {
		fmt.Printf("=== Detailed Hierarchy ===\n")
		for _, ns := range cluster.GetNamespaces() {
			fmt.Printf("\nNamespace: %s (UID: %s)\n", ns.GetName(), ns.GetUid())
			
			for _, wl := range ns.GetWorkloads() {
				fmt.Printf("  Workload: %s [%s] (UID: %s)\n", wl.GetName(), wl.GetKind(), wl.GetUid())
				
				for _, pod := range wl.GetPods() {
					fmt.Printf("    Pod: %s (UID: %s, Node: %s)\n", pod.GetName(), pod.GetUid(), pod.GetNodeUid())
					
					for _, ctr := range pod.GetContainers() {
						fmt.Printf("      Container: %s (UID: %s, Image: %s)\n", ctr.GetName(), ctr.GetUid(), ctr.GetImage())
					}
				}
			}
		}
	} else {
		fmt.Printf("(Use -v flag for detailed hierarchy)\n")
	}
	
	// Display file info
	fmt.Printf("\n=== File Info ===\n")
	fmt.Printf("Filename:        %s\n", filename)
	fmt.Printf("Compressed size: %d bytes\n", len(bz))
	fmt.Printf("Raw proto size:  %d bytes\n", len(raw))
	compressionRatio := float64(len(raw)) / float64(len(bz))
	fmt.Printf("Compression:     %.2fx\n", compressionRatio)
}