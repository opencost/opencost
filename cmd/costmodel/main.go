package main

import (
	"os"

	"github.com/kubecost/cost-model/pkg/cmd"
	"k8s.io/klog"
)

func main() {
	// runs the appropriate application mode using the default cost-model command
	// see: github.com/kubecost/cost-model/pkg/cmd package for details
	if err := cmd.Execute(nil); err != nil {
		klog.Fatal(err)
		os.Exit(1)
	}
}
