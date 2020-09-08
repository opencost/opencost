package costmodel

import (
	"fmt"

	cloudProvider "github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/thanos"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

var (
	logCollectionEnabled    bool   = env.IsLogCollectionEnabled()
	productAnalyticsEnabled bool   = env.IsProductAnalyticsEnabled()
	errorReportingEnabled   bool   = env.IsErrorReportingEnabled()
	valuesReportingEnabled  bool   = env.IsValuesReportingEnabled()
	clusterProfile          string = env.GetClusterProfile()
)

// writeReportingFlags writes the reporting flags to the cluster info map
func writeReportingFlags(clusterInfo map[string]string) {
	clusterInfo["logCollection"] = fmt.Sprintf("%t", logCollectionEnabled)
	clusterInfo["productAnalytics"] = fmt.Sprintf("%t", productAnalyticsEnabled)
	clusterInfo["errorReporting"] = fmt.Sprintf("%t", errorReportingEnabled)
	clusterInfo["valuesReporting"] = fmt.Sprintf("%t", valuesReportingEnabled)
}

// writeClusterProfile writes the data associated with the cluster profile
func writeClusterProfile(clusterInfo map[string]string) {
	clusterInfo["clusterProfile"] = clusterProfile
}

func writeThanosFlags(clusterInfo map[string]string) {
	// Include Thanos Offset Duration if Applicable
	clusterInfo["thanosEnabled"] = fmt.Sprintf("%t", thanos.IsEnabled())
	if thanos.IsEnabled() {
		clusterInfo["thanosOffset"] = thanos.Offset()
	}
}

// GetClusterInfo provides specific information about the cluster cloud provider as well as
// generic configuration values.
func GetClusterInfo(kubeClient kubernetes.Interface, cloud cloudProvider.Provider) map[string]string {
	data, err := cloud.ClusterInfo()

	// Ensure we create the info object if it doesn't exist
	if data == nil {
		data = make(map[string]string)
	}

	kc, ok := kubeClient.(*kubernetes.Clientset)
	if ok && data != nil {
		v, err := kc.ServerVersion()
		if err != nil {
			klog.Infof("Could not get k8s version info: %s", err.Error())
		} else if v != nil {
			data["version"] = v.Major + "." + v.Minor
		}
	} else {
		klog.Infof("Could not get k8s version info: %s", err.Error())
	}

	writeClusterProfile(data)
	writeReportingFlags(data)
	writeThanosFlags(data)

	return data
}
