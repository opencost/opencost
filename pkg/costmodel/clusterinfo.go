package costmodel

import (
	"fmt"

	cloudProvider "github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/config"
	"github.com/kubecost/cost-model/pkg/costmodel/clusters"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/thanos"
	"github.com/kubecost/cost-model/pkg/util/json"

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

// writeThanosFlags includes the configured thanos flags on the cluster info
func writeThanosFlags(clusterInfo map[string]string) {
	// Include Thanos Offset Duration if Applicable
	clusterInfo["thanosEnabled"] = fmt.Sprintf("%t", thanos.IsEnabled())
	if thanos.IsEnabled() {
		clusterInfo["thanosOffset"] = thanos.Offset()
	}
}

// localClusterInfoProvider gets the local cluster info from the cloud provider and kubernetes
type localClusterInfoProvider struct {
	k8s      kubernetes.Interface
	provider cloudProvider.Provider
}

// GetClusterInfo returns a string map containing the local cluster info
func (dlcip *localClusterInfoProvider) GetClusterInfo() map[string]string {
	data, err := dlcip.provider.ClusterInfo()

	// Ensure we create the info object if it doesn't exist
	if data == nil {
		data = make(map[string]string)
	}

	kc, ok := dlcip.k8s.(*kubernetes.Clientset)
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

// NewLocalClusterInfoProvider creates a new clusters.LocalClusterInfoProvider implementation for providing local
// cluster information
func NewLocalClusterInfoProvider(k8s kubernetes.Interface, cloud cloudProvider.Provider) clusters.ClusterInfoProvider {
	return &localClusterInfoProvider{
		k8s:      k8s,
		provider: cloud,
	}
}

// configuredClusterInfoProvider just provides the cluster info directly from the config file source.
type configuredClusterInfoProvider struct {
	config *config.ConfigFile
}

// GetClusterInfo returns a string map containing the local cluster info
func (ccip *configuredClusterInfoProvider) GetClusterInfo() map[string]string {
	clusterInfo := map[string]string{}

	data, err := ccip.config.Refresh()
	if err != nil {
		return clusterInfo
	}

	err = json.Unmarshal(data, &clusterInfo)
	if err != nil {
		log.Warningf("ClusterInfo failed to load from configuration: %s", err)
		return clusterInfo
	}

	return clusterInfo
}

// NewConfiguredClusterInfoProvider instantiates and returns a cluster info provider which loads cluster info from
// a config file.
func NewConfiguredClusterInfoProvider(config *config.ConfigFile) clusters.ClusterInfoProvider {
	return &configuredClusterInfoProvider{
		config: config,
	}
}

// clusterInfoWriteOnRequest writes the cluster info result to a config whenever it's requested
type clusterInfoWriteOnRequest struct {
	clusterInfo clusters.ClusterInfoProvider
	config      *config.ConfigFile
}

// GetClusterInfo returns a string map containing the local cluster info
func (ciw *clusterInfoWriteOnRequest) GetClusterInfo() map[string]string {
	cInfo := ciw.clusterInfo.GetClusterInfo()

	result, err := json.Marshal(cInfo)
	if err != nil {
		log.Warningf("Failed to write the cluster info: %s", err)
		return cInfo
	}

	err = ciw.config.Write(result)
	if err != nil {
		log.Warningf("Failed to write the cluster info to config: %s", err)
	}

	return cInfo
}

// NewClusterInfoWriteOnRequest instantiates and returns a cluster info provider which writes the cluster info to a configuration
// before each request.
func NewClusterInfoWriteOnRequest(clusterInfo clusters.ClusterInfoProvider, config *config.ConfigFile) clusters.ClusterInfoProvider {
	return &clusterInfoWriteOnRequest{
		clusterInfo: clusterInfo,
		config:      config,
	}
}
