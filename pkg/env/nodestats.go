package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	// Node Stats Client Configuration
	NodeStatsForceKubeProxyEnvVar = "NODESTATS_FORCE_KUBE_PROXY"
	NodeStatsLocalProxyEnvVar     = "NODESTATS_LOCAL_PROXY"
	NodeStatsInsecureEnvVar       = "NODESTATS_INSECURE"
	NodeStatsCertFileEnvVar       = "NODESTATS_CERT_FILE"
	NodeStatsKeyFileEnvVar        = "NODESTATS_KEY_FILE"
)

// IsNodeStatsForceKubeProxy returns true if the node stats client should force the kube proxy direct end
// point formatting
func IsNodeStatsForceKubeProxy() bool {
	return env.GetBool(NodeStatsForceKubeProxyEnvVar, false)
}

// GetNodeStatsLocalProxy returns the fully qualified local proxy endpoint for the node stats client IFF the proxyAPI
// is selected.
func GetNodeStatsLocalProxy() string {
	return env.Get(NodeStatsLocalProxyEnvVar, "")
}

// IsNodeStatsInsecure returns true if the node stats client should skip TLS verification
func IsNodeStatsInsecure() bool {
	return env.GetBool(NodeStatsInsecureEnvVar, false)
}

// GetNodeStatsCertFile returns the path of the cert file
func GetNodeStatsCertFile() string {
	return env.Get(NodeStatsCertFileEnvVar, "")
}

// GetNodeStatsKeyFile returns the path of the key file
func GetNodeStatsKeyFile() string {
	return env.Get(NodeStatsKeyFileEnvVar, "")
}
