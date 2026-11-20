package env

import "github.com/opencost/opencost/core/pkg/env"

// External.NodeLabels environment variables configure the ConfigMap used to read custom node labels.
// Set EXTERNAL_NODELABELS_CONFIG_MAP_NAME to enable this feature. The namespace defaults to the
// agent's namespace if not specified.
// For block-scalar ConfigMaps, set EXTERNAL_NODELABELS_KEY to the data key containing the YAML
// document and EXTERNAL_NODELABELS_ROUTE to the path within the YAML document that contains the
// node labels.

const (
	ExternalNodeLabelsConfigMapNameEnvVar = "EXTERNAL_NODELABELS_CONFIG_MAP_NAME"
	ExternalNodeLabelsNamespaceEnvVar     = "EXTERNAL_NODELABELS_NAMESPACE"
	ExternalNodeLabelsKeyEnvVar           = "EXTERNAL_NODELABELS_KEY"
	ExternalNodeLabelsRouteEnvVar         = "EXTERNAL_NODELABELS_ROUTE"
)

// GetExternalNodeLabelsConfigMapName returns the name of the ConfigMap that contains the external node labels.
func GetExternalNodeLabelsConfigMapName() string {
	return env.Get(ExternalNodeLabelsConfigMapNameEnvVar, "")
}

// GetExternalNodeLabelsNamespace returns the namespace of the external node labels ConfigMap.
// An empty string means the agent's own namespace should be used.
func GetExternalNodeLabelsNamespace() string {
	return env.Get(ExternalNodeLabelsNamespaceEnvVar, "")
}

// GetExternalNodeLabelsKey returns the ConfigMap data key that holds the YAML document
// for block-scalar ConfigMaps. Empty for traditional ConfigMaps.
func GetExternalNodeLabelsKey() string {
	return env.Get(ExternalNodeLabelsKeyEnvVar, "")
}

// GetExternalNodeLabelsRoute returns the dot-separated path to the labels map within
// the parsed YAML document. Empty for traditional ConfigMaps.
func GetExternalNodeLabelsRoute() string {
	return env.Get(ExternalNodeLabelsRouteEnvVar, "")
}
