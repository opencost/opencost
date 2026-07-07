package externallabels

// Config holds the configuration for reading external labels from a ConfigMap.
// ConfigMapName is required; all other fields are optional.
//
// For traditional ConfigMaps (labels directly in data), leave Key and Route empty.
// For block-scalar ConfigMaps, set Key to the data entry holding the YAML document and
// Route to the dot-separated path to the labels map, e.g. "metadata.externalLabels".
type Config struct {
	// ConfigMapName is the name of the ConfigMap to watch.
	ConfigMapName string
	// Namespace is the namespace of the ConfigMap. Defaults to the agent's own namespace when empty.
	Namespace string
	// Key is the ConfigMap data key that holds the YAML document (block-scalar ConfigMaps only).
	Key string
	// Route is the dot-separated path to the labels map within the parsed YAML document.
	Route string
}
