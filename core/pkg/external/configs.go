package external

// Config identifies a ConfigMap to watch and describes how to extract
// labels from its data. ConfigMapName is required; all other fields are optional.
// Set Key and Route only when labels are embedded inside a YAML document
// stored as a block-scalar value; leave both empty for a flat key/value ConfigMap.
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
