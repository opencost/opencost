package external

// Config stores the configuration for external labels.
// Currently, only NodeLabelConfig is supported, and ConfigMaps
// are the only supported source of external node labels.
type Config struct {
	nodeLabelConfig *NodeLabelConfig
}

func NewConfig(nodeCfg *NodeLabelConfig) *Config {
	return &Config{
		nodeLabelConfig: nodeCfg,
	}
}

// NodeLabelConfig returns the node label configuration.
// It returns nil if node labels are not configured.
func (c *Config) NodeLabelConfig() *NodeLabelConfig {
	if c == nil {
		return nil
	}
	return c.nodeLabelConfig
}

// HasNodeLabelConfig reports whether node labels are configured.
func (c *Config) HasNodeLabelConfig() bool {
	return c != nil && c.nodeLabelConfig != nil
}

// NodeLabelConfig identifies a ConfigMap to watch and describes how to extract
// labels from its data. ConfigMapName is required; all other fields are optional.
// Set Key and Route only when labels are embedded inside a YAML document
// stored as a block-scalar value; leave both empty for a flat key/value ConfigMap.
type NodeLabelConfig struct {
	// configMapName is the name of the ConfigMap to watch.
	configMapName string
	// namespace is the namespace of the ConfigMap. Defaults to the agent's own namespace when empty.
	namespace string
	// key is the ConfigMap data key that holds the YAML document (block-scalar ConfigMaps only).
	key string
	// route is the dot-separated path to the labels map within the parsed YAML document.
	route string
}

func NewNodeLabelConfig(
	configMapName string,
	namespace string,
	key string,
	route string,
) *NodeLabelConfig {
	return &NodeLabelConfig{
		configMapName: configMapName,
		namespace:     namespace,
		key:           key,
		route:         route,
	}
}

func (nlc *NodeLabelConfig) ConfigMapName() string {
	return nlc.configMapName
}

func (nlc *NodeLabelConfig) Namespace() string {
	return nlc.namespace
}

func (nlc *NodeLabelConfig) Key() string {
	return nlc.key
}

func (nlc *NodeLabelConfig) Route() string {
	return nlc.route
}
