package external

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigMapSource implements LabelSource for Kubernetes ConfigMaps.
type ConfigMapSource struct {
	cfg *Config
}

func (cms *ConfigMapSource) Extract(data map[string]string) (map[string]string, error) {
	if cms.cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	// Traditional ConfigMap — labels live directly in data.
	if cms.cfg.Key == "" && cms.cfg.Route == "" {
		return data, nil
	}

	// Block-scalar ConfigMap — extract the YAML document from data[Key].
	raw, ok := data[cms.cfg.Key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in ConfigMap %s", cms.cfg.Key, cms.cfg.ConfigMapName)
	}

	// yaml.v3 always unmarshals to a yaml.Node type.
	// yaml.Unmarshal creates a root node whose first child is the
	// actual YAML document.
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(raw), &root); err != nil {
		return nil, fmt.Errorf("failed to parse YAML from key %q in ConfigMap %s: %w", cms.cfg.Key, cms.cfg.ConfigMapName, err)
	}

	current := &root

	if current.Kind == yaml.DocumentNode {
		if len(current.Content) == 0 {
			return nil, fmt.Errorf(
				"empty YAML document in key %q of ConfigMap %s",
				cms.cfg.Key,
				cms.cfg.ConfigMapName,
			)
		}

		current = current.Content[0]
	}

	// Traverse the dot-separated route.
	for _, segment := range strings.Split(cms.cfg.Route, ".") {
		if segment == "" {
			continue
		}

		if current.Kind != yaml.MappingNode {
			return nil, fmt.Errorf(
				"route segment %q: parent is not a map in ConfigMap %s",
				segment,
				cms.cfg.ConfigMapName,
			)
		}

		next, found := findMappingValue(current, segment)
		if !found {
			return nil, fmt.Errorf(
				"route segment %q not found in ConfigMap %s",
				segment,
				cms.cfg.ConfigMapName,
			)
		}

		current = next
	}

	if current.Kind != yaml.MappingNode {
		return nil, fmt.Errorf(
			"route %q does not point to a map in ConfigMap %s",
			cms.cfg.Route,
			cms.cfg.ConfigMapName,
		)
	}

	labels := make(map[string]string, len(current.Content)/2)

	// MappingNode.Content contains alternating key/value nodes:
	// [key1, value1, key2, value2, ...].
	for i := 0; i < len(current.Content); i += 2 {
		keyNode := current.Content[i]
		valueNode := current.Content[i+1]

		if keyNode.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf(
				"label key at line %d must be a scalar in ConfigMap %s",
				keyNode.Line,
				cms.cfg.ConfigMapName,
			)
		}

		value, err := yamlLabelValue(valueNode)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid label %q in ConfigMap %s: %w",
				keyNode.Value,
				cms.cfg.ConfigMapName,
				err,
			)
		}

		labels[keyNode.Value] = value
	}

	return labels, nil
}

func findMappingValue(node *yaml.Node, key string) (*yaml.Node, bool) {
	for i := 0; i < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]

		if keyNode.Kind == yaml.ScalarNode && keyNode.Value == key {
			return valueNode, true
		}
	}

	return nil, false
}

func yamlLabelValue(node *yaml.Node) (string, error) {
	if node.Kind != yaml.ScalarNode {
		switch node.Kind {
		// errors out when the map has a sequence type
		// labels:
		//   environments:
		//     - dev
		//     - staging
		//     - prod
		case yaml.SequenceNode:
			return "", fmt.Errorf(
				"lists are not supported as label values at line %d",
				node.Line,
			)
		// errors out when the map has a inner map:
		// labels:
		//   inner-map:
		//     my-key: "value"
		//     other-key: "value"
		case yaml.MappingNode:
			return "", fmt.Errorf(
				"nested maps are not supported as label values at line %d",
				node.Line,
			)
		// errors out when the map has a pointer to another part of the yaml
		// defaults: &defaults
		//   priority: 42
		//   environment: prod
		// labels: *defaults
		case yaml.AliasNode:
			return "", fmt.Errorf(
				"aliases are not supported as label values at line %d",
				node.Line,
			)
		// default case
		default:
			return "", fmt.Errorf(
				"unsupported YAML node kind %d at line %d",
				node.Kind,
				node.Line,
			)
		}
	}

	switch node.Tag {
	// Only supported values in the labelValue type. For now.
	case "!!str", "!!int", "!!float", "!!bool":
		return node.Value, nil
	// cannot have null in labels
	// eg:
	//  labels:
	//	 priority: ~
	case "!!null":
		return "", fmt.Errorf(
			"null values are not supported at line %d",
			node.Line,
		)
	// default case not supported any other.
	default:
		return "", fmt.Errorf(
			"unsupported scalar type %q at line %d",
			node.Tag,
			node.Line,
		)
	}
}
