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

	// yaml.v3 always unmarshals maps as map[string]interface{}, no dual-type handling needed.
	var doc map[string]interface{}
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, fmt.Errorf("failed to parse YAML from key %q in ConfigMap %s: %w", cms.cfg.Key, cms.cfg.ConfigMapName, err)
	}

	// Traverse the dot-separated route.
	var current interface{} = doc
	for _, segment := range strings.Split(cms.cfg.Route, ".") {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("route segment %q: parent is not a map in ConfigMap %s", segment, cms.cfg.ConfigMapName)
		}
		current, ok = m[segment]
		if !ok {
			return nil, fmt.Errorf("route segment %q not found in ConfigMap %s", segment, cms.cfg.ConfigMapName)
		}
	}

	// Convert the reached node to map[string]string.
	m, ok := current.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("route %q does not point to a map in ConfigMap %s", cms.cfg.Route, cms.cfg.ConfigMapName)
	}
	labels := make(map[string]string, len(m))
	for k, v := range m {
		labels[k] = fmt.Sprintf("%v", v)
	}

	return labels, nil
}
