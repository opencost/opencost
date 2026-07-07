package externallabels

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

const parseYAMLPrefix = "(parse_yaml)"

// ParseFunc returns a function compatible with ConfigMapWatcher.WatchFunc that
// parses a ConfigMap's data into a flat map[string]string of labels and forwards
// them to provider.
//
// Two modes are supported:
//
// Traditional — Key and Route are both empty. ConfigMap.data is used as-is;
// every key/value pair becomes a label.
//
// Block-scalar — Key names the data entry that holds an embedded YAML document.
// Route is a dot-separated path to the labels map within that document, prefixed
// with "(parse_yaml)", e.g. "(parse_yaml)prometheusK8s.externalLabels".
// Non-string values (bool, int, …) are coerced to their string representation same way Kubernetes does it.
func ParseFunc(cfg *Config, provider Provider) func(string, map[string]string) error {
	return func(name string, data map[string]string) error {
		// Traditional ConfigMap — labels live directly in data.
		if cfg.Key == "" && cfg.Route == "" {
			return provider.Update(name, data)
		}

		// Block-scalar ConfigMap — extract the YAML document from data[Key].
		raw, ok := data[cfg.Key]
		if !ok {
			return fmt.Errorf("ExternalLabels: key %q not found in ConfigMap %s", cfg.Key, name)
		}

		route := cfg.Route
		if !strings.HasPrefix(route, parseYAMLPrefix) {
			return fmt.Errorf("ExternalLabels: route %q must start with %q for block-scalar ConfigMaps", route, parseYAMLPrefix)
		}
		route = strings.TrimPrefix(route, parseYAMLPrefix)

		// yaml.v3 always unmarshals maps as map[string]interface{}, no dual-type handling needed.
		var doc map[string]interface{}
		if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
			return fmt.Errorf("ExternalLabels: failed to parse YAML from key %q in ConfigMap %s: %w", cfg.Key, name, err)
		}

		// Traverse the dot-separated route.
		var current interface{} = doc
		for _, segment := range strings.Split(route, ".") {
			m, ok := current.(map[string]interface{})
			if !ok {
				return fmt.Errorf("ExternalLabels: route segment %q: parent is not a map in ConfigMap %s", segment, name)
			}
			current, ok = m[segment]
			if !ok {
				return fmt.Errorf("ExternalLabels: route segment %q not found in ConfigMap %s", segment, name)
			}
		}

		// Convert the reached node to map[string]string.
		m, ok := current.(map[string]interface{})
		if !ok {
			return fmt.Errorf("ExternalLabels: route %q does not point to a map in ConfigMap %s", cfg.Route, name)
		}
		labels := make(map[string]string, len(m))
		for k, v := range m {
			labels[k] = fmt.Sprintf("%v", v)
		}

		return provider.Update(name, labels)
	}
}
