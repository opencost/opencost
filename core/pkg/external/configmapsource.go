package external

import (
	"fmt"
	"maps"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigMapSource implements LabelSource for Kubernetes ConfigMaps.
type ConfigMapSource struct {
	cfg *Config
}

func (cms *ConfigMapSource) ExtractNodeLabels(data map[string]string) (map[string]string, error) {
	if cms.cfg == nil {
		return nil, fmt.Errorf("nil config")
	}

	nlCfg := cms.cfg.NodeLabelConfig()
	if nlCfg == nil {
		return nil, fmt.Errorf("no node label config")
	}

	cm := nlCfg.ConfigMapName()
	key := nlCfg.Key()
	route := nlCfg.Route()
	// Traditional ConfigMap — labels live directly in data.
	if key == "" && route == "" {
		return maps.Clone(data), nil
	}

	// route is optional for block scalar. A root yaml node can be the map of node labels.
	if key == "" && route != "" {
		return nil, fmt.Errorf("key must be set for block scalar configMap")
	}

	// Block-scalar ConfigMap — extract the YAML document from data[Key].
	raw, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in ConfigMap %s", key, cm)
	}

	labels, err := parseIt(raw, route)
	if err != nil {
		return nil, fmt.Errorf("error parsing the yaml: %w", err)
	}

	return labels, nil
}

func parseNormally(input []byte) (map[string]string, error) {
	var m map[string]string
	err := yaml.Unmarshal(input, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	return m, nil
}

func parseRoute(input []byte, routes []string) (map[string]string, error) {
	// 1. parse as map[string]any
	var m map[string]any
	err := yaml.Unmarshal(input, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to parse root yaml: %w", err)
	}

	// 2. traverse the yaml based on the route. error if any of the routes don't exist
	for _, route := range routes {
		value, ok := m[route]
		if !ok {
			return nil, fmt.Errorf("failed to locate route: %s within yaml", route)
		}

		innerMap, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("route at %s is not a map", route)
		}

		m = innerMap
	}

	// 3. Now that we've traversed the route, our `m` value can be marshalled back into yaml,
	// and then unmarshalled regularly
	targetBytes, err := yaml.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal route yaml block: %w", err)
	}

	return parseNormally(targetBytes)
}

func parseIt(yamlData string, routeStr string) (map[string]string, error) {
	// do all the validation stuff ...

	input := []byte(yamlData)

	routeStr = strings.TrimSpace(routeStr)
	if routeStr == "" {
		// No route provided; parse the root YAML as the labels map.
		return parseNormally(input)
	}

	// Split routes and drop any empty segments (e.g. leading/trailing dots).
	routes := strings.Split(routeStr, ".")

	// no routes, just parse yaml as is
	if len(routes) == 0 {
		return parseNormally(input)
	}

	// when there are empty segments error out
	// Eg: external..labels
	for _, r := range routes {
		if r == "" {
			return nil, fmt.Errorf("invalid route %q: empty segment found", routeStr)
		}
	}

	// parse with routes
	return parseRoute(input, routes)

}
