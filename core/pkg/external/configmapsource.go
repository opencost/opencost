package external

import (
	"fmt"
	"maps"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Kubernetes label key/value constraints.
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
var (
	// nameSegment: 1–63 chars, alphanumeric start/end, [-_.] allowed between.
	reNameSegment = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$`)

	// DNS label: 1–63 chars, alphanumeric start/end, hyphens allowed between.
	reDNSLabel = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)

	// label value: empty OR 1–63 chars with same rules as name segment.
	reLabelValue = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$`)
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

	labels, err := parse(raw, route)
	if err != nil {
		return nil, fmt.Errorf("error parsing the yaml: %w", err)
	}

	// Drop any keys or values that don't satisfy the Kubernetes label spec.
	return filterValidLabels(labels), nil
}

// filterValidLabels removes entries from labels whose key or value does not
// satisfy the Kubernetes label syntax rules. The map is mutated in place.
func filterValidLabels(labels map[string]string) map[string]string {
	for k, v := range labels {
		if validateLabelKey(k) != nil || validateLabelValue(v) != nil {
			delete(labels, k)
		}
	}
	return labels
}

// validateLabelKey checks the optional-prefix/name structure of a label key.
func validateLabelKey(key string) error {
	if key == "" {
		return fmt.Errorf("label key must not be empty")
	}

	prefix, name, hasSep := strings.Cut(key, "/")

	// No Prefix such as
	// app.kubernetes.io/name
	if !hasSep {
		// No prefix — the whole key is the name segment.
		if err := validateNameSegment(key); err != nil {
			return fmt.Errorf("invalid label key %q: %w", key, err)
		}
		return nil
	}

	if err := validateDNSSubdomain(prefix); err != nil {
		return fmt.Errorf("invalid label key %q: prefix is not a valid DNS subdomain: %w", key, err)
	}
	if err := validateNameSegment(name); err != nil {
		return fmt.Errorf("invalid label key %q: name segment: %w", key, err)
	}
	return nil
}

// validateNameSegment checks the name part of a label key (up to 63 chars).
func validateNameSegment(name string) error {
	if name == "" {
		return fmt.Errorf("name segment must not be empty")
	}
	if len(name) > 63 {
		return fmt.Errorf("name segment %q exceeds 63 characters", name)
	}
	if !reNameSegment.MatchString(name) {
		return fmt.Errorf("name segment %q must begin and end with an alphanumeric character and may only contain [-_.]", name)
	}
	return nil
}

// validateDNSSubdomain checks that s is a valid DNS subdomain (≤253 chars,
// dot-separated DNS labels each ≤63 chars).
func validateDNSSubdomain(s string) error {
	if s == "" {
		return fmt.Errorf("DNS subdomain must not be empty")
	}
	if len(s) > 253 {
		return fmt.Errorf("DNS subdomain %q exceeds 253 characters", s)
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" {
			return fmt.Errorf("DNS subdomain %q contains an empty label (consecutive or trailing dots)", s)
		}
		if len(label) > 63 {
			return fmt.Errorf("DNS subdomain %q: label %q exceeds 63 characters", s, label)
		}
		if !reDNSLabel.MatchString(label) {
			return fmt.Errorf("DNS subdomain %q: label %q must begin and end with an alphanumeric character and may only contain hyphens", s, label)
		}
	}
	return nil
}

// validateLabelValue checks a label value (empty is allowed; otherwise ≤63 chars).
func validateLabelValue(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > 63 {
		return fmt.Errorf("label value %q exceeds 63 characters", value)
	}
	if !reLabelValue.MatchString(value) {
		return fmt.Errorf("label value %q must begin and end with an alphanumeric character and may only contain [-_.]", value)
	}
	return nil
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

func parse(yamlData string, routeStr string) (map[string]string, error) {
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
