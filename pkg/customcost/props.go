package customcost

import (
	"fmt"
	"strings"
)

type CustomCostProperty string

const (
	CustomCostDomainProp CustomCostProperty = "domain"
)

func ParseCustomCostProperties(props []string) ([]string, error) {
	var properties []string
	added := make(map[CustomCostProperty]struct{})

	for _, prop := range props {
		property, err := ParseCustomCostProperty(prop)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse property: %w", err)
		}

		if _, ok := added[property]; !ok {
			added[property] = struct{}{}
			properties = append(properties, string(property))
		}
	}

	return properties, nil
}

func ParseCustomCostProperty(text string) (CustomCostProperty, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case strings.TrimSpace(strings.ToLower(string(CustomCostDomainProp))):
		return CustomCostDomainProp, nil
	}

	return "", fmt.Errorf("invalid custom cost property: %s", text)
}
