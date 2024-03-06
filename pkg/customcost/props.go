package customcost

import (
	"fmt"
	"strings"
)

type CustomCostProperty string

// TODO add custom cost properties
const (
	CustomCostInvoiceEntityIDProp string = "invoiceEntityID"
	CustomCostAccountIDProp       string = "accountID"
	CustomCostProviderProp        string = "provider"
	CustomCostProviderIDProp      string = "providerID"
	CustomCostCategoryProp        string = "category"
	CustomCostServiceProp         string = "service"
)

func ParseCustomProperties(props []string) ([]CustomCostProperty, error) {
	properties := []CustomCostProperty{}
	added := make(map[CustomCostProperty]struct{})

	for _, prop := range props {
		property, err := ParseCustomCostProperty(prop)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse property: %w", err)
		}

		if _, ok := added[property]; !ok {
			added[property] = struct{}{}
			properties = append(properties, property)
		}
	}

	return properties, nil
}

func ParseCustomCostProperty(text string) (CustomCostProperty, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case "invoiceentityid":
		return CustomCostProperty(CustomCostInvoiceEntityIDProp), nil
	case "accountid":
		return CustomCostProperty(CustomCostAccountIDProp), nil
	case "provider":
		return CustomCostProperty(CustomCostProviderProp), nil
	case "providerid":
		return CustomCostProperty(CustomCostProviderIDProp), nil
	case "category":
		return CustomCostProperty(CustomCostCategoryProp), nil
	case "service":
		return CustomCostProperty(CustomCostServiceProp), nil
	}

	return "", fmt.Errorf("invalid custom cost property: %s", text)
}
