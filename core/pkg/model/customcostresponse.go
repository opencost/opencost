package model

import "github.com/opencost/opencost/core/pkg/opencost"

// see design at https://link.excalidraw.com/l/ABLQ24dkKai/CBEQtjH6Mr
// for additional details on how these objects work in the context of
// opencost's plugin system

type CustomCostResponse struct {
	// provides metadata on the Custom CostResponse
	// deliberately left unstructured
	Metadata map[string]string
	// declared by plugin
	// eg snowflake == "data management",
	// datadog == "observability" etc
	// intended for top level agg
	Costsource string
	// the name of the custom cost source
	// e.g., "datadog"
	Domain string
	// the version of the Custom Cost response
	// is set by the plugin, will vary between
	// different plugins
	Version string
	// FOCUS billing currency
	Currency string
	// the window of the returned objects
	Window opencost.Window
	// array of CustomCosts
	Costs []*CustomCost
	// any errors in processing
	Errors []error
}

// designed to provide a superset of the FOCUS spec
// https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec/releases/latest/download/spec.pdf
type CustomCost struct {
	// provides metadata on the Custom CostResponse
	// deliberately left unstructured
	Metadata map[string]string
	// the region that the resource was incurred
	// corresponds to 'availability zone' of FOCUS
	Zone string
	// FOCUS billed Cost
	BilledCost float32
	// FOCUS billing account name
	AccountName string
	// FOCUS charge category
	ChargeCategory string
	// FOCUS charge description
	Description string
	// FOCUS List Cost
	ListCost float32
	// FOCUS List Unit Price
	ListUnitPrice float32
	// FOCUS Resource Name
	ResourceName string
	// FOCUS Resource type
	// if not set, assumed to be domain
	ResourceType string
	// ID of the individual cost. should be globally
	// unique. Assigned by plugin on read
	Id string
	// the provider's ID for the cost, if
	// available
	// FOCUS resource ID
	ProviderId string
	// the window of the returned specific
	// custom cost
	// equivalent to charge period start/end of FOCUS
	Window *opencost.Window
	// Returns key/value sets of labels
	// equivalent to Tags in focus spec
	Labels map[string]string
	// FOCUS usage quantity
	UsageQty float32
	// FOCUS usage Unit
	UsageUnit string
	// Optional struct to implement other focus
	// spec attributes
	ExtendedAttributes *ExtendedCustomCostAttributes
}

// These parts of the FOCUS spec are not expected
// to be implemented by every plugin
// however, if these bits of information are available,
// they should be provided
type ExtendedCustomCostAttributes struct {
	// FOCUS billing period start/end
	BillingPeriod *opencost.Window
	// FOCUS Billing Account ID
	AccountID string
	// FOCUS Charge Frequency
	ChargeFrequency string
	// FOCUS Charge Subcategory
	Subcategory string
	// FOCUS Commitment Discount Category
	CommitmentDiscountCategory string
	// FOCUS Commitment Discount ID
	CommitmentDiscountID string
	// FOCUS Commitment Discount Name
	CommitmentDiscountName string
	// FOCUS Commitment Discount Type
	CommitmentDiscountType string
	// FOCUS Effective Cost
	EffectiveCost float32
	// FOCUS Invoice Issuer
	InvoiceIssuer string
	// FOCUS Provider
	// if unset, assumed to be domain
	Provider string
	// FOCUS Publisher
	// if unset, assumed to be domain
	Publisher string
	// FOCUS Service Category
	// if unset, assumed to be cost source
	ServiceCategory string
	// FOCUS Service Name
	// if unset, assumed to be cost source
	ServiceName string
	// FOCUS SKU ID
	SkuID string
	// FOCUS SKU Price ID
	SkuPriceID string
	// FOCUS Sub Account ID
	SubAccountID string
	// FOCUS Sub Account Name
	SubAccountName string
	// FOCUS Pricing Quantity
	PricingQuantity float32
	// FOCUS Pricing Unit
	PricingUnit string
	// FOCUS Pricing Category
	PricingCategory string
}

func (c *CustomCostResponse) Clone() CustomCostResponse {
	win := c.GetWindow().Clone()
	costClones := []*CustomCost{}

	for _, cost := range c.GetCosts() {
		clone := cost.Clone()
		costClones = append(costClones, &clone)
	}

	errClones := []error{}
	for _, err := range c.GetErrors() {
		errClones = append(errClones, err)
	}
	return CustomCostResponse{
		Metadata:   cloneMap(c.GetMetadata()),
		Costsource: c.GetCostSource(),
		Domain:     c.GetDomain(),
		Version:    c.GetVersion(),
		Currency:   c.GetCurrency(),
		Window:     win,
		Costs:      costClones,
		Errors:     errClones,
	}
}

func cloneMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := map[string]string{}
	for key, val := range input {
		result[key] = val
	}
	return result
}

func (c *CustomCost) Clone() CustomCost {
	win := c.GetWindow().Clone()
	ext := c.GetExtendedAttributes().Clone()
	return CustomCost{
		Metadata:           cloneMap(c.GetMetadata()),
		Zone:               c.GetCostIncurredZone(),
		BilledCost:         c.GetBilledCost(),
		AccountName:        c.GetAccountName(),
		ChargeCategory:     c.GetChargeCategory(),
		Description:        c.GetDescription(),
		ListCost:           c.GetListCost(),
		ListUnitPrice:      c.GetListUnitPrice(),
		ResourceName:       c.GetResourceName(),
		ResourceType:       c.GetResourceType(),
		Id:                 c.GetID(),
		ProviderId:         c.GetProviderID(),
		Window:             &win,
		Labels:             cloneMap(c.GetLabels()),
		UsageQty:           c.GetUsageQuantity(),
		UsageUnit:          c.GetUsageUnit(),
		ExtendedAttributes: &ext,
	}
}
func (e *ExtendedCustomCostAttributes) Clone() ExtendedCustomCostAttributes {
	win := e.BillingPeriod.Clone()
	return ExtendedCustomCostAttributes{
		BillingPeriod:              &win,
		AccountID:                  e.GetAccountID(),
		ChargeFrequency:            e.GetChargeFrequency(),
		Subcategory:                e.GetSubcategory(),
		CommitmentDiscountCategory: e.GetCommitmentDiscountCategory(),
		CommitmentDiscountID:       e.GetCommitmentDiscountID(),
		CommitmentDiscountName:     e.GetCommitmentDiscountName(),
		CommitmentDiscountType:     e.GetCommitmentDiscountType(),
		EffectiveCost:              e.GetEffectiveCost(),
		InvoiceIssuer:              e.GetInvoiceIssuer(),
		Provider:                   e.GetProvider(),
		Publisher:                  e.GetPublisher(),
		ServiceCategory:            e.GetServiceCategory(),
		ServiceName:                e.GetServiceName(),
		SkuID:                      e.GetSKUID(),
		SkuPriceID:                 e.GetSKUPriceID(),
		SubAccountID:               e.GetSubAccountID(),
		SubAccountName:             e.GetSubAccountName(),
		PricingQuantity:            e.GetPricingQuantity(),
		PricingUnit:                e.GetPricingUnit(),
		PricingCategory:            e.GetPricingCategory(),
	}
}

func (e *ExtendedCustomCostAttributes) GetBillingPeriod() *opencost.Window {
	return e.BillingPeriod
}

func (e *ExtendedCustomCostAttributes) GetAccountID() string {
	return e.AccountID
}

func (e *ExtendedCustomCostAttributes) GetChargeFrequency() string {
	return e.ChargeFrequency
}

func (e *ExtendedCustomCostAttributes) GetSubcategory() string {
	return e.Subcategory
}

func (e *ExtendedCustomCostAttributes) GetCommitmentDiscountCategory() string {
	return e.CommitmentDiscountCategory
}

func (e *ExtendedCustomCostAttributes) GetCommitmentDiscountID() string {
	return e.CommitmentDiscountID
}

func (e *ExtendedCustomCostAttributes) GetCommitmentDiscountName() string {
	return e.CommitmentDiscountName
}

func (e *ExtendedCustomCostAttributes) GetCommitmentDiscountType() string {
	return e.CommitmentDiscountType
}

func (e *ExtendedCustomCostAttributes) GetEffectiveCost() float32 {
	return e.EffectiveCost
}

func (e *ExtendedCustomCostAttributes) GetInvoiceIssuer() string {
	return e.InvoiceIssuer
}

func (e *ExtendedCustomCostAttributes) GetProvider() string {
	return e.Provider
}

func (e *ExtendedCustomCostAttributes) GetPublisher() string {
	return e.Publisher
}

func (e *ExtendedCustomCostAttributes) GetServiceCategory() string {
	return e.ServiceCategory
}

func (e *ExtendedCustomCostAttributes) GetServiceName() string {
	return e.ServiceName
}

func (e *ExtendedCustomCostAttributes) GetSKUID() string {
	return e.SkuID
}

func (e *ExtendedCustomCostAttributes) GetSKUPriceID() string {
	return e.SkuPriceID
}

func (e *ExtendedCustomCostAttributes) GetSubAccountID() string {
	return e.SubAccountID
}

func (e *ExtendedCustomCostAttributes) GetSubAccountName() string {
	return e.SubAccountName
}
func (e *ExtendedCustomCostAttributes) GetPricingQuantity() float32 {
	return e.PricingQuantity
}
func (e *ExtendedCustomCostAttributes) GetPricingUnit() string {
	return e.PricingUnit
}

func (e *ExtendedCustomCostAttributes) GetPricingCategory() string {
	return e.PricingCategory
}

func (d *CustomCost) GetMetadata() map[string]string {
	return d.Metadata
}

func (d *CustomCost) GetCostIncurredZone() string {
	return d.Zone
}

func (d *CustomCost) GetBilledCost() float32 {
	return d.BilledCost
}

func (d *CustomCost) GetAccountName() string {
	return d.AccountName
}

func (d *CustomCost) GetChargeCategory() string {
	return d.ChargeCategory
}

func (d *CustomCost) GetDescription() string {
	return d.Description
}

func (d *CustomCost) GetListCost() float32 {
	return d.ListCost
}

func (d *CustomCost) GetListUnitPrice() float32 {
	return d.ListUnitPrice
}

func (d *CustomCost) GetResourceName() string {
	return d.ResourceName
}

func (d *CustomCost) GetID() string {
	return d.Id
}

func (d *CustomCost) GetProviderID() string {
	return d.ProviderId
}

func (d *CustomCost) GetWindow() *opencost.Window {
	return d.Window
}

func (d *CustomCost) GetLabels() map[string]string {
	return d.Labels
}

func (d *CustomCost) GetUsageQuantity() float32 {
	return d.UsageQty
}

func (d *CustomCost) GetUsageUnit() string {
	return d.UsageUnit
}

func (d *CustomCost) GetExtendedAttributes() *ExtendedCustomCostAttributes {
	return d.ExtendedAttributes
}

func (d *CustomCost) GetResourceType() string {
	return d.ResourceType
}

func (d *CustomCostResponse) GetMetadata() map[string]string {
	return d.Metadata
}

func (d *CustomCostResponse) GetCostSource() string {
	return d.Costsource
}

func (d *CustomCostResponse) GetDomain() string {
	return d.Domain
}

func (d *CustomCostResponse) GetVersion() string {
	return d.Version
}

func (d *CustomCostResponse) GetCurrency() string {
	return d.Currency
}

func (d *CustomCostResponse) GetWindow() opencost.Window {
	return d.Window
}

func (d *CustomCostResponse) GetCosts() []*CustomCost {
	return d.Costs
}

func (d *CustomCostResponse) GetErrors() []error {
	return d.Errors
}
