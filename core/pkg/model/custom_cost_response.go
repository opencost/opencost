package model

import "github.com/opencost/opencost/core/pkg/opencost"

type CustomCostResponse struct {
	Metadata   map[string]string
	Costsource string
	Domain     string
	Version    string
	Currency   string
	Window     opencost.Window
	Costs      [][]*CustomCost
	Errors     []error
}

type CustomCost struct {
	Metadata       map[string]string
	Zone           string
	BilledCost     float32
	AccountName    string
	ChargeCategory string
	Description    string
	ListCost       float32
	ListUnitPrice  float32
	ResourceName   string
	ResourceType   string
	Id             string
	ProviderId     string

	Window             *opencost.Window
	Labels             map[string]string
	UsageQty           float32
	UsageUnit          string
	ExtendedAttributes *ExtendedCustomCostAttributes
}

type ExtendedCustomCostAttributes struct {
	BillingPeriod              *opencost.Window
	AccountID                  string
	ChargeFrequency            string
	Subcategory                string
	CommitmentDiscountCategory string
	CommitmentDiscountID       string
	CommitmentDiscountName     string
	CommitmentDiscountType     string
	EffectiveCost              float32
	InvoiceIssuer              string
	Provider                   string
	Publisher                  string
	ServiceCategory            string
	ServiceName                string
	SkuID                      string
	SkuPriceID                 string
	SubAccountID               string
	SubAccountName             string
	PricingQuantity            float32
	PricingUnit                string
	PricingCategory            string
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

func (d *CustomCostResponse) GetCosts() [][]*CustomCost {
	return d.Costs
}

func (d *CustomCostResponse) GetErrors() []error {
	return d.Errors
}
