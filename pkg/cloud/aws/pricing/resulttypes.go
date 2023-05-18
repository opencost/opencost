package pricing

import (
	"fmt"
	"strings"
)

// Response maps a response for a service specific query to the pricing api
// T is the attributes structure found in the Products response
// Terms: is keyed by offer type (ex OnDemand), SKU which can be matched with products, and SKU.offerTermCode
type Response[T any] struct {
	Products map[string]Product[T]                  `json:"products"`
	Terms    map[string]map[string]map[string]*Term `json:"terms"`
}

// Product represents a purchased SKU
type Product[T any] struct {
	Sku        string `json:"sku"`
	Attributes T      `json:"attributes"`
}

// EC2ProductAttributes represents metadata about the product used to map to a node.
type EC2ProductAttributes struct {
	Location        string `json:"location"`
	InstanceType    string `json:"instanceType"`
	Memory          string `json:"memory"`
	Storage         string `json:"storage"`
	VCPU            string `json:"vcpu"`
	UsageType       string `json:"usagetype"`
	OperatingSystem string `json:"operatingSystem"`
	PreInstalledSw  string `json:"preInstalledSw"`
	InstanceFamily  string `json:"instanceFamily"`
	CapacityStatus  string `json:"capacitystatus"`
	GPU             string `json:"gpu"` // GPU represents the number of GPU on the instance
}

// Term defines pricing for a SKU on an offer term
type Term struct {
	Sku             string                     `json:"sku"`
	OfferTermCode   string                     `json:"offerTermCode"`
	PriceDimensions map[string]*PriceDimension `json:"priceDimensions"`
}

func (ot *Term) String() string {
	var strs []string
	for k, rc := range ot.PriceDimensions {
		strs = append(strs, fmt.Sprintf("%s:%s", k, rc.String()))
	}
	return fmt.Sprintf("%s:%s", ot.Sku, strings.Join(strs, ","))
}

// PriceDimension encodes data about the price of a product
type PriceDimension struct {
	Unit         string       `json:"unit"`
	PricePerUnit PricePerUnit `json:"pricePerUnit"`
}

func (rc *PriceDimension) String() string {
	return fmt.Sprintf("{unit: %s, pricePerUnit: %v", rc.Unit, rc.PricePerUnit)
}

// PricePerUnit is the localized currency. (TODO: support non-USD)
type PricePerUnit struct {
	USD string `json:"USD,omitempty"`
	CNY string `json:"CNY,omitempty"`
}

// OnDemandRateCodes is are sets of identifiers for offerTermCodes matching 'On Demand' rates
var OnDemandRateCodes = map[string]struct{}{
	"JRTCKXETXF": {},
}

var OnDemandRateCodesCn = map[string]struct{}{
	"99YE2YK9UR": {},
	"5Y9WH78GDR": {},
	"KW44MY7SZN": {},
}
