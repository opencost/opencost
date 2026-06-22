package aws

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"
)

const (
	awsPricingBaseURL      = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/"
	awsChinaPricingBaseURL = "https://pricing.cn-north-1.amazonaws.com.cn/offers/v1.0/cn/"
	pricingCurrentPath     = "/current/"
	pricingIndexFile       = "index.json"
	chinaRegionPrefix      = "cn-"
)

// OnDemandRateCodes is are sets of identifiers for offerTermCodes matching 'On Demand' rates
var OnDemandRateCodes = map[string]struct{}{
	"JRTCKXETXF": {},
}

var OnDemandRateCodesCn = map[string]struct{}{
	"99YE2YK9UR": {},
	"5Y9WH78GDR": {},
	"KW44MY7SZN": {},
}

// HourlyRateCode is appended to a node sku
const (
	HourlyRateCode   = "6YS6EN2CT7"
	HourlyRateCodeCn = "Q7UJUT2CE6"
)

func getListPriceURL(service, region string) string {
	if env.GetAWSPricingURL() != "" { // Allow override of pricing URL
		return env.GetAWSPricingURL()
	}
	baseURL := awsPricingBaseURL

	if strings.HasPrefix(region, chinaRegionPrefix) {
		baseURL = awsChinaPricingBaseURL
	}

	baseURL += service + pricingCurrentPath

	if region != "" {
		baseURL += region + "/"
	}
	return baseURL + pricingIndexFile
}

func QueryEC2PriceList(
	region string,
	handleProduct func(*PriceListEC2Product),
	handleTerm func(term *PriceListEC2Term),
) error {
	pricingURL := getListPriceURL("AmazonEC2", region)

	log.Infof("starting download of \"%s\", which is quite large ...", pricingURL)
	resp, err := http.Get(pricingURL)
	if err != nil {
		return fmt.Errorf("bogus fetch of \"%s\": %w", pricingURL, err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warnf("failed to close response body: %v", err)
		}
	}()

	dec := json.NewDecoder(resp.Body)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			log.Infof("done loading \"%s\"\n", resp.Request.URL.String())
			break
		} else if err != nil {
			log.Errorf("error parsing response json %v", resp.Body)
			break
		}
		if t == "products" {
			_, err := dec.Token() // this should parse the opening "{""
			if err != nil {
				return err
			}
			for dec.More() {
				_, err := dec.Token() // the sku token
				if err != nil {
					return err
				}
				product := &PriceListEC2Product{}

				err = dec.Decode(&product)
				if err != nil {
					log.Errorf("Error parsing response from \"%s\": %v", resp.Request.URL.String(), err.Error())
					break
				}

				handleProduct(product)

			}
		}
		if t == "terms" {
			_, err := dec.Token() // this should parse the opening "{""
			if err != nil {
				return err
			}
			termType, err := dec.Token()
			if err != nil {
				return err
			}
			if termType == "OnDemand" {
				_, err := dec.Token()
				if err != nil { // again, should parse an opening "{"
					return err
				}
				for dec.More() {
					_, err := dec.Token() // sku
					if err != nil {
						return err
					}
					_, err = dec.Token() // another opening "{"
					if err != nil {
						return err
					}
					// SKUOndemand
					_, err = dec.Token()
					if err != nil {
						return err
					}
					offerTerm := &PriceListEC2Term{}
					err = dec.Decode(&offerTerm)
					if err != nil {
						log.Errorf("Error decoding AWS Offer Term: %s", err.Error())
					}

					handleTerm(offerTerm)

					_, err = dec.Token()
					if err != nil {
						return err
					}
				}
				_, err = dec.Token()
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// PriceListEC2Response maps a k8s node to an AWS Pricing "product"
type PriceListEC2Response struct {
	Products map[string]*PriceListEC2Product `json:"products"`
	Terms    PriceListEC2Terms               `json:"terms"`
}

// PriceListEC2Product represents a purchased SKU
type PriceListEC2Product struct {
	Sku        string                        `json:"sku"`
	Attributes PriceListEC2ProductAttributes `json:"attributes"`
}

// PriceListEC2ProductAttributes represents metadata about the product used to map to a node.
type PriceListEC2ProductAttributes struct {
	ServiceCode  string `json:"servicecode"`
	InstanceType string `json:"instanceType"`
	UsageType    string `json:"usagetype"`
	Operation    string `json:"operation"`
	Location     string `json:"location"`
	LocationType string `json:"locationType"`
	RegionCode   string `json:"regionCode"`
	ServiceName  string `json:"servicename"`

	// These fields do not appear to return in the api anymore
	Memory          string `json:"memory"`
	Storage         string `json:"storage"`
	VCpu            string `json:"vcpu"`
	OperatingSystem string `json:"operatingSystem"`
	PreInstalledSw  string `json:"preInstalledSw"`
	InstanceFamily  string `json:"instanceFamily"`
	CapacityStatus  string `json:"capacitystatus"`
	GPU             string `json:"gpu"` // GPU represents the number of GPU on the instance
	MarketOption    string `json:"marketOption"`
}

// PriceListEC2Terms are how you pay for the node: OnDemand, Reserved
type PriceListEC2Terms struct {
	OnDemand map[string]map[string]*PriceListEC2Term `json:"OnDemand"`
	Reserved map[string]map[string]*PriceListEC2Term `json:"Reserved"`
}

// PriceListEC2Term is a sku extension used to pay for the node.
type PriceListEC2Term struct {
	Sku             string                                 `json:"sku"`
	OfferTermCode   string                                 `json:"offerTermCode"`
	PriceDimensions map[string]*PriceListEC2PriceDimension `json:"priceDimensions"`
}

func (t *PriceListEC2Term) String() string {
	var strs []string
	for k, rc := range t.PriceDimensions {
		strs = append(strs, fmt.Sprintf("%s:%s", k, rc.String()))
	}
	return fmt.Sprintf("%s:%s", t.Sku, strings.Join(strs, ","))
}

// PriceListEC2PriceDimension encodes data about the price of a product
type PriceListEC2PriceDimension struct {
	Unit         string                   `json:"unit"`
	PricePerUnit PriceListEC2PricePerUnit `json:"pricePerUnit"`
}

func (pd *PriceListEC2PriceDimension) String() string {
	return fmt.Sprintf("{unit: %s, pricePerUnit: %v", pd.Unit, pd.PricePerUnit)
}

// PriceListEC2PricePerUnit is the localized currency.
type PriceListEC2PricePerUnit struct {
	USD string `json:"USD,omitempty"`
	CNY string `json:"CNY,omitempty"`
}

// ForCurrency returns the price string for the given currency code, falling
// back to USD if the code is unrecognized or the field is empty.
func (p PriceListEC2PricePerUnit) ForCurrency(code string) string {
	switch strings.ToUpper(code) {
	case "CNY":
		if p.CNY != "" {
			return p.CNY
		}
	}
	return p.USD
}
