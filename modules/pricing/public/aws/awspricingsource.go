package aws

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

const AWSPricingSourceType pricingmodel.PricingSourceType = "aws_pricing_list_api"

type AWSPricingSourceConfig struct {
	CurrencyCode string
}

type AWSPricingSource struct {
	config AWSPricingSourceConfig
}

func NewAWSPricingSource(cfg AWSPricingSourceConfig) *AWSPricingSource {
	return &AWSPricingSource{config: cfg}
}

func (p *AWSPricingSource) PricingSourceType() pricingmodel.PricingSourceType {
	return AWSPricingSourceType
}

// PricingSourceKey returns the PricingSourceType because it is meant to run single instance.
func (p *AWSPricingSource) PricingSourceKey() string {
	return string(AWSPricingSourceType)
}

func (p *AWSPricingSource) GetPricing() (*pricing.PricingSet, error) {
	log.Infof("PricingSource (AWS): starting EC2 pricing list download (large file, this may take a while)")
	start := time.Now()

	ps := &pricing.PricingSet{
		Nodes:   []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{},
	}
	skuToNodeKey := make(map[string]pricingmodel.NodeKey)

	var productCount, termCount int
	const logInterval = 50000

	// When parsing product we create keys based off of product attributes and link those to a SKU.
	handleProduct := func(product *PriceListEC2Product) {
		productCount++
		if productCount%logInterval == 0 {
			log.Infof("PricingSource (AWS): processed %d products...", productCount)
		}
		attr := product.Attributes
		if attr.LocationType != "AWS Region" {
			return
		}

		if (!strings.HasPrefix(attr.UsageType, "BoxUsage") && !strings.Contains(attr.UsageType, "-BoxUsage")) ||
			(attr.CapacityStatus != "Used" && attr.CapacityStatus != "") ||
			(attr.MarketOption != "OnDemand" && attr.MarketOption != "") {
			return
		}

		if attr.OperatingSystem != "" && attr.OperatingSystem != "NA" && attr.OperatingSystem != "Linux" {
			return
		}

		if attr.RegionCode == "" || attr.InstanceType == "" {
			return
		}

		skuToNodeKey[product.Sku] = pricingmodel.NodeKey{
			Provider:    shared.ProviderAWS,
			Region:      attr.RegionCode,
			NodeType:    attr.InstanceType,
			UsageType:   shared.UsageTypeOnDemand,
			PricingType: pricingmodel.NodePricingTypeTotal,
		}
	}

	// Terms are used to define pricing and have the sku to look up the appropriate key.
	handleTerm := func(term *PriceListEC2Term) {
		termCount++
		if termCount%logInterval == 0 {
			log.Infof("PricingSource (AWS): processed %d terms, %d pricing entries so far...", termCount, len(ps.Nodes))
		}
		nodeKey, ok := skuToNodeKey[term.Sku]
		if !ok {
			return
		}
		hourlyRateCode := HourlyRateCode
		if _, ok = OnDemandRateCodes[term.OfferTermCode]; !ok {
			if _, okCN := OnDemandRateCodesCn[term.OfferTermCode]; !okCN {
				// Skip if term is not OnDemand
				return
			}
			hourlyRateCode = HourlyRateCodeCn
		}
		priceDimensionKey := strings.Join([]string{term.Sku, term.OfferTermCode, hourlyRateCode}, ".")

		pricingDimension, ok := term.PriceDimensions[priceDimensionKey]
		if !ok {
			return
		}

		priceStr := pricingDimension.PricePerUnit.ForCurrency(p.config.CurrencyCode)
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Errorf("failed to parse str to float '%s': %s", priceStr, err.Error())
			return
		}

		// Parse the currency from config, default to USD if invalid
		currency, err := unit.ParseCurrency(p.config.CurrencyCode)
		if err != nil {
			log.Warnf("invalid currency code '%s', defaulting to USD: %s", p.config.CurrencyCode, err.Error())
			currency = unit.USD
		}

		priceObj := pricing.Price{
			Currency: currency,
			Unit:     unit.Hour,
			Price:    price,
		}

		nodePricing := &pricing.NodePricing{
			Properties: pricing.NodePricingProperties{
				Provider:     pricing.Provider(nodeKey.Provider),
				Region:       nodeKey.Region,
				InstanceType: nodeKey.NodeType,
				Provisioning: pricing.ProvisioningOnDemand,
			},
			Prices: pricing.Prices{
				currency: []pricing.Price{
					priceObj,
				},
			},
		}

		ps.Nodes = append(ps.Nodes, nodePricing)
	}

	err := QueryEC2PriceList("", handleProduct, handleTerm)
	if err != nil {
		return nil, fmt.Errorf("failed to query list pricing data %w", err)
	}

	log.Infof("PricingSource (AWS): completed in %s — %d products, %d terms, %d pricing entries",
		time.Since(start).Round(time.Second), productCount, termCount, len(ps.Nodes))

	return ps, nil
}
