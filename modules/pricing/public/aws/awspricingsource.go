package aws

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/pricing"
	"github.com/opencost/opencost/core/pkg/unit"
)

type AWSPricingSourceConfig struct {
	CurrencyCode string
}

type AWSPricingSource struct {
	config AWSPricingSourceConfig
}

func NewAWSPricingSource(cfg AWSPricingSourceConfig) *AWSPricingSource {
	return &AWSPricingSource{config: cfg}
}

func (p *AWSPricingSource) GetPricing() (*pricing.PricingSet, error) {
	log.Infof("PricingSource (AWS): starting EC2 pricing list download (large file, this may take a while)")
	start := time.Now()

	ps := &pricing.PricingSet{
		Nodes:   []*pricing.NodePricing{},
		Volumes: []*pricing.VolumePricing{},
	}
	skuToNodeKey := make(map[string]nodeKey)
	skuToVolumeKey := make(map[string]volumeKey)

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

		// Handle EC2 instances
		if (strings.HasPrefix(attr.UsageType, "BoxUsage") || strings.Contains(attr.UsageType, "-BoxUsage")) &&
			(attr.CapacityStatus == "Used" || attr.CapacityStatus == "") &&
			(attr.MarketOption == "OnDemand" || attr.MarketOption == "") {

			if attr.OperatingSystem != "" && attr.OperatingSystem != "NA" && attr.OperatingSystem != "Linux" {
				return
			}

			if attr.RegionCode == "" || attr.InstanceType == "" {
				return
			}

			skuToNodeKey[product.Sku] = nodeKey{
				Region:       attr.RegionCode,
				InstanceType: attr.InstanceType,
			}
			return
		}

		// Handle EBS volumes
		if strings.Contains(attr.UsageType, "EBS:Volume") {
			// Extract the volume type from the usage type (e.g., "USE1-EBS:VolumeUsage.gp3" -> "EBS:VolumeUsage.gp3")
			usageTypeMatch := usageTypeRegex.FindStringSubmatch(attr.UsageType)
			if len(usageTypeMatch) == 0 {
				return
			}
			usageTypeNoRegion := usageTypeMatch[len(usageTypeMatch)-1]

			// Map to volume type
			volumeType, ok := awsVolumeTypes[usageTypeNoRegion]
			if !ok {
				return
			}

			if attr.RegionCode == "" {
				return
			}

			skuToVolumeKey[product.Sku] = volumeKey{
				Region:     attr.RegionCode,
				VolumeType: volumeType,
				UsageType:  usageTypeNoRegion,
			}
		}
	}

	// Terms are used to define pricing and have the sku to look up the appropriate key.
	handleTerm := func(term *PriceListEC2Term) {
		termCount++
		if termCount%logInterval == 0 {
			log.Infof("PricingSource (AWS): processed %d terms, %d node pricing, %d volume pricing so far...",
				termCount, len(ps.Nodes), len(ps.Volumes))
		}

		// Check if this SKU is for a node or volume we're tracking
		nk, isNode := skuToNodeKey[term.Sku]
		vk, isVolume := skuToVolumeKey[term.Sku]

		if !isNode && !isVolume {
			return
		}

		// Determine the hourly rate code based on the offer term
		hourlyRateCode := HourlyRateCode
		if _, ok := OnDemandRateCodes[term.OfferTermCode]; !ok {
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
			log.Errorf("failed to parse price '%s': %s", priceStr, err.Error())
			return
		}

		// Parse the currency from config, default to USD if invalid
		currency, err := unit.ParseCurrency(p.config.CurrencyCode)
		if err != nil {
			log.Warnf("invalid currency code '%s', defaulting to USD: %s", p.config.CurrencyCode, err.Error())
			currency = unit.USD
		}

		// Handle node pricing
		if isNode {
			priceObj := pricing.Price{
				Currency: currency,
				Unit:     unit.Hour,
				Price:    price,
			}

			nodePricing := &pricing.NodePricing{
				Properties: pricing.NodePricingProperties{
					Provider:     pricing.AWSProvider,
					Region:       nk.Region,
					InstanceType: nk.InstanceType,
					Provisioning: pricing.ProvisioningOnDemand,
				},
				Prices: pricing.Prices{
					currency: []pricing.Price{priceObj},
				},
			}

			ps.Nodes = append(ps.Nodes, nodePricing)
		}

		// Handle volume pricing
		if isVolume {
			// AWS volume pricing is per GB-month, convert to per GB-hour
			hourlyPrice := price / 730.0

			priceObj := pricing.Price{
				Currency: currency,
				Unit:     unit.Hour,
				Price:    hourlyPrice,
			}

			volumePricing := &pricing.VolumePricing{
				Properties: pricing.VolumePricingProperties{
					Provider:   pricing.AWSProvider,
					Region:     vk.Region,
					VolumeType: vk.VolumeType,
				},
				Prices: pricing.Prices{
					currency: []pricing.Price{priceObj},
				},
			}

			ps.Volumes = append(ps.Volumes, volumePricing)
		}
	}

	err := QueryEC2PriceList("", handleProduct, handleTerm)
	if err != nil {
		return nil, fmt.Errorf("failed to query list pricing data %w", err)
	}

	log.Infof("PricingSource (AWS): completed in %s — %d products, %d terms, %d node pricing, %d volume pricing",
		time.Since(start).Round(time.Second), productCount, termCount, len(ps.Nodes), len(ps.Volumes))

	return ps, nil
}
