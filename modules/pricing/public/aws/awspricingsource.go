package aws

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/cloud"
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
		NodePricing:             []*pricing.NodePricing{},
		PersistentVolumePricing: []*pricing.PersistentVolumePricing{},
	}
	skuToNodeKey := make(map[string]nodeKey)
	seenNodeKeys := make(map[nodeKey]struct{})
	skuToVolumeKey := make(map[string]volumeKey)
	seenVolumeKeys := make(map[volumeKey]struct{})

	var productCount, termCount int
	const logInterval = 50000

	region := ""
	if strings.ToUpper(p.config.CurrencyCode) == "CNY" {
		region = "cn-north-1"
		log.Infof("PricingSource (AWS): Using China pricing endpoint for CNY currency")
	}

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

		// Handle EC2 instances.
		// We only want the base Linux on-demand price:
		//   - UsageType must be a BoxUsage (compute hour charge)
		//   - CapacityStatus must be "Used" (not a capacity reservation)
		//   - MarketOption must be "OnDemand" (not Spot)
		//   - OperatingSystem must be Linux (or not returned by API)
		//   - PreInstalledSw must be "NA" (no paid software bundle)
		// All of these can appear empty when the API omits the field, so we
		// treat empty as "unknown" and require the affirmative value where it
		// matters, except OperatingSystem where empty/NA is acceptable.
		if (strings.HasPrefix(attr.UsageType, "BoxUsage") || strings.Contains(attr.UsageType, "-BoxUsage")) &&
			(attr.CapacityStatus == "Used" || attr.CapacityStatus == "") &&
			(attr.MarketOption == "OnDemand" || attr.MarketOption == "") {

			// Skip non-Linux operating systems; allow empty/NA (field may not be returned).
			if attr.OperatingSystem != "" && attr.OperatingSystem != "NA" && attr.OperatingSystem != "Linux" {
				return
			}

			// Skip software bundles (SQL Server, etc.); allow empty (field may not be returned).
			if attr.PreInstalledSw != "" && attr.PreInstalledSw != "NA" {
				return
			}

			// Skip capacity reservations; allow empty (field may not be returned).
			if attr.CapacityStatus != "" && attr.CapacityStatus != "Used" {
				return
			}

			if attr.RegionCode == "" || attr.InstanceType == "" {
				return
			}

			nk := nodeKey{
				Region:       attr.RegionCode,
				InstanceType: attr.InstanceType,
			}
			if _, seen := seenNodeKeys[nk]; seen {
				return
			}
			seenNodeKeys[nk] = struct{}{}
			skuToNodeKey[product.Sku] = nk
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

			vk := volumeKey{
				Region:     attr.RegionCode,
				VolumeType: volumeType,
				UsageType:  usageTypeNoRegion,
			}
			if _, seen := seenVolumeKeys[vk]; seen {
				return
			}
			seenVolumeKeys[vk] = struct{}{}
			skuToVolumeKey[product.Sku] = vk
		}
	}

	// Terms are used to define pricing and have the sku to look up the appropriate key.
	handleTerm := func(term *PriceListEC2Term) {
		termCount++
		if termCount%logInterval == 0 {
			log.Infof("PricingSource (AWS): processed %d terms, %d node pricing, %d volume pricing so far...",
				termCount, len(ps.NodePricing), len(ps.PersistentVolumePricing))
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

		// Handle node pricing
		if isNode {
			nodePricing := &pricing.NodePricing{
				Properties: pricing.NodePricingProperties{
					Provider:     cloud.ProviderAWS,
					Region:       nk.Region,
					InstanceType: nk.InstanceType,
					Provisioning: pricing.ProvisioningOnDemand,
				},
				Prices: pricing.Prices{
					pricing.ResourceNode: pricing.Price{
						Unit:  unit.Hour,
						Price: price,
					},
				},
			}

			ps.NodePricing = append(ps.NodePricing, nodePricing)
		}

		// Handle volume pricing
		if isVolume {
			// AWS volume pricing is per GB-month, convert to per GB-hour
			hourlyPrice := price / 730.0

			volumePricing := &pricing.PersistentVolumePricing{
				Properties: pricing.PersistentVolumePricingProperties{
					Provider:   cloud.ProviderAWS,
					Region:     vk.Region,
					VolumeType: vk.VolumeType,
				},
				Prices: pricing.Prices{
					pricing.ResourceStorage: pricing.Price{
						Unit:  unit.Hour,
						Price: hourlyPrice,
					},
				},
			}

			ps.PersistentVolumePricing = append(ps.PersistentVolumePricing, volumePricing)
		}
	}

	err := QueryEC2PriceList(region, handleProduct, handleTerm)
	if err != nil {
		return nil, fmt.Errorf("failed to query list pricing data %w", err)
	}

	log.Infof("PricingSource (AWS): completed in %s — %d products, %d terms, %d node pricing, %d volume pricing",
		time.Since(start).Round(time.Second), productCount, termCount, len(ps.NodePricing), len(ps.PersistentVolumePricing))

	return ps, nil
}
